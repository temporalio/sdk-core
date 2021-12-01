//! Management of workflow tasks

mod cache_manager;
mod concurrency_manager;

use crate::{
    errors::{WorkflowMissingError, WorkflowUpdateError},
    machines::{ProtoCommand, WFCommand, WFMachinesError},
    pending_activations::PendingActivations,
    pollers::GatewayRef,
    protosext::{ValidPollWFTQResponse, WfActivationExt},
    task_token::TaskToken,
    telemetry::metrics::MetricsContext,
    worker::NewLocalAct,
    workflow::{
        workflow_tasks::{
            cache_manager::WorkflowCacheManager, concurrency_manager::WorkflowConcurrencyManager,
        },
        HistoryPaginator, HistoryUpdate, LocalResolution, WorkflowCachingPolicy, WorkflowManager,
        LEGACY_QUERY_ID,
    },
};
use crossbeam::queue::SegQueue;
use futures::FutureExt;
use parking_lot::Mutex;
use std::{fmt::Debug, time::Instant};
use temporal_sdk_core_protos::coresdk::{
    workflow_activation::{
        create_query_activation, wf_activation_job, QueryWorkflow, WfActivation,
    },
    workflow_commands::QueryResult,
    FromPayloadsExt,
};
use tokio::sync::watch;

/// Centralizes concerns related to applying new workflow tasks and reporting the activations they
/// produce.
///
/// It is intentionally free of any interactions with the server client to promote testability
pub struct WorkflowTaskManager {
    /// Manages threadsafe access to workflow machine instances
    workflow_machines: WorkflowConcurrencyManager,
    /// Workflows may generate new activations immediately upon completion (ex: while replaying, or
    /// when cancelling an activity in try-cancel/abandon mode), or for other reasons such as a
    /// requested eviction. They queue here.
    pending_activations: PendingActivations,
    /// Holds activations which are purely query activations needed to respond to legacy queries.
    /// Activations may only be added here for runs which do not have other pending activations.
    pending_legacy_queries: SegQueue<WfActivation>,
    /// Holds poll wft responses from the server that need to be applied
    ready_buffered_wft: SegQueue<ValidPollWFTQResponse>,
    /// Used to wake blocked workflow task polling
    pending_activations_notifier: watch::Sender<bool>,
    /// Lock guarded cache manager, which is the authority for limit-based workflow machine eviction
    /// from the cache.
    // TODO: Also should be moved inside concurrency manager, but there is some complexity around
    //   how inserts to it happen that requires a little thought (or a custom LRU impl)
    cache_manager: Mutex<WorkflowCacheManager>,

    metrics: MetricsContext,
}

#[derive(Clone, Debug)]
pub(crate) struct OutstandingTask {
    pub info: WorkflowTaskInfo,
    /// If set the outstanding task has query from the old `query` field which must be fulfilled
    /// upon finishing replay
    pub legacy_query: Option<QueryWorkflow>,
    start_time: Instant,
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum OutstandingActivation {
    /// A normal activation with a joblist
    Normal {
        /// True if there is an eviction in the joblist
        contains_eviction: bool,
    },
    /// An activation for a legacy query
    LegacyQuery,
}

impl OutstandingActivation {
    const fn has_eviction(self) -> bool {
        matches!(
            self,
            OutstandingActivation::Normal {
                contains_eviction: true
            }
        )
    }
}

/// Contains important information about a given workflow task that we need to memorize while
/// lang handles it.
#[derive(Clone, Debug)]
pub struct WorkflowTaskInfo {
    pub task_token: TaskToken,
    pub attempt: u32,
}

#[derive(Debug, derive_more::From)]
pub(crate) enum NewWfTaskOutcome {
    /// A new activation for the workflow should be issued to lang
    IssueActivation(WfActivation),
    /// The poll loop should be restarted, there is nothing to do
    TaskBuffered,
    /// The workflow task should be auto-completed with an empty command list, as it must be replied
    /// to but there is no meaningful work for lang to do.
    Autocomplete,
    /// Workflow task had partial history and workflow was not present in the cache.
    CacheMiss,
    /// The workflow task ran into problems while being applied and we must now evict the workflow
    Evict(WorkflowUpdateError),
    /// No action should be taken. Possibly we are waiting for local activities to complete
    DoNothing,
}

#[derive(Debug)]
pub enum FailedActivationOutcome {
    NoReport,
    Report(TaskToken),
    ReportLegacyQueryFailure(TaskToken),
}

#[derive(Debug)]
pub(crate) struct ServerCommandsWithWorkflowInfo {
    pub task_token: TaskToken,
    pub action: ActivationAction,
}

#[derive(Debug)]
pub(crate) enum ActivationAction {
    /// We should respond that the workflow task is complete
    WftComplete {
        commands: Vec<ProtoCommand>,
        query_responses: Vec<QueryResult>,
        local_activities: Vec<NewLocalAct>,
        force_new_wft: bool,
    },
    /// We should respond to a legacy query request
    RespondLegacyQuery { result: QueryResult },
    /// There are local activities that need to be performed, but no new commands to send. The WFT
    /// should not be completed yet, but delayed until near the timeout or the LA resolves
    LocalActivitiesDelayWft { local_activities: Vec<NewLocalAct> },
}

macro_rules! machine_mut {
    ($myself:ident, $run_id:ident, $task_token:expr, $clos:expr) => {{
        $myself
            .workflow_machines
            .access($run_id, $clos)
            .await
            .map_err(|source| WorkflowUpdateError {
                source,
                run_id: $run_id.to_owned(),
                task_token: $task_token,
            })
    }};
}

impl WorkflowTaskManager {
    pub(crate) fn new(
        pending_activations_notifier: watch::Sender<bool>,
        eviction_policy: WorkflowCachingPolicy,
        metrics: MetricsContext,
    ) -> Self {
        Self {
            workflow_machines: WorkflowConcurrencyManager::new(),
            pending_activations: Default::default(),
            pending_legacy_queries: Default::default(),
            ready_buffered_wft: Default::default(),
            pending_activations_notifier,
            cache_manager: Mutex::new(WorkflowCacheManager::new(eviction_policy, metrics.clone())),
            metrics,
        }
    }

    pub(crate) fn next_pending_activation(&self) -> Option<WfActivation> {
        // Dispatch pending legacy queries first
        if let leg_q @ Some(_) = self.pending_legacy_queries.pop() {
            return leg_q;
        }
        // It is important that we do not issue pending activations for any workflows which already
        // have an outstanding activation. If we did, it can result in races where an in-progress
        // completion may appear to be the last in a task (no more pending activations) because
        // concurrently a poll happened to dequeue the pending activation at the right time.
        // NOTE: This all goes away with the handles-per-workflow poll approach.
        let maybe_act = self
            .pending_activations
            .pop_first_matching(|rid| self.workflow_machines.get_activation(rid).is_none());
        if let Some(pending_info) = maybe_act {
            if let Ok(act) = self
                .workflow_machines
                .access_sync(&pending_info.run_id, |wfm| wfm.machines.get_wf_activation())
                .and_then(|mut act| {
                    if let Some(reason) = pending_info.needs_eviction {
                        act.append_evict_job(reason);
                    }
                    self.insert_outstanding_activation(&act)?;
                    Ok(act)
                })
            {
                self.cache_manager.lock().touch(&act.run_id);
                Some(act)
            } else {
                self.request_eviction(
                    &pending_info.run_id,
                    "Tried to apply pending activation for missing run",
                );
                // Continue trying to return a valid pending activation
                self.next_pending_activation()
            }
        } else {
            None
        }
    }

    pub(crate) fn next_buffered_poll(&self) -> Option<ValidPollWFTQResponse> {
        self.ready_buffered_wft.pop()
    }

    pub(crate) fn outstanding_wft(&self) -> usize {
        self.workflow_machines.outstanding_wft()
    }

    /// Request a workflow eviction. This will queue up an activation to evict the workflow from
    /// the lang side. Workflow will not *actually* be evicted until lang replies to that activation
    ///
    /// Returns, if found, the number of attempts on the current workflow task
    pub(crate) fn request_eviction(&self, run_id: &str, reason: impl Into<String>) -> Option<u32> {
        if self.workflow_machines.exists(run_id) {
            if !self.activation_has_eviction(run_id) {
                let reason = reason.into();
                debug!(%run_id, %reason, "Eviction requested");
                // Queue up an eviction activation
                self.pending_activations
                    .notify_needs_eviction(run_id, reason);
                let _ = self.pending_activations_notifier.send(true);
            }
            self.workflow_machines
                .get_task(run_id)
                .map(|wt| wt.info.attempt)
        } else {
            warn!(%run_id, "Eviction requested for unknown run");
            None
        }
    }

    /// Evict a workflow from the cache by its run id and enqueue a pending activation to evict the
    /// workflow. Any existing pending activations will be destroyed, and any outstanding
    /// activations invalidated.
    ///
    /// Returns that workflow's task info if it was present.
    fn evict_run(&self, run_id: &str) {
        debug!(run_id=%run_id, "Evicting run");

        self.cache_manager.lock().remove(run_id);
        let maybe_buffered = self.workflow_machines.evict(run_id);
        self.pending_activations.remove_all_with_run_id(run_id);

        if let Some(buffered) = maybe_buffered {
            // If we just evicted something and there was a buffered poll response for the workflow,
            // it is now ready to be produced by the next poll. (Not immediate next, since, ignoring
            // other workflows, the next poll will be the eviction we just produced. Buffered polls
            // always are popped after pending activations)
            self.make_buffered_poll_ready(buffered);
        }
    }

    /// Given a validated poll response from the server, prepare an activation (if there is one) to
    /// be sent to lang. If applying the response to the workflow's state does not produce a new
    /// activation, `None` is returned.
    ///
    /// The new activation is immediately considered to be an outstanding workflow task - so it is
    /// expected that new activations will be dispatched to lang right away.
    pub(crate) async fn apply_new_poll_resp(
        &self,
        work: ValidPollWFTQResponse,
        gateway: &GatewayRef,
    ) -> NewWfTaskOutcome {
        let mut work = if let Some(w) = self.workflow_machines.buffer_resp_if_outstanding_work(work)
        {
            w
        } else {
            return NewWfTaskOutcome::TaskBuffered;
        };

        debug!(
            task_token = %&work.task_token,
            history_length = %work.history.events.len(),
            "Applying new workflow task from server"
        );
        let task_start_time = Instant::now();

        // Check if there is a legacy query we either need to immediately issue an activation for
        // (if there is no more replay work to do) or we need to store for later answering.
        let legacy_query = work.legacy_query.take().map(|q| QueryWorkflow {
            query_id: LEGACY_QUERY_ID.to_string(),
            query_type: q.query_type,
            arguments: Vec::from_payloads(q.query_args),
        });

        let (info, mut next_activation) =
            match self.instantiate_or_update_workflow(work, gateway).await {
                Ok((info, next_activation)) => (info, next_activation),
                Err(e) => {
                    if let WFMachinesError::CacheMiss = e.source {
                        return NewWfTaskOutcome::CacheMiss;
                    }
                    return NewWfTaskOutcome::Evict(e);
                }
            };

        // Immediately dispatch query activation if no other jobs
        let legacy_query = if next_activation.jobs.is_empty() {
            if let Some(lq) = legacy_query {
                debug!("Dispatching legacy query {:?}", &lq);
                next_activation
                    .jobs
                    .push(wf_activation_job::Variant::QueryWorkflow(lq).into());
            }
            None
        } else {
            legacy_query
        };

        self.workflow_machines
            .insert_wft(
                &next_activation.run_id,
                OutstandingTask {
                    info,
                    legacy_query,
                    start_time: task_start_time,
                },
            )
            .expect("Workflow machines must exist, we just created/updated them");

        if next_activation.jobs.is_empty() {
            let outstanding_las = self
                .workflow_machines
                .access_sync(&next_activation.run_id, |wfm| {
                    wfm.machines.outstanding_local_activity_count()
                })
                .expect("Workflow machines must exist, we just created/updated them");
            if outstanding_las > 0 {
                // If there are outstanding local activities, we don't want to complete the workflow
                // task just yet. We want to give them a chance to complete. If they take longer
                // than the WFT timeout, we will force a new WFT just before the timeout.
                // TODO: Implement that
                NewWfTaskOutcome::DoNothing
            } else {
                NewWfTaskOutcome::Autocomplete
            }
        } else {
            if let Err(wme) = self.insert_outstanding_activation(&next_activation) {
                return NewWfTaskOutcome::Evict(wme.into());
            }
            NewWfTaskOutcome::IssueActivation(next_activation)
        }
    }

    /// Record a successful activation. Returns (if any) commands that should be reported to the
    /// server as part of wft completion
    pub(crate) async fn successful_activation(
        &self,
        run_id: &str,
        mut commands: Vec<WFCommand>,
    ) -> Result<Option<ServerCommandsWithWorkflowInfo>, WorkflowUpdateError> {
        // No-command replies to evictions can simply skip everything
        if commands.is_empty() && self.activation_has_eviction(run_id) {
            return Ok(None);
        }

        let task_token = if let Some(entry) = self.workflow_machines.get_task(run_id) {
            entry.info.task_token.clone()
        } else {
            if !self.activation_has_eviction(run_id) {
                // Don't bother warning if this was an eviction, since it's normal to issue
                // eviction activations without an associated workflow task in that case.
                warn!(
                    run_id,
                    "Attempted to complete activation for nonexistent run"
                );
            }
            return Ok(None);
        };

        // If the only command from the activation is a legacy query response, that means we need
        // to respond differently than a typical activation.
        let ret = if matches!(&commands.as_slice(),
                    &[WFCommand::QueryResponse(qr)] if qr.query_id == LEGACY_QUERY_ID)
        {
            let qr = match commands.remove(0) {
                WFCommand::QueryResponse(qr) => qr,
                _ => unreachable!("We just verified this is the only command"),
            };
            Some(ServerCommandsWithWorkflowInfo {
                task_token,
                action: ActivationAction::RespondLegacyQuery { result: qr },
            })
        } else {
            // First strip out query responses from other commands that actually affect machines
            // Would be prettier with `drain_filter`
            let mut i = 0;
            let mut query_responses = vec![];
            while i < commands.len() {
                if matches!(commands[i], WFCommand::QueryResponse(_)) {
                    if let WFCommand::QueryResponse(qr) = commands.remove(i) {
                        if qr.query_id == LEGACY_QUERY_ID {
                            return Err(WorkflowUpdateError {
                                source: WFMachinesError::Fatal(
                                    "Legacy query activation response included other commands, \
                                    this is not allowed and constitutes an error in the lang SDK"
                                        .to_string(),
                                ),
                                run_id: run_id.to_string(),
                                task_token: Some(task_token),
                            });
                        }
                        query_responses.push(qr);
                    }
                } else {
                    i += 1;
                }
            }

            let (are_pending, server_cmds, local_activities) = machine_mut!(
                self,
                run_id,
                Some(task_token.clone()),
                |wfm: &mut WorkflowManager| async move {
                    // Send commands from lang into the machines then check if the workflow run needs
                    // another activation and mark it if so
                    wfm.push_commands(commands).await?;
                    let are_pending = wfm.apply_next_task_if_ready().await?;
                    // We want to fetch the outgoing commands only after a next WFT may have been
                    // applied, as outgoing server commands may be affected.
                    Ok((
                        are_pending,
                        wfm.get_server_commands(),
                        wfm.drain_queued_local_activities(),
                    ))
                }
                .boxed()
            )?;

            if are_pending {
                self.needs_activation(run_id);
            }

            // We only actually want to send commands back to the server if there are no more
            // pending activations and we are caught up on replay. We don't want to complete a wft
            // if already saw the final event in the workflow.
            if !self.pending_activations.has_pending(run_id) && !server_cmds.replaying {
                if server_cmds.commands.is_empty() && !local_activities.is_empty() {
                    Some(ServerCommandsWithWorkflowInfo {
                        task_token,
                        action: ActivationAction::LocalActivitiesDelayWft { local_activities },
                    })
                } else {
                    // TODO: Ideally local activities isn't in this branch at all But we need to
                    //  force new wft if there are incomplete local activities and new commands to
                    //  send. Force flag does need to appear. Other branch should ideally be only
                    //  one dealing with LAs

                    Some(ServerCommandsWithWorkflowInfo {
                        task_token,
                        action: ActivationAction::WftComplete {
                            commands: server_cmds.commands,
                            query_responses,
                            force_new_wft: !local_activities.is_empty(),
                            local_activities,
                        },
                    })
                }
            } else if query_responses.is_empty() {
                None
            } else {
                Some(ServerCommandsWithWorkflowInfo {
                    task_token,
                    action: ActivationAction::WftComplete {
                        commands: vec![],
                        query_responses,
                        local_activities: vec![],
                        force_new_wft: false,
                    },
                })
            }
        };
        Ok(ret)
    }

    /// Record that an activation failed, returns enum that indicates if failure should be reported
    /// to the server
    pub(crate) fn failed_activation(&self, run_id: &str) -> FailedActivationOutcome {
        let tt = if let Some(tt) = self
            .workflow_machines
            .get_task(run_id)
            .map(|t| t.info.task_token.clone())
        {
            tt
        } else {
            warn!(
                "No info for workflow with run id {} found when trying to fail activation",
                run_id
            );
            return FailedActivationOutcome::NoReport;
        };
        if let Some(m) = self.workflow_machines.run_metrics(run_id) {
            m.wf_task_failed();
        }
        // If the outstanding activation is a legacy query task, report that we need to fail it
        if let Some(OutstandingActivation::LegacyQuery) =
            self.workflow_machines.get_activation(run_id)
        {
            FailedActivationOutcome::ReportLegacyQueryFailure(tt)
        } else {
            // Blow up any cached data associated with the workflow
            let should_report = self
                .request_eviction(run_id, "Activation failed by lang")
                .map_or(true, |attempt| attempt <= 1);
            if should_report {
                FailedActivationOutcome::Report(tt)
            } else {
                FailedActivationOutcome::NoReport
            }
        }
    }

    /// Will create a new workflow manager if needed for the workflow activation, if not, it will
    /// feed the existing manager the updated history we received from the server.
    ///
    /// Returns the next workflow activation and some info about it, if an activation is needed.
    async fn instantiate_or_update_workflow(
        &self,
        poll_wf_resp: ValidPollWFTQResponse,
        gateway: &GatewayRef,
    ) -> Result<(WorkflowTaskInfo, WfActivation), WorkflowUpdateError> {
        let run_id = poll_wf_resp.workflow_execution.run_id.clone();

        let wft_info = WorkflowTaskInfo {
            attempt: poll_wf_resp.attempt,
            task_token: poll_wf_resp.task_token,
        };

        match self
            .workflow_machines
            .create_or_update(
                &run_id,
                HistoryUpdate::new(
                    HistoryPaginator::new(
                        poll_wf_resp.history,
                        poll_wf_resp.workflow_execution.workflow_id.clone(),
                        poll_wf_resp.workflow_execution.run_id.clone(),
                        poll_wf_resp.next_page_token,
                        gateway.gw.clone(),
                    ),
                    poll_wf_resp.previous_started_event_id,
                ),
                &poll_wf_resp.workflow_execution.workflow_id,
                &gateway.options.namespace,
                &poll_wf_resp.workflow_type,
                &self.metrics,
            )
            .await
        {
            Ok(mut activation) => {
                // If there are in-poll queries, insert jobs for those queries into the activation
                if !poll_wf_resp.query_requests.is_empty() {
                    let query_jobs = poll_wf_resp
                        .query_requests
                        .into_iter()
                        .map(|q| wf_activation_job::Variant::QueryWorkflow(q).into());
                    activation.jobs.extend(query_jobs);
                }

                Ok((wft_info, activation))
            }
            Err(source) => Err(WorkflowUpdateError {
                source,
                run_id,
                task_token: Some(wft_info.task_token),
            }),
        }
    }

    /// Called after every workflow activation completion or failure, updates outstanding task
    /// status & issues evictions if required. It is important this is called *after* potentially
    /// reporting a successful WFT to server, as some replies (task not found) may require an
    /// eviction, which could be avoided if this is called too early.
    ///
    /// Returns true if WFT was marked completed internally
    pub(crate) fn after_wft_report(&self, run_id: &str, did_complete_wft: bool) -> bool {
        let mut just_evicted = false;

        if let Some(OutstandingActivation::Normal {
            contains_eviction: true,
        }) = self.workflow_machines.get_activation(run_id)
        {
            self.evict_run(run_id);
            just_evicted = true;
        };

        // Workflows with no more pending activations (IE: They have completed a WFT) must be
        // removed from the outstanding tasks map
        let retme = if !self.pending_activations.has_pending(run_id) {
            if !just_evicted {
                // Check if there was a legacy query which must be fulfilled, and if there is create
                // a new pending activation for it.
                if let Some(ref mut ot) = &mut *self
                    .workflow_machines
                    .get_task_mut(run_id)
                    .expect("Machine must exist")
                {
                    if let Some(query) = ot.legacy_query.take() {
                        let na = create_query_activation(run_id.to_string(), [query]);
                        self.pending_legacy_queries.push(na);
                        let _ = self.pending_activations_notifier.send(true);
                        return false;
                    }
                }

                // Evict run id if cache is full. Non-sticky will always evict.
                let maybe_evicted = self.cache_manager.lock().insert(run_id);
                if let Some(evicted_run_id) = maybe_evicted {
                    self.request_eviction(&evicted_run_id, "Workflow cache full");
                }

                // If there was a buffered poll response from the server, it is now ready to
                // be handled.
                if let Some(buffd) = self.workflow_machines.take_buffered_poll(run_id) {
                    self.make_buffered_poll_ready(buffd);
                }
            }

            // The evict may or may not have already done this, but even when we aren't evicting
            // we want to clear the outstanding workflow task since it's now complete.
            self.workflow_machines
                .complete_wft(run_id, did_complete_wft)
                .is_some()
        } else {
            false
        };
        self.on_activation_done(run_id);
        retme
    }

    /// Must be called after *every* activation is replied to, regardless of whether or not we
    /// had some issue reporting it to server or anything else. This upholds the invariant that
    /// every activation we issue to lang has exactly one reply.
    ///
    /// Any subsequent action that needs to be taken will be created as a new activation
    fn on_activation_done(&self, run_id: &str) {
        if self.workflow_machines.delete_activation(run_id).is_some() {
            let _ = self.pending_activations_notifier.send(true);
        }
        // It's possible the activation is already removed due to completing an eviction
    }

    /// Let a workflow know that something we've been waiting locally on has resolved, like a local
    /// activity or side effect
    #[instrument(level = "debug", skip(self, resolved))]
    pub(crate) async fn notify_of_local_result(
        &self,
        run_id: &str,
        seq_id: u32,
        resolved: LocalResolution,
    ) -> Result<(), WorkflowUpdateError> {
        self.workflow_machines
            .access_sync(run_id, |wfm: &mut WorkflowManager| {
                wfm.notify_of_local_result(seq_id, resolved)
            })?
            .map_err(|wfme| WorkflowUpdateError {
                source: wfme,
                run_id: run_id.to_string(),
                task_token: None,
            })?;

        self.needs_activation(run_id);
        Ok(())
    }

    fn make_buffered_poll_ready(&self, buffd: ValidPollWFTQResponse) {
        self.ready_buffered_wft.push(buffd);
    }

    fn insert_outstanding_activation(
        &self,
        act: &WfActivation,
    ) -> Result<(), WorkflowMissingError> {
        let act_type = if act.is_legacy_query() {
            OutstandingActivation::LegacyQuery
        } else {
            OutstandingActivation::Normal {
                contains_eviction: act.eviction_index().is_some(),
            }
        };
        match self
            .workflow_machines
            .insert_activation(&act.run_id, act_type)
        {
            Ok(None) => Ok(()),
            Ok(Some(previous)) => {
                // This is a panic because we have screwed up core logic if this is violated. It
                // must be upheld.
                panic!(
                    "Attempted to insert a new outstanding activation {}, but there already was \
                     one outstanding: {:?}",
                    act, previous
                );
            }
            Err(e) => Err(e),
        }
    }

    fn activation_has_eviction(&self, run_id: &str) -> bool {
        self.workflow_machines
            .get_activation(run_id)
            .map(OutstandingActivation::has_eviction)
            .unwrap_or_default()
    }

    fn needs_activation(&self, run_id: &str) {
        self.pending_activations.notify_needs_activation(run_id);
        let _ = self.pending_activations_notifier.send(true);
    }
}
