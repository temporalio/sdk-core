//! Management of workflow tasks

use crate::{
    errors::WorkflowUpdateError,
    machines::{ProtoCommand, WFCommand},
    pending_activations::PendingActivations,
    protos::coresdk::{
        workflow_activation::{
            create_evict_activation, create_query_activation, wf_activation_job, QueryWorkflow,
            WfActivation,
        },
        workflow_commands::QueryResult,
        PayloadsExt,
    },
    protosext::ValidPollWFTQResponse,
    task_token::TaskToken,
    workflow::{
        HistoryUpdate, WorkflowCachingPolicy, WorkflowConcurrencyManager, WorkflowError,
        WorkflowManager, LEGACY_QUERY_ID,
    },
    WfActivationUpdate,
};
use dashmap::{DashMap, DashSet};
use std::{collections::VecDeque, fmt::Debug};
use tokio::sync::mpsc::UnboundedSender;
use tracing::Span;

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
    /// Used to track which workflows (by run id) have outstanding workflow tasks, and information
    /// about that task, as well as (possibly) the most recent buffered poll from the server for
    /// this workflow run
    outstanding_workflow_tasks: DashMap<String, OutstandingTask>,
    /// A set of run ids for which there is a currently outstanding workflow activation
    outstanding_activations: DashSet<String>,
    /// Holds (by task queue) poll wft responses from the server that need to be applied
    ready_buffered_wft: DashMap<String, VecDeque<ValidPollWFTQResponse>>,
    /// Used to wake blocked workflow task polling
    workflow_activations_update: UnboundedSender<WfActivationUpdate>,
    eviction_policy: WorkflowCachingPolicy,
}

struct OutstandingTask {
    pub info: WorkflowTaskInfo,
    /// If set, it indicates there is a buffered poll response from the server that applies to this
    /// run. This can happen when lang takes too long to complete a task and the task times out, for
    /// example. Upon next completion, the buffered response will be removed and pushed into
    /// [ready_buffered_wft].
    pub buffered_resp: Option<ValidPollWFTQResponse>,
}

/// Contains important information about a given workflow task that we need to memorize while
/// lang handles it.
#[derive(Clone, Debug)]
pub struct WorkflowTaskInfo {
    pub task_token: TaskToken,
    pub run_id: String,
    pub attempt: u32,
    pub task_queue: String,
}

#[derive(Debug, derive_more::From)]
pub enum NewWfTaskOutcome {
    /// A new activation for the workflow should be issued to lang
    IssueActivation(WfActivation),
    /// The poll loop should be restarted, there is nothing to do
    RestartPollLoop,
    /// The workflow task should be auto-completed with an empty command list, as it must be replied
    /// to but there is no meaningful work for lang to do.
    Autocomplete,
}

#[derive(Debug)]
pub enum FailedActivationOutcome {
    NoReport,
    Report(TaskToken),
}

#[derive(Debug)]
pub struct ServerCommandsWithWorkflowInfo {
    pub task_token: TaskToken,
    pub task_queue: String,
    pub action: ActivationAction,
}

#[derive(Debug)]
pub enum ActivationAction {
    /// We should respond that the workflow task is complete
    WftComplete {
        commands: Vec<ProtoCommand>,
        query_responses: Vec<QueryResult>,
    },
    /// We should respond to a legacy query request
    RespondLegacyQuery { result: QueryResult },
}

impl WorkflowTaskManager {
    pub(crate) fn new(
        workflow_activations_update: UnboundedSender<WfActivationUpdate>,
        eviction_policy: WorkflowCachingPolicy,
    ) -> Self {
        Self {
            workflow_machines: WorkflowConcurrencyManager::new(),
            pending_activations: Default::default(),
            outstanding_workflow_tasks: Default::default(),
            outstanding_activations: Default::default(),
            ready_buffered_wft: Default::default(),
            workflow_activations_update,
            eviction_policy,
        }
    }

    pub fn shutdown(&self) {
        self.workflow_machines.shutdown();
    }

    pub fn next_pending_activation(&self, task_queue: &str) -> Option<WfActivation> {
        // It is important that we do not issue pending activations for any workflows which already
        // have an outstanding activation. If we did, it can result in races where an in-progress
        // completion may appear to be the last in a task (no more pending activations) because
        // concurrently a poll happened to dequeue the pending activation at the right time.
        // NOTE: This all goes away with the handles-per-workflow poll approach.
        let maybe_act = self
            .pending_activations
            .pop_first_matching(task_queue, |rid| {
                !self.outstanding_activations.contains(rid)
            });
        if let Some(act) = maybe_act.as_ref() {
            self.outstanding_activations.insert(act.run_id.clone());
        }
        maybe_act
    }

    pub fn next_buffered_poll(&self, task_queue: &str) -> Option<ValidPollWFTQResponse> {
        self.ready_buffered_wft
            .get_mut(task_queue)
            .map(|mut dq| dq.pop_front())
            .flatten()
    }

    pub(crate) fn activation_done(&self, run_id: &str) {
        if self.outstanding_activations.remove(run_id).is_some() {
            let _ = self
                .workflow_activations_update
                .send(WfActivationUpdate::NewPendingActivation);
        }
    }

    #[cfg(test)]
    pub fn outstanding_wft(&self) -> usize {
        self.outstanding_workflow_tasks.len()
    }

    /// Evict a workflow from the cache by its run id and enqueue a pending activation to evict the
    /// workflow. Any existing pending activations will be destroyed, and any outstanding
    /// activations invalidated.
    ///
    /// Returns that workflow's task info if it was present.
    pub fn evict_run(&self, run_id: &str) -> Option<WorkflowTaskInfo> {
        debug!(run_id=%run_id, "Evicting run");
        self.workflow_machines.evict(run_id);
        self.outstanding_activations.remove(run_id);
        self.pending_activations.remove_all_with_run_id(run_id);

        if let Some((
            _,
            OutstandingTask {
                info,
                buffered_resp,
            },
        )) = self.outstanding_workflow_tasks.remove(run_id)
        {
            // Queue up an eviction activation
            self.pending_activations.push(
                create_evict_activation(run_id.to_string()),
                info.task_queue.clone(),
            );
            let _ =
                self.workflow_activations_update
                    .send(WfActivationUpdate::WorkflowTaskComplete {
                        task_queue: info.task_queue.clone(),
                    });
            // If we just evicted something and there was a buffered poll response for the workflow,
            // it is now ready to be produced by the next poll. (Not immediate next, since, ignoring
            // other workflows, the next poll will be the eviction we just produced. Buffered polls
            // always are popped after pending activations)
            if let Some(buffered_poll) = buffered_resp {
                self.ready_buffered_wft
                    .entry(info.task_queue.clone())
                    .or_default()
                    .push_back(buffered_poll);
            }
            Some(info)
        } else {
            None
        }
    }

    /// Given a validated poll response from the server, prepare an activation (if there is one) to
    /// be sent to lang. If applying the response to the workflow's state does not produce a new
    /// activation, `None` is returned.
    ///
    /// The new activation is immediately considered to be an outstanding workflow task - so it is
    /// expected that new activations will be dispatched to lang right away.
    pub(crate) fn apply_new_poll_resp(
        &self,
        work: ValidPollWFTQResponse,
    ) -> Result<NewWfTaskOutcome, WorkflowUpdateError> {
        debug!(
            task_token = %&work.task_token,
            history_length = %work.history.events.len(),
            "Applying new workflow task from server"
        );

        if let Some(mut outstanding_entry) = self
            .outstanding_workflow_tasks
            .get_mut(&work.workflow_execution.run_id)
        {
            debug!("Got new WFT for a run with one outstanding");
            outstanding_entry.value_mut().buffered_resp = Some(work);
            return Ok(NewWfTaskOutcome::RestartPollLoop);
        }

        // Check if there is a legacy query we should immediately send an activation for
        // TODO:
        //   Add a test where query comes in while doing a long replay and do the right thing
        //   We can have no outstanding workflow task (reasonably), in which case we set on
        if let Some(q) = work.legacy_query {
            let info = WorkflowTaskInfo {
                task_token: work.task_token,
                run_id: work.workflow_execution.run_id.clone(),
                attempt: work.attempt,
                task_queue: work.task_queue,
            };
            self.outstanding_workflow_tasks.insert(
                info.run_id.clone(),
                OutstandingTask {
                    info,
                    buffered_resp: None,
                },
            );
            // TODO: Can I avoid this now? Just create an empty activation intentionally?
            let na = create_query_activation(
                work.workflow_execution.run_id,
                [QueryWorkflow {
                    query_id: LEGACY_QUERY_ID.to_string(),
                    query_type: q.query_type,
                    arguments: Vec::from_payloads(q.query_args),
                }],
            );
            return Ok(NewWfTaskOutcome::IssueActivation(na));
        }

        let (info, next_activation) = self.instantiate_or_update_workflow(work)?;
        self.outstanding_workflow_tasks.insert(
            info.run_id.clone(),
            OutstandingTask {
                info,
                buffered_resp: None,
            },
        );

        if !next_activation.jobs.is_empty() {
            self.outstanding_activations
                .insert(next_activation.run_id.clone());
            Ok(NewWfTaskOutcome::IssueActivation(next_activation))
        } else {
            // TODO: Problem is here we would've had WF info inserted, but no activation.
            Ok(NewWfTaskOutcome::Autocomplete)
        }
    }

    /// Record a successful activation. Returns (if any) commands that should be reported to the
    /// server as part of wft completion
    pub(crate) fn successful_activation(
        &self,
        run_id: &str,
        commands: Vec<WFCommand>,
    ) -> Result<Option<ServerCommandsWithWorkflowInfo>, WorkflowUpdateError> {
        let (task_token, task_queue) =
            if let Some(entry) = self.outstanding_workflow_tasks.get(run_id) {
                // Note: Ideally we could return these as refs but dashmap makes that hard. Likely
                //   not a real perf concern.
                (entry.info.task_token.clone(), entry.info.task_queue.clone())
            } else {
                warn!(
                    run_id,
                    "Attempted to complete activation for nonexistent run"
                );
                return Ok(None);
            };

        // If the only command in the activation is a legacy query response, that means we need
        // to respond differently than a typical activation.
        let ret = if matches!(&commands.as_slice(),
                    &[WFCommand::QueryResponse(qr)] if qr.query_id == LEGACY_QUERY_ID)
        {
            let mut commands = commands;
            let qr = match commands.remove(0) {
                WFCommand::QueryResponse(qr) => qr,
                _ => unreachable!("We just verified this is the only command"),
            };
            Some(ServerCommandsWithWorkflowInfo {
                task_token,
                task_queue,
                action: ActivationAction::RespondLegacyQuery { result: qr },
            })
        } else {
            // First strip out query responses from other commands that actually affect machines
            let mut saw_legacy_cmd = false;
            let query_responses = commands
                .iter()
                .filter_map(|cmd| {
                    if let WFCommand::QueryResponse(qr) = cmd {
                        if qr.query_id == LEGACY_QUERY_ID {
                            saw_legacy_cmd = true;
                        }
                        // TODO: Ugh avoid clone
                        Some(qr.clone())
                    } else {
                        None
                    }
                })
                .collect();
            if saw_legacy_cmd {
                return Err(WorkflowUpdateError {
                    source: WorkflowError::LegacyQueryResponseIncludedOtherCommands,
                    run_id: run_id.to_string(),
                });
            }
            warn!("Query resps: {:#?}", &query_responses);
            // Send commands from lang into the machines
            self.access_wf_machine(run_id, move |mgr| mgr.push_commands(commands))?;
            self.enqueue_next_activation_if_needed(run_id)?;
            // We want to fetch the outgoing commands only after any new activation has been queued,
            // as doing so may have altered the outgoing commands.
            let server_cmds = self.access_wf_machine(run_id, |w| Ok(w.get_server_commands()))?;
            // We only actually want to send commands back to the server if there are no more pending
            // activations and we are caught up on replay.
            if !self.pending_activations.has_pending(run_id) && !server_cmds.replaying {
                Some(ServerCommandsWithWorkflowInfo {
                    task_token,
                    task_queue,
                    action: ActivationAction::WftComplete {
                        commands: server_cmds.commands,
                        query_responses,
                    },
                })
            } else {
                if !query_responses.is_empty() {
                    error!("Query only resp replaying: {}", server_cmds.replaying);
                    Some(ServerCommandsWithWorkflowInfo {
                        task_token,
                        task_queue,
                        action: ActivationAction::WftComplete {
                            commands: vec![],
                            query_responses,
                        },
                    })
                } else {
                    None
                }
            }
        };

        Ok(ret)
    }

    /// Record that an activation failed, returns true if the failure should be reported to the
    /// server
    pub fn failed_activation(&self, run_id: &str) -> FailedActivationOutcome {
        let tt = if let Some(entry) = self.outstanding_workflow_tasks.get(run_id) {
            entry.info.task_token.clone()
        } else {
            warn!(
                "No info for workflow with run id {} found when trying to fail activation",
                run_id
            );
            return FailedActivationOutcome::NoReport;
        };
        // Blow up any cached data associated with the workflow
        let should_report = if let Some(wti) = self.evict_run(run_id) {
            // Only report to server if the last task wasn't also a failure (avoid spam)
            wti.attempt <= 1
        } else {
            true
        };
        self.after_wft_report(run_id);
        if should_report {
            FailedActivationOutcome::Report(tt)
        } else {
            FailedActivationOutcome::NoReport
        }
    }

    /// Will create a new workflow manager if needed for the workflow activation, if not, it will
    /// feed the existing manager the updated history we received from the server.
    ///
    /// Returns the next workflow activation and some info about it, if an activation is needed.
    fn instantiate_or_update_workflow(
        &self,
        poll_wf_resp: ValidPollWFTQResponse,
    ) -> Result<(WorkflowTaskInfo, WfActivation), WorkflowUpdateError> {
        let run_id = poll_wf_resp.workflow_execution.run_id.clone();

        let wft_info = WorkflowTaskInfo {
            run_id: run_id.clone(),
            attempt: poll_wf_resp.attempt,
            task_queue: poll_wf_resp.task_queue,
            task_token: poll_wf_resp.task_token,
        };

        match self.workflow_machines.create_or_update(
            &run_id,
            HistoryUpdate::new(
                poll_wf_resp.history,
                poll_wf_resp.previous_started_event_id,
                poll_wf_resp.started_event_id,
            ),
            poll_wf_resp.workflow_execution,
        ) {
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
            Err(source) => Err(WorkflowUpdateError { source, run_id }),
        }
    }

    /// Check if thew workflow run needs another activation and queue it up if there is one by
    /// pushing it into the pending activations list
    fn enqueue_next_activation_if_needed(&self, run_id: &str) -> Result<bool, WorkflowUpdateError> {
        let mut new_activation = false;
        let next_activation =
            self.access_wf_machine(run_id, move |mgr| mgr.get_next_activation())?;
        if !next_activation.jobs.is_empty() {
            let wf_entry = self
                .outstanding_workflow_tasks
                .get(run_id)
                .expect("Workflow info is present in map if there is a new pending activation");
            self.pending_activations
                .push(next_activation, wf_entry.info.task_queue.clone());
            new_activation = true;
            let _ = self
                .workflow_activations_update
                .send(WfActivationUpdate::NewPendingActivation);
        }
        Ok(new_activation)
    }

    /// Called after every WFT completion or failure, updates outstanding task status & issues
    /// evictions if required. It is important this is called *after* reporting a successful WFT
    /// to server, as some replies (task not found) may require an eviction, which could be avoided
    /// if this is called too early.
    pub(crate) fn after_wft_report(&self, run_id: &str) {
        // Workflows with no more pending activations (IE: They have completed a WFT) must be
        // removed from the outstanding tasks map
        if !self.pending_activations.has_pending(run_id) {
            // Blow them up if we're in non-sticky mode as well
            if self.eviction_policy == WorkflowCachingPolicy::NonSticky {
                self.evict_run(run_id);
            }

            self.outstanding_workflow_tasks.remove(run_id);
            // The evict may or may not have already done this, but even when we aren't evicting
            // we want to remove from the run id mapping if we completed the wft, since the task
            // token will not be reused.
            if let Some((_, wti)) = self.outstanding_workflow_tasks.remove(run_id) {
                warn!("Removed b/c wft complete");
                let _ = self.workflow_activations_update.send(
                    WfActivationUpdate::WorkflowTaskComplete {
                        task_queue: wti.info.task_queue,
                    },
                );
            }
        }
    }

    /// Wraps access to `self.workflow_machines.access`, properly passing in the current tracing
    /// span to the wf machines thread.
    fn access_wf_machine<F, Fout>(
        &self,
        run_id: &str,
        mutator: F,
    ) -> Result<Fout, WorkflowUpdateError>
    where
        F: FnOnce(&mut WorkflowManager) -> Result<Fout, WorkflowError> + Send + 'static,
        Fout: Send + Debug + 'static,
    {
        let curspan = Span::current();
        let mutator = move |wfm: &mut WorkflowManager| {
            let _e = curspan.enter();
            mutator(wfm)
        };
        self.workflow_machines
            .access(run_id, mutator)
            .map_err(|source| WorkflowUpdateError {
                source,
                run_id: run_id.to_owned(),
            })
    }
}
