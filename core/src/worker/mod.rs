mod activities;
mod dispatcher;
mod wft_delivery;

pub use temporal_sdk_core_api::worker::{WorkerConfig, WorkerConfigBuilder};

pub(crate) use activities::{
    ExecutingLAId, LocalActRequest, LocalActivityExecutionResult, LocalActivityResolution,
    NewLocalAct,
};
pub(crate) use dispatcher::WorkerDispatcher;

use crate::{
    errors::CompleteWfError,
    pollers::{
        new_activity_task_buffer, new_workflow_task_buffer, BoxedActPoller, BoxedWFPoller,
        GatewayRef, Poller, WorkflowTaskPoller,
    },
    protosext::{legacy_query_failure, ValidPollWFTQResponse},
    telemetry::metrics::{
        activity_poller, workflow_poller, workflow_sticky_poller, MetricsContext,
    },
    worker::{
        activities::{DispatchOrTimeoutLA, LACompleteAction, LocalActivityManager},
        wft_delivery::WFTSource,
    },
    workflow::{
        workflow_tasks::{
            ActivationAction, FailedActivationOutcome, NewWfTaskOutcome,
            ServerCommandsWithWorkflowInfo, WorkflowTaskManager,
        },
        EmptyWorkflowCommandErr, LocalResolution, WFMachinesError, WorkflowCachingPolicy,
    },
    ActivityHeartbeat, CompleteActivityError, PollActivityError, PollWfError,
};
use activities::{LocalInFlightActInfo, WorkerActivityTasks};
use futures::{Future, TryFutureExt};
use std::{convert::TryInto, future, sync::Arc};
use temporal_client::WorkflowTaskCompletion;
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::activity_execution_result,
        activity_task::ActivityTask,
        workflow_activation::WorkflowActivation,
        workflow_completion::{self, workflow_activation_completion, WorkflowActivationCompletion},
    },
    temporal::api::{
        enums::v1::{TaskQueueKind, WorkflowTaskFailedCause},
        failure::v1::Failure,
        taskqueue::v1::{StickyExecutionAttributes, TaskQueue},
        workflowservice::v1::{PollActivityTaskQueueResponse, PollWorkflowTaskQueueResponse},
    },
    TaskToken,
};
use tokio::sync::{watch, Notify, Semaphore};
use tonic::Code;
use tracing_futures::Instrument;

/// A worker polls on a certain task queue
pub struct Worker {
    config: WorkerConfig,
    server_gateway: Arc<GatewayRef>,

    /// Will be populated when this worker should poll on a sticky WFT queue
    sticky_name: Option<String>,

    /// Buffers workflow task polling in the event we need to return a pending activation while
    /// a poll is ongoing. Sticky and nonsticky polling happens inside of it.
    wf_task_source: WFTSource,
    /// Workflow task management
    wft_manager: WorkflowTaskManager,
    /// Manages activity tasks for this worker/task queue
    at_task_mgr: Option<WorkerActivityTasks>,
    /// Manages local activities
    local_act_mgr: LocalActivityManager,
    /// Ensures we stay at or below this worker's maximum concurrent workflow limit
    workflows_semaphore: Semaphore,
    /// Used to wake blocked workflow task polling when there is some change to workflow activations
    /// that should cause us to restart the loop
    pending_activations_notify: Arc<Notify>,
    /// Watched during shutdown to wait for all WFTs to complete. Should be notified any time
    /// a WFT is completed.
    wfts_drained_notify: Arc<Notify>,
    /// Has shutdown been called?
    shutdown_requested: watch::Receiver<bool>,
    shutdown_sender: watch::Sender<bool>,

    metrics: MetricsContext,
}

impl Worker {
    pub(crate) fn new(
        config: WorkerConfig,
        sticky_queue_name: Option<String>,
        sg: Arc<GatewayRef>,
        metrics: MetricsContext,
    ) -> Self {
        metrics.worker_registered();

        let max_nonsticky_polls = if sticky_queue_name.is_some() {
            config.max_nonsticky_polls()
        } else {
            config.max_concurrent_wft_polls
        };
        let max_sticky_polls = config.max_sticky_polls();
        let wft_metrics = metrics.with_new_attrs([workflow_poller()]);
        let mut wf_task_poll_buffer = new_workflow_task_buffer(
            sg.gw.clone(),
            config.task_queue.clone(),
            false,
            max_nonsticky_polls,
            max_nonsticky_polls * 2,
        );
        wf_task_poll_buffer.set_num_pollers_handler(move |np| wft_metrics.record_num_pollers(np));
        let sticky_queue_poller = sticky_queue_name.as_ref().map(|sqn| {
            let sticky_metrics = metrics.with_new_attrs([workflow_sticky_poller()]);
            let mut sp = new_workflow_task_buffer(
                sg.gw.clone(),
                sqn.clone(),
                true,
                max_sticky_polls,
                max_sticky_polls * 2,
            );
            sp.set_num_pollers_handler(move |np| sticky_metrics.record_num_pollers(np));
            sp
        });
        let act_poll_buffer = if config.no_remote_activities {
            None
        } else {
            let mut ap = new_activity_task_buffer(
                sg.gw.clone(),
                config.task_queue.clone(),
                config.max_concurrent_at_polls,
                config.max_concurrent_at_polls * 2,
            );
            let act_metrics = metrics.with_new_attrs([activity_poller()]);
            ap.set_num_pollers_handler(move |np| act_metrics.record_num_pollers(np));
            Some(Box::from(ap)
                as Box<
                    dyn Poller<PollActivityTaskQueueResponse> + Send + Sync,
                >)
        };
        let wf_task_poll_buffer = Box::new(WorkflowTaskPoller::new(
            wf_task_poll_buffer,
            sticky_queue_poller,
        ));
        Self::new_with_pollers(
            config,
            sticky_queue_name,
            sg,
            wf_task_poll_buffer,
            act_poll_buffer,
            metrics,
        )
    }

    pub(crate) fn new_with_pollers(
        config: WorkerConfig,
        sticky_queue_name: Option<String>,
        sg: Arc<GatewayRef>,
        wft_poller: BoxedWFPoller,
        act_poller: Option<BoxedActPoller>,
        metrics: MetricsContext,
    ) -> Self {
        let cache_policy = if config.max_cached_workflows == 0 {
            WorkflowCachingPolicy::NonSticky
        } else {
            WorkflowCachingPolicy::Sticky {
                max_cached_workflows: config.max_cached_workflows,
            }
        };
        let pa_notif = Arc::new(Notify::new());
        let wfts_drained_notify = Arc::new(Notify::new());
        let (shut_tx, shut_rx) = watch::channel(false);
        Self {
            server_gateway: sg.clone(),
            sticky_name: sticky_queue_name,
            wf_task_source: WFTSource::new(wft_poller),
            wft_manager: WorkflowTaskManager::new(pa_notif.clone(), cache_policy, metrics.clone()),
            at_task_mgr: act_poller.map(|ap| {
                WorkerActivityTasks::new(
                    config.max_outstanding_activities,
                    ap,
                    sg.gw.clone(),
                    metrics.clone(),
                    config.max_heartbeat_throttle_interval,
                    config.default_heartbeat_throttle_interval,
                )
            }),
            local_act_mgr: LocalActivityManager::new(
                config.max_outstanding_local_activities,
                sg.options.namespace.clone(),
            ),
            workflows_semaphore: Semaphore::new(config.max_outstanding_workflow_tasks),
            config,
            shutdown_requested: shut_rx,
            shutdown_sender: shut_tx,
            pending_activations_notify: pa_notif,
            wfts_drained_notify,
            metrics,
        }
    }

    /// Will shutdown the worker. Does not resolve until all outstanding workflow tasks have been
    /// completed
    pub(crate) async fn shutdown(&self) {
        let _ = self.shutdown_sender.send(true);
        // First, we want to stop polling of both activity and workflow tasks
        if let Some(atm) = self.at_task_mgr.as_ref() {
            atm.notify_shutdown();
        }
        self.wf_task_source.stop_pollers();
        // Next we need to wait for all local activities to finish so no more workflow task
        // heartbeats will be generated
        self.local_act_mgr.shutdown_and_wait_all_finished().await;
        // Then we need to wait for any tasks generated as a result of completing WFTs, which
        // heartbeating generates
        self.wf_task_source
            .wait_for_tasks_from_complete_to_drain()
            .await;
        // wait until all outstanding workflow tasks have been completed
        self.all_wfts_drained().await;
        // Wait for activities to finish
        if let Some(acts) = self.at_task_mgr.as_ref() {
            acts.wait_all_finished().await;
        }
    }

    /// Finish shutting down by consuming the background pollers and freeing all resources
    pub(crate) async fn finalize_shutdown(self) {
        tokio::join!(self.wf_task_source.shutdown(), async {
            if let Some(b) = self.at_task_mgr {
                b.shutdown().await;
            }
        });
    }

    pub(crate) fn outstanding_workflow_tasks(&self) -> usize {
        self.wft_manager.outstanding_wft()
    }

    #[cfg(test)]
    pub(crate) fn available_wft_permits(&self) -> usize {
        self.workflows_semaphore.available_permits()
    }

    /// Get new activity tasks (may be local or nonlocal). Local activities are returned first
    /// before polling the server if there are any.
    ///
    /// Returns `Ok(None)` in the event of a poll timeout or if the polling loop should otherwise
    /// be restarted
    pub(crate) async fn activity_poll(&self) -> Result<Option<ActivityTask>, PollActivityError> {
        let act_mgr_poll = async {
            if let Some(ref act_mgr) = self.at_task_mgr {
                act_mgr.poll().await
            } else {
                future::pending().await
            }
        };

        tokio::select! {
            biased;

            r = self.local_act_mgr.next_pending() => {
                match r {
                    Some(DispatchOrTimeoutLA::Dispatch(r)) => Ok(Some(r)),
                    Some(DispatchOrTimeoutLA::Timeout { run_id, resolution, task }) => {
                        self.notify_local_result(
                            &run_id, LocalResolution::LocalActivity(resolution)).await;
                        Ok(task)
                    },
                    None => Ok(None)
                }
            },
            r = act_mgr_poll => r,
        }
    }

    /// Attempt to record an activity heartbeat
    pub(crate) fn record_heartbeat(&self, details: ActivityHeartbeat) {
        if let Some(at_mgr) = self.at_task_mgr.as_ref() {
            let tt = details.task_token.clone();
            if let Err(e) = at_mgr.record_heartbeat(details) {
                warn!(task_token = ?tt, details = ?e, "Activity heartbeat failed.");
            }
        }
    }

    pub(crate) async fn complete_activity(
        &self,
        task_token: TaskToken,
        status: activity_execution_result::Status,
    ) -> Result<(), CompleteActivityError> {
        if task_token.is_local_activity_task() {
            let as_la_res: LocalActivityExecutionResult = status.try_into()?;
            match self.local_act_mgr.complete(&task_token, &as_la_res) {
                LACompleteAction::Report(info) => {
                    self.complete_local_act(as_la_res, info, None).await
                }
                LACompleteAction::LangDoesTimerBackoff(backoff, info) => {
                    // This la needs to write a failure marker, and then we will tell lang how
                    // long of a timer to schedule to back off for. We do this because there are
                    // no other situations where core generates "internal" commands so it is much
                    // simpler for lang to reply with the timer / next LA command than to do it
                    // internally. Plus, this backoff hack we'd like to eliminate eventually.
                    self.complete_local_act(as_la_res, info, Some(backoff))
                        .await
                }
                LACompleteAction::WillBeRetried => {
                    // Nothing to do here
                }
                LACompleteAction::Untracked => {
                    warn!("Tried to complete untracked local activity {}", task_token);
                }
            }
            return Ok(());
        }

        if let Some(atm) = &self.at_task_mgr {
            atm.complete(task_token, status, self.server_gateway.gw.as_ref())
                .await
        } else {
            error!(
                "Tried to complete activity {} on a worker that does not have an activity manager",
                task_token
            );
            Ok(())
        }
    }
    pub(crate) async fn next_workflow_activation(&self) -> Result<WorkflowActivation, PollWfError> {
        // The poll needs to be in a loop because we can't guarantee tail call optimization in Rust
        // (simply) and we really, really need that for long-poll retries.
        loop {
            // We must first check if there are pending workflow activations for workflows that are
            // currently replaying or otherwise need immediate jobs, and issue those before
            // bothering the server.
            if let Some(pa) = self.wft_manager.next_pending_activation() {
                debug!(activation=%pa, "Sending pending activation to lang");
                return Ok(pa);
            }

            // Apply any buffered poll responses from the server. Must come after pending
            // activations, since there may be an eviction etc for whatever run is popped here.
            if let Some(buff_wft) = self.wft_manager.next_buffered_poll() {
                match self.apply_server_work(buff_wft).await? {
                    Some(a) => return Ok(a),
                    _ => continue,
                }
            }

            let selected_f = tokio::select! {
                biased;

                // If an activation is completed while we are waiting on polling, we need to restart
                // the loop right away to provide any potential new pending activation.
                // Continue here means that we unnecessarily add another permit to the poll buffer,
                // this will go away when polling is done in the background.
                _ = self.pending_activations_notify.notified() => continue,
                r = self.workflow_poll_or_wfts_drained() => r,
            }?;

            if let Some(work) = selected_f {
                self.metrics.wf_tq_poll_ok();
                if let Some(a) = self.apply_server_work(work).await? {
                    return Ok(a);
                }
            }

            // Make sure that polling looping doesn't hog up the whole scheduler. Realistically
            // this probably only happens when mock responses return at full speed.
            tokio::task::yield_now().await;
        }
    }

    pub(crate) async fn complete_workflow_activation(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<(), CompleteWfError> {
        let wfstatus = completion.status;
        let report_outcome = match wfstatus {
            Some(workflow_activation_completion::Status::Successful(success)) => {
                self.wf_activation_success(&completion.run_id, success)
                    .await
            }

            Some(workflow_activation_completion::Status::Failed(failure)) => {
                self.wf_activation_failed(
                    &completion.run_id,
                    WorkflowTaskFailedCause::Unspecified,
                    failure,
                )
                .await
            }
            None => {
                return Err(CompleteWfError::MalformedWorkflowCompletion {
                    reason: "Workflow completion had empty status field".to_owned(),
                    completion: None,
                })
            }
        }?;
        self.after_workflow_activation(&completion.run_id, report_outcome);
        Ok(())
    }

    /// Tell the worker a workflow task has completed, for tracking max outstanding WFTs
    pub(crate) fn return_workflow_task_permit(&self) {
        self.workflows_semaphore.add_permits(1);
    }

    pub(crate) fn request_wf_eviction(&self, run_id: &str, reason: impl Into<String>) {
        self.wft_manager.request_eviction(run_id, reason);
    }

    /// Resolves with WFT poll response or `PollWfError::ShutDown` if WFTs have been drained
    async fn workflow_poll_or_wfts_drained(
        &self,
    ) -> Result<Option<ValidPollWFTQResponse>, PollWfError> {
        let mut shutdown_requested = self.shutdown_requested.clone();
        loop {
            tokio::select! {
                biased;

                r = self.workflow_poll().map_err(Into::into) => {
                    if matches!(r, Err(PollWfError::ShutDown)) {
                        // Don't actually return shutdown until workflow tasks are drained.
                        // Outstanding tasks being completed will generate new pending activations
                        // which will cause us to abort this function.
                        self.all_wfts_drained().await;
                    }
                    return r
                },
                _ = shutdown_requested.changed() => {},
            }
        }
    }

    /// Wait until not at the outstanding workflow task limit, and then poll this worker's task
    /// queue for new workflow tasks.
    ///
    /// Returns `Ok(None)` in the event of a poll timeout, or if there was some gRPC error that
    /// callers can't do anything about.
    async fn workflow_poll(&self) -> Result<Option<ValidPollWFTQResponse>, PollWfError> {
        // We can't say we're shut down if there are outstanding LAs, as they could end up WFT
        // heartbeating which is a "new" workflow task that we need to accept and process as long as
        // the LA is outstanding. Similarly, if we already have such tasks (from a WFT completion),
        // then we must fetch them from the source before we can say workflow polling is shutdown.
        if *self.shutdown_requested.borrow()
            && !self.wf_task_source.has_tasks_from_complete()
            && self.local_act_mgr.num_outstanding() == 0
        {
            return Err(PollWfError::ShutDown);
        }

        let sem = self
            .workflows_semaphore
            .acquire()
            .await
            .expect("outstanding workflow tasks semaphore not dropped");

        let res = self
            .wf_task_source
            .next_wft()
            .await
            .ok_or(PollWfError::ShutDown)??;

        if res == PollWorkflowTaskQueueResponse::default() {
            // We get the default proto in the event that the long poll times out.
            debug!("Poll wft timeout");
            self.metrics.wf_tq_poll_empty();
            return Ok(None);
        }

        if let Some(dur) = res.sched_to_start() {
            self.metrics.wf_task_sched_to_start_latency(dur);
        }

        let work: ValidPollWFTQResponse = res.try_into().map_err(|resp| {
            PollWfError::TonicError(tonic::Status::new(
                Code::DataLoss,
                format!(
                    "Server returned a poll WFT response we couldn't interpret: {:?}",
                    resp
                ),
            ))
        })?;

        // Only permanently take a permit in the event the poll finished completely
        sem.forget();
        Ok(Some(work))
    }

    /// Apply validated poll responses from the server. Returns an activation if one should be
    /// issued to lang, or returns `None` in which case the polling loop should be restarted
    /// (ex: Got a new workflow task for a run but lang is already handling an activation for that
    /// same run)
    async fn apply_server_work(
        &self,
        work: ValidPollWFTQResponse,
    ) -> Result<Option<WorkflowActivation>, PollWfError> {
        let we = work.workflow_execution.clone();
        let tt = work.task_token.clone();
        let res = self
            .wft_manager
            .apply_new_poll_resp(work, &self.server_gateway)
            .await;
        Ok(match res {
            NewWfTaskOutcome::IssueActivation(a) => {
                debug!(activation=%a, "Sending activation to lang");
                Some(a)
            }
            NewWfTaskOutcome::TaskBuffered => {
                // If the task was buffered, it's not actually outstanding, so we can
                // immediately return a permit.
                self.return_workflow_task_permit();
                None
            }
            NewWfTaskOutcome::Autocomplete | NewWfTaskOutcome::LocalActsOutstanding => {
                debug!(workflow_execution=?we,
                       "No new work for lang to perform after polling server");
                self.complete_workflow_activation(WorkflowActivationCompletion {
                    task_queue: self.config.task_queue.clone(),
                    run_id: we.run_id,
                    status: Some(workflow_completion::Success::from_variants(vec![]).into()),
                })
                .await?;
                None
            }
            NewWfTaskOutcome::CacheMiss => {
                debug!(workflow_execution=?we, "Unable to process workflow task with partial \
                history because workflow cache does not contain workflow anymore.");
                self.server_gateway
                    .fail_workflow_task(
                        tt,
                        WorkflowTaskFailedCause::ResetStickyTaskQueue,
                        Some(Failure {
                            message: "Unable to process workflow task with partial history \
                                      because workflow cache does not contain workflow anymore."
                                .to_string(),
                            ..Default::default()
                        }),
                    )
                    .await?;
                self.return_workflow_task_permit();
                None
            }
            NewWfTaskOutcome::Evict(e) => {
                warn!(error=?e, run_id=%we.run_id, "Error while applying poll response to workflow");
                self.request_wf_eviction(
                    &we.run_id,
                    format!("Error while applying poll response to workflow: {:?}", e),
                );
                None
            }
        })
    }

    /// Handle a successful workflow activation
    ///
    /// Returns true if we actually reported WFT completion to server (success or failure)
    async fn wf_activation_success(
        &self,
        run_id: &str,
        success: workflow_completion::Success,
    ) -> Result<WFTReportOutcome, CompleteWfError> {
        // Convert to wf commands
        let cmds = success
            .commands
            .into_iter()
            .map(|c| c.try_into())
            .collect::<Result<Vec<_>, EmptyWorkflowCommandErr>>()
            .map_err(|_| CompleteWfError::MalformedWorkflowCompletion {
                reason:
                    "At least one workflow command in the completion contained an empty variant"
                        .to_owned(),
                completion: None,
            })?;

        match self
            .wft_manager
            .successful_activation(run_id, cmds, |acts| self.local_act_mgr.enqueue(acts))
            .await
        {
            Ok(Some(ServerCommandsWithWorkflowInfo {
                task_token,
                action:
                    ActivationAction::WftComplete {
                        commands,
                        query_responses,
                        force_new_wft,
                    },
            })) => {
                debug!("Sending commands to server: {:?}", &commands);
                if !query_responses.is_empty() {
                    debug!("Sending query responses to server: {:?}", &query_responses);
                }
                let mut completion = WorkflowTaskCompletion {
                    task_token,
                    commands,
                    query_responses,
                    sticky_attributes: None,
                    return_new_workflow_task: force_new_wft,
                    force_create_new_workflow_task: force_new_wft,
                };
                let sticky_attrs = self.get_sticky_attrs();
                // Do not return new WFT if we would not cache, because returned new WFTs are always
                // partial.
                if sticky_attrs.is_none() {
                    completion.return_new_workflow_task = false;
                }
                completion.sticky_attributes = sticky_attrs;

                self.handle_wft_reporting_errs(run_id, || async {
                    let maybe_wft = self
                        .server_gateway
                        .complete_workflow_task(completion)
                        .instrument(span!(tracing::Level::DEBUG, "Complete WFT call"))
                        .await?;
                    if let Some(wft) = maybe_wft.workflow_task {
                        self.wf_task_source.add_wft_from_completion(wft);
                    }
                    Ok(())
                })
                .await?;
                Ok(WFTReportOutcome {
                    reported_to_server: true,
                    failed: false,
                })
            }
            Ok(Some(ServerCommandsWithWorkflowInfo {
                task_token,
                action: ActivationAction::RespondLegacyQuery { result },
                ..
            })) => {
                self.server_gateway
                    .respond_legacy_query(task_token, result)
                    .await?;
                Ok(WFTReportOutcome {
                    reported_to_server: true,
                    failed: false,
                })
            }
            Ok(None) => Ok(WFTReportOutcome {
                reported_to_server: false,
                failed: false,
            }),
            Err(update_err) => {
                // Automatically fail the workflow task in the event we couldn't update machines
                let fail_cause = if matches!(&update_err.source, WFMachinesError::Nondeterminism(_))
                {
                    WorkflowTaskFailedCause::NonDeterministicError
                } else {
                    WorkflowTaskFailedCause::Unspecified
                };
                let wft_fail_str = format!("{:?}", update_err);
                self.wf_activation_failed(
                    run_id,
                    fail_cause,
                    Failure::application_failure(wft_fail_str.clone(), false).into(),
                )
                .await
            }
        }
    }

    /// Handle a failed workflow completion
    ///
    /// Returns true if we actually reported WFT completion to server
    async fn wf_activation_failed(
        &self,
        run_id: &str,
        cause: WorkflowTaskFailedCause,
        failure: workflow_completion::Failure,
    ) -> Result<WFTReportOutcome, CompleteWfError> {
        Ok(match self.wft_manager.failed_activation(run_id) {
            FailedActivationOutcome::Report(tt) => {
                warn!(run_id, failure=?failure, "Failing workflow activation");
                self.handle_wft_reporting_errs(run_id, || async {
                    self.server_gateway
                        .fail_workflow_task(tt, cause, failure.failure.map(Into::into))
                        .await
                })
                .await?;
                WFTReportOutcome {
                    reported_to_server: true,
                    failed: true,
                }
            }
            FailedActivationOutcome::ReportLegacyQueryFailure(task_token) => {
                warn!(run_id, failure=?failure, "Failing legacy query request");
                self.server_gateway
                    .respond_legacy_query(task_token, legacy_query_failure(failure))
                    .await?;
                WFTReportOutcome {
                    reported_to_server: true,
                    failed: true,
                }
            }
            FailedActivationOutcome::NoReport => WFTReportOutcome {
                reported_to_server: false,
                failed: true,
            },
        })
    }

    fn after_workflow_activation(&self, run_id: &str, report_outcome: WFTReportOutcome) {
        self.wft_manager
            .after_wft_report(run_id, report_outcome.reported_to_server);
        if report_outcome.reported_to_server || report_outcome.failed {
            // If we failed the WFT but didn't report anything, we still want to release the WFT
            // permit since the server will eventually time out the task and we've already evicted
            // the run.
            self.return_workflow_task_permit();
        }
        self.wfts_drained_notify.notify_waiters();
    }

    /// Handle server errors from either completing or failing a workflow task. Returns any errors
    /// that can't be automatically handled.
    async fn handle_wft_reporting_errs<T, Fut>(
        &self,
        run_id: &str,
        completer: impl FnOnce() -> Fut,
    ) -> Result<(), CompleteWfError>
    where
        Fut: Future<Output = Result<T, tonic::Status>>,
    {
        let mut should_evict = false;
        let res = match completer().await {
            Err(err) => {
                match err.code() {
                    // Silence unhandled command errors since the lang SDK cannot do anything about
                    // them besides poll again, which it will do anyway.
                    tonic::Code::InvalidArgument if err.message() == "UnhandledCommand" => {
                        warn!(error = %err, run_id, "Unhandled command response when completing");
                        should_evict = true;
                        Ok(())
                    }
                    tonic::Code::NotFound => {
                        warn!(error = %err, run_id, "Task not found when completing");
                        should_evict = true;
                        Ok(())
                    }
                    _ => Err(err),
                }
            }
            _ => Ok(()),
        };
        if should_evict {
            self.request_wf_eviction(run_id, "Error reporting WFT to server");
        }
        res.map_err(Into::into)
    }

    async fn complete_local_act(
        &self,
        la_res: LocalActivityExecutionResult,
        info: LocalInFlightActInfo,
        backoff: Option<prost_types::Duration>,
    ) {
        self.notify_local_result(
            &info.la_info.workflow_exec_info.run_id,
            LocalResolution::LocalActivity(LocalActivityResolution {
                seq: info.la_info.schedule_cmd.seq,
                result: la_res,
                runtime: info.dispatch_time.elapsed(),
                attempt: info.attempt,
                backoff,
                original_schedule_time: Some(info.la_info.schedule_time),
            }),
        )
        .await
    }

    async fn notify_local_result(&self, run_id: &str, res: LocalResolution) {
        if let Err(e) = self.wft_manager.notify_of_local_result(run_id, res).await {
            error!(
                "Problem with local resolution on run {}: {:?} -- will evict the workflow",
                run_id, e
            );
            self.request_wf_eviction(run_id, "Issue while processing local resolution");
        }
    }

    /// Return the sticky execution attributes that should be used to complete workflow tasks
    /// for this worker (if any).
    fn get_sticky_attrs(&self) -> Option<StickyExecutionAttributes> {
        self.sticky_name
            .as_ref()
            .map(|sq| StickyExecutionAttributes {
                worker_task_queue: Some(TaskQueue {
                    name: sq.clone(),
                    kind: TaskQueueKind::Sticky as i32,
                }),
                schedule_to_start_timeout: Some(
                    self.config.sticky_queue_schedule_to_start_timeout.into(),
                ),
            })
    }

    /// Resolves when there are no more outstanding WFTs
    async fn all_wfts_drained(&self) {
        while self.outstanding_workflow_tasks() != 0 {
            self.wfts_drained_notify.notified().await;
        }
    }
}

struct WFTReportOutcome {
    reported_to_server: bool,
    failed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_help::mock_gateway;

    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse;
    use test_utils::fake_sg_opts;

    #[tokio::test]
    async fn activity_timeouts_dont_eat_permits() {
        let mut mock_gateway = mock_gateway();
        mock_gateway
            .expect_poll_activity_task()
            .returning(|_| Ok(PollActivityTaskQueueResponse::default()));
        let gwref = GatewayRef::new(Arc::new(mock_gateway), fake_sg_opts());

        let cfg = WorkerConfigBuilder::default()
            .task_queue("whatever")
            .max_outstanding_activities(5_usize)
            .build()
            .unwrap();
        let worker = Worker::new(cfg, None, Arc::new(gwref), Default::default());
        assert_eq!(worker.activity_poll().await.unwrap(), None);
        assert_eq!(worker.at_task_mgr.unwrap().remaining_activity_capacity(), 5);
    }

    #[tokio::test]
    async fn workflow_timeouts_dont_eat_permits() {
        let mut mock_gateway = mock_gateway();
        mock_gateway
            .expect_poll_workflow_task()
            .returning(|_, _| Ok(PollWorkflowTaskQueueResponse::default()));
        let gwref = GatewayRef::new(Arc::new(mock_gateway), fake_sg_opts());

        let cfg = WorkerConfigBuilder::default()
            .task_queue("whatever")
            .max_outstanding_workflow_tasks(5_usize)
            .build()
            .unwrap();
        let worker = Worker::new(cfg, None, Arc::new(gwref), Default::default());
        assert_eq!(worker.workflow_poll().await.unwrap(), None);
        assert_eq!(worker.workflows_semaphore.available_permits(), 5);
    }

    #[tokio::test]
    async fn activity_errs_dont_eat_permits() {
        let mut mock_gateway = mock_gateway();
        mock_gateway
            .expect_poll_activity_task()
            .returning(|_| Err(tonic::Status::internal("ahhh")));
        let gwref = GatewayRef::new(Arc::new(mock_gateway), fake_sg_opts());

        let cfg = WorkerConfigBuilder::default()
            .task_queue("whatever")
            .max_outstanding_activities(5_usize)
            .build()
            .unwrap();
        let worker = Worker::new(cfg, None, Arc::new(gwref), Default::default());
        assert!(worker.activity_poll().await.is_err());
        assert_eq!(worker.at_task_mgr.unwrap().remaining_activity_capacity(), 5);
    }

    #[tokio::test]
    async fn workflow_errs_dont_eat_permits() {
        let mut mock_gateway = mock_gateway();
        mock_gateway
            .expect_poll_workflow_task()
            .returning(|_, _| Err(tonic::Status::internal("ahhh")));
        let gwref = GatewayRef::new(Arc::new(mock_gateway), fake_sg_opts());

        let cfg = WorkerConfigBuilder::default()
            .task_queue("whatever")
            .max_outstanding_workflow_tasks(5_usize)
            .build()
            .unwrap();
        let worker = Worker::new(cfg, None, Arc::new(gwref), Default::default());
        assert!(worker.workflow_poll().await.is_err());
        assert_eq!(worker.workflows_semaphore.available_permits(), 5);
    }

    #[test]
    fn max_polls_calculated_properly() {
        let cfg = WorkerConfigBuilder::default()
            .task_queue("whatever")
            .build()
            .unwrap();
        assert_eq!(cfg.max_nonsticky_polls(), 1);
        assert_eq!(cfg.max_sticky_polls(), 4);
    }

    #[test]
    fn max_polls_zero_is_err() {
        assert!(WorkerConfigBuilder::default()
            .task_queue("whatever")
            .max_concurrent_wft_polls(0_usize)
            .build()
            .is_err());
    }
}
