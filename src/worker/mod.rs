mod activities;
mod config;
mod dispatcher;

pub use config::{WorkerConfig, WorkerConfigBuilder};

pub(crate) use activities::NewLocalAct;
pub(crate) use dispatcher::WorkerDispatcher;

use crate::{
    errors::CompleteWfError,
    machines::{EmptyWorkflowCommandErr, WFMachinesError},
    pollers::{
        new_activity_task_buffer, new_workflow_task_buffer, BoxedActPoller, BoxedWFPoller,
        GatewayRef, Poller, WorkflowTaskPoller,
    },
    protosext::{legacy_query_failure, ValidPollWFTQResponse, WorkflowTaskCompletion},
    task_token::TaskToken,
    telemetry::metrics::{
        activity_poller, workflow_poller, workflow_sticky_poller, MetricsContext,
    },
    worker::activities::LocalActivityManager,
    workflow::{
        workflow_tasks::{
            ActivationAction, FailedActivationOutcome, NewWfTaskOutcome,
            ServerCommandsWithWorkflowInfo, WorkflowTaskManager,
        },
        LocalResolution, WorkflowCachingPolicy,
    },
    ActivityHeartbeat, CompleteActivityError, PollActivityError, PollWfError,
};
use activities::WorkerActivityTasks;
use futures::{Future, TryFutureExt};
use std::{convert::TryInto, sync::Arc};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{activity_result, ActivityResult},
        activity_task::ActivityTask,
        workflow_activation::WfActivation,
        workflow_completion::{self, wf_activation_completion, WfActivationCompletion},
    },
    temporal::api::{
        enums::v1::{TaskQueueKind, WorkflowTaskFailedCause},
        failure::v1::Failure,
        taskqueue::v1::{StickyExecutionAttributes, TaskQueue},
        workflowservice::v1::{PollActivityTaskQueueResponse, PollWorkflowTaskQueueResponse},
    },
};
use tokio::sync::{watch, Mutex, Semaphore};
use tonic::Code;
use tracing_futures::Instrument;

/// A worker polls on a certain task queue
pub struct Worker {
    config: WorkerConfig,
    server_gateway: Arc<GatewayRef>,

    /// Will be populated when this worker should poll on a sticky WFT queue
    sticky_name: Option<String>,

    // TODO: Worth moving inside wf task mgr too?
    /// Buffers workflow task polling in the event we need to return a pending activation while
    /// a poll is ongoing. Sticky and nonsticky polling happens inside of it.
    wf_task_poll_buffer: BoxedWFPoller,
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
    pending_activations_notification_receiver: Mutex<watch::Receiver<bool>>,
    /// Watched during shutdown to wait for all WFTs to complete
    wfts_drained: watch::Receiver<bool>,
    /// notifies when all WFTs have been drained after shutdown
    wfts_drained_sender: watch::Sender<bool>,
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
        let (pan_tx, pan_rx) = watch::channel(true);
        let (wftd_tx, wftd_rx) = watch::channel(false);
        let (shut_tx, shut_rx) = watch::channel(false);
        Self {
            server_gateway: sg.clone(),
            sticky_name: sticky_queue_name,
            wf_task_poll_buffer: wft_poller,
            wft_manager: WorkflowTaskManager::new(pan_tx, cache_policy, metrics.clone()),
            at_task_mgr: act_poller.map(|ap| {
                WorkerActivityTasks::new(
                    config.max_outstanding_activities,
                    ap,
                    sg.gw.clone(),
                    metrics.clone(),
                )
            }),
            local_act_mgr: LocalActivityManager::new(config.max_outstanding_local_activities),
            workflows_semaphore: Semaphore::new(config.max_outstanding_workflow_tasks),
            config,
            shutdown_requested: shut_rx,
            shutdown_sender: shut_tx,
            wfts_drained: wftd_rx,
            wfts_drained_sender: wftd_tx,
            pending_activations_notification_receiver: Mutex::new(pan_rx),
            metrics,
        }
    }

    /// Will shutdown the worker. Does not resolve until all outstanding workflow tasks have been
    /// completed
    pub(crate) async fn shutdown(&self) {
        let _ = self.shutdown_sender.send(true);
        if let Some(atm) = self.at_task_mgr.as_ref() {
            atm.notify_shutdown();
        }
        self.wf_task_poll_buffer.notify_shutdown();
        // Notify in case shutdown was requested while there were no more outstanding WFTs.
        // This is required because the only other place where we notify wfts_drained is on
        // activation completion and activation polling checks for wfts_drained.
        self.maybe_notify_wtfs_drained();
        // wait until all outstanding workflow tasks have been completed before shutting down
        if !*self.wfts_drained.borrow() {
            self.wfts_drained
                .clone()
                .changed()
                .await
                .expect("wfts_drained should not be dropped");
        }
    }

    /// Finish shutting down by consuming the background pollers and freeing all resources
    pub(crate) async fn finalize_shutdown(self) {
        self.wf_task_poll_buffer.shutdown_box().await;
        if let Some(b) = self.at_task_mgr {
            b.shutdown().await;
        }
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
        if let Some(ref act_mgr) = self.at_task_mgr {
            tokio::select! {
                biased;

                r = self.local_act_mgr.next_pending() => Ok(r),
                r = act_mgr.poll() => r,
                _ = self.shutdown_notifier() => {
                    Err(PollActivityError::ShutDown)
                }
            }
        } else {
            tokio::select! {
                biased;

                r = self.local_act_mgr.next_pending() => Ok(r),
                _ = self.shutdown_notifier() => {
                    Err(PollActivityError::ShutDown)
                }
            }
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
        status: activity_result::Status,
    ) -> Result<(), CompleteActivityError> {
        if task_token.is_local_activity_task() {
            if let Some(la_info) = self.local_act_mgr.complete(&task_token).await {
                if let Err(e) = self
                    .wft_manager
                    .notify_of_local_result(
                        &la_info.workflow_execution.run_id,
                        la_info.seq,
                        LocalResolution::LocalActivity(ActivityResult {
                            status: Some(status),
                        }),
                    )
                    .await
                {
                    error!(
                        "Problem completing local activity {}: {:?} -- will evict the workflow",
                        task_token, e
                    );
                    self.request_wf_eviction(
                        &la_info.workflow_execution.run_id,
                        "Issue while completing local activity",
                    );
                }
            } else {
                error!("Tried to complete untracked local activity {}", task_token);
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

    pub(crate) async fn next_workflow_activation(&self) -> Result<WfActivation, PollWfError> {
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
            let mut pending_activations_notification =
                self.pending_activations_notification_receiver.lock().await;

            let selected_f = tokio::select! {
                biased;

                // If an activation is completed while we are waiting on polling, we need to restart
                // the loop right away to provide any potential new pending activation.
                // Continue here means that we unnecessarily add another permit to the poll buffer,
                // this will go away when polling is done in the background.
                _ = pending_activations_notification.changed() => continue,
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
        completion: WfActivationCompletion,
    ) -> Result<(), CompleteWfError> {
        let wfstatus = completion.status;
        let did_complete_wft = match wfstatus {
            Some(wf_activation_completion::Status::Successful(success)) => {
                self.wf_activation_success(&completion.run_id, success)
                    .await
            }
            Some(wf_activation_completion::Status::Failed(failure)) => {
                self.wf_activation_failed(&completion.run_id, failure).await
            }
            None => Err(CompleteWfError::MalformedWorkflowCompletion {
                reason: "Workflow completion had empty status field".to_owned(),
                completion: None,
            }),
        }?;
        self.after_workflow_activation(&completion.run_id, did_complete_wft);
        Ok(())
    }

    fn maybe_notify_wtfs_drained(&self) {
        if *self.shutdown_requested.borrow() && self.outstanding_workflow_tasks() == 0 {
            self.wfts_drained_sender
                .send(true)
                .expect("wfts_drained sender shouldn't be dropped");
        }
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
        loop {
            if *self.wfts_drained.borrow() {
                debug!("Returning shutdown error");
                return Err(PollWfError::ShutDown);
            } else if *self.shutdown_requested.borrow() {
                self.wfts_drained
                    .clone()
                    .changed()
                    .await
                    .expect("wfts_drained should not be dropped");
            } else {
                let mut shutdown_requested = self.shutdown_requested.clone();
                tokio::select! {
                    biased;

                    r = self.workflow_poll()
                        .map_err(Into::into) => match r {
                         Err(PollWfError::ShutDown) => {},
                        _ => return r,
                    },
                    _ = shutdown_requested.changed() => {},
                }
            };
        }
    }

    /// Wait until not at the outstanding workflow task limit, and then poll this worker's task
    /// queue for new workflow tasks.
    ///
    /// Returns `Ok(None)` in the event of a poll timeout, or if there was some gRPC error that
    /// callers can't do anything about.
    async fn workflow_poll(&self) -> Result<Option<ValidPollWFTQResponse>, PollWfError> {
        let sem = self
            .workflows_semaphore
            .acquire()
            .await
            .expect("outstanding workflow tasks semaphore not dropped");

        let res = self
            .wf_task_poll_buffer
            .poll()
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
    ) -> Result<Option<WfActivation>, PollWfError> {
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
            NewWfTaskOutcome::Autocomplete => {
                debug!(workflow_execution=?we,
                       "No work for lang to perform after polling server. Sending autocomplete.");
                self.complete_workflow_activation(WfActivationCompletion {
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
            NewWfTaskOutcome::DoNothing => None,
        })
    }

    /// Handle a successful workflow activation
    ///
    /// Returns true if we actually reported WFT completion to server (success or failure)
    async fn wf_activation_success(
        &self,
        run_id: &str,
        success: workflow_completion::Success,
    ) -> Result<bool, CompleteWfError> {
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

        match self.wft_manager.successful_activation(run_id, cmds).await {
            Ok(Some(ServerCommandsWithWorkflowInfo {
                task_token,
                action:
                    ActivationAction::WftComplete {
                        commands,
                        query_responses,
                        local_activities,
                        force_new_wft,
                    },
            })) => {
                self.local_act_mgr.enqueue(local_activities);

                debug!("Sending commands to server: {:?}", &commands);
                if !query_responses.is_empty() {
                    debug!("Sending query responses to server: {:?}", &query_responses);
                }
                let mut completion = WorkflowTaskCompletion {
                    task_token,
                    commands,
                    query_responses,
                    sticky_attributes: None,
                    return_new_workflow_task: false,
                    force_create_new_workflow_task: force_new_wft,
                };
                let sticky_attrs = self.get_sticky_attrs();
                completion.sticky_attributes = sticky_attrs;
                self.handle_wft_reporting_errs(run_id, || async {
                    self.server_gateway
                        .complete_workflow_task(completion)
                        .instrument(span!(tracing::Level::DEBUG, "Complete WFT call"))
                        .await
                })
                .await?;
                Ok(true)
            }
            Ok(Some(ServerCommandsWithWorkflowInfo {
                action: ActivationAction::LocalActivitiesDelayWft { local_activities },
                ..
            })) => {
                self.local_act_mgr.enqueue(local_activities);
                Ok(false)
            }
            Ok(Some(ServerCommandsWithWorkflowInfo {
                task_token,
                action: ActivationAction::RespondLegacyQuery { result },
                ..
            })) => {
                self.server_gateway
                    .respond_legacy_query(task_token, result)
                    .await?;
                Ok(true)
            }
            Ok(None) => Ok(false),
            Err(update_err) => {
                // Automatically fail the workflow task in the event we couldn't update machines
                let fail_cause = if matches!(&update_err.source, WFMachinesError::Nondeterminism(_))
                {
                    WorkflowTaskFailedCause::NonDeterministicError
                } else {
                    WorkflowTaskFailedCause::Unspecified
                };

                warn!(run_id, error=?update_err, "Failing workflow task");

                if let Some(ref tt) = update_err.task_token {
                    let wft_fail_str = format!("{:?}", update_err);
                    self.handle_wft_reporting_errs(run_id, || async {
                        self.server_gateway
                            .fail_workflow_task(
                                tt.clone(),
                                fail_cause,
                                Some(Failure::application_failure(wft_fail_str.clone(), false)),
                            )
                            .await
                    })
                    .await?;
                    // We must evict the workflow since we've failed a WFT
                    self.request_wf_eviction(
                        run_id,
                        format!("Workflow task failure: {}", wft_fail_str),
                    );
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
        }
    }

    /// Handle a failed workflow completion
    ///
    /// Returns true if we actually reported WFT completion to server
    async fn wf_activation_failed(
        &self,
        run_id: &str,
        failure: workflow_completion::Failure,
    ) -> Result<bool, CompleteWfError> {
        Ok(match self.wft_manager.failed_activation(run_id) {
            FailedActivationOutcome::Report(tt) => {
                self.handle_wft_reporting_errs(run_id, || async {
                    self.server_gateway
                        .fail_workflow_task(
                            tt,
                            WorkflowTaskFailedCause::Unspecified,
                            failure.failure.map(Into::into),
                        )
                        .await
                })
                .await?;
                true
            }
            FailedActivationOutcome::ReportLegacyQueryFailure(task_token) => {
                self.server_gateway
                    .respond_legacy_query(task_token, legacy_query_failure(failure))
                    .await?;
                true
            }
            FailedActivationOutcome::NoReport => false,
        })
    }

    fn after_workflow_activation(&self, run_id: &str, did_complete_wft: bool) {
        self.wft_manager.after_wft_report(run_id, did_complete_wft);
        if did_complete_wft {
            self.return_workflow_task_permit();
        }
        self.maybe_notify_wtfs_drained();
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

    /// A future that resolves to true the shutdown flag has been set to true, false is simply
    /// a signal that a poll loop should be restarted. Only meant to be called from polling funcs.
    async fn shutdown_notifier(&self) {
        if *self.shutdown_requested.borrow() {
            return;
        }
        let _ = self.shutdown_requested.clone().changed().await;
    }
}

impl WorkerConfig {
    fn max_nonsticky_polls(&self) -> usize {
        ((self.max_concurrent_wft_polls as f32 * self.nonsticky_to_sticky_poll_ratio) as usize)
            .max(1)
    }
    fn max_sticky_polls(&self) -> usize {
        self.max_concurrent_wft_polls
            .saturating_sub(self.max_nonsticky_polls())
            .max(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{pollers::MockServerGatewayApis, test_help::fake_sg_opts};
    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse;

    #[tokio::test]
    async fn activity_timeouts_dont_eat_permits() {
        let mut mock_gateway = MockServerGatewayApis::new();
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
        let mut mock_gateway = MockServerGatewayApis::new();
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
        let mut mock_gateway = MockServerGatewayApis::new();
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
        let mut mock_gateway = MockServerGatewayApis::new();
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
