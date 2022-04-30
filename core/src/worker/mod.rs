mod activities;
pub(crate) mod client;
mod wft_delivery;

pub use temporal_sdk_core_api::worker::{WorkerConfig, WorkerConfigBuilder};

pub(crate) use activities::{
    ExecutingLAId, LocalActRequest, LocalActivityExecutionResult, LocalActivityResolution,
    NewLocalAct,
};

use crate::{
    abstractions::MeteredSemaphore,
    errors::CompleteWfError,
    pollers::{
        new_activity_task_buffer, new_workflow_task_buffer, BoxedActPoller, BoxedWFPoller, Poller,
        WorkflowTaskPoller,
    },
    protosext::{legacy_query_failure, ValidPollWFTQResponse},
    telemetry::{
        metrics::{
            activity_poller, local_activity_worker_type, workflow_poller, workflow_sticky_poller,
            workflow_worker_type, MetricsContext,
        },
        VecDisplayer,
    },
    worker::{
        activities::{DispatchOrTimeoutLA, LACompleteAction, LocalActivityManager},
        client::WorkerClientBag,
        wft_delivery::WFTSource,
    },
    workflow::{
        workflow_tasks::{
            ActivationAction, FailedActivationOutcome, NewWfTaskOutcome,
            ServerCommandsWithWorkflowInfo, WorkflowTaskManager,
        },
        EmptyWorkflowCommandErr, LocalResolution, WFMachinesError, WorkflowCachingPolicy,
    },
    ActivityHeartbeat, CompleteActivityError, PollActivityError, PollWfError, WorkerTrait,
};
use activities::{LocalInFlightActInfo, WorkerActivityTasks};
use futures::{Future, TryFutureExt};
use std::{convert::TryInto, future, sync::Arc};
use temporal_client::WorkflowTaskCompletion;
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::activity_execution_result,
        activity_task::ActivityTask,
        workflow_activation::{remove_from_cache::EvictionReason, WorkflowActivation},
        workflow_completion::{self, workflow_activation_completion, WorkflowActivationCompletion},
        ActivityTaskCompletion,
    },
    temporal::api::{
        enums::v1::{TaskQueueKind, WorkflowTaskFailedCause},
        failure::v1::Failure,
        taskqueue::v1::{StickyExecutionAttributes, TaskQueue},
        workflowservice::v1::{PollActivityTaskQueueResponse, PollWorkflowTaskQueueResponse},
    },
    TaskToken,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tonic::Code;
use tracing_futures::Instrument;

#[cfg(test)]
use crate::worker::client::WorkerClient;
use crate::workflow::workflow_tasks::EvictionRequestResult;

/// A worker polls on a certain task queue
pub struct Worker {
    config: WorkerConfig,
    wf_client: Arc<WorkerClientBag>,

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
    workflows_semaphore: MeteredSemaphore,
    /// Used to wake blocked workflow task polling when there is some change to workflow activations
    /// that should cause us to restart the loop
    pending_activations_notify: Arc<Notify>,
    /// Watched during shutdown to wait for all WFTs to complete. Should be notified any time
    /// a WFT is completed.
    wfts_drained_notify: Arc<Notify>,
    /// Has shutdown been called?
    shutdown_token: CancellationToken,
    /// Will be called at the end of each activation completion
    post_activate_hook: Option<Box<dyn Fn(&Self) + Send + Sync>>,

    metrics: MetricsContext,
}

#[async_trait::async_trait]
impl WorkerTrait for Worker {
    async fn poll_workflow_activation(&self) -> Result<WorkflowActivation, PollWfError> {
        self.next_workflow_activation().await
    }

    #[instrument(level = "debug", skip(self))]
    async fn poll_activity_task(&self) -> Result<ActivityTask, PollActivityError> {
        loop {
            match self.activity_poll().await.transpose() {
                Some(r) => break r,
                None => {
                    tokio::task::yield_now().await;
                    continue;
                }
            }
        }
    }

    async fn complete_workflow_activation(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<(), CompleteWfError> {
        self.complete_workflow_activation(completion).await
    }

    #[instrument(level = "debug", skip(self, completion),
    fields(completion=%&completion))]
    async fn complete_activity_task(
        &self,
        completion: ActivityTaskCompletion,
    ) -> Result<(), CompleteActivityError> {
        let task_token = TaskToken(completion.task_token);
        let status = if let Some(s) = completion.result.and_then(|r| r.status) {
            s
        } else {
            return Err(CompleteActivityError::MalformedActivityCompletion {
                reason: "Activity completion had empty result/status field".to_owned(),
                completion: None,
            });
        };

        self.complete_activity(task_token, status).await
    }

    fn record_activity_heartbeat(&self, details: ActivityHeartbeat) {
        self.record_heartbeat(details);
    }

    fn request_workflow_eviction(&self, run_id: &str) {
        self.request_wf_eviction(
            run_id,
            "Eviction explicitly requested by lang",
            EvictionReason::LangRequested,
        );
    }

    fn get_config(&self) -> &WorkerConfig {
        &self.config
    }

    /// Begins the shutdown process, tells pollers they should stop. Is idempotent.
    fn initiate_shutdown(&self) {
        self.shutdown_token.cancel();
        // First, we want to stop polling of both activity and workflow tasks
        if let Some(atm) = self.at_task_mgr.as_ref() {
            atm.notify_shutdown();
        }
        self.wf_task_source.stop_pollers();
        info!("Initiated shutdown");
    }

    async fn shutdown(&self) {
        self.shutdown().await
    }

    async fn finalize_shutdown(self) {
        self.shutdown().await;
        self.finalize_shutdown().await
    }
}

impl Worker {
    pub(crate) fn new(
        config: WorkerConfig,
        sticky_queue_name: Option<String>,
        client: Arc<WorkerClientBag>,
        metrics: MetricsContext,
    ) -> Self {
        info!(task_queue = %config.task_queue, "Initializing worker");
        metrics.worker_registered();

        let max_nonsticky_polls = if sticky_queue_name.is_some() {
            config.max_nonsticky_polls()
        } else {
            config.max_concurrent_wft_polls
        };
        let max_sticky_polls = config.max_sticky_polls();
        let wft_metrics = metrics.with_new_attrs([workflow_poller()]);
        let mut wf_task_poll_buffer = new_workflow_task_buffer(
            client.clone(),
            config.task_queue.clone(),
            false,
            max_nonsticky_polls,
            max_nonsticky_polls * 2,
        );
        wf_task_poll_buffer.set_num_pollers_handler(move |np| wft_metrics.record_num_pollers(np));
        let sticky_queue_poller = sticky_queue_name.as_ref().map(|sqn| {
            let sticky_metrics = metrics.with_new_attrs([workflow_sticky_poller()]);
            let mut sp = new_workflow_task_buffer(
                client.clone(),
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
                client.clone(),
                config.task_queue.clone(),
                config.max_concurrent_at_polls,
                config.max_concurrent_at_polls * 2,
                config.max_task_queue_activities_per_second,
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
            client,
            wf_task_poll_buffer,
            act_poll_buffer,
            metrics,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_test(config: WorkerConfig, client: impl WorkerClient + 'static) -> Self {
        Self::new(config, None, Arc::new(client.into()), Default::default())
    }

    /// Returns number of currently cached workflows
    pub fn cached_workflows(&self) -> usize {
        self.wft_manager.cached_workflows()
    }

    pub(crate) fn new_with_pollers(
        config: WorkerConfig,
        sticky_queue_name: Option<String>,
        client: Arc<WorkerClientBag>,
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
        Self {
            wf_client: client.clone(),
            sticky_name: sticky_queue_name,
            wf_task_source: WFTSource::new(wft_poller),
            wft_manager: WorkflowTaskManager::new(pa_notif.clone(), cache_policy, metrics.clone()),
            at_task_mgr: act_poller.map(|ap| {
                WorkerActivityTasks::new(
                    config.max_outstanding_activities,
                    ap,
                    client.clone(),
                    metrics.clone(),
                    config.max_heartbeat_throttle_interval,
                    config.default_heartbeat_throttle_interval,
                )
            }),
            local_act_mgr: LocalActivityManager::new(
                config.max_outstanding_local_activities,
                config.namespace.clone(),
                metrics.with_new_attrs([local_activity_worker_type()]),
            ),
            workflows_semaphore: MeteredSemaphore::new(
                config.max_outstanding_workflow_tasks,
                metrics.with_new_attrs([workflow_worker_type()]),
                MetricsContext::available_task_slots,
            ),
            config,
            shutdown_token: CancellationToken::new(),
            post_activate_hook: None,
            pending_activations_notify: pa_notif,
            wfts_drained_notify,
            metrics,
        }
    }

    /// Will shutdown the worker. Does not resolve until all outstanding workflow tasks have been
    /// completed
    pub(crate) async fn shutdown(&self) {
        self.initiate_shutdown();
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
        self.workflows_semaphore.sem.available_permits()
    }

    /// Get new activity tasks (may be local or nonlocal). Local activities are returned first
    /// before polling the server if there are any.
    ///
    /// Returns `Ok(None)` in the event of a poll timeout or if the polling loop should otherwise
    /// be restarted
    async fn activity_poll(&self) -> Result<Option<ActivityTask>, PollActivityError> {
        let act_mgr_poll = async {
            if let Some(ref act_mgr) = self.at_task_mgr {
                act_mgr.poll().await
            } else {
                self.shutdown_token.cancelled().await;
                Err(PollActivityError::ShutDown)
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
                    None => {
                        if self.shutdown_token.is_cancelled() {
                            return Err(PollActivityError::ShutDown);
                        }
                        Ok(None)
                    }
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
            atm.complete(task_token, status, &**self.wf_client).await
        } else {
            error!(
                "Tried to complete activity {} on a worker that does not have an activity manager",
                task_token
            );
            Ok(())
        }
    }

    #[instrument(level = "debug", skip(self), fields(run_id))]
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

            if self.config.max_cached_workflows > 0 {
                if let Some(cache_cap_fut) = self.wft_manager.wait_for_cache_capacity() {
                    tokio::select! {
                        biased;
                        // We must loop up if there's a new pending activation, since those are for
                        // already-cached workflows and may include evictions which will change if
                        // we are still waiting or not.
                        _ = self.pending_activations_notify.notified() => {
                            continue
                        },
                        _ = cache_cap_fut => {}
                    };
                }
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

    #[instrument(level = "debug", skip(self, completion),
    fields(completion=%&completion, run_id=%completion.run_id))]
    pub(crate) async fn complete_workflow_activation(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<(), CompleteWfError> {
        info!("Completing WF act");
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
                    EvictionReason::LangFail,
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

        info!("Completed WF act");
        self.wft_manager
            .after_wft_report(&completion.run_id, report_outcome.reported_to_server);
        info!("WF act after report");
        if report_outcome.reported_to_server || report_outcome.failed {
            // If we failed the WFT but didn't report anything, we still want to release the WFT
            // permit since the server will eventually time out the task and we've already evicted
            // the run.
            self.return_workflow_task_permit();
        }
        self.wfts_drained_notify.notify_waiters();

        if let Some(h) = &self.post_activate_hook {
            h(self);
        }
        info!("WF act done");

        Ok(())
    }

    /// Tell the worker a workflow task has completed, for tracking max outstanding WFTs
    pub(crate) fn return_workflow_task_permit(&self) {
        self.workflows_semaphore.add_permit();
    }

    /// Request a workflow eviction. Returns true if we actually queued up a new eviction request.
    pub(crate) fn request_wf_eviction(
        &self,
        run_id: &str,
        message: impl Into<String>,
        reason: EvictionReason,
    ) -> bool {
        match self.wft_manager.request_eviction(run_id, message, reason) {
            EvictionRequestResult::EvictionRequested(_) => true,
            EvictionRequestResult::NotFound => false,
            EvictionRequestResult::EvictionAlreadyRequested(_) => false,
        }
    }

    /// Sets a function to be called at the end of each activation completion
    pub(crate) fn set_post_activate_hook(
        &mut self,
        callback: impl Fn(&Self) + Send + Sync + 'static,
    ) {
        self.post_activate_hook = Some(Box::new(callback))
    }

    /// Used for replay workers - causes the worker to shutdown when the given run reaches the
    /// given event number
    pub(crate) fn set_shutdown_on_run_reaches_event(&mut self, run_id: String, last_event: i64) {
        self.set_post_activate_hook(move |worker| {
            if worker
                .wft_manager
                .most_recently_processed_event(&run_id)
                .unwrap_or_default()
                >= last_event
            {
                worker.initiate_shutdown();
            }
        });
    }

    /// Resolves with WFT poll response or `PollWfError::ShutDown` if WFTs have been drained
    async fn workflow_poll_or_wfts_drained(
        &self,
    ) -> Result<Option<ValidPollWFTQResponse>, PollWfError> {
        let mut shutdown_seen = false;
        loop {
            // If we've already seen shutdown once it's important we don't freak out and
            // restart the loop constantly while waiting for poll to finish shutting down.
            let shutdown_restarter = async {
                if shutdown_seen {
                    future::pending::<()>().await;
                } else {
                    self.shutdown_token.cancelled().await;
                };
            };
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
                _ = shutdown_restarter => {
                    shutdown_seen = true;
                },
            }
        }
    }

    /// Wait until not at the outstanding workflow task limit, and then poll this worker's task
    /// queue for new workflow tasks.
    ///
    /// Returns `Ok(None)` in the event of a poll timeout, if there was some gRPC error that
    /// callers can't do anything about, or any other reason to restart the poll loop.
    async fn workflow_poll(&self) -> Result<Option<ValidPollWFTQResponse>, PollWfError> {
        // We can't say we're shut down if there are outstanding LAs, as they could end up WFT
        // heartbeating which is a "new" workflow task that we need to accept and process as long as
        // the LA is outstanding. Similarly, if we already have such tasks (from a WFT completion),
        // then we must fetch them from the source before we can say workflow polling is shutdown.
        if self.shutdown_token.is_cancelled()
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

        let work = if self.config.max_cached_workflows > 0 {
            // Add the workflow to cache management. We do not even attempt insert if cache
            // size is zero because we do not want to generate eviction requests for
            // workflows which may immediately generate pending activations.
            if let Some(ready_to_work) = self.wft_manager.add_new_run_to_cache(work).await {
                ready_to_work
            } else {
                return Ok(None);
            }
        } else {
            work
        };

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
        let res = self
            .wft_manager
            .apply_new_poll_resp(work, self.wf_client.clone())
            .await;
        Ok(match res {
            NewWfTaskOutcome::IssueActivation(a) => {
                debug!(activation=%a, "Sending activation to lang");
                Some(a)
            }
            NewWfTaskOutcome::TaskBuffered => {
                // Though the task is not outstanding in the lang sense, it is outstanding from the
                // server perspective. We used to return a permit here, but that doesn't actually
                // make much sense.
                None
            }
            NewWfTaskOutcome::Autocomplete | NewWfTaskOutcome::LocalActsOutstanding => {
                debug!(workflow_execution=?we,
                       "No new work for lang to perform after polling server");
                self.complete_workflow_activation(WorkflowActivationCompletion {
                    run_id: we.run_id,
                    status: Some(workflow_completion::Success::from_variants(vec![]).into()),
                })
                .await?;
                None
            }
            NewWfTaskOutcome::Evict(e) => {
                warn!(error=?e, run_id=%we.run_id, "Error while applying poll response to workflow");
                let did_issue_eviction = self.request_wf_eviction(
                    &we.run_id,
                    format!("Error while applying poll response to workflow: {:?}", e),
                    e.evict_reason(),
                );
                // If we didn't actually need to issue an eviction, then return the WFT permit.
                // EX: The workflow we tried to evict wasn't in the cache.
                if !did_issue_eviction {
                    self.return_workflow_task_permit();
                }
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
                debug!("Sending commands to server: {}", commands.display());
                if !query_responses.is_empty() {
                    debug!(
                        "Sending query responses to server: {}",
                        query_responses.display()
                    );
                }
                let mut completion = WorkflowTaskCompletion {
                    task_token,
                    commands,
                    query_responses,
                    sticky_attributes: None,
                    return_new_workflow_task: true,
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
                        .wf_client
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
                self.wf_client
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
                    update_err.evict_reason(),
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
        reason: EvictionReason,
        failure: workflow_completion::Failure,
    ) -> Result<WFTReportOutcome, CompleteWfError> {
        Ok(
            match self.wft_manager.failed_activation(
                run_id,
                reason,
                format!("Workflow activation completion failed: {:?}", failure),
            ) {
                FailedActivationOutcome::Report(tt) => {
                    warn!(run_id, failure=?failure, "Failing workflow activation");
                    self.handle_wft_reporting_errs(run_id, || async {
                        self.wf_client
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
                    self.wf_client
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
            },
        )
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
        let mut should_evict = None;
        let res = match completer().await {
            Err(err) => {
                match err.code() {
                    // Silence unhandled command errors since the lang SDK cannot do anything about
                    // them besides poll again, which it will do anyway.
                    tonic::Code::InvalidArgument if err.message() == "UnhandledCommand" => {
                        debug!(error = %err, run_id, "Unhandled command response when completing");
                        should_evict = Some(EvictionReason::UnhandledCommand);
                        Ok(())
                    }
                    tonic::Code::NotFound => {
                        warn!(error = %err, run_id, "Task not found when completing");
                        should_evict = Some(EvictionReason::TaskNotFound);
                        Ok(())
                    }
                    _ => Err(err),
                }
            }
            _ => Ok(()),
        };
        if let Some(reason) = should_evict {
            self.request_wf_eviction(run_id, "Error reporting WFT to server", reason);
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
            self.request_wf_eviction(
                run_id,
                "Issue while processing local resolution",
                e.evict_reason(),
            );
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

#[derive(Debug, Copy, Clone)]
struct WFTReportOutcome {
    reported_to_server: bool,
    failed: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{test_help::test_worker_cfg, worker::client::mocks::mock_workflow_client};
    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse;

    #[tokio::test]
    async fn activity_timeouts_dont_eat_permits() {
        let mut mock_client = mock_workflow_client();
        mock_client
            .expect_poll_activity_task()
            .returning(|_, _| Ok(PollActivityTaskQueueResponse::default()));

        let cfg = test_worker_cfg()
            .max_outstanding_activities(5_usize)
            .build()
            .unwrap();
        let worker = Worker::new_test(cfg, mock_client);
        assert_eq!(worker.activity_poll().await.unwrap(), None);
        assert_eq!(worker.at_task_mgr.unwrap().remaining_activity_capacity(), 5);
    }

    #[tokio::test]
    async fn workflow_timeouts_dont_eat_permits() {
        let mut mock_client = mock_workflow_client();
        mock_client
            .expect_poll_workflow_task()
            .returning(|_, _| Ok(PollWorkflowTaskQueueResponse::default()));

        let cfg = test_worker_cfg()
            .max_outstanding_workflow_tasks(5_usize)
            .max_cached_workflows(5_usize)
            .build()
            .unwrap();
        let worker = Worker::new_test(cfg, mock_client);
        assert_eq!(worker.workflow_poll().await.unwrap(), None);
        assert_eq!(worker.workflows_semaphore.sem.available_permits(), 5);
    }

    #[tokio::test]
    async fn activity_errs_dont_eat_permits() {
        let mut mock_client = mock_workflow_client();
        mock_client
            .expect_poll_activity_task()
            .returning(|_, _| Err(tonic::Status::internal("ahhh")));

        let cfg = test_worker_cfg()
            .max_outstanding_activities(5_usize)
            .build()
            .unwrap();
        let worker = Worker::new_test(cfg, mock_client);
        assert!(worker.activity_poll().await.is_err());
        assert_eq!(worker.at_task_mgr.unwrap().remaining_activity_capacity(), 5);
    }

    #[tokio::test]
    async fn workflow_errs_dont_eat_permits() {
        let mut mock_client = mock_workflow_client();
        mock_client
            .expect_poll_workflow_task()
            .returning(|_, _| Err(tonic::Status::internal("ahhh")));

        let cfg = test_worker_cfg()
            .max_outstanding_workflow_tasks(5_usize)
            .max_cached_workflows(5_usize)
            .build()
            .unwrap();
        let worker = Worker::new_test(cfg, mock_client);
        assert!(worker.workflow_poll().await.is_err());
        assert_eq!(worker.workflows_semaphore.sem.available_permits(), 5);
    }

    #[test]
    fn max_polls_calculated_properly() {
        let cfg = test_worker_cfg().build().unwrap();
        assert_eq!(cfg.max_nonsticky_polls(), 1);
        assert_eq!(cfg.max_sticky_polls(), 4);
    }

    #[test]
    fn max_polls_zero_is_err() {
        assert!(test_worker_cfg()
            .max_concurrent_wft_polls(0_usize)
            .build()
            .is_err());
    }
}
