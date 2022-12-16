mod activities;
pub(crate) mod client;
mod workflow;

pub use temporal_sdk_core_api::worker::{WorkerConfig, WorkerConfigBuilder};

pub(crate) use activities::{
    ExecutingLAId, LocalActRequest, LocalActivityExecutionResult, LocalActivityResolution,
    NewLocalAct,
};
#[cfg(test)]
pub(crate) use workflow::ManagedWFFunc;
pub(crate) use workflow::{wft_poller::new_wft_poller, LEGACY_QUERY_ID};

use crate::{
    errors::CompleteWfError,
    pollers::{
        new_activity_task_buffer, new_workflow_task_buffer, BoxedActPoller, Poller,
        WorkflowTaskPoller,
    },
    protosext::{validate_activity_completion, ValidPollWFTQResponse},
    telemetry::metrics::{
        activity_poller, local_activity_worker_type, workflow_poller, workflow_sticky_poller,
        MetricsContext,
    },
    worker::{
        activities::{DispatchOrTimeoutLA, LACompleteAction, LocalActivityManager},
        client::WorkerClient,
        workflow::{LocalResolution, WorkflowBasics, Workflows},
    },
    ActivityHeartbeat, CompleteActivityError, PollActivityError, PollWfError, WorkerTrait,
};
use activities::{LocalInFlightActInfo, WorkerActivityTasks};
use futures::Stream;
use std::{convert::TryInto, future, sync::Arc};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::activity_execution_result,
        activity_task::ActivityTask,
        workflow_activation::{remove_from_cache::EvictionReason, WorkflowActivation},
        workflow_completion::WorkflowActivationCompletion,
        ActivityTaskCompletion,
    },
    temporal::api::{
        enums::v1::TaskQueueKind,
        taskqueue::v1::{StickyExecutionAttributes, TaskQueue},
        workflowservice::v1::PollActivityTaskQueueResponse,
    },
    TaskToken,
};
use tokio_util::sync::CancellationToken;

/// A worker polls on a certain task queue
pub struct Worker {
    config: WorkerConfig,
    wf_client: Arc<dyn WorkerClient>,

    /// Manages all workflows and WFT processing
    workflows: Workflows,
    /// Manages activity tasks for this worker/task queue
    at_task_mgr: Option<WorkerActivityTasks>,
    /// Manages local activities
    local_act_mgr: Arc<LocalActivityManager>,
    /// Has shutdown been called?
    shutdown_token: CancellationToken,
    /// Will be called at the end of each activation completion
    #[allow(clippy::type_complexity)] // Sorry clippy, there's no simple way to re-use here.
    post_activate_hook: Option<Box<dyn Fn(&Self, &str, usize) + Send + Sync>>,
}

#[async_trait::async_trait]
impl WorkerTrait for Worker {
    async fn poll_workflow_activation(&self) -> Result<WorkflowActivation, PollWfError> {
        self.next_workflow_activation().await
    }

    #[instrument(skip(self))]
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
        info!(
            task_queue=%self.config.task_queue,
            namespace=%self.config.namespace,
            "Initiated shutdown",
        );
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
        client: Arc<dyn WorkerClient>,
        metrics: MetricsContext,
    ) -> Self {
        info!(task_queue=%config.task_queue,
              namespace=%config.namespace,
              "Initializing worker");
        metrics.worker_registered();

        let shutdown_token = CancellationToken::new();
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
            shutdown_token.child_token(),
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
                shutdown_token.child_token(),
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
                shutdown_token.child_token(),
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
        let wft_stream = new_wft_poller(wf_task_poll_buffer, metrics.clone());
        Self::new_with_pollers(
            config,
            sticky_queue_name,
            client,
            wft_stream,
            act_poll_buffer,
            metrics,
            shutdown_token,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_test(config: WorkerConfig, client: impl WorkerClient + 'static) -> Self {
        Self::new(config, None, Arc::new(client), MetricsContext::no_op())
    }

    pub(crate) fn new_with_pollers(
        config: WorkerConfig,
        sticky_queue_name: Option<String>,
        client: Arc<dyn WorkerClient>,
        wft_stream: impl Stream<Item = Result<ValidPollWFTQResponse, tonic::Status>> + Send + 'static,
        act_poller: Option<BoxedActPoller>,
        metrics: MetricsContext,
        shutdown_token: CancellationToken,
    ) -> Self {
        let local_act_mgr = Arc::new(LocalActivityManager::new(
            config.max_outstanding_local_activities,
            config.namespace.clone(),
            metrics.with_new_attrs([local_activity_worker_type()]),
        ));
        let lam_clone = local_act_mgr.clone();
        let local_act_req_sink = move |requests| lam_clone.enqueue(requests);
        let at_task_mgr = act_poller.map(|ap| {
            WorkerActivityTasks::new(
                config.max_outstanding_activities,
                config.max_worker_activities_per_second,
                ap,
                client.clone(),
                metrics.clone(),
                config.max_heartbeat_throttle_interval,
                config.default_heartbeat_throttle_interval,
            )
        });
        if at_task_mgr.is_none() {
            info!("Activity polling is disabled for this worker");
        }
        Self {
            wf_client: client.clone(),
            workflows: Workflows::new(
                WorkflowBasics {
                    max_cached_workflows: config.max_cached_workflows,
                    max_outstanding_wfts: config.max_outstanding_workflow_tasks,
                    shutdown_token: shutdown_token.child_token(),
                    metrics,
                    namespace: config.namespace.clone(),
                    task_queue: config.task_queue.clone(),
                    ignore_evicts_on_shutdown: config.ignore_evicts_on_shutdown,
                },
                sticky_queue_name.map(|sq| StickyExecutionAttributes {
                    worker_task_queue: Some(TaskQueue {
                        name: sq,
                        kind: TaskQueueKind::Sticky as i32,
                    }),
                    schedule_to_start_timeout: Some(
                        config
                            .sticky_queue_schedule_to_start_timeout
                            .try_into()
                            .expect("timeout fits into proto"),
                    ),
                }),
                client,
                wft_stream,
                local_act_req_sink,
                at_task_mgr
                    .as_ref()
                    .map(|mgr| mgr.get_handle_for_workflows()),
            ),
            at_task_mgr,
            local_act_mgr,
            config,
            shutdown_token,
            post_activate_hook: None,
        }
    }

    /// Will shutdown the worker. Does not resolve until all outstanding workflow tasks have been
    /// completed
    async fn shutdown(&self) {
        self.initiate_shutdown();
        // Next we need to wait for all local activities to finish so no more workflow task
        // heartbeats will be generated
        self.local_act_mgr
            .wait_all_outstanding_tasks_finished()
            .await;
        // Wait for workflows to finish
        self.workflows
            .shutdown()
            .await
            .expect("Workflow processing terminates cleanly");
        // Wait for activities to finish
        if let Some(acts) = self.at_task_mgr.as_ref() {
            acts.wait_all_finished().await;
        }
    }

    /// Finish shutting down by consuming the background pollers and freeing all resources
    async fn finalize_shutdown(self) {
        if let Some(b) = self.at_task_mgr {
            b.shutdown().await;
        }
    }

    pub(crate) fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    /// Returns number of currently cached workflows
    pub async fn cached_workflows(&self) -> usize {
        self.workflows
            .get_state_info()
            .await
            .map(|r| r.cached_workflows)
            .unwrap_or_default()
    }

    /// Returns number of currently outstanding workflow tasks
    #[cfg(test)]
    pub(crate) async fn outstanding_workflow_tasks(&self) -> usize {
        self.workflows
            .get_state_info()
            .await
            .map(|r| r.outstanding_wft)
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) async fn available_wft_permits(&self) -> usize {
        self.workflows
            .get_state_info()
            .await
            .expect("You can only check for available permits before shutdown")
            .available_wft_permits
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
                // We expect the local activity branch below to produce shutdown when appropriate if
                // there are no activity pollers.
                future::pending::<()>().await;
                unreachable!()
            }
        };

        tokio::select! {
            biased;

            r = self.local_act_mgr.next_pending() => {
                match r {
                    Some(DispatchOrTimeoutLA::Dispatch(r)) => Ok(Some(r)),
                    Some(DispatchOrTimeoutLA::Timeout { run_id, resolution, task }) => {
                        self.notify_local_result(
                            &run_id, LocalResolution::LocalActivity(resolution));
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

    #[instrument(skip(self, task_token, status),
                 fields(task_token=%&task_token, status=%&status,
                        task_queue=%self.config.task_queue, workflow_id, run_id))]
    pub(crate) async fn complete_activity(
        &self,
        task_token: TaskToken,
        status: activity_execution_result::Status,
    ) -> Result<(), CompleteActivityError> {
        validate_activity_completion(&status)?;
        if task_token.is_local_activity_task() {
            let as_la_res: LocalActivityExecutionResult = status.try_into()?;
            match self.local_act_mgr.complete(&task_token, &as_la_res) {
                LACompleteAction::Report(info) => self.complete_local_act(as_la_res, info, None),
                LACompleteAction::LangDoesTimerBackoff(backoff, info) => {
                    // This la needs to write a failure marker, and then we will tell lang how
                    // long of a timer to schedule to back off for. We do this because there are
                    // no other situations where core generates "internal" commands so it is much
                    // simpler for lang to reply with the timer / next LA command than to do it
                    // internally. Plus, this backoff hack we'd like to eliminate eventually.
                    self.complete_local_act(as_la_res, info, Some(backoff));
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
            atm.complete(task_token, status, &*self.wf_client).await;
        } else {
            error!(
                "Tried to complete activity {} on a worker that does not have an activity manager",
                task_token
            );
        }
        Ok(())
    }

    #[instrument(skip(self), fields(run_id, workflow_id, task_queue=%self.config.task_queue))]
    pub(crate) async fn next_workflow_activation(&self) -> Result<WorkflowActivation, PollWfError> {
        let r = self.workflows.next_workflow_activation().await;
        // In the event workflows are shutdown, begin shutdown of everything else, since that's
        // about to happen anyway. Tell the local activity manager that, so that it can know to
        // cancel any remaining outstanding LAs and shutdown.
        if matches!(r, Err(PollWfError::ShutDown)) {
            self.initiate_shutdown();
            self.local_act_mgr.workflows_have_shutdown();
        }
        r
    }

    #[instrument(skip(self, completion),
                 fields(completion=%&completion, run_id=%completion.run_id, workflow_id,
                        task_queue=%self.config.task_queue))]
    pub(crate) async fn complete_workflow_activation(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<(), CompleteWfError> {
        let run_id = completion.run_id.clone();
        let most_recent_event = self.workflows.activation_completed(completion).await?;
        if let Some(h) = &self.post_activate_hook {
            h(self, &run_id, most_recent_event);
        }
        Ok(())
    }

    /// Request a workflow eviction
    pub(crate) fn request_wf_eviction(
        &self,
        run_id: &str,
        message: impl Into<String>,
        reason: EvictionReason,
    ) {
        self.workflows.request_eviction(run_id, message, reason);
    }

    /// Sets a function to be called at the end of each activation completion
    pub(crate) fn set_post_activate_hook(
        &mut self,
        callback: impl Fn(&Self, &str, usize) + Send + Sync + 'static,
    ) {
        self.post_activate_hook = Some(Box::new(callback))
    }

    fn complete_local_act(
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
                original_schedule_time: info.la_info.schedule_cmd.original_schedule_time,
            }),
        )
    }

    fn notify_local_result(&self, run_id: &str, res: LocalResolution) {
        self.workflows.notify_of_local_result(run_id, res);
    }
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

    #[test]
    fn max_polls_calculated_properly() {
        let mut wcb = WorkerConfigBuilder::default();
        let cfg = wcb
            .namespace("default")
            .task_queue("whatever")
            .worker_build_id("test_bin_id")
            .max_concurrent_wft_polls(5_usize)
            .build()
            .unwrap();
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
