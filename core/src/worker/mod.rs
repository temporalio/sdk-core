mod activities;
pub(crate) mod client;
mod workflow;

pub use temporal_sdk_core_api::worker::{WorkerConfig, WorkerConfigBuilder};
#[cfg(feature = "save_wf_inputs")]
pub use workflow::replay_wf_state_inputs;

pub(crate) use activities::{
    ExecutingLAId, LocalActRequest, LocalActivityExecutionResult, LocalActivityResolution,
    NewLocalAct,
};
pub(crate) use workflow::{wft_poller::new_wft_poller, LEGACY_QUERY_ID};

#[cfg(test)]
pub(crate) use workflow::ManagedWFFunc;

use crate::{
    abstractions::MeteredSemaphore,
    errors::CompleteWfError,
    pollers::{new_activity_task_buffer, new_workflow_task_buffer, WorkflowTaskPoller},
    protosext::validate_activity_completion,
    telemetry::{
        metrics::{
            activity_poller, activity_worker_type, local_activity_worker_type, workflow_poller,
            workflow_sticky_poller, workflow_worker_type, MetricsContext,
        },
        TelemetryInstance,
    },
    worker::{
        activities::{LACompleteAction, LocalActivityManager, NextPendingLAAction},
        client::WorkerClient,
        workflow::{LAReqSink, LocalResolution, WorkflowBasics, Workflows},
    },
    ActivityHeartbeat, CompleteActivityError, PollActivityError, PollWfError, WorkerTrait,
};
use activities::WorkerActivityTasks;
use std::{
    convert::TryInto,
    future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
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
        workflowservice::v1::get_system_info_response,
    },
    TaskToken,
};
use tokio::sync::mpsc::unbounded_channel;
use tokio_util::sync::CancellationToken;

use crate::{abstractions::dbg_panic, pollers::BoxedActPoller};
#[cfg(test)]
use {
    crate::{
        pollers::{BoxedPoller, MockPermittedPollBuffer},
        protosext::ValidPollWFTQResponse,
    },
    futures::stream::BoxStream,
    futures_util::StreamExt,
    temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse,
};

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
    post_activate_hook: Option<Box<dyn Fn(&Self, PostActivateHookData) + Send + Sync>>,
    /// Set when non-local activities are complete and should stop being polled
    non_local_activities_complete: Arc<AtomicBool>,
    /// Set when local activities are complete and should stop being polled
    local_activities_complete: Arc<AtomicBool>,
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
        if !self.shutdown_token.is_cancelled() {
            info!(
                task_queue=%self.config.task_queue,
                namespace=%self.config.namespace,
                "Initiated shutdown",
            );
        }
        self.shutdown_token.cancel();
        // First, we want to stop polling of both activity and workflow tasks
        if let Some(atm) = self.at_task_mgr.as_ref() {
            atm.initiate_shutdown();
        }
        // Let the manager know that shutdown has been initiated to try to unblock the local
        // activity poll in case this worker is an activity-only worker.
        self.local_act_mgr.shutdown_initiated();
        if !self.workflows.ever_polled() {
            self.local_act_mgr.workflows_have_shutdown();
        }
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
        telem_instance: Option<&TelemetryInstance>,
    ) -> Self {
        info!(task_queue=%config.task_queue, namespace=%config.namespace, "Initializing worker");
        let metrics = if let Some(ti) = telem_instance {
            MetricsContext::top_level(config.namespace.clone(), ti)
                .with_task_q(config.task_queue.clone())
        } else {
            MetricsContext::no_op()
        };
        metrics.worker_registered();

        Self::new_with_pollers(
            config,
            sticky_queue_name,
            client,
            TaskPollers::Real,
            metrics,
            telem_instance,
        )
    }

    #[cfg(test)]
    pub(crate) fn new_test(config: WorkerConfig, client: impl WorkerClient + 'static) -> Self {
        Self::new(config, None, Arc::new(client), None)
    }

    #[allow(clippy::too_many_arguments)] // Not much worth combining here
    pub(crate) fn new_with_pollers(
        mut config: WorkerConfig,
        sticky_queue_name: Option<String>,
        client: Arc<dyn WorkerClient>,
        task_pollers: TaskPollers,
        metrics: MetricsContext,
        telem_instance: Option<&TelemetryInstance>,
    ) -> Self {
        let shutdown_token = CancellationToken::new();
        let wft_semaphore = Arc::new(MeteredSemaphore::new(
            config.max_outstanding_workflow_tasks,
            metrics.with_new_attrs([workflow_worker_type()]),
            MetricsContext::available_task_slots,
        ));
        let act_semaphore = Arc::new(MeteredSemaphore::new(
            config.max_outstanding_activities,
            metrics.with_new_attrs([activity_worker_type()]),
            MetricsContext::available_task_slots,
        ));

        let (wft_stream, act_poller) = match task_pollers {
            TaskPollers::Real => {
                let max_nonsticky_polls = if sticky_queue_name.is_some() {
                    config.max_nonsticky_polls()
                } else {
                    config.max_concurrent_wft_polls
                };
                let max_sticky_polls = config.max_sticky_polls();
                let wft_metrics = metrics.with_new_attrs([workflow_poller()]);
                let wf_task_poll_buffer = new_workflow_task_buffer(
                    client.clone(),
                    TaskQueue {
                        name: config.task_queue.clone(),
                        kind: TaskQueueKind::Normal as i32,
                        normal_name: "".to_string(),
                    },
                    max_nonsticky_polls,
                    wft_semaphore.clone(),
                    shutdown_token.child_token(),
                    Some(move |np| {
                        wft_metrics.record_num_pollers(np);
                    }),
                );
                let sticky_queue_poller = sticky_queue_name.as_ref().map(|sqn| {
                    let sticky_metrics = metrics.with_new_attrs([workflow_sticky_poller()]);
                    new_workflow_task_buffer(
                        client.clone(),
                        TaskQueue {
                            name: sqn.clone(),
                            kind: TaskQueueKind::Sticky as i32,
                            normal_name: config.task_queue.clone(),
                        },
                        max_sticky_polls,
                        wft_semaphore.clone(),
                        shutdown_token.child_token(),
                        Some(move |np| {
                            sticky_metrics.record_num_pollers(np);
                        }),
                    )
                });
                let act_poll_buffer = if config.no_remote_activities {
                    None
                } else {
                    let act_metrics = metrics.with_new_attrs([activity_poller()]);
                    let ap = new_activity_task_buffer(
                        client.clone(),
                        config.task_queue.clone(),
                        config.max_concurrent_at_polls,
                        act_semaphore.clone(),
                        config.max_task_queue_activities_per_second,
                        shutdown_token.child_token(),
                        Some(move |np| act_metrics.record_num_pollers(np)),
                        config.max_worker_activities_per_second,
                    );
                    Some(Box::from(ap) as BoxedActPoller)
                };
                let wf_task_poll_buffer = Box::new(WorkflowTaskPoller::new(
                    wf_task_poll_buffer,
                    sticky_queue_poller,
                ));
                let wft_stream = new_wft_poller(wf_task_poll_buffer, metrics.clone());
                #[cfg(test)]
                let wft_stream = wft_stream.left_stream();
                (wft_stream, act_poll_buffer)
            }
            #[cfg(test)]
            TaskPollers::Mocked {
                wft_stream,
                act_poller,
            } => {
                let ap =
                    act_poller.map(|ap| MockPermittedPollBuffer::new(act_semaphore.clone(), ap));
                let wft_semaphore = wft_semaphore.clone();
                let wfs = wft_stream.then(move |s| {
                    let wft_semaphore = wft_semaphore.clone();
                    async move {
                        let permit = wft_semaphore
                            .acquire_owned()
                            .await
                            .expect("Mock WFT stream should not see closed semaphore");
                        s.map(|s| (s, permit))
                    }
                });
                let wfs = wfs.right_stream();
                (wfs, ap.map(|ap| Box::new(ap) as BoxedActPoller))
            }
        };

        let (hb_tx, hb_rx) = unbounded_channel();
        let local_act_mgr = Arc::new(LocalActivityManager::new(
            config.max_outstanding_local_activities,
            config.namespace.clone(),
            hb_tx,
            metrics.with_new_attrs([local_activity_worker_type()]),
        ));
        let at_task_mgr = act_poller.map(|ap| {
            WorkerActivityTasks::new(
                act_semaphore,
                ap,
                client.clone(),
                metrics.clone(),
                config.max_heartbeat_throttle_interval,
                config.default_heartbeat_throttle_interval,
                config.graceful_shutdown_period,
            )
        });
        let poll_on_non_local_activities = at_task_mgr.is_some();
        if !poll_on_non_local_activities {
            info!("Activity polling is disabled for this worker");
        };
        let la_sink = LAReqSink::new(local_act_mgr.clone(), config.wf_state_inputs.clone());
        Self {
            wf_client: client.clone(),
            workflows: Workflows::new(
                build_wf_basics(
                    &mut config,
                    metrics,
                    shutdown_token.child_token(),
                    client.capabilities().cloned().unwrap_or_default(),
                ),
                sticky_queue_name.map(|sq| StickyExecutionAttributes {
                    worker_task_queue: Some(TaskQueue {
                        name: sq,
                        kind: TaskQueueKind::Sticky as i32,
                        normal_name: config.task_queue.clone(),
                    }),
                    schedule_to_start_timeout: Some(
                        config
                            .sticky_queue_schedule_to_start_timeout
                            .try_into()
                            .expect("timeout fits into proto"),
                    ),
                }),
                client,
                wft_semaphore,
                wft_stream,
                la_sink,
                local_act_mgr.clone(),
                hb_rx,
                at_task_mgr
                    .as_ref()
                    .map(|mgr| mgr.get_handle_for_workflows()),
                telem_instance,
            ),
            at_task_mgr,
            local_act_mgr,
            config,
            shutdown_token,
            post_activate_hook: None,
            // Non-local activities are already complete if configured not to poll for them.
            non_local_activities_complete: Arc::new(AtomicBool::new(!poll_on_non_local_activities)),
            local_activities_complete: Default::default(),
        }
    }

    /// Will shutdown the worker. Does not resolve until all outstanding workflow tasks have been
    /// completed
    async fn shutdown(&self) {
        self.initiate_shutdown();
        // We need to wait for all local activities to finish so no more workflow task heartbeats
        // will be generated
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
            acts.shutdown().await;
        }
    }

    /// Finish shutting down by consuming the background pollers and freeing all resources
    async fn finalize_shutdown(self) {
        self.shutdown().await;
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

    #[allow(unused)]
    pub(crate) fn available_wft_permits(&self) -> usize {
        self.workflows.available_wft_permits()
    }
    #[cfg(test)]
    pub(crate) fn unused_wft_permits(&self) -> usize {
        self.workflows.unused_wft_permits()
    }

    /// Get new activity tasks (may be local or nonlocal). Local activities are returned first
    /// before polling the server if there are any.
    ///
    /// Returns `Ok(None)` in the event of a poll timeout or if the polling loop should otherwise
    /// be restarted
    async fn activity_poll(&self) -> Result<Option<ActivityTask>, PollActivityError> {
        let local_activities_complete = self.local_activities_complete.load(Ordering::Relaxed);
        let non_local_activities_complete =
            self.non_local_activities_complete.load(Ordering::Relaxed);
        if local_activities_complete && non_local_activities_complete {
            return Err(PollActivityError::ShutDown);
        }
        let act_mgr_poll = async {
            if non_local_activities_complete {
                future::pending::<()>().await;
                unreachable!()
            }
            if let Some(ref act_mgr) = self.at_task_mgr {
                let res = act_mgr.poll().await;
                if let Err(err) = res.as_ref() {
                    if matches!(err, PollActivityError::ShutDown) {
                        self.non_local_activities_complete
                            .store(true, Ordering::Relaxed);
                        return Ok(None);
                    }
                };
                res.map(Some)
            } else {
                // We expect the local activity branch below to produce shutdown when appropriate if
                // there are no activity pollers.
                future::pending::<()>().await;
                unreachable!()
            }
        };
        let local_activities_poll = async {
            if local_activities_complete {
                future::pending::<()>().await;
                unreachable!()
            }
            match self.local_act_mgr.next_pending().await {
                Some(NextPendingLAAction::Dispatch(r)) => Ok(Some(r)),
                Some(NextPendingLAAction::Autocomplete(action)) => {
                    Ok(self.handle_la_complete_action(action))
                }
                None => {
                    if self.shutdown_token.is_cancelled() {
                        self.local_activities_complete
                            .store(true, Ordering::Relaxed);
                    }
                    Ok(None)
                }
            }
        };

        tokio::select! {
            biased;

            r = local_activities_poll => r,
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
            self.complete_local_act(task_token, as_la_res);
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
            // This is covering the situation where WFT pollers dying is the reason for shutdown
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
        self.workflows
            .activation_completed(
                completion,
                false,
                self.post_activate_hook
                    .as_ref()
                    .map(|h| |data: PostActivateHookData| h(self, data)),
            )
            .await?;
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
        callback: impl Fn(&Self, PostActivateHookData) + Send + Sync + 'static,
    ) {
        self.post_activate_hook = Some(Box::new(callback))
    }

    fn complete_local_act(&self, task_token: TaskToken, la_res: LocalActivityExecutionResult) {
        if self
            .handle_la_complete_action(self.local_act_mgr.complete(&task_token, la_res))
            .is_some()
        {
            dbg_panic!("Should never be a task from direct completion");
        }
    }

    fn handle_la_complete_action(&self, action: LACompleteAction) -> Option<ActivityTask> {
        match action {
            LACompleteAction::Report {
                run_id,
                resolution,
                task,
            } => {
                self.notify_local_result(&run_id, LocalResolution::LocalActivity(resolution));
                task
            }
            LACompleteAction::WillBeRetried(task) => task,
            LACompleteAction::Untracked => None,
        }
    }

    fn notify_local_result(&self, run_id: &str, res: LocalResolution) {
        self.workflows.notify_of_local_result(run_id, res);
    }
}

pub struct PostActivateHookData<'a> {
    pub run_id: &'a str,
    pub most_recent_event: usize,
    pub replaying: bool,
}

fn build_wf_basics(
    config: &mut WorkerConfig,
    metrics: MetricsContext,
    shutdown_token: CancellationToken,
    server_capabilities: get_system_info_response::Capabilities,
) -> WorkflowBasics {
    WorkflowBasics {
        max_cached_workflows: config.max_cached_workflows,
        shutdown_token,
        metrics,
        namespace: config.namespace.clone(),
        task_queue: config.task_queue.clone(),
        ignore_evicts_on_shutdown: config.ignore_evicts_on_shutdown,
        fetching_concurrency: config.fetching_concurrency,
        server_capabilities,
        #[cfg(feature = "save_wf_inputs")]
        wf_state_inputs: config.wf_state_inputs.take(),
    }
}

pub(crate) enum TaskPollers {
    Real,
    #[cfg(test)]
    Mocked {
        wft_stream: BoxStream<'static, Result<ValidPollWFTQResponse, tonic::Status>>,
        act_poller: Option<BoxedPoller<PollActivityTaskQueueResponse>>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        advance_fut, test_help::test_worker_cfg, worker::client::mocks::mock_workflow_client,
    };
    use futures::FutureExt;

    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse;

    #[tokio::test]
    async fn activity_timeouts_maintain_permit() {
        let mut mock_client = mock_workflow_client();
        mock_client
            .expect_poll_activity_task()
            .returning(|_, _| Ok(PollActivityTaskQueueResponse::default()));

        let cfg = test_worker_cfg()
            .max_outstanding_activities(5_usize)
            .build()
            .unwrap();
        let worker = Worker::new_test(cfg, mock_client);
        let fut = worker.poll_activity_task();
        advance_fut!(fut);
        assert_eq!(
            worker
                .at_task_mgr
                .as_ref()
                .unwrap()
                .remaining_activity_capacity(),
            5
        );
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
