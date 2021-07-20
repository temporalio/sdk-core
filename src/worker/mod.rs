mod config;
mod dispatcher;

pub use crate::worker::config::{WorkerConfig, WorkerConfigBuilder};
pub use dispatcher::{WorkerDispatcher, WorkerStatus};

use crate::{
    activity::WorkerActivityTasks,
    pollers::{
        new_activity_task_buffer, new_workflow_task_buffer, BoxedWFPoller, WorkflowTaskPoller,
    },
    protos::{
        coresdk::activity_task::ActivityTask,
        temporal::api::enums::v1::TaskQueueKind,
        temporal::api::taskqueue::v1::{StickyExecutionAttributes, TaskQueue},
        temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse,
    },
    protosext::ValidPollWFTQResponse,
    task_token::TaskToken,
    ActivityHeartbeat, PollActivityError, PollWfError, ServerGatewayApis,
};
use std::{convert::TryInto, sync::Arc};
use tokio::sync::Semaphore;

#[cfg(test)]
use crate::pollers::BoxedActPoller;

/// A worker polls on a certain task queue
pub struct Worker {
    config: WorkerConfig,

    /// Will be populated when this worker should poll on a sticky WFT queue
    sticky_name: Option<String>,

    /// Buffers workflow task polling in the event we need to return a pending activation while
    /// a poll is ongoing. Sticky and nonsticky polling happens inside of it.
    wf_task_poll_buffer: BoxedWFPoller,
    /// Manages activity tasks for this worker/task queue
    at_task_mgr: Option<WorkerActivityTasks>,

    /// Ensures we stay at or below this worker's maximum concurrent workflow limit
    workflows_semaphore: Semaphore,
}

impl Worker {
    pub(crate) fn new<SG: ServerGatewayApis + Send + Sync + 'static>(
        config: WorkerConfig,
        sticky_queue_name: Option<String>,
        sg: Arc<SG>,
    ) -> Self {
        let max_nonsticky_polls = if sticky_queue_name.is_some() {
            config.max_nonsticky_polls()
        } else {
            config.max_concurrent_wft_polls
        };
        let max_sticky_polls = config.max_sticky_polls();
        let wf_task_poll_buffer = new_workflow_task_buffer(
            sg.clone(),
            config.task_queue.clone(),
            max_nonsticky_polls,
            max_nonsticky_polls * 2,
        );
        let sticky_queue_poller = sticky_queue_name.as_ref().map(|sqn| {
            new_workflow_task_buffer(
                sg.clone(),
                sqn.clone(),
                max_sticky_polls,
                max_sticky_polls * 2,
            )
        });
        let at_task_mgr = if config.no_remote_activities {
            None
        } else {
            Some(WorkerActivityTasks::new(
                config.max_outstanding_activities,
                Box::from(new_activity_task_buffer(
                    sg.clone(),
                    config.task_queue.clone(),
                    config.max_concurrent_at_polls,
                    config.max_concurrent_at_polls * 2,
                )),
                sg,
            ))
        };
        let wf_task_poll_buffer = Box::new(WorkflowTaskPoller::new(
            wf_task_poll_buffer,
            sticky_queue_poller,
        ));
        Self {
            sticky_name: sticky_queue_name,
            wf_task_poll_buffer,
            at_task_mgr,
            workflows_semaphore: Semaphore::new(config.max_outstanding_workflow_tasks),
            config,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_pollers<SG: ServerGatewayApis + Send + Sync + 'static>(
        config: WorkerConfig,
        sticky_queue_name: Option<String>,
        sg: Arc<SG>,
        wft_poller: BoxedWFPoller,
        act_poller: Option<BoxedActPoller>,
    ) -> Self {
        let max_acts = config.max_outstanding_activities;
        Self {
            sticky_name: sticky_queue_name,
            wf_task_poll_buffer: wft_poller,
            at_task_mgr: act_poller.map(move |ap| WorkerActivityTasks::new(max_acts, ap, sg)),
            workflows_semaphore: Semaphore::new(config.max_outstanding_workflow_tasks),
            config,
        }
    }

    /// Tell the worker to begin the shutdown process. Can be used before [await_shutdown] if
    /// polling should be ceased before it is possible to consume the worker instance.
    pub(crate) fn notify_shutdown(&self) {
        self.wf_task_poll_buffer.notify_shutdown();
        if let Some(b) = self.at_task_mgr.as_ref() {
            b.notify_shutdown();
        }
    }

    /// Resolves when shutdown of the worker is complete
    pub(crate) async fn shutdown_complete(self) {
        self.wf_task_poll_buffer.shutdown_box().await;
        if let Some(b) = self.at_task_mgr {
            b.shutdown().await;
        }
    }

    #[cfg(test)]
    pub(crate) fn outstanding_workflow_tasks(&self) -> usize {
        self.config.max_outstanding_workflow_tasks - self.workflows_semaphore.available_permits()
    }

    /// Wait until not at the outstanding activity limit, and then poll this worker's task queue for
    /// new activities.
    ///
    /// Returns `Ok(None)` in the event of a poll timeout
    pub(crate) async fn activity_poll(&self) -> Result<Option<ActivityTask>, PollActivityError> {
        // No activity polling is allowed if this worker said it only handles local activities
        let act_mgr = self
            .at_task_mgr
            .as_ref()
            .ok_or_else(|| PollActivityError::NoWorkerForQueue(self.config.task_queue.clone()))?;

        act_mgr.poll().await
    }

    /// Wait until not at the outstanding workflow task limit, and then poll this worker's task
    /// queue for new workflow tasks.
    ///
    /// Returns `Ok(None)` in the event of a poll timeout
    pub(crate) async fn workflow_poll(&self) -> Result<Option<ValidPollWFTQResponse>, PollWfError> {
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
            return Ok(None);
        }

        let work: ValidPollWFTQResponse = res
            .try_into()
            .map_err(PollWfError::BadPollResponseFromServer)?;

        // Only permanently take a permit in the event the poll finished completely
        sem.forget();
        Ok(Some(work))
    }

    /// Attempt to record an activity heartbeat
    pub(crate) fn record_heartbeat(&self, details: ActivityHeartbeat) {
        if let Some(at_mgr) = self.at_task_mgr.as_ref() {
            let tt = details.task_token.clone();
            if let Err(e) = at_mgr.record_heartbeat(details) {
                warn!(task_token = ?tt, details = ?e, "Activity heartbeat failed.")
            }
        }
    }

    /// Return the sticky execution attributes that should be used to complete workflow tasks
    /// for this worker (if any).
    pub(crate) fn get_sticky_attrs(&self) -> Option<StickyExecutionAttributes> {
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

    /// Tell the worker an activity has completed, for tracking max outstanding activities
    pub(crate) fn activity_done(&self, task_token: &TaskToken) {
        if let Some(mgr) = self.at_task_mgr.as_ref() {
            mgr.mark_complete(task_token);
        }
    }

    /// Tell the worker a workflow task has completed, for tracking max outstanding WFTs
    pub(crate) fn workflow_task_done(&self) {
        self.workflows_semaphore.add_permits(1)
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
    use crate::pollers::MockServerGatewayApis;
    use crate::protos::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse;

    #[tokio::test]
    async fn activity_timeouts_dont_eat_permits() {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_poll_activity_task()
            .returning(|_| Ok(PollActivityTaskQueueResponse::default()));

        let cfg = WorkerConfigBuilder::default()
            .task_queue("whatever")
            .max_outstanding_activities(5usize)
            .build()
            .unwrap();
        let worker = Worker::new(cfg, None, Arc::new(mock_gateway));
        assert_eq!(worker.activity_poll().await.unwrap(), None);
        assert_eq!(worker.at_task_mgr.unwrap().remaining_activity_capacity(), 5);
    }

    #[tokio::test]
    async fn workflow_timeouts_dont_eat_permits() {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_poll_workflow_task()
            .returning(|_| Ok(PollWorkflowTaskQueueResponse::default()));

        let cfg = WorkerConfigBuilder::default()
            .task_queue("whatever")
            .max_outstanding_workflow_tasks(5usize)
            .build()
            .unwrap();
        let worker = Worker::new(cfg, None, Arc::new(mock_gateway));
        assert_eq!(worker.workflow_poll().await.unwrap(), None);
        assert_eq!(worker.workflows_semaphore.available_permits(), 5);
    }

    #[tokio::test]
    async fn activity_errs_dont_eat_permits() {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_poll_activity_task()
            .returning(|_| Err(tonic::Status::internal("ahhh")));

        let cfg = WorkerConfigBuilder::default()
            .task_queue("whatever")
            .max_outstanding_activities(5usize)
            .build()
            .unwrap();
        let worker = Worker::new(cfg, None, Arc::new(mock_gateway));
        assert!(worker.activity_poll().await.is_err());
        assert_eq!(worker.at_task_mgr.unwrap().remaining_activity_capacity(), 5);
    }

    #[tokio::test]
    async fn workflow_errs_dont_eat_permits() {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_poll_workflow_task()
            .returning(|_| Err(tonic::Status::internal("ahhh")));

        let cfg = WorkerConfigBuilder::default()
            .task_queue("whatever")
            .max_outstanding_workflow_tasks(5usize)
            .build()
            .unwrap();
        let worker = Worker::new(cfg, None, Arc::new(mock_gateway));
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
