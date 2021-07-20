use crate::{
    activity::PendingActivityCancel,
    pollers::{
        new_activity_task_buffer, new_workflow_task_buffer, BoxedActPoller, BoxedWFPoller, Poller,
        WorkflowTaskPoller,
    },
    protos::{
        temporal::api::enums::v1::TaskQueueKind,
        temporal::api::taskqueue::v1::{StickyExecutionAttributes, TaskQueue},
        temporal::api::workflowservice::v1::{
            PollActivityTaskQueueResponse, PollWorkflowTaskQueueResponse,
        },
    },
    protosext::ValidPollWFTQResponse,
    PollActivityError, PollWfError, ServerGatewayApis,
};
use crossbeam::queue::SegQueue;
use std::{convert::TryInto, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

/// Defines per-worker configuration options
#[derive(Debug, Clone, derive_builder::Builder)]
#[builder(setter(into), build_fn(validate = "Self::validate"))]
// TODO: per-second queue limits
pub struct WorkerConfig {
    /// What task queue will this worker poll from? This task queue name will be used for both
    /// workflow and activity polling.
    pub task_queue: String,
    /// The maximum allowed number of workflow tasks that will ever be given to this worker at one
    /// time. Note that one workflow task may require multiple activations - so the WFT counts as
    /// "outstanding" until all activations it requires have been completed.
    #[builder(default = "100")]
    pub max_outstanding_workflow_tasks: usize,
    /// The maximum allowed number of activity tasks that will ever be given to this worker at one
    /// time.
    #[builder(default = "100")]
    pub max_outstanding_activities: usize,
    /// Maximum number of concurrent poll workflow task requests we will perform at a time on this
    /// worker's task queue. See also [WorkerConfig::nonsticky_to_sticky_poll_ratio]. Must be at
    /// least 1.
    #[builder(default = "5")]
    pub max_concurrent_wft_polls: usize,
    /// [WorkerConfig::max_concurrent_wft_polls] * this number = the number of max pollers that will
    /// be allowed for the nonsticky queue when sticky tasks are enabled. If both defaults are used,
    /// the sticky queue will allow 4 max pollers while the nonsticky queue will allow one. The
    /// minimum for either poller is 1, so if `max_concurrent_wft_polls` is 1 and sticky queues are
    /// enabled, there will be 2 concurrent polls.
    #[builder(default = "0.2")]
    pub nonsticky_to_sticky_poll_ratio: f32,
    /// Maximum number of concurrent poll activity task requests we will perform at a time on this
    /// worker's task queue
    #[builder(default = "5")]
    pub max_concurrent_at_polls: usize,
    /// If set to true this worker will only handle workflow tasks and local activities, it will not
    /// poll for activity tasks. This option exists because
    #[builder(default = "false")]
    pub no_remote_activities: bool,
    /// How long a workflow task is allowed to sit on the sticky queue before it is timed out
    /// and moved to the non-sticky queue where it may be picked up by any worker.
    #[builder(default = "Duration::from_secs(10)")]
    pub sticky_queue_schedule_to_start_timeout: Duration,
}

impl WorkerConfigBuilder {
    fn validate(&self) -> Result<(), String> {
        if self.max_concurrent_wft_polls == Some(0) {
            return Err("`max_concurrent_wft_polls` must be at least 1".to_owned());
        }
        Ok(())
    }
}

/// A worker polls on a certain task queue
pub(crate) struct Worker {
    config: WorkerConfig,

    /// Will be populated when this worker should poll on a sticky WFT queue
    sticky_name: Option<String>,

    /// Buffers workflow task polling in the event we need to return a pending activation while
    /// a poll is ongoing. Sticky and nonsticky polling happens inside of it.
    wf_task_poll_buffer: BoxedWFPoller,
    /// Buffers activity task polling in the event we need to return a cancellation while a poll is
    /// ongoing. May be `None` if this worker does not poll for activities.
    at_task_poll_buffer: Option<BoxedActPoller>,
    /// Buffers activity task cancellations we have learned about
    cancelled_activities_buffer: SegQueue<PendingActivityCancel>,

    /// Ensures we stay at or below this worker's maximum concurrent workflow limit
    workflows_semaphore: Semaphore,
    /// Ensures we stay at or below this worker's maximum concurrent activity limit
    activities_semaphore: Semaphore,
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
        let at_task_poll_buffer = if config.no_remote_activities {
            None
        } else {
            Some(Box::new(new_activity_task_buffer(
                sg,
                config.task_queue.clone(),
                config.max_concurrent_at_polls,
                config.max_concurrent_at_polls * 2,
            ))
                as Box<
                    dyn Poller<PollActivityTaskQueueResponse> + Send + Sync,
                >)
        };
        let wf_task_poll_buffer = Box::new(WorkflowTaskPoller::new(
            wf_task_poll_buffer,
            sticky_queue_poller,
        ));
        Self {
            sticky_name: sticky_queue_name,
            wf_task_poll_buffer,
            at_task_poll_buffer,
            cancelled_activities_buffer: Default::default(),
            workflows_semaphore: Semaphore::new(config.max_outstanding_workflow_tasks),
            activities_semaphore: Semaphore::new(config.max_outstanding_activities),
            config,
        }
    }

    #[cfg(test)]
    pub(crate) fn new_with_pollers(
        config: WorkerConfig,
        sticky_queue_name: Option<String>,
        wft_poller: BoxedWFPoller,
        act_poller: Option<BoxedActPoller>,
    ) -> Self {
        Self {
            sticky_name: sticky_queue_name,
            wf_task_poll_buffer: wft_poller,
            at_task_poll_buffer: act_poller,
            cancelled_activities_buffer: Default::default(),
            workflows_semaphore: Semaphore::new(config.max_outstanding_workflow_tasks),
            activities_semaphore: Semaphore::new(config.max_outstanding_activities),
            config,
        }
    }

    /// Tell the worker to begin the shutdown process. Can be used before [await_shutdown] if
    /// polling should be ceased before it is possible to consume the worker instance.
    pub(crate) fn notify_shutdown(&self) {
        self.wf_task_poll_buffer.notify_shutdown();
        if let Some(b) = self.at_task_poll_buffer.as_ref() {
            b.notify_shutdown();
        }
    }

    /// Resolves when shutdown of the worker is complete
    pub(crate) async fn shutdown_complete(self) {
        self.wf_task_poll_buffer.shutdown_box().await;
        if let Some(b) = self.at_task_poll_buffer {
            b.shutdown_box().await;
        }
    }

    pub(crate) fn task_queue(&self) -> &str {
        &self.config.task_queue
    }

    #[cfg(test)]
    pub(crate) fn outstanding_activities(&self) -> usize {
        self.config.max_outstanding_activities - self.activities_semaphore.available_permits()
    }

    #[cfg(test)]
    pub(crate) fn outstanding_workflow_tasks(&self) -> usize {
        self.config.max_outstanding_workflow_tasks - self.workflows_semaphore.available_permits()
    }

    /// Wait until not at the outstanding activity limit, and then poll this worker's task queue for
    /// new activities.
    ///
    /// Returns `Ok(None)` in the event of a poll timeout
    pub(crate) async fn activity_poll(
        &self,
    ) -> Result<Option<PollActivityTaskQueueResponse>, PollActivityError> {
        // No activity polling is allowed if this worker said it only handles local activities
        let poll_buff = self
            .at_task_poll_buffer
            .as_ref()
            .ok_or_else(|| PollActivityError::NoWorkerForQueue(self.config.task_queue.clone()))?;

        // Acquire and subsequently forget a permit for an outstanding activity. When they are
        // completed, we must add a new permit to the semaphore, since holding the permit the entire
        // time lang does work would be a challenge.
        let sem = self
            .activities_semaphore
            .acquire()
            .await
            .expect("outstanding activity semaphore not closed");

        let res = poll_buff
            .poll()
            .await
            .ok_or(PollActivityError::ShutDown)??;
        if res == PollActivityTaskQueueResponse::default() {
            return Ok(None);
        }
        // Only permanently take a permit in the event the poll finished completely
        sem.forget();
        Ok(Some(res))
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
    pub(crate) fn activity_done(&self) {
        self.activities_semaphore.add_permits(1)
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
        assert_eq!(worker.activities_semaphore.available_permits(), 5);
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
        assert_eq!(worker.activities_semaphore.available_permits(), 5);
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
