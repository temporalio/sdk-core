use crate::{
    machines::ProtoCommand,
    pollers::{
        self, new_activity_task_buffer, new_workflow_task_buffer, PollActivityTaskBuffer,
        PollWorkflowTaskBuffer,
    },
    protos::temporal::api::enums::v1::TaskQueueKind,
    protos::temporal::api::taskqueue::v1::{StickyExecutionAttributes, TaskQueue},
    protos::temporal::api::workflowservice::v1::{
        PollActivityTaskQueueResponse, PollWorkflowTaskQueueResponse,
        RespondWorkflowTaskCompletedResponse,
    },
    protosext::ValidPollWFTQResponse,
    task_token::TaskToken,
    PollActivityError, PollWfError, ServerGatewayApis,
};
use std::time::Duration;
use std::{convert::TryInto, sync::Arc};
use tokio::sync::Semaphore;

/// Defines per-worker configuration options
#[derive(Debug, Clone, derive_builder::Builder)]
#[builder(setter(into))]
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
    /// worker's task queue
    #[builder(default = "5")]
    pub max_concurrent_wft_polls: usize,
    /// Maximum number of concurrent poll activity task requests we will perform at a time on this
    /// worker's task queue
    #[builder(default = "5")]
    pub max_concurrent_at_polls: usize,
    /// If set to true this worker will only handle workflow tasks and local activities, it will not
    /// poll for activity tasks. This option exists because
    #[builder(default = "false")]
    pub no_remote_activities: bool,
}

/// A worker polls on a certain task queue
pub(crate) struct Worker {
    config: WorkerConfig,

    /// Will be populated when this worker should poll on a sticky WFT queue
    sticky_queue: Option<StickyQueue>,

    /// Buffers workflow task polling in the event we need to return a pending activation while
    /// a poll is ongoing
    wf_task_poll_buffer: PollWorkflowTaskBuffer,
    /// Buffers activity task polling in the event we need to return a cancellation while a poll is
    /// ongoing. May be `None` if this worker does not poll for activities.
    at_task_poll_buffer: Option<PollActivityTaskBuffer>,

    /// Ensures we stay at or below this worker's maximum concurrent workflow limit
    workflows_semaphore: Semaphore,
    /// Ensures we stay at or below this worker's maximum concurrent activity limit
    activities_semaphore: Semaphore,
}

struct StickyQueue {
    name: String,
    poll_buffer: PollWorkflowTaskBuffer,
}

impl Worker {
    pub(crate) fn new<SG: ServerGatewayApis + Send + Sync + 'static>(
        config: WorkerConfig,
        sticky_queue_name: Option<String>,
        sg: Arc<SG>,
    ) -> Self {
        let wf_task_poll_buffer = new_workflow_task_buffer(
            sg.clone(),
            config.task_queue.clone(),
            config.max_concurrent_wft_polls,
            config.max_concurrent_wft_polls * 2,
        );
        let sticky_queue = sticky_queue_name.map(|sqn| StickyQueue {
            poll_buffer: new_workflow_task_buffer(
                sg.clone(),
                sqn.clone(),
                // TODO: Split this number across normal and sticky poller? Sticky should have more?
                config.max_concurrent_wft_polls,
                config.max_concurrent_wft_polls * 2,
            ),
            name: sqn,
        });
        let at_task_poll_buffer = if config.no_remote_activities {
            None
        } else {
            Some(new_activity_task_buffer(
                sg,
                config.task_queue.clone(),
                config.max_concurrent_at_polls,
                config.max_concurrent_at_polls * 2,
            ))
        };
        Self {
            sticky_queue,
            wf_task_poll_buffer,
            at_task_poll_buffer,
            workflows_semaphore: Semaphore::new(config.max_outstanding_workflow_tasks),
            activities_semaphore: Semaphore::new(config.max_outstanding_activities),
            config,
        }
    }
    pub(crate) fn shutdown(&self) {
        self.wf_task_poll_buffer.shutdown();
        if let Some(sq) = self.sticky_queue.as_ref() {
            sq.poll_buffer.shutdown();
        }
        if let Some(b) = self.at_task_poll_buffer.as_ref() {
            b.shutdown();
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

        let res = poll_buff.poll().await?;
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

        // If we use sticky queues, poll both the sticky and non-sticky queue
        let res = if let Some(sq) = self.sticky_queue.as_ref() {
            tokio::select! {
                // This bias exists for testing, where the test mocks currently don't populate
                // sticky queues. In production, the bias is unimportant.
                biased;

                r = self.wf_task_poll_buffer.poll() => r,
                r = sq.poll_buffer.poll() => r
            }
        } else {
            self.wf_task_poll_buffer.poll().await
        }?;
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

    /// Use the given server gateway to respond that the workflow task is completed. Properly
    /// attaches sticky task queue information if appropriate.
    pub(crate) async fn complete_workflow_task(
        &self,
        sg: &(dyn ServerGatewayApis + Sync),
        task_token: TaskToken,
        commands: Vec<ProtoCommand>,
    ) -> pollers::Result<RespondWorkflowTaskCompletedResponse> {
        let stq = self
            .sticky_queue
            .as_ref()
            .map(|sq| StickyExecutionAttributes {
                worker_task_queue: Some(TaskQueue {
                    name: sq.name.clone(),
                    kind: TaskQueueKind::Sticky as i32,
                }),
                // TODO: Set appropriately
                schedule_to_start_timeout: Some(Duration::from_secs(60).into()),
            });
        debug!("Sending commands to server: {:?}", &commands);
        sg.complete_workflow_task(task_token, commands, stq).await
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
}
