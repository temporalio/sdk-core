use crate::{
    activity::InflightActivityDetails,
    pollers::{
        new_activity_task_buffer, new_workflow_task_buffer, PollActivityTaskBuffer,
        PollWorkflowTaskBuffer,
    },
    protos::{
        coresdk::activity_task::ActivityTask,
        temporal::api::workflowservice::v1::{
            PollActivityTaskQueueResponse, PollWorkflowTaskQueueResponse,
        },
    },
    task_token::TaskToken,
    PollActivityError, PollWfError, ServerGatewayApis,
};
use futures::TryFutureExt;
use std::sync::Arc;
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

impl Worker {
    pub(crate) fn new<SG: ServerGatewayApis + Send + Sync + 'static>(
        config: WorkerConfig,
        sg: Arc<SG>,
    ) -> Self {
        let wf_task_poll_buffer = new_workflow_task_buffer(
            sg.clone(),
            config.task_queue.clone(),
            config.max_concurrent_wft_polls,
            config.max_concurrent_wft_polls * 2,
        );
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
            wf_task_poll_buffer,
            at_task_poll_buffer,
            workflows_semaphore: Semaphore::new(config.max_outstanding_workflow_tasks),
            activities_semaphore: Semaphore::new(config.max_outstanding_activities),
            config,
        }
    }
    pub(crate) fn task_queue(&self) -> &str {
        &self.config.task_queue
    }

    /// Wait until not at the outstanding activity limit, and then poll this worker's task queue for
    /// new activities.
    pub(crate) async fn activity_poll(
        &self,
    ) -> Result<PollActivityTaskQueueResponse, PollActivityError> {
        // TODO: Explicit error?
        let poll_buff = self
            .at_task_poll_buffer
            .as_ref()
            .expect("Poll on activity worker means it actually has a poller");

        // Acquire and immediately forget a permit for an outstanding activity. When they are
        // completed, we must add a new permit to the semaphore, since holding the permit the
        // entire time lang does work would be a challenge. TODO: Maybe actually a good way, tho?
        self.activities_semaphore
            .acquire()
            .await
            .expect("outstanding activity semaphore not closed")
            .forget();

        poll_buff.poll().map_err(Into::into).await
    }

    /// Wait until not at the outstanding workflow task limit, and then poll this worker's task
    /// queue for new workflow tasks.
    pub(crate) async fn workflow_poll(&self) -> Result<PollWorkflowTaskQueueResponse, PollWfError> {
        self.workflows_semaphore
            .acquire()
            .await
            .expect("outstanding workflow tasks semaphore not dropped")
            .forget();
        debug!("Polling server");
        self.wf_task_poll_buffer.poll().map_err(Into::into).await
    }

    /// Tell the worker an activity has completed, for tracking max outstanding activities
    pub(crate) fn activity_done(&self) {
        self.activities_semaphore.add_permits(1)
    }
}
