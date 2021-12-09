use std::time::Duration;
use temporal_sdk_core_protos::coresdk::{
    child_workflow::ChildWorkflowCancellationType,
    common::{Payload, RetryPolicy},
    workflow_commands::{
        ActivityCancellationType, ScheduleActivity, ScheduleLocalActivity,
        StartChildWorkflowExecution,
    },
};

pub trait IntoWorkflowCommand {
    type WFCommandType;

    /// Produces a workflow command from some options
    fn into_command(self, seq: u32) -> Self::WFCommandType;
}

/// Options for scheduling an activity
#[derive(Default, Debug)]
pub struct ActivityOptions {
    /// Identifier to use for tracking the activity in Workflow history.
    /// The `activityId` can be accessed by the activity function.
    /// Does not need to be unique.
    ///
    /// If `None` use the context's sequence number
    pub activity_id: Option<String>,
    /// Type of activity to schedule
    pub activity_type: String,
    /// Input to the activity
    pub input: Payload,
    /// Task queue to schedule the activity in
    pub task_queue: String,
    /// Time that the Activity Task can stay in the Task Queue before it is picked up by a Worker.
    /// Do not specify this timeout unless using host specific Task Queues for Activity Tasks are
    /// being used for routing.
    /// `schedule_to_start_timeout` is always non-retryable.
    /// Retrying after this timeout doesn't make sense as it would just put the Activity Task back
    /// into the same Task Queue.
    pub schedule_to_start_timeout: Option<Duration>,
    /// Maximum time of a single Activity execution attempt.
    /// Note that the Temporal Server doesn't detect Worker process failures directly.
    /// It relies on this timeout to detect that an Activity that didn't complete on time.
    /// So this timeout should be as short as the longest possible execution of the Activity body.
    /// Potentially long running Activities must specify `heartbeat_timeout` and heartbeat from the
    /// activity periodically for timely failure detection.
    /// Either this option or `schedule_to_close_timeout` is required.
    pub start_to_close_timeout: Option<Duration>,
    /// Total time that a workflow is willing to wait for Activity to complete.
    /// `schedule_to_close_timeout` limits the total time of an Activity's execution including
    /// retries (use `start_to_close_timeout` to limit the time of a single attempt).
    /// Either this option or `start_to_close_timeout` is required.
    pub schedule_to_close_timeout: Option<Duration>,
    /// Heartbeat interval. Activity must heartbeat before this interval passes after a last
    /// heartbeat or activity start.
    pub heartbeat_timeout: Option<Duration>,
    /// Determines what the SDK does when the Activity is cancelled.
    pub cancellation_type: ActivityCancellationType,
}

impl IntoWorkflowCommand for ActivityOptions {
    type WFCommandType = ScheduleActivity;
    fn into_command(self, seq: u32) -> ScheduleActivity {
        ScheduleActivity {
            seq,
            activity_id: match self.activity_id {
                None => seq.to_string(),
                Some(aid) => aid,
            },
            activity_type: self.activity_type,
            task_queue: self.task_queue,
            schedule_to_close_timeout: self.schedule_to_close_timeout.map(Into::into),
            schedule_to_start_timeout: self.schedule_to_start_timeout.map(Into::into),
            start_to_close_timeout: self.start_to_close_timeout.map(Into::into),
            heartbeat_timeout: self.heartbeat_timeout.map(Into::into),
            cancellation_type: self.cancellation_type as i32,
            arguments: vec![self.input],
            ..Default::default()
        }
    }
}

/// Options for scheduling a local activity
#[derive(Default, Debug, Clone)]
pub struct LocalActivityOptions {
    /// Identifier to use for tracking the activity in Workflow history.
    /// The `activityId` can be accessed by the activity function.
    /// Does not need to be unique.
    ///
    /// If `None` use the context's sequence number
    pub activity_id: Option<String>,
    /// Type of activity to schedule
    pub activity_type: String,
    /// Input to the activity
    pub input: Payload,
    /// Retry policy
    pub retry_policy: RetryPolicy,
    /// Override attempt number rather than using 1.
    /// Ideally we would not expose this in a released Rust SDK, but it's needed for test.
    pub attempt: Option<u32>,
}

impl IntoWorkflowCommand for LocalActivityOptions {
    type WFCommandType = ScheduleLocalActivity;
    fn into_command(self, seq: u32) -> ScheduleLocalActivity {
        ScheduleLocalActivity {
            seq,
            attempt: self.attempt.unwrap_or(1),
            activity_id: match self.activity_id {
                None => seq.to_string(),
                Some(aid) => aid,
            },
            activity_type: self.activity_type,
            arguments: vec![self.input],
            retry_policy: Some(self.retry_policy),
            ..Default::default()
        }
    }
}

/// Options for scheduling a child workflow
#[derive(Default, Debug, Clone)]
pub struct ChildWorkflowOptions {
    /// Workflow ID
    pub workflow_id: String,
    /// Type of workflow to schedule
    pub workflow_type: String,
    /// Input to send the child Workflow
    pub input: Vec<Payload>,
    /// Cancellation strategy for the child workflow
    pub cancel_type: ChildWorkflowCancellationType,
}

impl IntoWorkflowCommand for ChildWorkflowOptions {
    type WFCommandType = StartChildWorkflowExecution;
    fn into_command(self, seq: u32) -> StartChildWorkflowExecution {
        StartChildWorkflowExecution {
            seq,
            workflow_id: self.workflow_id,
            workflow_type: self.workflow_type,
            input: self.input,
            cancellation_type: self.cancel_type as i32,
            ..Default::default()
        }
    }
}
