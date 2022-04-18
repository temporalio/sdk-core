use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration as StdDuration, SystemTime};

use prost_types::{Duration, Timestamp};
use temporal_sdk_core_api::Worker;
use temporal_sdk_core_protos::coresdk::{
    activity_task,
    common::{Payload, RetryPolicy, WorkflowExecution},
    ActivityHeartbeat,
};

/// Used within activities to get info, heartbeat management etc.
#[derive(Clone)]
#[allow(dead_code)]
pub struct ActContext {
    pub(crate) worker: Arc<dyn Worker>,
    pub(crate) input: Vec<Payload>,
    pub(crate) heartbeat_details: Vec<Payload>,
    /// #NOTE future usage?
    pub(crate) header_fields: HashMap<String, Payload>,
    pub(crate) info: ActivityInfo,
}

#[derive(Clone)]
pub struct ActivityInfo {
    pub task_token: Vec<u8>,
    pub workflow_type: String,
    pub workflow_namespace: String,
    pub workflow_execution: Option<WorkflowExecution>,
    pub activity_id: String,
    pub activity_type: String,
    pub task_queue: String,
    pub heartbeat_timeout: Option<Duration>,
    /// Time of activity scheduled by a workflow
    pub scheduled_time: Option<Timestamp>,
    /// Time of activity start
    pub started_time: Option<Timestamp>,
    /// Time of activity timeout
    pub deadline: Option<Timestamp>,
    /// Attempt starts from 1, and increased by 1 for every retry if retry policy is specified.
    pub attempt: u32,

    /// #NOTE Not in sdk-go
    pub current_attempt_scheduled_time: Option<Timestamp>,
    pub retry_policy: Option<RetryPolicy>,
    pub is_local: bool,
}

impl ActContext {
    /// Construct new Activity Context
    pub fn new(
        worker: Arc<dyn Worker>,
        task_queue: String,
        task_token: Vec<u8>,
        task: activity_task::Start,
    ) -> Self {
        let activity_task::Start {
            workflow_namespace,
            workflow_type,
            workflow_execution,
            activity_id,
            activity_type,
            header_fields,
            input,
            heartbeat_details,
            scheduled_time,
            current_attempt_scheduled_time,
            started_time,
            attempt,
            schedule_to_close_timeout,
            start_to_close_timeout,
            heartbeat_timeout,
            retry_policy,
            is_local,
        } = task;
        let deadline = calculate_deadline(
            scheduled_time.as_ref(),
            started_time.as_ref(),
            start_to_close_timeout.as_ref(),
            schedule_to_close_timeout.as_ref(),
        );

        ActContext {
            worker,
            input,
            heartbeat_details,
            header_fields,
            info: ActivityInfo {
                task_token,
                task_queue,
                workflow_type,
                workflow_namespace,
                workflow_execution,
                activity_id,
                activity_type,
                heartbeat_timeout,
                scheduled_time,
                started_time,
                deadline,
                attempt,

                current_attempt_scheduled_time,
                retry_policy,
                is_local,
            },
        }
    }

    /// Retrieve extra parameters.  The first input is always popped and passed to the
    /// ActivityFuntion for the currently executing activity.  However, if more parameters are
    /// passed, perhaps from another language's SDK, explicit access is available from extra_inputs
    pub fn extra_inputs(&mut self) -> &mut [Payload] {
        &mut self.input
    }

    /// Extract heartbeat details from last failed attempt. This is used in combination with retry policy.
    pub fn get_heartbeat_details(&self) -> &[Payload] {
        &self.heartbeat_details
    }

    /// RecordHeartbeat sends heartbeat for the currently executing activity
    pub fn record_heartbeat(&self, details: Vec<Payload>) {
        self.worker.record_activity_heartbeat(ActivityHeartbeat {
            task_token: self.info.task_token.clone(),
            details,
        })
    }

    /// Get activity info of the executing activity
    pub fn get_info(&self) -> &ActivityInfo {
        &self.info
    }
}

/// Deadline calculation.  This is a port of
/// https://github.com/temporalio/sdk-go/blob/8651550973088f27f678118f997839fb1bb9e62f/internal/activity.go#L225
fn calculate_deadline(
    scheduled_time: Option<&Timestamp>,
    started_time: Option<&Timestamp>,
    start_to_close_timeout: Option<&Duration>,
    schedule_to_close_timeout: Option<&Duration>,
) -> Option<Timestamp> {
    match (
        scheduled_time,
        started_time,
        start_to_close_timeout,
        schedule_to_close_timeout,
    ) {
        (
            Some(scheduled),
            Some(started),
            Some(start_to_close_timeout),
            Some(schedule_to_close_timeout),
        ) => {
            let scheduled: SystemTime = maybe_convert_timestamp(scheduled)?;
            let started: SystemTime = maybe_convert_timestamp(started)?;
            let start_to_close_timeout: StdDuration =
                start_to_close_timeout.clone().try_into().ok()?;
            let schedule_to_close_timeout: StdDuration =
                schedule_to_close_timeout.clone().try_into().ok()?;

            let start_to_close_deadline: SystemTime =
                started.checked_add(start_to_close_timeout)?;
            if schedule_to_close_timeout > StdDuration::ZERO {
                let schedule_to_close_deadline =
                    scheduled.checked_add(schedule_to_close_timeout)?;
                // Minimum of the two deadlines.
                if schedule_to_close_deadline < start_to_close_deadline {
                    Some(schedule_to_close_deadline.into())
                } else {
                    Some(start_to_close_deadline.into())
                }
            } else {
                Some(start_to_close_deadline.into())
            }
        }
        _ => None,
    }
}

/// Helper function lifted from prost_types::Timestamp implementation to prevent double cloning in
/// error construction
fn maybe_convert_timestamp(timestamp: &Timestamp) -> Option<SystemTime> {
    let mut timestamp = timestamp.clone();
    timestamp.normalize();

    let system_time = if timestamp.seconds >= 0 {
        std::time::UNIX_EPOCH.checked_add(StdDuration::from_secs(timestamp.seconds as u64))
    } else {
        std::time::UNIX_EPOCH.checked_sub(StdDuration::from_secs((-timestamp.seconds) as u64))
    };

    system_time.and_then(|system_time| {
        system_time.checked_add(StdDuration::from_nanos(timestamp.nanos as u64))
    })
}
