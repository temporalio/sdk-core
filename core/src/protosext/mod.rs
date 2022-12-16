use crate::{
    worker::{LocalActivityExecutionResult, LEGACY_QUERY_ID},
    CompleteActivityError, TaskToken,
};
use anyhow::anyhow;
use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt::{Debug, Display, Formatter},
    time::{Duration, SystemTime},
};
use temporal_sdk_core_protos::{
    constants::{LOCAL_ACTIVITY_MARKER_NAME, PATCH_MARKER_NAME},
    coresdk::{
        activity_result::{activity_execution_result, activity_execution_result::Status},
        common::{
            decode_change_marker_details, extract_local_activity_marker_data,
            extract_local_activity_marker_details,
        },
        external_data::LocalActivityMarkerData,
        workflow_activation::{
            query_to_job, workflow_activation_job, QueryWorkflow, WorkflowActivation,
            WorkflowActivationJob,
        },
        workflow_commands::{
            query_result, ActivityCancellationType, QueryResult, ScheduleLocalActivity,
        },
        workflow_completion,
    },
    temporal::api::{
        common::v1::{Payload, RetryPolicy, WorkflowExecution},
        enums::v1::EventType,
        failure::v1::Failure,
        history::v1::{history_event, History, HistoryEvent, MarkerRecordedEventAttributes},
        query::v1::WorkflowQuery,
        workflowservice::v1::PollWorkflowTaskQueueResponse,
    },
    utilities::TryIntoOrNone,
};

/// A validated version of a [PollWorkflowTaskQueueResponse]
#[derive(Clone, PartialEq)]
#[allow(clippy::manual_non_exhaustive)] // Clippy doesn't understand it's only for *in* this crate
pub struct ValidPollWFTQResponse {
    pub task_token: TaskToken,
    pub task_queue: String,
    pub workflow_execution: WorkflowExecution,
    pub workflow_type: String,
    pub history: History,
    pub next_page_token: Vec<u8>,
    pub attempt: u32,
    pub previous_started_event_id: i64,
    pub started_event_id: i64,
    /// If this is present, `history` will be empty. This is not a very "tight" design, but it's
    /// enforced at construction time. From the `query` field.
    pub legacy_query: Option<WorkflowQuery>,
    /// Query requests from the `queries` field
    pub query_requests: Vec<QueryWorkflow>,

    /// Zero-size field to prevent explicit construction
    _cant_construct_me: (),
}

impl Debug for ValidPollWFTQResponse {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ValidWFT {{ task_token: {}, task_queue: {}, workflow_execution: {:?}, \
             workflow_type: {}, attempt: {}, previous_started_event_id: {}, started_event_id {}, \
             history_length: {}, first_evt_in_hist_id: {:?}, legacy_query: {:?}, queries: {:?} }}",
            self.task_token,
            self.task_queue,
            self.workflow_execution,
            self.workflow_type,
            self.attempt,
            self.previous_started_event_id,
            self.started_event_id,
            self.history.events.len(),
            self.history.events.get(0).map(|e| e.event_id),
            self.legacy_query,
            self.query_requests
        )
    }
}

impl TryFrom<PollWorkflowTaskQueueResponse> for ValidPollWFTQResponse {
    /// We return the poll response itself if it was invalid
    type Error = PollWorkflowTaskQueueResponse;

    fn try_from(value: PollWorkflowTaskQueueResponse) -> Result<Self, Self::Error> {
        match value {
            PollWorkflowTaskQueueResponse {
                task_token,
                workflow_execution_task_queue: Some(tq),
                workflow_execution: Some(workflow_execution),
                workflow_type: Some(workflow_type),
                history: Some(history),
                next_page_token,
                attempt,
                previous_started_event_id,
                started_event_id,
                query,
                queries,
                ..
            } => {
                let query_requests = queries
                    .into_iter()
                    .map(|(id, q)| query_to_job(id, q))
                    .collect();

                Ok(Self {
                    task_token: TaskToken(task_token),
                    task_queue: tq.name,
                    workflow_execution,
                    workflow_type: workflow_type.name,
                    history,
                    next_page_token,
                    attempt: attempt as u32,
                    previous_started_event_id,
                    started_event_id,
                    legacy_query: query,
                    query_requests,
                    _cant_construct_me: (),
                })
            }
            _ => Err(value),
        }
    }
}

pub(crate) trait WorkflowActivationExt {
    /// Returns true if this activation has one and only one job to perform a legacy query
    fn is_legacy_query(&self) -> bool;
}

impl WorkflowActivationExt for WorkflowActivation {
    fn is_legacy_query(&self) -> bool {
        matches!(&self.jobs.as_slice(), &[WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::QueryWorkflow(qr))
                }] if qr.query_id == LEGACY_QUERY_ID)
    }
}

/// Create a legacy query failure result
pub(crate) fn legacy_query_failure(fail: workflow_completion::Failure) -> QueryResult {
    QueryResult {
        query_id: LEGACY_QUERY_ID.to_string(),
        variant: Some(query_result::Variant::Failed(
            fail.failure.unwrap_or_default(),
        )),
    }
}

pub(crate) trait HistoryEventExt {
    /// If this history event represents a `patched` marker, return the info about
    /// it. Returns `None` if it is any other kind of event or marker.
    fn get_patch_marker_details(&self) -> Option<(String, bool)>;
    /// If this history event represents a local activity marker, return true.
    fn is_local_activity_marker(&self) -> bool;
    /// If this history event represents a local activity marker, return the marker id info.
    /// Returns `None` if it is any other kind of event or marker or the data is invalid.
    fn extract_local_activity_marker_data(&self) -> Option<LocalActivityMarkerData>;
    /// If this history event represents a local activity marker, return all the contained data.
    /// Returns `None` if it is any other kind of event or marker or the data is invalid.
    fn into_local_activity_marker_details(self) -> Option<CompleteLocalActivityData>;
}

impl HistoryEventExt for HistoryEvent {
    fn get_patch_marker_details(&self) -> Option<(String, bool)> {
        if self.event_type() == EventType::MarkerRecorded {
            match &self.attributes {
                Some(history_event::Attributes::MarkerRecordedEventAttributes(
                    MarkerRecordedEventAttributes {
                        marker_name,
                        details,
                        ..
                    },
                )) if marker_name == PATCH_MARKER_NAME => decode_change_marker_details(details),
                _ => None,
            }
        } else {
            None
        }
    }

    fn is_local_activity_marker(&self) -> bool {
        if self.event_type() == EventType::MarkerRecorded {
            return matches!(&self.attributes,
                Some(history_event::Attributes::MarkerRecordedEventAttributes(
                    MarkerRecordedEventAttributes { marker_name, .. },
                )) if marker_name == LOCAL_ACTIVITY_MARKER_NAME
            );
        }
        false
    }

    fn extract_local_activity_marker_data(&self) -> Option<LocalActivityMarkerData> {
        if self.event_type() == EventType::MarkerRecorded {
            match &self.attributes {
                Some(history_event::Attributes::MarkerRecordedEventAttributes(
                    MarkerRecordedEventAttributes {
                        marker_name,
                        details,
                        ..
                    },
                )) if marker_name == LOCAL_ACTIVITY_MARKER_NAME => {
                    extract_local_activity_marker_data(details)
                }
                _ => None,
            }
        } else {
            None
        }
    }

    fn into_local_activity_marker_details(self) -> Option<CompleteLocalActivityData> {
        if self.event_type() == EventType::MarkerRecorded {
            match self.attributes {
                Some(history_event::Attributes::MarkerRecordedEventAttributes(
                    MarkerRecordedEventAttributes {
                        marker_name,
                        mut details,
                        failure,
                        ..
                    },
                )) if marker_name == LOCAL_ACTIVITY_MARKER_NAME => {
                    let (data, ok_res) = extract_local_activity_marker_details(&mut details);
                    let data = data?;
                    let result = match (ok_res, failure) {
                        (Some(r), None) => Ok(r),
                        (None | Some(_), Some(f)) => Err(f),
                        (None, None) => Ok(Default::default()),
                    };
                    Some(CompleteLocalActivityData {
                        marker_dat: data,
                        result,
                    })
                }
                _ => None,
            }
        } else {
            None
        }
    }
}

pub(crate) struct CompleteLocalActivityData {
    pub marker_dat: LocalActivityMarkerData,
    pub result: Result<Payload, Failure>,
}

pub(crate) fn validate_activity_completion(
    status: &activity_execution_result::Status,
) -> Result<(), CompleteActivityError> {
    match status {
        Status::Completed(c) if c.result.is_none() => {
            Err(CompleteActivityError::MalformedActivityCompletion {
                reason: "Activity completions must contain a `result` payload \
                             (which may be empty)"
                    .to_string(),
                completion: None,
            })
        }
        _ => Ok(()),
    }
}

impl TryFrom<activity_execution_result::Status> for LocalActivityExecutionResult {
    type Error = CompleteActivityError;

    fn try_from(s: activity_execution_result::Status) -> Result<Self, Self::Error> {
        match s {
            Status::Completed(c) => Ok(LocalActivityExecutionResult::Completed(c)),
            Status::Failed(f)
                if f.failure
                    .as_ref()
                    .map(|fail| fail.is_timeout().is_some())
                    .unwrap_or_default() =>
            {
                Ok(LocalActivityExecutionResult::TimedOut(f))
            }
            Status::Failed(f) => Ok(LocalActivityExecutionResult::Failed(f)),
            Status::Cancelled(cancel) => Ok(LocalActivityExecutionResult::Cancelled(cancel)),
            Status::WillCompleteAsync(_) => {
                Err(CompleteActivityError::MalformedActivityCompletion {
                    reason: "Local activities cannot be completed async".to_string(),
                    completion: None,
                })
            }
        }
    }
}

/// Validated version of [ScheduleLocalActivity]. See it for field docs.
/// One or both of `schedule_to_close_timeout` and `start_to_close_timeout` are guaranteed to exist.
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(Default))]
pub struct ValidScheduleLA {
    pub seq: u32,
    pub activity_id: String,
    pub activity_type: String,
    pub attempt: u32,
    pub original_schedule_time: Option<SystemTime>,
    pub headers: HashMap<String, Payload>,
    pub arguments: Vec<Payload>,
    pub schedule_to_start_timeout: Option<Duration>,
    pub close_timeouts: LACloseTimeouts,
    pub retry_policy: RetryPolicy,
    pub local_retry_threshold: Duration,
    pub cancellation_type: ActivityCancellationType,
}

#[derive(Debug, Clone, Copy)]
pub enum LACloseTimeouts {
    ScheduleOnly(Duration),
    StartOnly(Duration),
    Both { sched: Duration, start: Duration },
}

impl LACloseTimeouts {
    /// Splits into (schedule_to_close, start_to_close) options, one or both of which is guaranteed
    /// to be populated
    pub fn into_sched_and_start(self) -> (Option<Duration>, Option<Duration>) {
        match self {
            LACloseTimeouts::ScheduleOnly(x) => (Some(x), None),
            LACloseTimeouts::StartOnly(x) => (None, Some(x)),
            LACloseTimeouts::Both { sched, start } => (Some(sched), Some(start)),
        }
    }
}

#[cfg(test)]
impl Default for LACloseTimeouts {
    fn default() -> Self {
        LACloseTimeouts::ScheduleOnly(Duration::from_secs(100))
    }
}

impl ValidScheduleLA {
    pub fn from_schedule_la(
        v: ScheduleLocalActivity,
        wf_exe_timeout: Option<Duration>,
    ) -> Result<Self, anyhow::Error> {
        let original_schedule_time = v
            .original_schedule_time
            .map(|x| {
                x.try_into()
                    .map_err(|_| anyhow!("Could not convert original_schedule_time"))
            })
            .transpose()?;
        let sched_to_close = v
            .schedule_to_close_timeout
            .map(|x| {
                x.try_into()
                    .map_err(|_| anyhow!("Could not convert schedule_to_close_timeout"))
            })
            .transpose()?
            // Default to execution timeout if unset
            .or(wf_exe_timeout);
        let mut schedule_to_start_timeout = v
            .schedule_to_start_timeout
            .map(|x| {
                x.try_into()
                    .map_err(|_| anyhow!("Could not convert schedule_to_start_timeout"))
            })
            .transpose()?;
        // Clamp schedule-to-start if larger than schedule-to-close
        if let Some((sched_to_start, sched_to_close)) =
            schedule_to_start_timeout.as_mut().zip(sched_to_close)
        {
            if *sched_to_start > sched_to_close {
                *sched_to_start = sched_to_close;
            }
        }
        let close_timeouts = match (
            sched_to_close,
            v.start_to_close_timeout
                .map(|x| {
                    x.try_into()
                        .map_err(|_| anyhow!("Could not convert start_to_close_timeout"))
                })
                .transpose()?,
        ) {
            (Some(sch), None) => LACloseTimeouts::ScheduleOnly(sch),
            (None, Some(start)) => LACloseTimeouts::StartOnly(start),
            (Some(sched), Some(mut start)) => {
                // Clamp start-to-close if larger than schedule-to-close
                if start > sched {
                    start = sched;
                }
                LACloseTimeouts::Both { sched, start }
            }
            (None, None) => {
                return Err(anyhow!(
                    "One of schedule_to_close or start_to_close timeouts must be set"
                ))
            }
        };
        let retry_policy = v.retry_policy.unwrap_or_default();
        let local_retry_threshold = v
            .local_retry_threshold
            .clone()
            .try_into_or_none()
            .unwrap_or_else(|| Duration::from_secs(60));
        let cancellation_type = ActivityCancellationType::from_i32(v.cancellation_type)
            .unwrap_or(ActivityCancellationType::WaitCancellationCompleted);
        Ok(ValidScheduleLA {
            seq: v.seq,
            activity_id: v.activity_id,
            activity_type: v.activity_type,
            attempt: v.attempt,
            original_schedule_time,
            headers: v.headers,
            arguments: v.arguments,
            schedule_to_start_timeout,
            close_timeouts,
            retry_policy,
            local_retry_threshold,
            cancellation_type,
        })
    }
}

impl Display for ValidScheduleLA {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ValidScheduleLA({}, {})", self.seq, self.activity_type)
    }
}
