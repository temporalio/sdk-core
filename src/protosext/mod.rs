use crate::{
    machines::{
        LocalActivityExecutionResult, ProtoCommand, HAS_CHANGE_MARKER_NAME,
        LOCAL_ACTIVITY_MARKER_NAME,
    },
    task_token::TaskToken,
    workflow::LEGACY_QUERY_ID,
    CompleteActivityError,
};
use anyhow::anyhow;
use std::{
    collections::HashMap,
    convert::TryFrom,
    fmt::{Display, Formatter},
    time::Duration,
};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{activity_execution_result, activity_execution_result::Status},
        common::{
            decode_change_marker_details, extract_local_activity_marker_data,
            extract_local_activity_marker_details, Payload as SDKPayload, RetryPolicy,
        },
        external_data::LocalActivityMarkerData,
        workflow_activation::{wf_activation_job, QueryWorkflow, WfActivation, WfActivationJob},
        workflow_commands::{
            query_result, ActivityCancellationType, QueryResult, ScheduleLocalActivity,
        },
        workflow_completion, FromPayloadsExt,
    },
    temporal::api::{
        common::v1::{Payload, WorkflowExecution},
        enums::v1::EventType,
        failure::v1::Failure,
        history::v1::{history_event, History, HistoryEvent, MarkerRecordedEventAttributes},
        query::v1::WorkflowQuery,
        taskqueue::v1::StickyExecutionAttributes,
        workflowservice::v1::PollWorkflowTaskQueueResponse,
    },
};

/// A validated version of a [PollWorkflowTaskQueueResponse]
#[derive(Debug, Clone, PartialEq)]
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
                    .map(|(id, q)| QueryWorkflow {
                        query_id: id,
                        query_type: q.query_type,
                        arguments: Vec::from_payloads(q.query_args),
                    })
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

/// A version of [RespondWorkflowTaskCompletedRequest] that will finish being filled out by the
/// server client
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowTaskCompletion {
    /// The task token that would've been received from [crate::Core::poll_workflow_activation] API.
    pub task_token: TaskToken,
    /// A list of new commands to send to the server, such as starting a timer.
    pub commands: Vec<ProtoCommand>,
    /// If set, indicate that next task should be queued on sticky queue with given attributes.
    pub sticky_attributes: Option<StickyExecutionAttributes>,
    /// Responses to queries in the `queries` field of the workflow task.
    pub query_responses: Vec<QueryResult>,
    pub return_new_workflow_task: bool,
    pub force_create_new_workflow_task: bool,
}

pub(crate) trait WfActivationExt {
    /// Returns true if this activation has one and only one job to perform a legacy query
    fn is_legacy_query(&self) -> bool;
}

impl WfActivationExt for WfActivation {
    fn is_legacy_query(&self) -> bool {
        matches!(&self.jobs.as_slice(), &[WfActivationJob {
                    variant: Some(wf_activation_job::Variant::QueryWorkflow(qr))
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
                )) if marker_name == HAS_CHANGE_MARKER_NAME => {
                    decode_change_marker_details(details)
                }
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
                    let result = if let Some(r) = ok_res {
                        Ok(r)
                    } else {
                        let fail = failure?;
                        Err(fail)
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

impl TryFrom<activity_execution_result::Status> for LocalActivityExecutionResult {
    type Error = CompleteActivityError;

    fn try_from(s: activity_execution_result::Status) -> Result<Self, Self::Error> {
        match s {
            Status::Completed(c) => Ok(LocalActivityExecutionResult::Completed(c)),
            Status::Failed(f) => Ok(LocalActivityExecutionResult::Failed(f)),
            Status::Cancelled(cancel) => Ok(LocalActivityExecutionResult::Cancelled {
                cancel,
                do_not_record_marker: false,
            }),
            Status::WillCompleteAsync(_) => {
                Err(CompleteActivityError::MalformedActivityCompletion {
                    reason: "Local activities cannot be completed async".to_string(),
                    completion: None,
                })
            }
        }
    }
}

pub trait TryIntoOrNone<F, T> {
    /// Turn an option of something into an option of another thing, trying to convert along the way
    /// and returning `None` if that conversion fails
    fn try_into_or_none(self) -> Option<T>;
}

impl<F, T> TryIntoOrNone<F, T> for Option<F>
where
    F: TryInto<T>,
{
    fn try_into_or_none(self) -> Option<T> {
        self.map(TryInto::try_into).transpose().ok().flatten()
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
    pub header_fields: HashMap<String, SDKPayload>,
    pub arguments: Vec<SDKPayload>,
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
        let sched_to_close = v
            .schedule_to_close_timeout
            .map(|x| {
                x.try_into()
                    .map_err(|_| anyhow!("Could not convert schedule_to_close_timeout"))
            })
            .transpose()?
            // Default to execution timeout if unset
            .or(wf_exe_timeout);
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
            (Some(sched), Some(start)) => LACloseTimeouts::Both { sched, start },
            (None, None) => {
                return Err(anyhow!(
                    "One of schedule_to_close or start_to_close timeouts must be set"
                ))
            }
        };
        let schedule_to_start_timeout = v
            .schedule_to_start_timeout
            .map(|x| {
                x.try_into()
                    .map_err(|_| anyhow!("Could not convert schedule_to_start_timeout"))
            })
            .transpose()?;
        let retry_policy = v
            .retry_policy
            .ok_or(anyhow!("Retry policy must be defined!"))?;
        let local_retry_threshold = v
            .local_retry_threshold
            .clone()
            .try_into_or_none()
            .unwrap_or_else(|| Duration::from_secs(60));
        let cancellation_type = ActivityCancellationType::from_i32(v.cancellation_type)
            .unwrap_or(ActivityCancellationType::WaitCancellationCompleted);
        // TODO: Clamps
        Ok(ValidScheduleLA {
            seq: v.seq,
            activity_id: v.activity_id,
            activity_type: v.activity_type,
            attempt: v.attempt,
            header_fields: v.header_fields,
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
