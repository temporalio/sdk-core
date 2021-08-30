use crate::{
    machines::{ProtoCommand, HAS_CHANGE_MARKER_NAME},
    task_token::TaskToken,
    workflow::LEGACY_QUERY_ID,
};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    coresdk::{
        common::decode_change_marker_details,
        workflow_activation::{wf_activation_job, QueryWorkflow, WfActivation, WfActivationJob},
        workflow_commands::{query_result, QueryResult},
        workflow_completion, FromPayloadsExt,
    },
    temporal::api::{
        common::v1::WorkflowExecution,
        enums::v1::EventType,
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
                history: Some(history),
                next_page_token,
                attempt,
                previous_started_event_id,
                started_event_id,
                query,
                queries,
                ..
            } => {
                if !next_page_token.is_empty() {
                    // TODO: Support history pagination
                    unimplemented!("History pagination not yet implemented");
                }

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
    /// If this history event represents a `changed` marker, return the info about
    /// it. Returns `None` if it is any other kind of event or marker.
    fn get_changed_marker_details(&self) -> Option<(String, bool)>;
}

impl HistoryEventExt for HistoryEvent {
    fn get_changed_marker_details(&self) -> Option<(String, bool)> {
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
}
