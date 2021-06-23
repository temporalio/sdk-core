use crate::{
    machines::ProtoCommand,
    protos::{
        coresdk::{
            workflow_activation::QueryWorkflow,
            workflow_commands::{workflow_command, QueryResult, WorkflowCommand},
            workflow_completion::{wf_activation_completion, WfActivationCompletion},
            PayloadsExt,
        },
        temporal::api::common::v1::WorkflowExecution,
        temporal::api::history::v1::History,
        temporal::api::query::v1::WorkflowQuery,
        temporal::api::taskqueue::v1::StickyExecutionAttributes,
        temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse,
    },
    task_token::TaskToken,
};
use std::convert::TryFrom;

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
        if value.query.is_some()
            && value
                .history
                .as_ref()
                .map(|h| !h.events.is_empty())
                .unwrap_or(false)
        {
            // TODO: Apparently this is totally possible. Need to allow both kinds of responses
            //   at the same time -- add test
            error!("Poll WFTQ response had a `query` field with a nonempty history");
            return Err(value);
        }

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

/// Makes converting outgoing lang commands into [WfActivationCompletion]s easier
pub trait IntoCompletion {
    /// The conversion function
    fn into_completion(self, run_id: String) -> WfActivationCompletion;
}

impl IntoCompletion for workflow_command::Variant {
    fn into_completion(self, run_id: String) -> WfActivationCompletion {
        WfActivationCompletion::from_cmd(self, run_id)
    }
}

impl<I, V> IntoCompletion for I
where
    I: IntoIterator<Item = V>,
    V: Into<WorkflowCommand>,
{
    fn into_completion(self, run_id: String) -> WfActivationCompletion {
        let success = self.into_iter().map(Into::into).collect::<Vec<_>>().into();
        WfActivationCompletion {
            run_id,
            status: Some(wf_activation_completion::Status::Successful(success)),
        }
    }
}

/// A version of [RespondWorkflowTaskCompletedRequest] that will finish being filled out by the
/// server client
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowTaskCompletion {
    /// The task token that would've been received from [crate::Core::poll_workflow_task] API.
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
