mod history_info;
pub(crate) use history_info::HistoryInfo;
pub(crate) use history_info::HistoryInfoError;

use crate::{
    protos::coresdk::{
        workflow_commands::{workflow_command, WorkflowCommand},
        workflow_completion::{wf_activation_completion, WfActivationCompletion},
    },
    protos::temporal::api::common::v1::WorkflowExecution,
    protos::temporal::api::history::v1::History,
    protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse,
    task_token::TaskToken,
};
use std::convert::TryFrom;

/// A validated version of a [PollWorkflowTaskQueueResponse]
#[derive(Debug, Clone, PartialEq)]
pub struct ValidPollWFTQResponse {
    pub task_token: TaskToken,
    pub task_queue: String,
    pub workflow_execution: WorkflowExecution,
    pub history: History,
    pub next_page_token: Vec<u8>,
    pub attempt: u32,
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
                ..
            } => {
                if !next_page_token.is_empty() {
                    // TODO: Support history pagination
                    unimplemented!("History pagination not yet implemented");
                }

                Ok(Self {
                    task_token: TaskToken(task_token),
                    task_queue: tq.name,
                    workflow_execution,
                    history,
                    next_page_token,
                    attempt: attempt as u32,
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
