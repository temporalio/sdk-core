mod history_info;
pub(crate) use history_info::HistoryInfo;
pub(crate) use history_info::HistoryInfoError;

use crate::{
    protos::temporal::api::common::v1::WorkflowExecution,
    protos::temporal::api::history::v1::History,
    protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse,
    task_token::TaskToken,
};
use std::convert::TryFrom;

/// A validated version of a [PollWorkflowTaskQueueResponse]
pub(crate) struct ValidPollWFTQResponse {
    pub task_token: TaskToken,
    pub workflow_execution: WorkflowExecution,
    pub history: History,
    pub next_page_token: Vec<u8>,
}

impl TryFrom<PollWorkflowTaskQueueResponse> for ValidPollWFTQResponse {
    /// We return the poll response itself if it was invalid
    type Error = PollWorkflowTaskQueueResponse;

    fn try_from(value: PollWorkflowTaskQueueResponse) -> Result<Self, Self::Error> {
        match value {
            PollWorkflowTaskQueueResponse {
                task_token,
                workflow_execution: Some(workflow_execution),
                history: Some(history),
                next_page_token,
                ..
            } => Ok(Self {
                task_token: TaskToken(task_token),
                workflow_execution,
                history,
                next_page_token,
            }),
            _ => Err(value),
        }
    }
}
