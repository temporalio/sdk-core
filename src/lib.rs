use crate::protos::coresdk::poll_sdk_task_resp::Task::WfTask;
use crate::protos::coresdk::SdkwfTask;
use crate::protos::temporal::api::workflowservice::v1::workflow_service_client::WorkflowServiceClient;
use crate::protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;
use crate::SDKServiceError::{WorkflowServiceClientConnectionError, WorkflowServicePollError};
use pollers::poll_task::PollTask;
use pollers::workflow_poll_task::WorkflowPollTask;
use protos::coresdk::{CompleteSdkTaskReq, CompleteSdkTaskResp, PollSdkTaskReq, PollSdkTaskResp};
use tonic::transport::{Channel, Error};
use tonic::Status;

mod machines;
mod pollers;
pub mod protos;

type Result<T, E = SDKServiceError> = std::result::Result<T, E>;

// TODO: Should probably enforce Send + Sync
#[async_trait::async_trait]
pub trait CoreSDKService {
    async fn poll_sdk_task(&self, req: PollSdkTaskReq) -> Result<PollSdkTaskResp>;
    async fn complete_sdk_task(&self, req: CompleteSdkTaskReq) -> Result<CompleteSdkTaskResp>;
}

#[derive(thiserror::Error, Debug)]
pub enum SDKServiceError {
    #[error("Failed to connect to temporal service")]
    WorkflowServiceClientConnectionError(#[from] tonic::transport::Error),
    #[error("Failed to poll a task")]
    WorkflowServicePollError(#[from] tonic::Status),
}

struct CoreSdkGateway {}

#[async_trait::async_trait]
impl CoreSDKService for CoreSdkGateway {
    #[allow(unused)]
    async fn poll_sdk_task(&self, req: PollSdkTaskReq) -> Result<PollSdkTaskResp, SDKServiceError> {
        // TODO move connection creation out of the poll API and reuse it across calls.
        let mut client = WorkflowServiceClient::connect("http://[::1]:7233").await?;
        let mut poll_task = WorkflowPollTask::new(
            &mut client,
            String::from(""), // TODO populate
            String::from(""), // TODO populate
            String::from(""), // TODO populate
            String::from(""), // TODO populate
        );
        let response = poll_task.poll().await?;
        // TODO instead of returning poll response we should run state machine and return an event.
        Ok(PollSdkTaskResp {
            task_token: vec![],
            task: Some(WfTask(SdkwfTask {
                original: Some(response),
            })),
        })
    }

    async fn complete_sdk_task(
        &self,
        req: CompleteSdkTaskReq,
    ) -> Result<CompleteSdkTaskResp, SDKServiceError> {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn foo() {}
}
