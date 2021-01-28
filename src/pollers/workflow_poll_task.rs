use crate::{
    pollers::poll_task::PollTask,
    pollers::poll_task::Result,
    protos::temporal::api::{
        enums::v1::TaskQueueKind, taskqueue::v1::TaskQueue,
        workflowservice::v1::workflow_service_client::WorkflowServiceClient,
        workflowservice::v1::PollWorkflowTaskQueueRequest,
        workflowservice::v1::PollWorkflowTaskQueueResponse,
    },
};
use tonic::{codegen::Future, Response, Status};

struct WorkflowPollTask<'a> {
    service: &'a mut WorkflowServiceClient<tonic::transport::Channel>,
    namespace: String,
    task_queue: String,
    identity: String,
    binary_checksum: String,
}

#[async_trait::async_trait]
impl PollTask<PollWorkflowTaskQueueResponse> for WorkflowPollTask<'_> {
    async fn poll(&mut self) -> Result<PollWorkflowTaskQueueResponse> {
        let request = tonic::Request::new(PollWorkflowTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: self.task_queue.clone(),
                kind: TaskQueueKind::Unspecified as i32,
            }),
            identity: self.identity.clone(),
            binary_checksum: self.binary_checksum.clone(),
        });

        Ok(self
            .service
            .poll_workflow_task_queue(request)
            .await?
            .into_inner())
    }
}
