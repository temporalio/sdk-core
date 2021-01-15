use tonic::codegen::Future;
use tonic::{Response, Status};

use crate::pollers::poll_task;
use crate::protos::temporal::api::enums::v1 as enums;
use crate::protos::temporal::api::taskqueue::v1 as tq;
use crate::protos::temporal::api::workflowservice::v1 as temporal;
use crate::protos::temporal::api::workflowservice::v1::{
    PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse,
};

struct WorkflowPollTask<'a> {
    service:
        &'a mut temporal::workflow_service_client::WorkflowServiceClient<tonic::transport::Channel>,
    namespace: String,
    task_queue: String,
    identity: String,
    binary_checksum: String,
}

#[async_trait::async_trait]
impl poll_task::PollTask<temporal::PollWorkflowTaskQueueResponse> for WorkflowPollTask<'_> {
    async fn poll(&mut self) -> poll_task::Result<PollWorkflowTaskQueueResponse> {
        let request = tonic::Request::new(temporal::PollWorkflowTaskQueueRequest {
            namespace: self.namespace.to_string(),
            task_queue: Some(tq::TaskQueue {
                name: self.task_queue.to_string(),
                kind: enums::TaskQueueKind::Unspecified as i32,
            }),
            identity: self.identity.to_string(),
            binary_checksum: self.binary_checksum.to_string(),
        });

        Ok(self
            .service
            .poll_workflow_task_queue(request)
            .await?
            .into_inner())
    }
}
