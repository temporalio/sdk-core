use tonic::codegen::Future;
use tonic::{Response, Status};

use crate::pollers::poll_task::{PollTask, Result};
use crate::protos::temporal::api::enums::v1 as enums;
use crate::protos::temporal::api::taskqueue::v1 as tq;
use crate::protos::temporal::api::workflowservice::v1 as temporal;
use crate::protos::temporal::api::workflowservice::v1::{
    PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse,
};
use temporal::workflow_service_client::WorkflowServiceClient;

pub(crate) struct WorkflowPollTask<'a> {
    service: &'a mut WorkflowServiceClient<tonic::transport::Channel>,
    namespace: String,
    task_queue: String,
    identity: String,
    binary_checksum: String,
}

impl WorkflowPollTask<'_> {
    pub(crate) fn new(
        service: &mut WorkflowServiceClient<tonic::transport::Channel>,
        namespace: String,
        task_queue: String,
        identity: String,
        binary_checksum: String,
    ) -> WorkflowPollTask {
        return WorkflowPollTask {
            service,
            namespace,
            task_queue,
            identity,
            binary_checksum,
        };
    }
}

#[async_trait::async_trait]
impl PollTask<temporal::PollWorkflowTaskQueueResponse> for WorkflowPollTask<'_> {
    async fn poll(&mut self) -> Result<PollWorkflowTaskQueueResponse> {
        let request = tonic::Request::new(temporal::PollWorkflowTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(tq::TaskQueue {
                name: self.task_queue.clone(),
                kind: enums::TaskQueueKind::Unspecified as i32,
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
