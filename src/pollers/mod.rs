use crate::protos::temporal::api::enums::v1::TaskQueueKind;
use crate::protos::temporal::api::taskqueue::v1::TaskQueue;
use crate::protos::temporal::api::workflowservice::v1::workflow_service_client::WorkflowServiceClient;
use crate::protos::temporal::api::workflowservice::v1::{
    PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse,
};
use crate::Result;
use crate::WorkflowTaskProvider;
use url::Url;

/// Provides
pub(crate) struct ServerGateway {
    service: WorkflowServiceClient<tonic::transport::Channel>,
    namespace: String,
    task_queue: String,
    identity: String,
    binary_checksum: String,
}

impl ServerGateway {
    async fn poll(&self) -> Result<PollWorkflowTaskQueueResponse> {
        let request = tonic::Request::new(PollWorkflowTaskQueueRequest {
            namespace: self.namespace.to_string(),
            task_queue: Some(TaskQueue {
                name: self.task_queue.to_string(),
                kind: TaskQueueKind::Unspecified as i32,
            }),
            identity: self.identity.to_string(),
            binary_checksum: self.binary_checksum.to_string(),
        });

        Ok(self
            .service
            .clone()
            .poll_workflow_task_queue(request)
            .await?
            .into_inner())
    }

    pub(crate) async fn connect(target_url: Url, task_queue: String) -> Result<Self> {
        let service = WorkflowServiceClient::connect(target_url.to_string()).await?;
        Ok(Self {
            service,
            namespace: "".to_string(),
            task_queue,
            identity: "".to_string(),
            binary_checksum: "".to_string(),
        })
    }
}

#[async_trait::async_trait]
impl WorkflowTaskProvider for ServerGateway {
    async fn get_work(&self, task_queue: &str) -> Result<PollWorkflowTaskQueueResponse> {
        self.poll().await
    }
}
