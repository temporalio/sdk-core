use crate::protos::temporal::api::enums::v1::TaskQueueKind;
use crate::protos::temporal::api::taskqueue::v1::TaskQueue;
use crate::protos::temporal::api::workflowservice::v1::workflow_service_client::WorkflowServiceClient;
use crate::protos::temporal::api::workflowservice::v1::{
    PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse,
};
use crate::Result;
use crate::WorkflowTaskProvider;
use url::Url;

#[derive(Clone)]
pub(crate) struct ServerGatewayOptions {
    pub namespace: String,
    pub identity: String,
    pub worker_binary_id: String,
}

impl ServerGatewayOptions {
    pub(crate) async fn connect(&self, target_url: Url) -> Result<ServerGateway> {
        let service = WorkflowServiceClient::connect(target_url.to_string()).await?;
        Ok(ServerGateway {
            service,
            opts: self.clone(),
        })
    }
}
/// Provides
pub(crate) struct ServerGateway {
    service: WorkflowServiceClient<tonic::transport::Channel>,
    opts: ServerGatewayOptions,
}

impl ServerGateway {
    async fn poll(&self, task_queue: &str) -> Result<PollWorkflowTaskQueueResponse> {
        let request = tonic::Request::new(PollWorkflowTaskQueueRequest {
            namespace: self.opts.namespace.to_string(),
            task_queue: Some(TaskQueue {
                name: task_queue.to_string(),
                kind: TaskQueueKind::Unspecified as i32,
            }),
            identity: self.opts.identity.to_string(),
            binary_checksum: self.opts.worker_binary_id.to_string(),
        });

        Ok(self
            .service
            .clone()
            .poll_workflow_task_queue(request)
            .await?
            .into_inner())
    }
}

#[async_trait::async_trait]
impl WorkflowTaskProvider for ServerGateway {
    async fn get_work(&self, task_queue: &str) -> Result<PollWorkflowTaskQueueResponse> {
        self.poll(task_queue).await
    }
}
