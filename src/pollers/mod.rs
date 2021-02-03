use std::time::Duration;

use crate::{
    protos::temporal::api::enums::v1::TaskQueueKind,
    protos::temporal::api::taskqueue::v1::TaskQueue,
    protos::temporal::api::workflowservice::v1::workflow_service_client::WorkflowServiceClient,
    protos::temporal::api::workflowservice::v1::{
        PollWorkflowTaskQueueRequest, PollWorkflowTaskQueueResponse,
    },
    Result, WorkflowTaskProvider,
};
use tonic::{transport::Channel, Request, Status};
use url::Url;

#[derive(Clone)]
pub(crate) struct ServerGatewayOptions {
    pub namespace: String,
    pub identity: String,
    pub worker_binary_id: String,
    pub long_poll_timeout: Duration,
}

impl ServerGatewayOptions {
    pub(crate) async fn connect(&self, target_url: Url) -> Result<ServerGateway> {
        let channel = Channel::from_shared(target_url.to_string())?
            .connect()
            .await?;
        let service = WorkflowServiceClient::with_interceptor(channel, intercept);
        Ok(ServerGateway {
            service,
            opts: self.clone(),
        })
    }
}

/// This function will get called on each outbound request. Returning a
/// `Status` here will cancel the request and have that status returned to
/// the caller.
fn intercept(mut req: Request<()>) -> Result<Request<()>, Status> {
    // TODO convert error
    let metadata = req.metadata_mut();
    metadata.insert("grpc-timeout", "50000m".parse().unwrap());
    metadata.insert("client-name", "core-sdk".parse().unwrap());
    println!("Intercepting request: {:?}", req);
    Ok(req)
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
