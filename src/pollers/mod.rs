use std::time::Duration;

use crate::{
    machines::ProtoCommand,
    protos::temporal::api::{
        enums::v1::TaskQueueKind,
        taskqueue::v1::TaskQueue,
        workflowservice::v1::{
            workflow_service_client::WorkflowServiceClient, PollWorkflowTaskQueueRequest,
            PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedRequest,
            RespondWorkflowTaskCompletedResponse,
        },
    },
    workflow::{PollWorkflowTaskQueueApi, RespondWorkflowTaskCompletedApi},
    Result,
};
use tonic::{transport::Channel, Request, Status};
use url::Url;

/// Options for the connection to the temporal server
#[derive(Clone, Debug)]
pub struct ServerGatewayOptions {
    /// The URL of the Temporal server to connect to
    pub target_url: Url,

    /// What namespace will we operate under
    pub namespace: String,

    /// A human-readable string that can identify your worker
    ///
    /// TODO: Probably belongs in future worker abstraction
    pub identity: String,

    /// A string that should be unique to the exact worker code/binary being executed
    pub worker_binary_id: String,

    /// Timeout for long polls (polling of task queues)
    pub long_poll_timeout: Duration,
}

impl ServerGatewayOptions {
    /// Attempt to establish a connection to the Temporal server
    pub async fn connect(&self) -> Result<ServerGateway> {
        let channel = Channel::from_shared(self.target_url.to_string())?
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
    let metadata = req.metadata_mut();
    // TODO: Only apply this to long poll requests
    metadata.insert(
        "grpc-timeout",
        "50000m".parse().expect("Static value is parsable"),
    );
    metadata.insert(
        "client-name",
        "core-sdk".parse().expect("Static value is parsable"),
    );
    Ok(req)
}

/// Contains an instance of a client for interacting with the temporal server
pub struct ServerGateway {
    /// Client for interacting with workflow service
    pub service: WorkflowServiceClient<tonic::transport::Channel>,
    /// Options gateway was initialized with
    pub opts: ServerGatewayOptions,
}

#[async_trait::async_trait]
impl PollWorkflowTaskQueueApi for ServerGateway {
    async fn poll(&self, task_queue: &str) -> Result<PollWorkflowTaskQueueResponse> {
        let request = PollWorkflowTaskQueueRequest {
            namespace: self.opts.namespace.to_string(),
            task_queue: Some(TaskQueue {
                name: task_queue.to_string(),
                kind: TaskQueueKind::Unspecified as i32,
            }),
            identity: self.opts.identity.to_string(),
            binary_checksum: self.opts.worker_binary_id.to_string(),
        };

        Ok(self
            .service
            .clone()
            .poll_workflow_task_queue(request)
            .await?
            .into_inner())
    }
}

#[async_trait::async_trait]
impl RespondWorkflowTaskCompletedApi for ServerGateway {
    async fn complete(
        &self,
        task_token: Vec<u8>,
        commands: Vec<ProtoCommand>,
    ) -> Result<RespondWorkflowTaskCompletedResponse> {
        let request = RespondWorkflowTaskCompletedRequest {
            task_token,
            commands,
            identity: self.opts.identity.to_string(),
            binary_checksum: self.opts.worker_binary_id.to_string(),
            namespace: self.opts.namespace.to_string(),
            ..Default::default()
        };
        Ok(self
            .service
            .clone()
            .respond_workflow_task_completed(request)
            .await?
            .into_inner())
    }
}

#[cfg(test)]
mockall::mock! {
    pub(crate) ServerGateway {}
    #[async_trait::async_trait]
    impl PollWorkflowTaskQueueApi for ServerGateway {
        async fn poll(&self, task_queue: &str) -> Result<PollWorkflowTaskQueueResponse>;
    }
    #[async_trait::async_trait]
    impl RespondWorkflowTaskCompletedApi for ServerGateway {
        async fn complete(&self, task_token: Vec<u8>, commands: Vec<ProtoCommand>) -> Result<RespondWorkflowTaskCompletedResponse>;
    }
}
