use std::time::Duration;

use crate::{
    machines::ProtoCommand,
    protos::temporal::api::{
        common::v1::WorkflowType,
        enums::v1::TaskQueueKind,
        taskqueue::v1::TaskQueue,
        workflowservice::v1::{
            workflow_service_client::WorkflowServiceClient, PollWorkflowTaskQueueRequest,
            PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedRequest,
            RespondWorkflowTaskCompletedResponse,
        },
        workflowservice::v1::{StartWorkflowExecutionRequest, StartWorkflowExecutionResponse},
    },
    workflow::{
        PollWorkflowTaskQueueApi, RespondWorkflowTaskCompletedApi, StartWorkflowExecutionApi,
    },
    Result,
};
use tonic::{transport::Channel, Request, Status};
use url::Url;
use uuid::Uuid;

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
#[allow(clippy::unnecessary_wraps)] // Clippy lies because we need to pass to `with_interceptor`
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

/// This trait provides ways to call the temporal server itself
pub trait ServerGatewayApis:
    PollWorkflowTaskQueueApi + RespondWorkflowTaskCompletedApi + StartWorkflowExecutionApi
{
}

impl<T> ServerGatewayApis for T where
    T: PollWorkflowTaskQueueApi + RespondWorkflowTaskCompletedApi + StartWorkflowExecutionApi
{
}

#[async_trait::async_trait]
impl PollWorkflowTaskQueueApi for ServerGateway {
    async fn poll_workflow_task(&self, task_queue: &str) -> Result<PollWorkflowTaskQueueResponse> {
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
    async fn complete_workflow_task(
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

#[async_trait::async_trait]
impl StartWorkflowExecutionApi for ServerGateway {
    async fn start_workflow(
        &self,
        namespace: &str,
        task_queue: &str,
        workflow_id: &str,
        workflow_type: &str,
    ) -> Result<StartWorkflowExecutionResponse> {
        let request_id = Uuid::new_v4().to_string();

        Ok(self
            .service
            .clone()
            .start_workflow_execution(StartWorkflowExecutionRequest {
                namespace: namespace.to_string(),
                workflow_id: workflow_id.to_string(),
                workflow_type: Some(WorkflowType {
                    name: workflow_type.to_string(),
                }),
                task_queue: Some(TaskQueue {
                    name: task_queue.to_string(),
                    kind: 0,
                }),
                request_id,
                ..Default::default()
            })
            .await?
            .into_inner())
    }
}

#[cfg(test)]
mockall::mock! {
    pub ServerGateway {}
    #[async_trait::async_trait]
    impl PollWorkflowTaskQueueApi for ServerGateway {
        async fn poll_workflow_task(&self, task_queue: &str) -> Result<PollWorkflowTaskQueueResponse>;
    }
    #[async_trait::async_trait]
    impl RespondWorkflowTaskCompletedApi for ServerGateway {
        async fn complete_workflow_task(&self, task_token: Vec<u8>, commands: Vec<ProtoCommand>) -> Result<RespondWorkflowTaskCompletedResponse>;
    }
    #[async_trait::async_trait]
    impl StartWorkflowExecutionApi for ServerGateway {
        async fn start_workflow(
            &self,
            namespace: &str,
            task_queue: &str,
            workflow_id: &str,
            workflow_type: &str,
        ) -> Result<StartWorkflowExecutionResponse>;
    }
}
