use std::time::Duration;

use crate::protos::temporal::api::common::v1::{Payloads, WorkflowExecution};
use crate::protos::temporal::api::workflowservice::v1::{
    PollActivityTaskQueueRequest, PollActivityTaskQueueResponse, SignalWorkflowExecutionRequest,
    SignalWorkflowExecutionResponse,
};
use crate::{
    machines::ProtoCommand,
    protos::temporal::api::{
        common::v1::WorkflowType,
        enums::v1::{TaskQueueKind, WorkflowTaskFailedCause},
        failure::v1::Failure,
        taskqueue::v1::TaskQueue,
        workflowservice::v1::{
            workflow_service_client::WorkflowServiceClient, PollWorkflowTaskQueueRequest,
            PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedRequest,
            RespondWorkflowTaskCompletedResponse, RespondWorkflowTaskFailedRequest,
            RespondWorkflowTaskFailedResponse, StartWorkflowExecutionRequest,
            StartWorkflowExecutionResponse,
        },
    },
    CoreError, Result,
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

/// This trait provides ways to call the temporal server
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait ServerGatewayApis {
    /// Starts workflow execution.
    async fn start_workflow(
        &self,
        namespace: String,
        task_queue: String,
        workflow_id: String,
        workflow_type: String,
    ) -> Result<StartWorkflowExecutionResponse>;

    /// Fetch new work. Should block indefinitely if there is no work.
    async fn poll_task(&self, req: PollTaskRequest) -> Result<PollTaskResponse>;

    /// Fetch new work. Should block indefinitely if there is no work.
    async fn poll_workflow_task(&self, task_queue: String) -> Result<PollTaskResponse>;

    /// Fetch new work. Should block indefinitely if there is no work.
    async fn poll_activity_task(&self, task_queue: String) -> Result<PollTaskResponse>;

    /// Complete a task by sending it to the server. `task_token` is the task token that would've
    /// been received from [PollWorkflowTaskQueueApi::poll]. `commands` is a list of new commands
    /// to send to the server, such as starting a timer.
    async fn complete_workflow_task(
        &self,
        task_token: Vec<u8>,
        commands: Vec<ProtoCommand>,
    ) -> Result<RespondWorkflowTaskCompletedResponse>;

    /// Fail task by sending the failure to the server. `task_token` is the task token that would've
    /// been received from [PollWorkflowTaskQueueApi::poll].
    async fn fail_workflow_task(
        &self,
        task_token: Vec<u8>,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse>;

    /// Send a signal to a certain workflow instance
    async fn signal_workflow_execution(
        &self,
        workflow_id: String,
        run_id: String,
        signal_name: String,
        payloads: Option<Payloads>,
    ) -> Result<SignalWorkflowExecutionResponse>;
}

pub enum PollTaskRequest {
    Workflow(String),
    Activity(String),
}

pub enum PollTaskResponse {
    WorkflowTask(PollWorkflowTaskQueueResponse),
    ActivityTask(PollActivityTaskQueueResponse),
}

#[async_trait::async_trait]
impl ServerGatewayApis for ServerGateway {
    async fn start_workflow(
        &self,
        namespace: String,
        task_queue: String,
        workflow_id: String,
        workflow_type: String,
    ) -> Result<StartWorkflowExecutionResponse> {
        let request_id = Uuid::new_v4().to_string();

        Ok(self
            .service
            .clone()
            .start_workflow_execution(StartWorkflowExecutionRequest {
                namespace,
                workflow_id,
                workflow_type: Some(WorkflowType {
                    name: workflow_type,
                }),
                task_queue: Some(TaskQueue {
                    name: task_queue,
                    kind: 0,
                }),
                request_id,
                ..Default::default()
            })
            .await?
            .into_inner())
    }

    async fn poll_task(&self, req: PollTaskRequest) -> Result<PollTaskResponse> {
        match req {
            PollTaskRequest::Workflow(task_queue) => {
                Ok(Self::poll_workflow_task(self, task_queue).await?)
            }
            PollTaskRequest::Activity(task_queue) => {
                Ok(Self::poll_activity_task(self, task_queue).await?)
            }
        }
    }

    async fn poll_workflow_task(&self, task_queue: String) -> Result<PollTaskResponse> {
        let request = PollWorkflowTaskQueueRequest {
            namespace: self.opts.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: task_queue,
                kind: TaskQueueKind::Unspecified as i32,
            }),
            identity: self.opts.identity.clone(),
            binary_checksum: self.opts.worker_binary_id.clone(),
        };

        Ok(PollTaskResponse::WorkflowTask(
            self.service
                .clone()
                .poll_workflow_task_queue(request)
                .await?
                .into_inner(),
        ))
    }

    async fn poll_activity_task(&self, task_queue: String) -> Result<PollTaskResponse> {
        let request = PollActivityTaskQueueRequest {
            namespace: self.opts.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: task_queue,
                kind: TaskQueueKind::Unspecified as i32,
            }),
            identity: self.opts.identity.clone(),
            task_queue_metadata: None,
        };

        Ok(PollTaskResponse::ActivityTask(
            self.service
                .clone()
                .poll_activity_task_queue(request)
                .await?
                .into_inner(),
        ))
    }

    async fn complete_workflow_task(
        &self,
        task_token: Vec<u8>,
        commands: Vec<ProtoCommand>,
    ) -> Result<RespondWorkflowTaskCompletedResponse> {
        let request = RespondWorkflowTaskCompletedRequest {
            task_token,
            commands,
            identity: self.opts.identity.clone(),
            binary_checksum: self.opts.worker_binary_id.clone(),
            namespace: self.opts.namespace.clone(),
            ..Default::default()
        };
        match self
            .service
            .clone()
            .respond_workflow_task_completed(request)
            .await
        {
            Ok(pwtr) => Ok(pwtr.into_inner()),
            Err(ts) => {
                if ts.code() == tonic::Code::InvalidArgument && ts.message() == "UnhandledCommand" {
                    Err(CoreError::UnhandledCommandWhenCompleting)
                } else {
                    Err(ts.into())
                }
            }
        }
    }

    async fn fail_workflow_task(
        &self,
        task_token: Vec<u8>,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse> {
        let request = RespondWorkflowTaskFailedRequest {
            task_token,
            cause: cause as i32,
            failure,
            identity: self.opts.identity.clone(),
            binary_checksum: self.opts.worker_binary_id.clone(),
            namespace: self.opts.namespace.clone(),
        };
        Ok(self
            .service
            .clone()
            .respond_workflow_task_failed(request)
            .await?
            .into_inner())
    }

    async fn signal_workflow_execution(
        &self,
        workflow_id: String,
        run_id: String,
        signal_name: String,
        payloads: Option<Payloads>,
    ) -> Result<SignalWorkflowExecutionResponse> {
        Ok(self
            .service
            .clone()
            .signal_workflow_execution(SignalWorkflowExecutionRequest {
                namespace: self.opts.namespace.clone(),
                workflow_execution: Some(WorkflowExecution {
                    workflow_id,
                    run_id,
                }),
                signal_name,
                input: payloads,
                identity: self.opts.identity.clone(),
                ..Default::default()
            })
            .await?
            .into_inner())
    }
}
