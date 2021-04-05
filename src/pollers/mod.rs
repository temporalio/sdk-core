use std::time::Duration;

use crate::{
    machines::ProtoCommand,
    protos::temporal::api::{
        common::v1::{Payloads, WorkflowExecution, WorkflowType},
        enums::v1::{TaskQueueKind, WorkflowTaskFailedCause},
        failure::v1::Failure,
        taskqueue::v1::TaskQueue,
        workflowservice::v1::{
            workflow_service_client::WorkflowServiceClient, PollActivityTaskQueueRequest,
            PollActivityTaskQueueResponse, PollWorkflowTaskQueueRequest,
            PollWorkflowTaskQueueResponse, RespondActivityTaskCanceledRequest,
            RespondActivityTaskCanceledResponse, RespondActivityTaskCompletedRequest,
            RespondActivityTaskCompletedResponse, RespondActivityTaskFailedRequest,
            RespondActivityTaskFailedResponse, RespondWorkflowTaskCompletedRequest,
            RespondWorkflowTaskCompletedResponse, RespondWorkflowTaskFailedRequest,
            RespondWorkflowTaskFailedResponse, SignalWorkflowExecutionRequest,
            SignalWorkflowExecutionResponse, StartWorkflowExecutionRequest,
            StartWorkflowExecutionResponse,
        },
    },
    CoreInitError,
};
use tonic::{transport::Channel, Request, Status};
use url::Url;
use uuid::Uuid;

pub type Result<T, E = Status> = std::result::Result<T, E>;

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
    pub async fn connect(&self) -> Result<ServerGateway, CoreInitError> {
        let channel = Channel::from_shared(self.target_url.to_string())?
            .connect()
            .await?;
        let interceptor = intercept(&self);
        let service = WorkflowServiceClient::with_interceptor(channel, interceptor);
        Ok(ServerGateway {
            service,
            opts: self.clone(),
        })
    }
}

/// This function will get called on each outbound request. Returning a `Status` here will cancel
/// the request and have that status returned to the caller.
fn intercept(opts: &ServerGatewayOptions) -> impl Fn(Request<()>) -> Result<Request<()>, Status> {
    let timeout_str = format!("{}m", opts.long_poll_timeout.as_millis());
    move |mut req: Request<()>| {
        let metadata = req.metadata_mut();
        // TODO: Only apply this to long poll requests
        metadata.insert(
            "grpc-timeout",
            timeout_str
                .parse()
                .expect("Timeout string construction cannot fail"),
        );
        metadata.insert(
            "client-name",
            "core-sdk".parse().expect("Static value is parsable"),
        );
        Ok(req)
    }
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

    /// Fetch new workflow tasks. Should block indefinitely if there is no work.
    async fn poll_workflow_task(&self, task_queue: String)
        -> Result<PollWorkflowTaskQueueResponse>;

    /// Fetch new activity tasks. Should block indefinitely if there is no work.
    async fn poll_activity_task(&self, task_queue: String)
        -> Result<PollActivityTaskQueueResponse>;

    /// Complete a workflow activation. `task_token` is the task token that would've been received
    /// from [crate::Core::poll_workflow_task] API. `commands` is a list of new commands to send to
    /// the server, such as starting a timer.
    async fn complete_workflow_task(
        &self,
        task_token: Vec<u8>,
        commands: Vec<ProtoCommand>,
    ) -> Result<RespondWorkflowTaskCompletedResponse>;

    /// Complete activity task by sending response to the server. `task_token` contains activity
    /// identifier that would've been received from [crate::Core::poll_activity_task] API. `result`
    /// is a blob that contains activity response.
    async fn complete_activity_task(
        &self,
        task_token: Vec<u8>,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse>;

    /// Cancel activity task by sending response to the server. `task_token` contains activity
    /// identifier that would've been received from [crate::Core::poll_activity_task] API. `details`
    /// is a blob that provides arbitrary user defined cancellation info.
    async fn cancel_activity_task(
        &self,
        task_token: Vec<u8>,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse>;

    /// Fail activity task by sending response to the server. `task_token` contains activity
    /// identifier that would've been received from [crate::Core::poll_activity_task] API. `failure`
    /// provides failure details, such as message, cause and stack trace.
    async fn fail_activity_task(
        &self,
        task_token: Vec<u8>,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse>;

    /// Fail task by sending the failure to the server. `task_token` is the task token that would've
    /// been received from [crate::Core::poll_workflow_task].
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

/// Contains poll task request. String parameter defines a task queue to be polled.
pub enum PollTaskRequest {
    /// Instructs core to poll for workflow task.
    Workflow(String),
    /// Instructs core to poll for activity task.
    Activity(String),
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

    async fn poll_workflow_task(
        &self,
        task_queue: String,
    ) -> Result<PollWorkflowTaskQueueResponse> {
        let request = PollWorkflowTaskQueueRequest {
            namespace: self.opts.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: task_queue,
                kind: TaskQueueKind::Unspecified as i32,
            }),
            identity: self.opts.identity.clone(),
            binary_checksum: self.opts.worker_binary_id.clone(),
        };

        Ok(self
            .service
            .clone()
            .poll_workflow_task_queue(request)
            .await?
            .into_inner())
    }

    async fn poll_activity_task(
        &self,
        task_queue: String,
    ) -> Result<PollActivityTaskQueueResponse> {
        let request = PollActivityTaskQueueRequest {
            namespace: self.opts.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: task_queue,
                kind: TaskQueueKind::Normal as i32,
            }),
            identity: self.opts.identity.clone(),
            task_queue_metadata: None,
        };

        Ok(self
            .service
            .clone()
            .poll_activity_task_queue(request)
            .await?
            .into_inner())
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
        Ok(self
            .service
            .clone()
            .respond_workflow_task_completed(request)
            .await?
            .into_inner())
    }

    async fn complete_activity_task(
        &self,
        task_token: Vec<u8>,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse> {
        Ok(self
            .service
            .clone()
            .respond_activity_task_completed(RespondActivityTaskCompletedRequest {
                task_token,
                result,
                identity: self.opts.identity.clone(),
                namespace: self.opts.namespace.clone(),
            })
            .await?
            .into_inner())
    }

    async fn cancel_activity_task(
        &self,
        task_token: Vec<u8>,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse> {
        Ok(self
            .service
            .clone()
            .respond_activity_task_canceled(RespondActivityTaskCanceledRequest {
                task_token,
                details,
                identity: self.opts.identity.clone(),
                namespace: self.opts.namespace.clone(),
            })
            .await?
            .into_inner())
    }

    async fn fail_activity_task(
        &self,
        task_token: Vec<u8>,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse> {
        Ok(self
            .service
            .clone()
            .respond_activity_task_failed(RespondActivityTaskFailedRequest {
                task_token,
                failure,
                identity: self.opts.identity.clone(),
                namespace: self.opts.namespace.clone(),
            })
            .await?
            .into_inner())
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
