mod poll_buffer;

#[cfg(test)]
pub use manual_mock::MockManualGateway;
pub use poll_buffer::{
    new_activity_task_buffer, new_workflow_task_buffer, PollActivityTaskBuffer,
    PollWorkflowTaskBuffer,
};

use crate::{
    machines::ProtoCommand,
    protos::temporal::api::{
        common::v1::{Payloads, WorkflowExecution, WorkflowType},
        enums::v1::{TaskQueueKind, WorkflowTaskFailedCause},
        failure::v1::Failure,
        taskqueue::v1::{StickyExecutionAttributes, TaskQueue},
        workflowservice::v1::{
            workflow_service_client::WorkflowServiceClient, PollActivityTaskQueueRequest,
            PollActivityTaskQueueResponse, PollWorkflowTaskQueueRequest,
            PollWorkflowTaskQueueResponse, RecordActivityTaskHeartbeatRequest,
            RecordActivityTaskHeartbeatResponse, RespondActivityTaskCanceledRequest,
            RespondActivityTaskCanceledResponse, RespondActivityTaskCompletedRequest,
            RespondActivityTaskCompletedResponse, RespondActivityTaskFailedRequest,
            RespondActivityTaskFailedResponse, RespondWorkflowTaskCompletedRequest,
            RespondWorkflowTaskCompletedResponse, RespondWorkflowTaskFailedRequest,
            RespondWorkflowTaskFailedResponse, SignalWorkflowExecutionRequest,
            SignalWorkflowExecutionResponse, StartWorkflowExecutionRequest,
            StartWorkflowExecutionResponse,
        },
    },
    task_token::TaskToken,
    CoreInitError,
};
use std::{
    fmt::{Debug, Formatter},
    time::Duration,
};
use tonic::{
    transport::{Certificate, Channel, Endpoint, Identity},
    Request, Status,
};
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

    /// A human-readable string that can identify this process
    pub identity: String,

    /// A string that should be unique to this process' binary
    pub worker_binary_id: String,

    /// Timeout for long polls (polling of task queues)
    pub long_poll_timeout: Duration,

    /// If specified, use TLS as configured by the [TlsConfig] struct. If this is set core will
    /// attempt to use TLS when connecting to the Temporal server. Lang SDK is expected to pass any
    /// certs or keys as bytes, loading them from disk itself if needed.
    pub tls_cfg: Option<TlsConfig>,
}

/// Configuration options for TLS
#[derive(Clone, Debug, Default)]
pub struct TlsConfig {
    /// Bytes representing the root CA certificate used by the server. If not set, and the server's
    /// cert is issued by someone the operating system trusts, verification will still work (ex:
    /// Cloud offering).
    pub server_root_ca_cert: Option<Vec<u8>>,
    /// Sets the domain name against which to verify the server's TLS certificate. If not provided,
    /// the domain name will be extracted from the URL used to connect.
    pub domain: Option<String>,
    /// TLS info for the client. If specified, core will attempt to use mTLS.
    pub client_tls_config: Option<ClientTlsConfig>,
}

/// If using mTLS, both the client cert and private key must be specified, this contains them.
#[derive(Clone)]
pub struct ClientTlsConfig {
    /// The certificate for this client
    pub client_cert: Vec<u8>,
    /// The private key for this client
    pub client_private_key: Vec<u8>,
}

impl Debug for ClientTlsConfig {
    // Intentionally omit details here since they could leak a key if ever printed
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientTlsConfig(..)")
    }
}

impl ServerGatewayOptions {
    /// Attempt to establish a connection to the Temporal server
    pub async fn connect(&self) -> Result<ServerGateway, CoreInitError> {
        let channel = Channel::from_shared(self.target_url.to_string())?;
        let channel = self.add_tls_to_channel(channel).await?;
        let channel = channel.connect().await?;
        let interceptor = intercept(&self);
        let service = WorkflowServiceClient::with_interceptor(channel, interceptor);
        Ok(ServerGateway {
            service,
            opts: self.clone(),
        })
    }

    /// If TLS is configured, set the appropriate options on the provided channel and return it.
    /// Passes it through if TLS options not set.
    async fn add_tls_to_channel(&self, channel: Endpoint) -> Result<Endpoint, CoreInitError> {
        if let Some(tls_cfg) = &self.tls_cfg {
            let mut tls = tonic::transport::ClientTlsConfig::new();

            if let Some(root_cert) = &tls_cfg.server_root_ca_cert {
                let server_root_ca_cert = Certificate::from_pem(root_cert);
                tls = tls.ca_certificate(server_root_ca_cert);
            }

            if let Some(domain) = &tls_cfg.domain {
                tls = tls.domain_name(domain);
            }

            if let Some(client_opts) = &tls_cfg.client_tls_config {
                let client_identity =
                    Identity::from_pem(&client_opts.client_cert, &client_opts.client_private_key);
                tls = tls.identity(client_identity);
            }

            return Ok(channel.tls_config(tls)?);
        }
        Ok(channel)
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
#[derive(Debug)]
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
        task_timeout: Option<Duration>,
    ) -> Result<StartWorkflowExecutionResponse>;

    /// Fetch new workflow tasks from the provided queue. Should block indefinitely if there is no
    /// work.
    async fn poll_workflow_task(&self, task_queue: String)
        -> Result<PollWorkflowTaskQueueResponse>;

    /// Fetch new activity tasks from the provided queue. Should block indefinitely if there is no
    /// work.
    async fn poll_activity_task(&self, task_queue: String)
        -> Result<PollActivityTaskQueueResponse>;

    /// Complete a workflow activation.
    ///
    /// * `task_token`: The task token that would've been received from
    /// [crate::Core::poll_workflow_task] API.
    /// * `commands`: A list of new commands to send to the server, such as starting a timer.
    /// * `sticky_attributes`: If set, indicate that next task should be queued on sticky queue with
    /// given attributes.
    async fn complete_workflow_task(
        &self,
        task_token: TaskToken,
        commands: Vec<ProtoCommand>,
        sticky_attributes: Option<StickyExecutionAttributes>,
    ) -> Result<RespondWorkflowTaskCompletedResponse>;

    /// Complete activity task by sending response to the server. `task_token` contains activity
    /// identifier that would've been received from [crate::Core::poll_activity_task] API. `result`
    /// is a blob that contains activity response.
    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse>;

    /// Report activity task heartbeat by sending details to the server. `task_token` contains activity
    /// identifier that would've been received from [crate::Core::poll_activity_task] API.
    /// `result` contains `cancel_requested` flag, which if set to true indicates that activity has been cancelled.
    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse>;

    /// Cancel activity task by sending response to the server. `task_token` contains activity
    /// identifier that would've been received from [crate::Core::poll_activity_task] API. `details`
    /// is a blob that provides arbitrary user defined cancellation info.
    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse>;

    /// Fail activity task by sending response to the server. `task_token` contains activity
    /// identifier that would've been received from [crate::Core::poll_activity_task] API. `failure`
    /// provides failure details, such as message, cause and stack trace.
    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse>;

    /// Fail task by sending the failure to the server. `task_token` is the task token that would've
    /// been received from [crate::Core::poll_workflow_task].
    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
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

#[async_trait::async_trait]
impl ServerGatewayApis for ServerGateway {
    async fn start_workflow(
        &self,
        namespace: String,
        task_queue: String,
        workflow_id: String,
        workflow_type: String,
        task_timeout: Option<Duration>,
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
                workflow_task_timeout: task_timeout.map(Into::into),
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
        task_token: TaskToken,
        commands: Vec<ProtoCommand>,
        sticky_attributes: Option<StickyExecutionAttributes>,
    ) -> Result<RespondWorkflowTaskCompletedResponse> {
        let request = RespondWorkflowTaskCompletedRequest {
            task_token: task_token.0,
            commands,
            identity: self.opts.identity.clone(),
            binary_checksum: self.opts.worker_binary_id.clone(),
            namespace: self.opts.namespace.clone(),
            sticky_attributes,
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
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse> {
        Ok(self
            .service
            .clone()
            .respond_activity_task_completed(RespondActivityTaskCompletedRequest {
                task_token: task_token.0,
                result,
                identity: self.opts.identity.clone(),
                namespace: self.opts.namespace.clone(),
            })
            .await?
            .into_inner())
    }

    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse> {
        Ok(self
            .service
            .clone()
            .record_activity_task_heartbeat(RecordActivityTaskHeartbeatRequest {
                task_token: task_token.0,
                details,
                identity: self.opts.identity.clone(),
                namespace: self.opts.namespace.clone(),
            })
            .await?
            .into_inner())
    }

    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse> {
        Ok(self
            .service
            .clone()
            .respond_activity_task_canceled(RespondActivityTaskCanceledRequest {
                task_token: task_token.0,
                details,
                identity: self.opts.identity.clone(),
                namespace: self.opts.namespace.clone(),
            })
            .await?
            .into_inner())
    }

    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse> {
        Ok(self
            .service
            .clone()
            .respond_activity_task_failed(RespondActivityTaskFailedRequest {
                task_token: task_token.0,
                failure,
                identity: self.opts.identity.clone(),
                namespace: self.opts.namespace.clone(),
            })
            .await?
            .into_inner())
    }

    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse> {
        let request = RespondWorkflowTaskFailedRequest {
            task_token: task_token.0,
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

#[cfg(test)]
mod manual_mock {
    use super::*;
    use std::future::Future;

    // Need a version of the mock that can return futures so we can return potentially pending
    // results. This is really annoying b/c of the async trait stuff. Need
    // https://github.com/asomers/mockall/issues/189 to be fixed for it to go away.

    mockall::mock! {
        pub ManualGateway {}
        impl ServerGatewayApis for ManualGateway {
            fn start_workflow<'a, 'b>(
                &self,
                namespace: String,
                task_queue: String,
                workflow_id: String,
                workflow_type: String,
                task_timeout: Option<Duration>,
            ) -> impl Future<Output = Result<StartWorkflowExecutionResponse>> + Send + 'b
                where 'a: 'b, Self: 'b;

            fn poll_workflow_task<'a, 'b>(&'a self, task_queue: String)
                -> impl Future<Output = Result<PollWorkflowTaskQueueResponse>> + Send + 'b
                where 'a: 'b, Self: 'b;

            fn poll_activity_task<'a, 'b>(&self, task_queue: String)
                -> impl Future<Output = Result<PollActivityTaskQueueResponse>> + Send + 'b
                where 'a: 'b, Self: 'b;

            fn complete_workflow_task<'a, 'b>(
                &self,
                task_token: TaskToken,
                commands: Vec<ProtoCommand>,
                sticky_attributes: Option<StickyExecutionAttributes>,
            ) -> impl Future<Output = Result<RespondWorkflowTaskCompletedResponse>> + Send + 'b
                where 'a: 'b, Self: 'b;

            fn complete_activity_task<'a, 'b>(
                &self,
                task_token: TaskToken,
                result: Option<Payloads>,
            ) -> impl Future<Output = Result<RespondActivityTaskCompletedResponse>> + Send + 'b
                where 'a: 'b, Self: 'b;

            fn cancel_activity_task<'a, 'b>(
                &self,
                task_token: TaskToken,
                details: Option<Payloads>,
            ) -> impl Future<Output = Result<RespondActivityTaskCanceledResponse>> + Send + 'b
                where 'a: 'b, Self: 'b;

            fn fail_activity_task<'a, 'b>(
                &self,
                task_token: TaskToken,
                failure: Option<Failure>,
            ) -> impl Future<Output = Result<RespondActivityTaskFailedResponse>> + Send + 'b
                where 'a: 'b, Self: 'b;

            fn fail_workflow_task<'a, 'b>(
                &self,
                task_token: TaskToken,
                cause: WorkflowTaskFailedCause,
                failure: Option<Failure>,
            ) -> impl Future<Output = Result<RespondWorkflowTaskFailedResponse>> + Send + 'b
                where 'a: 'b, Self: 'b;

            fn signal_workflow_execution<'a, 'b>(
                &self,
                workflow_id: String,
                run_id: String,
                signal_name: String,
                payloads: Option<Payloads>,
            ) -> impl Future<Output = Result<SignalWorkflowExecutionResponse>> + Send + 'b
                where 'a: 'b, Self: 'b;

            fn record_activity_heartbeat<'a, 'b>(
               &self,
               task_token: TaskToken,
               details: Option<Payloads>,
            ) -> impl Future<Output = Result<RecordActivityTaskHeartbeatResponse>> + Send + 'b
                where 'a: 'b, Self: 'b;
        }
    }
}
