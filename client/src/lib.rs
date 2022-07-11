#![warn(missing_docs)] // error if there are missing docs

//! This crate contains client implementations that can be used to contact the Temporal service.
//!
//! It implements auto-retry behavior and metrics collection.

#[macro_use]
extern crate tracing;

mod metrics;
mod raw;
mod retry;
mod workflow_handle;

pub use crate::retry::{CallType, RetryClient, RETRYABLE_ERROR_CODES};
pub use raw::WorkflowService;
pub use workflow_handle::{WorkflowExecutionInfo, WorkflowExecutionResult};

use crate::{
    metrics::{GrpcMetricSvc, MetricsContext},
    raw::{sealed::RawClientLike, AttachMetricLabels},
    sealed::{RawClientLikeUser, WfHandleClient},
    workflow_handle::UntypedWorkflowHandle,
};
use backoff::{ExponentialBackoff, SystemClock};
use http::uri::InvalidUri;
use opentelemetry::metrics::Meter;
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    ops::{Deref, DerefMut},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use temporal_sdk_core_protos::{
    coresdk::{workflow_commands::QueryResult, IntoPayloadsExt},
    temporal::api::{
        command::v1::Command,
        common::v1::{Payload, Payloads, WorkflowExecution, WorkflowType},
        enums::v1::{TaskQueueKind, WorkflowTaskFailedCause},
        failure::v1::Failure,
        query::v1::{WorkflowQuery, WorkflowQueryResult},
        taskqueue::v1::{StickyExecutionAttributes, TaskQueue, TaskQueueMetadata},
        workflowservice::v1::{workflow_service_client::WorkflowServiceClient, *},
    },
    TaskToken,
};
use tonic::{
    codegen::InterceptedService,
    metadata::{MetadataKey, MetadataValue},
    service::Interceptor,
    transport::{Certificate, Channel, Endpoint, Identity},
    Code, Status,
};
use tower::ServiceBuilder;
use url::Url;
use uuid::Uuid;

static LONG_POLL_METHOD_NAMES: [&str; 2] = ["PollWorkflowTaskQueue", "PollActivityTaskQueue"];
/// The server times out polls after 60 seconds. Set our timeout to be slightly beyond that.
const LONG_POLL_TIMEOUT: Duration = Duration::from_secs(70);
const OTHER_CALL_TIMEOUT: Duration = Duration::from_secs(30);

type Result<T, E = tonic::Status> = std::result::Result<T, E>;

/// Options for the connection to the temporal server. Construct with [ClientOptionsBuilder]
#[derive(Clone, Debug, derive_builder::Builder)]
#[non_exhaustive]
pub struct ClientOptions {
    /// The URL of the Temporal server to connect to
    #[builder(setter(into))]
    pub target_url: Url,

    /// The name of the SDK being implemented on top of core. Is set as `client-name` header in
    /// all RPC calls
    #[builder(setter(into))]
    pub client_name: String,

    /// The version of the SDK being implemented on top of core. Is set as `client-version` header
    /// in all RPC calls. The server decides if the client is supported based on this.
    #[builder(setter(into))]
    pub client_version: String,

    /// A human-readable string that can identify this process. Defaults to empty string.
    #[builder(default)]
    pub identity: String,

    /// If specified, use TLS as configured by the [TlsConfig] struct. If this is set core will
    /// attempt to use TLS when connecting to the Temporal server. Lang SDK is expected to pass any
    /// certs or keys as bytes, loading them from disk itself if needed.
    #[builder(setter(strip_option), default)]
    pub tls_cfg: Option<TlsConfig>,

    /// Retry configuration for the server client. Default is [RetryConfig::default]
    #[builder(default)]
    pub retry_config: RetryConfig,
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

/// Configuration for retrying requests to the server
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// initial wait time before the first retry.
    pub initial_interval: Duration,
    /// randomization jitter that is used as a multiplier for the current retry interval
    /// and is added or subtracted from the interval length.
    pub randomization_factor: f64,
    /// rate at which retry time should be increased, until it reaches max_interval.
    pub multiplier: f64,
    /// maximum amount of time to wait between retries.
    pub max_interval: Duration,
    /// maximum total amount of time requests should be retried for, if None is set then no limit
    /// will be used.
    pub max_elapsed_time: Option<Duration>,
    /// maximum number of retry attempts.
    pub max_retries: usize,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            initial_interval: Duration::from_millis(100), // 100 ms wait by default.
            randomization_factor: 0.2,                    // +-20% jitter.
            multiplier: 1.5, // each next retry delay will increase by 50%
            max_interval: Duration::from_secs(5), // until it reaches 5 seconds.
            max_elapsed_time: Some(Duration::from_secs(10)), // 10 seconds total allocated time for all retries.
            max_retries: 10,
        }
    }
}

impl RetryConfig {
    pub(crate) const fn poll_retry_policy() -> Self {
        Self {
            initial_interval: Duration::from_millis(200),
            randomization_factor: 0.2,
            multiplier: 2.0,
            max_interval: Duration::from_secs(10),
            max_elapsed_time: None,
            max_retries: 0,
        }
    }
}

impl From<RetryConfig> for ExponentialBackoff {
    fn from(c: RetryConfig) -> Self {
        Self {
            current_interval: c.initial_interval,
            initial_interval: c.initial_interval,
            randomization_factor: c.randomization_factor,
            multiplier: c.multiplier,
            max_interval: c.max_interval,
            max_elapsed_time: c.max_elapsed_time,
            clock: SystemClock::default(),
            start_time: Instant::now(),
        }
    }
}

impl Debug for ClientTlsConfig {
    // Intentionally omit details here since they could leak a key if ever printed
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientTlsConfig(..)")
    }
}

/// Errors thrown while attempting to establish a connection to the server
#[derive(thiserror::Error, Debug)]
pub enum ClientInitError {
    /// Invalid URI. Configuration error, fatal.
    #[error("Invalid URI: {0:?}")]
    InvalidUri(#[from] InvalidUri),
    /// Server connection error. Crashing and restarting the worker is likely best.
    #[error("Server connection error: {0:?}")]
    TonicTransportError(#[from] tonic::transport::Error),
    /// We couldn't successfully make the `get_system_info` call at connection time to establish
    /// server capabilities / verify server is responding.
    #[error("`get_system_info` call error after connection: {0:?}")]
    SystemInfoCallError(tonic::Status),
}

#[doc(hidden)]
/// Allows passing different kinds of clients into things that want to be flexible. Motivating
/// use-case was worker initialization.
///
/// Needs to exist in this crate to avoid blanket impl conflicts.
pub enum AnyClient {
    /// A high level client, like the type workers work with
    HighLevel(Arc<dyn WorkflowClientTrait + Send + Sync>),
    /// A low level gRPC client, wrapped with the typical interceptors
    LowLevel(Box<ConfiguredClient<WorkflowServiceClientWithMetrics>>),
}

impl<SGA> From<SGA> for AnyClient
where
    SGA: WorkflowClientTrait + Send + Sync + 'static,
{
    fn from(s: SGA) -> Self {
        Self::HighLevel(Arc::new(s))
    }
}
impl<SGA> From<Arc<SGA>> for AnyClient
where
    SGA: WorkflowClientTrait + Send + Sync + 'static,
{
    fn from(s: Arc<SGA>) -> Self {
        Self::HighLevel(s)
    }
}
impl From<RetryClient<ConfiguredClient<WorkflowServiceClientWithMetrics>>> for AnyClient {
    fn from(c: RetryClient<ConfiguredClient<WorkflowServiceClientWithMetrics>>) -> Self {
        Self::LowLevel(Box::new(c.into_inner()))
    }
}
impl From<ConfiguredClient<WorkflowServiceClientWithMetrics>> for AnyClient {
    fn from(c: ConfiguredClient<WorkflowServiceClientWithMetrics>) -> Self {
        Self::LowLevel(Box::new(c))
    }
}

/// A client with [ClientOptions] attached, which can be passed to initialize workers,
/// or can be used directly.
#[derive(Clone, Debug)]
pub struct ConfiguredClient<C> {
    client: C,
    options: ClientOptions,
    headers: Arc<RwLock<HashMap<String, String>>>,
    /// Capabilities as read from the `get_system_info` RPC call made on client connection
    capabilities: Option<get_system_info_response::Capabilities>,
}

impl<C> ConfiguredClient<C> {
    /// Set HTTP request headers overwriting previous headers
    pub fn set_headers(&self, headers: HashMap<String, String>) {
        let mut guard = self.headers.write();
        *guard = headers;
    }

    /// Returns the options the client is configured with
    pub fn options(&self) -> &ClientOptions {
        &self.options
    }

    /// Returns the server capabilities we (may have) learned about when establishing an initial
    /// connection
    pub fn capabilities(&self) -> Option<&get_system_info_response::Capabilities> {
        self.capabilities.as_ref()
    }

    /// De-constitute this type
    pub fn into_parts(self) -> (C, ClientOptions) {
        (self.client, self.options)
    }
}

// The configured client is effectively a "smart" (dumb) pointer
impl<C> Deref for ConfiguredClient<C> {
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}
impl<C> DerefMut for ConfiguredClient<C> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

impl ClientOptions {
    /// Attempt to establish a connection to the Temporal server in a specific namespace. The
    /// returned client is bound to that namespace.
    pub async fn connect(
        &self,
        namespace: impl Into<String>,
        metrics_meter: Option<&Meter>,
        headers: Option<Arc<RwLock<HashMap<String, String>>>>,
    ) -> Result<RetryClient<Client>, ClientInitError> {
        let client = self
            .connect_no_namespace(metrics_meter, headers)
            .await?
            .into_inner();
        let client = Client::new(client, namespace.into());
        let retry_client = RetryClient::new(client, self.retry_config.clone());
        Ok(retry_client)
    }

    /// Attempt to establish a connection to the Temporal server and return a gRPC client which is
    /// intercepted with retry, default headers functionality, and metrics if provided.
    ///
    /// See [RetryClient] for more
    pub async fn connect_no_namespace(
        &self,
        metrics_meter: Option<&Meter>,
        headers: Option<Arc<RwLock<HashMap<String, String>>>>,
    ) -> Result<RetryClient<ConfiguredClient<WorkflowServiceClientWithMetrics>>, ClientInitError>
    {
        let channel = Channel::from_shared(self.target_url.to_string())?;
        let channel = self.add_tls_to_channel(channel).await?;
        let channel = channel.connect().await?;
        let service = ServiceBuilder::new()
            .layer_fn(|channel| GrpcMetricSvc {
                inner: channel,
                metrics: metrics_meter.map(|mm| MetricsContext::new(vec![], mm)),
            })
            .service(channel);
        let headers = headers.unwrap_or_default();
        let interceptor = ServiceCallInterceptor {
            opts: self.clone(),
            headers: headers.clone(),
        };

        let mut client = ConfiguredClient {
            headers,
            client: WorkflowServiceClient::with_interceptor(service, interceptor),
            options: self.clone(),
            capabilities: None,
        };
        match client
            .get_system_info(GetSystemInfoRequest::default())
            .await
        {
            Ok(sysinfo) => {
                client.capabilities = sysinfo.into_inner().capabilities;
            }
            Err(status) => match status.code() {
                Code::Unimplemented => {}
                _ => return Err(ClientInitError::SystemInfoCallError(status)),
            },
        };
        Ok(RetryClient::new(client, self.retry_config.clone()))
    }

    /// If TLS is configured, set the appropriate options on the provided channel and return it.
    /// Passes it through if TLS options not set.
    async fn add_tls_to_channel(
        &self,
        channel: Endpoint,
    ) -> Result<Endpoint, tonic::transport::Error> {
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

            return channel.tls_config(tls);
        }
        Ok(channel)
    }
}

/// A version of [RespondWorkflowTaskCompletedRequest] that will finish being filled out by the
/// server client
#[derive(Debug, Clone, PartialEq)]
pub struct WorkflowTaskCompletion {
    /// The task token that would've been received from polling for a workflow activation
    pub task_token: TaskToken,
    /// A list of new commands to send to the server, such as starting a timer.
    pub commands: Vec<Command>,
    /// If set, indicate that next task should be queued on sticky queue with given attributes.
    pub sticky_attributes: Option<StickyExecutionAttributes>,
    /// Responses to queries in the `queries` field of the workflow task.
    pub query_responses: Vec<QueryResult>,
    /// Indicate that the task completion should return a new WFT if one is available
    pub return_new_workflow_task: bool,
    /// Force a new WFT to be created after this completion
    pub force_create_new_workflow_task: bool,
}

/// Interceptor which attaches common metadata (like "client-name") to every outgoing call
#[derive(Clone)]
pub struct ServiceCallInterceptor {
    opts: ClientOptions,
    /// Only accessed as a reader
    headers: Arc<RwLock<HashMap<String, String>>>,
}

impl Interceptor for ServiceCallInterceptor {
    /// This function will get called on each outbound request. Returning a `Status` here will
    /// cancel the request and have that status returned to the caller.
    fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        let metadata = request.metadata_mut();
        metadata.insert(
            "client-name",
            self.opts
                .client_name
                .parse()
                .unwrap_or_else(|_| MetadataValue::from_static("")),
        );
        metadata.insert(
            "client-version",
            self.opts
                .client_version
                .parse()
                .unwrap_or_else(|_| MetadataValue::from_static("")),
        );
        let headers = &*self.headers.read();
        for (k, v) in headers {
            if let (Ok(k), Ok(v)) = (MetadataKey::from_str(k), MetadataValue::from_str(v)) {
                metadata.insert(k, v);
            }
        }
        if metadata.get("grpc-timeout").is_none() {
            request.set_timeout(OTHER_CALL_TIMEOUT);
        }

        Ok(request)
    }
}

/// A [WorkflowServiceClient] with the default interceptors attached.
pub type WorkflowServiceClientWithMetrics = WorkflowServiceClient<InterceptedMetricsSvc>;
type InterceptedMetricsSvc = InterceptedService<GrpcMetricSvc, ServiceCallInterceptor>;

/// Contains an instance of a namespace-bound client for interacting with the Temporal server
#[derive(Debug, Clone)]
pub struct Client {
    /// Client for interacting with workflow service
    inner: ConfiguredClient<WorkflowServiceClientWithMetrics>,
    /// The namespace this client interacts with
    namespace: String,
    /// If set, attach as the worker build id to relevant calls
    bound_worker_build_id: Option<String>,
}

impl Client {
    /// Create a new client from an existing configured lower level client and a namespace
    pub fn new(
        client: ConfiguredClient<WorkflowServiceClientWithMetrics>,
        namespace: String,
    ) -> Self {
        Client {
            inner: client,
            namespace,
            bound_worker_build_id: None,
        }
    }

    /// Return an auto-retrying version of the underling grpc client (instrumented with metrics
    /// collection, if enabled).
    ///
    /// Note that it is reasonably cheap to clone the returned type if you need to own it. Such
    /// clones will keep re-using the same channel.
    pub fn raw_retry_client(&self) -> RetryClient<WorkflowServiceClientWithMetrics> {
        RetryClient::new(
            self.raw_client().clone(),
            self.inner.options.retry_config.clone(),
        )
    }

    /// Access the underling grpc client. This raw client is not bound to a specific namespace.
    ///
    /// Note that it is reasonably cheap to clone the returned type if you need to own it. Such
    /// clones will keep re-using the same channel.
    pub fn raw_client(&self) -> &WorkflowServiceClientWithMetrics {
        self.inner.deref()
    }

    /// Return the options this client was initialized with
    pub fn options(&self) -> &ClientOptions {
        &self.inner.options
    }

    /// Return the options this client was initialized with mutably
    pub fn options_mut(&mut self) -> &mut ClientOptions {
        &mut self.inner.options
    }

    /// Set a worker build id to be attached to relevant requests. Unlikely to be useful outside
    /// of core.
    pub fn set_worker_build_id(&mut self, id: String) {
        self.bound_worker_build_id = Some(id)
    }
}

/// This trait provides higher-level friendlier interaction with the server.
/// See the [WorkflowService] trait for a lower-level client.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait WorkflowClientTrait {
    /// Starts workflow execution.
    async fn start_workflow(
        &self,
        input: Vec<Payload>,
        task_queue: String,
        workflow_id: String,
        workflow_type: String,
        options: WorkflowOptions,
    ) -> Result<StartWorkflowExecutionResponse>;

    /// Fetch new workflow tasks from the provided queue. Should block indefinitely if there is no
    /// work.
    async fn poll_workflow_task(
        &self,
        task_queue: String,
        is_sticky: bool,
    ) -> Result<PollWorkflowTaskQueueResponse>;

    /// Fetch new activity tasks from the provided queue. Should block indefinitely if there is no
    /// work.
    async fn poll_activity_task(
        &self,
        task_queue: String,
        max_tasks_per_sec: Option<f64>,
    ) -> Result<PollActivityTaskQueueResponse>;

    /// Notifies the server that workflow tasks for a given workflow should be sent to the normal
    /// non-sticky task queue. This normally happens when workflow has been evicted from the cache.
    async fn reset_sticky_task_queue(
        &self,
        workflow_id: String,
        run_id: String,
    ) -> Result<ResetStickyTaskQueueResponse>;

    /// Complete a workflow activation.
    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> Result<RespondWorkflowTaskCompletedResponse>;

    /// Complete activity task by sending response to the server. `task_token` contains activity
    /// identifier that would've been received from polling for an activity task. `result` is a blob
    /// that contains activity response.
    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse>;

    /// Report activity task heartbeat by sending details to the server. `task_token` contains
    /// activity identifier that would've been received from polling for an activity task. `result`
    /// contains `cancel_requested` flag, which if set to true indicates that activity has been
    /// cancelled.
    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse>;

    /// Cancel activity task by sending response to the server. `task_token` contains activity
    /// identifier that would've been received from polling for an activity task. `details` is a
    /// blob that provides arbitrary user defined cancellation info.
    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse>;

    /// Fail activity task by sending response to the server. `task_token` contains activity
    /// identifier that would've been received from polling for an activity task. `failure` provides
    /// failure details, such as message, cause and stack trace.
    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse>;

    /// Fail task by sending the failure to the server. `task_token` is the task token that would've
    /// been received from polling for a workflow activation.
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

    /// Request a query of a certain workflow instance
    async fn query_workflow_execution(
        &self,
        workflow_id: String,
        run_id: String,
        query: WorkflowQuery,
    ) -> Result<QueryWorkflowResponse>;

    /// Get information about a workflow run
    async fn describe_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<DescribeWorkflowExecutionResponse>;

    /// Get history for a particular workflow run
    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse>;

    /// Respond to a legacy query-only workflow task
    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: QueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse>;

    /// Cancel a currently executing workflow
    async fn cancel_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        reason: String,
    ) -> Result<RequestCancelWorkflowExecutionResponse>;

    /// Terminate a currently executing workflow
    async fn terminate_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<TerminateWorkflowExecutionResponse>;

    /// Lists all available namespaces
    async fn list_namespaces(&self) -> Result<ListNamespacesResponse>;

    /// Returns options that were used to initialize the client
    fn get_options(&self) -> &ClientOptions;

    /// Returns the namespace this client is bound to
    fn namespace(&self) -> &str;
}

/// Optional fields supplied at the start of workflow execution
#[derive(Debug, Clone, Default)]
pub struct WorkflowOptions {
    /// Optionally indicates the default task timeout for workflow tasks
    pub task_timeout: Option<Duration>,

    /// Optionally associate extra search attributes with a workflow
    pub search_attributes: Option<HashMap<String, Payload>>,
}

#[async_trait::async_trait]
impl WorkflowClientTrait for Client {
    async fn start_workflow(
        &self,
        input: Vec<Payload>,
        task_queue: String,
        workflow_id: String,
        workflow_type: String,
        options: WorkflowOptions,
    ) -> Result<StartWorkflowExecutionResponse> {
        let request_id = Uuid::new_v4().to_string();

        Ok(self
            .wf_svc()
            .start_workflow_execution(StartWorkflowExecutionRequest {
                namespace: self.namespace.clone(),
                input: input.into_payloads(),
                workflow_id,
                workflow_type: Some(WorkflowType {
                    name: workflow_type,
                }),
                task_queue: Some(TaskQueue {
                    name: task_queue,
                    kind: 0,
                }),
                request_id,
                workflow_task_timeout: options.task_timeout.map(Into::into),
                search_attributes: options.search_attributes.map(Into::into),
                ..Default::default()
            })
            .await?
            .into_inner())
    }

    async fn poll_workflow_task(
        &self,
        task_queue: String,
        is_sticky: bool,
    ) -> Result<PollWorkflowTaskQueueResponse> {
        let request = PollWorkflowTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: task_queue,
                kind: if is_sticky {
                    TaskQueueKind::Sticky
                } else {
                    TaskQueueKind::Normal
                } as i32,
            }),
            identity: self.inner.options.identity.clone(),
            binary_checksum: self.bound_worker_build_id.clone().unwrap_or_default(),
        };

        Ok(self
            .wf_svc()
            .poll_workflow_task_queue(request)
            .await?
            .into_inner())
    }

    async fn poll_activity_task(
        &self,
        task_queue: String,
        max_tasks_per_sec: Option<f64>,
    ) -> Result<PollActivityTaskQueueResponse> {
        let request = PollActivityTaskQueueRequest {
            namespace: self.namespace.clone(),
            task_queue: Some(TaskQueue {
                name: task_queue,
                kind: TaskQueueKind::Normal as i32,
            }),
            identity: self.inner.options.identity.clone(),
            task_queue_metadata: max_tasks_per_sec.map(|tps| TaskQueueMetadata {
                max_tasks_per_second: Some(tps),
            }),
        };

        Ok(self
            .wf_svc()
            .poll_activity_task_queue(request)
            .await?
            .into_inner())
    }

    async fn reset_sticky_task_queue(
        &self,
        workflow_id: String,
        run_id: String,
    ) -> Result<ResetStickyTaskQueueResponse> {
        let request = ResetStickyTaskQueueRequest {
            namespace: self.namespace.clone(),
            execution: Some(WorkflowExecution {
                workflow_id,
                run_id,
            }),
        };
        Ok(self
            .wf_svc()
            .reset_sticky_task_queue(request)
            .await?
            .into_inner())
    }

    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> Result<RespondWorkflowTaskCompletedResponse> {
        let request = RespondWorkflowTaskCompletedRequest {
            task_token: request.task_token.into(),
            commands: request.commands,
            identity: self.inner.options.identity.clone(),
            sticky_attributes: request.sticky_attributes,
            return_new_workflow_task: request.return_new_workflow_task,
            force_create_new_workflow_task: request.force_create_new_workflow_task,
            binary_checksum: self.bound_worker_build_id.clone().unwrap_or_default(),
            query_results: request
                .query_responses
                .into_iter()
                .map(|qr| {
                    let (id, completed_type, query_result, error_message) = qr.into_components();
                    (
                        id,
                        WorkflowQueryResult {
                            result_type: completed_type as i32,
                            answer: query_result,
                            error_message,
                        },
                    )
                })
                .collect(),
            namespace: self.namespace.clone(),
        };
        Ok(self
            .wf_svc()
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
            .wf_svc()
            .respond_activity_task_completed(RespondActivityTaskCompletedRequest {
                task_token: task_token.0,
                result,
                identity: self.inner.options.identity.clone(),
                namespace: self.namespace.clone(),
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
            .wf_svc()
            .record_activity_task_heartbeat(RecordActivityTaskHeartbeatRequest {
                task_token: task_token.0,
                details,
                identity: self.inner.options.identity.clone(),
                namespace: self.namespace.clone(),
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
            .wf_svc()
            .respond_activity_task_canceled(RespondActivityTaskCanceledRequest {
                task_token: task_token.0,
                details,
                identity: self.inner.options.identity.clone(),
                namespace: self.namespace.clone(),
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
            .wf_svc()
            .respond_activity_task_failed(RespondActivityTaskFailedRequest {
                task_token: task_token.0,
                failure,
                identity: self.inner.options.identity.clone(),
                namespace: self.namespace.clone(),
                // TODO: Implement - https://github.com/temporalio/sdk-core/issues/293
                last_heartbeat_details: None,
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
            identity: self.inner.options.identity.clone(),
            binary_checksum: self.bound_worker_build_id.clone().unwrap_or_default(),
            namespace: self.namespace.clone(),
        };
        Ok(self
            .wf_svc()
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
            .wf_svc()
            .signal_workflow_execution(SignalWorkflowExecutionRequest {
                namespace: self.namespace.clone(),
                workflow_execution: Some(WorkflowExecution {
                    workflow_id,
                    run_id,
                }),
                signal_name,
                input: payloads,
                identity: self.inner.options.identity.clone(),
                ..Default::default()
            })
            .await?
            .into_inner())
    }

    async fn query_workflow_execution(
        &self,
        workflow_id: String,
        run_id: String,
        query: WorkflowQuery,
    ) -> Result<QueryWorkflowResponse> {
        Ok(self
            .wf_svc()
            .query_workflow(QueryWorkflowRequest {
                namespace: self.namespace.clone(),
                execution: Some(WorkflowExecution {
                    workflow_id,
                    run_id,
                }),
                query: Some(query),
                query_reject_condition: 1,
            })
            .await?
            .into_inner())
    }

    async fn describe_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<DescribeWorkflowExecutionResponse> {
        Ok(self
            .wf_svc()
            .describe_workflow_execution(DescribeWorkflowExecutionRequest {
                namespace: self.namespace.clone(),
                execution: Some(WorkflowExecution {
                    workflow_id,
                    run_id: run_id.unwrap_or_default(),
                }),
            })
            .await?
            .into_inner())
    }

    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse> {
        Ok(self
            .wf_svc()
            .get_workflow_execution_history(GetWorkflowExecutionHistoryRequest {
                namespace: self.namespace.clone(),
                execution: Some(WorkflowExecution {
                    workflow_id,
                    run_id: run_id.unwrap_or_default(),
                }),
                next_page_token: page_token,
                ..Default::default()
            })
            .await?
            .into_inner())
    }

    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: QueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse> {
        let (_, completed_type, query_result, error_message) = query_result.into_components();
        Ok(self
            .wf_svc()
            .respond_query_task_completed(RespondQueryTaskCompletedRequest {
                task_token: task_token.into(),
                completed_type: completed_type as i32,
                query_result,
                error_message,
                namespace: self.namespace.clone(),
            })
            .await?
            .into_inner())
    }

    async fn cancel_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        reason: String,
    ) -> Result<RequestCancelWorkflowExecutionResponse> {
        Ok(self
            .wf_svc()
            .request_cancel_workflow_execution(RequestCancelWorkflowExecutionRequest {
                namespace: self.namespace.clone(),
                workflow_execution: Some(WorkflowExecution {
                    workflow_id,
                    run_id: run_id.unwrap_or_default(),
                }),
                identity: self.inner.options.identity.clone(),
                request_id: "".to_string(),
                first_execution_run_id: "".to_string(),
                reason,
            })
            .await?
            .into_inner())
    }

    async fn terminate_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<TerminateWorkflowExecutionResponse> {
        Ok(self
            .wf_svc()
            .terminate_workflow_execution(TerminateWorkflowExecutionRequest {
                namespace: self.namespace.clone(),
                workflow_execution: Some(WorkflowExecution {
                    workflow_id,
                    run_id: run_id.unwrap_or_default(),
                }),
                reason: "".to_string(),
                details: None,
                identity: self.inner.options.identity.clone(),
                first_execution_run_id: "".to_string(),
            })
            .await?
            .into_inner())
    }

    async fn list_namespaces(&self) -> Result<ListNamespacesResponse> {
        Ok(self
            .wf_svc()
            .list_namespaces(ListNamespacesRequest::default())
            .await?
            .into_inner())
    }

    fn get_options(&self) -> &ClientOptions {
        &self.inner.options
    }

    fn namespace(&self) -> &str {
        &self.namespace
    }
}

mod sealed {
    use crate::{InterceptedMetricsSvc, RawClientLike, WorkflowClientTrait};

    pub trait RawClientLikeUser {
        type RawClientT: RawClientLike<SvcType = InterceptedMetricsSvc>;
        /// Used to access the client as a [WorkflowService] implementor rather than the raw struct
        fn wf_svc(&self) -> Self::RawClientT;
    }

    pub trait WfHandleClient: WorkflowClientTrait + RawClientLikeUser {}
    impl<T> WfHandleClient for T where T: WorkflowClientTrait + RawClientLikeUser {}
}

impl RawClientLikeUser for Client {
    type RawClientT = WorkflowServiceClientWithMetrics;

    fn wf_svc(&self) -> Self::RawClientT {
        self.raw_client().clone()
    }
}

/// Additional methods for workflow clients
pub trait WfClientExt: WfHandleClient + Sized {
    /// Create an untyped handle for a workflow execution, which can be used to do things like
    /// wait for that workflow's result. `run_id` may be left blank to target the latest run.
    fn get_untyped_workflow_handle(
        &self,
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
    ) -> UntypedWorkflowHandle<Self::RawClientT>
    where
        Self::RawClientT: Clone,
    {
        let rid = run_id.into();
        UntypedWorkflowHandle::new(
            self.wf_svc(),
            WorkflowExecutionInfo {
                namespace: self.namespace().to_string(),
                workflow_id: workflow_id.into(),
                run_id: if rid.is_empty() { None } else { Some(rid) },
            },
        )
    }
}
impl<T> WfClientExt for T where T: WfHandleClient + Sized {}
