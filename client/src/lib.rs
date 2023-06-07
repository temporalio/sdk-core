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
pub use metrics::ClientMetricProvider;
pub use raw::{HealthService, OperatorService, TestService, WorkflowService};
pub use temporal_sdk_core_protos::temporal::api::{
    enums::v1::ArchivalState,
    filter::v1::{StartTimeFilter, StatusFilter, WorkflowExecutionFilter, WorkflowTypeFilter},
    workflowservice::v1::{
        list_closed_workflow_executions_request::Filters as ListClosedFilters,
        list_open_workflow_executions_request::Filters as ListOpenFilters,
    },
};
pub use tonic;
pub use workflow_handle::{WorkflowExecutionInfo, WorkflowExecutionResult};

use crate::{
    metrics::{GrpcMetricSvc, MetricsContext},
    raw::{sealed::RawClientLike, AttachMetricLabels},
    sealed::WfHandleClient,
    workflow_handle::UntypedWorkflowHandle,
};
use backoff::{exponential, ExponentialBackoff, SystemClock};
use http::{uri::InvalidUri, Uri};
use once_cell::sync::OnceCell;
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
    grpc::health::v1::health_client::HealthClient,
    temporal::api::{
        common::v1::{Header, Payload, Payloads, WorkflowExecution, WorkflowType},
        enums::v1::{TaskQueueKind, WorkflowIdReusePolicy},
        failure::v1::Failure,
        operatorservice::v1::operator_service_client::OperatorServiceClient,
        query::v1::WorkflowQuery,
        replication::v1::ClusterReplicationConfig,
        taskqueue::v1::TaskQueue,
        testservice::v1::test_service_client::TestServiceClient,
        workflowservice::v1::{workflow_service_client::WorkflowServiceClient, *},
    },
    TaskToken,
};
use tonic::{
    body::BoxBody,
    client::GrpcService,
    codegen::InterceptedService,
    metadata::{MetadataKey, MetadataValue},
    service::Interceptor,
    transport::{Certificate, Channel, Endpoint, Identity},
    Code, Status,
};
use tower::ServiceBuilder;
use url::Url;
use uuid::Uuid;

static CLIENT_NAME_HEADER_KEY: &str = "client-name";
static CLIENT_VERSION_HEADER_KEY: &str = "client-version";
/// These must match the gRPC method names, not the snake case versions that exist in the Rust code.
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

    /// If set, override the origin used when connecting. May be useful in rare situations where tls
    /// verification needs to use a different name from what should be set as the `:authority`
    /// header. If [TlsConfig::domain] is set, and this is not, this will be set to
    /// `https://<domain>`, effectively making the `:authority` header consistent with the domain
    /// override.
    #[builder(default)]
    pub override_origin: Option<Uri>,
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

    pub(crate) const fn throttle_retry_policy() -> Self {
        Self {
            initial_interval: Duration::from_secs(1),
            randomization_factor: 0.2,
            multiplier: 2.0,
            max_interval: Duration::from_secs(10),
            max_elapsed_time: None,
            max_retries: 0,
        }
    }

    pub(crate) fn into_exp_backoff<C>(self, clock: C) -> exponential::ExponentialBackoff<C> {
        exponential::ExponentialBackoff {
            current_interval: self.initial_interval,
            initial_interval: self.initial_interval,
            randomization_factor: self.randomization_factor,
            multiplier: self.multiplier,
            max_interval: self.max_interval,
            max_elapsed_time: self.max_elapsed_time,
            clock,
            start_time: Instant::now(),
        }
    }
}

impl From<RetryConfig> for ExponentialBackoff {
    fn from(c: RetryConfig) -> Self {
        c.into_exp_backoff(SystemClock::default())
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

/// A client with [ClientOptions] attached, which can be passed to initialize workers,
/// or can be used directly. Is cheap to clone.
#[derive(Clone, Debug)]
pub struct ConfiguredClient<C> {
    client: C,
    options: Arc<ClientOptions>,
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
        metrics_meter: Option<&dyn ClientMetricProvider>,
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
        metrics_meter: Option<&dyn ClientMetricProvider>,
        headers: Option<Arc<RwLock<HashMap<String, String>>>>,
    ) -> Result<RetryClient<ConfiguredClient<TemporalServiceClientWithMetrics>>, ClientInitError>
    {
        let channel = Channel::from_shared(self.target_url.to_string())?;
        let channel = self.add_tls_to_channel(channel).await?;
        let channel = if let Some(origin) = self.override_origin.clone() {
            channel.origin(origin)
        } else {
            channel
        };
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
        let svc = InterceptedService::new(service, interceptor);

        let mut client = ConfiguredClient {
            headers,
            client: TemporalServiceClient::new(svc),
            options: Arc::new(self.clone()),
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
    async fn add_tls_to_channel(&self, mut channel: Endpoint) -> Result<Endpoint, ClientInitError> {
        if let Some(tls_cfg) = &self.tls_cfg {
            let mut tls = tonic::transport::ClientTlsConfig::new();

            if let Some(root_cert) = &tls_cfg.server_root_ca_cert {
                let server_root_ca_cert = Certificate::from_pem(root_cert);
                tls = tls.ca_certificate(server_root_ca_cert);
            }

            if let Some(domain) = &tls_cfg.domain {
                tls = tls.domain_name(domain);

                // This song and dance ultimately is just to make sure the `:authority` header ends
                // up correct on requests while we use TLS. Setting the header directly in our
                // interceptor doesn't work since seemingly it is overridden at some point by
                // something lower level.
                let uri: Uri = format!("https://{}", domain).parse()?;
                channel = channel.origin(uri);
            }

            if let Some(client_opts) = &tls_cfg.client_tls_config {
                let client_identity =
                    Identity::from_pem(&client_opts.client_cert, &client_opts.client_private_key);
                tls = tls.identity(client_identity);
            }

            return channel.tls_config(tls).map_err(Into::into);
        }
        Ok(channel)
    }
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
        if !metadata.contains_key(CLIENT_NAME_HEADER_KEY) {
            metadata.insert(
                CLIENT_NAME_HEADER_KEY,
                self.opts
                    .client_name
                    .parse()
                    .unwrap_or_else(|_| MetadataValue::from_static("")),
            );
        }
        if !metadata.contains_key(CLIENT_VERSION_HEADER_KEY) {
            metadata.insert(
                CLIENT_VERSION_HEADER_KEY,
                self.opts
                    .client_version
                    .parse()
                    .unwrap_or_else(|_| MetadataValue::from_static("")),
            );
        }
        let headers = &*self.headers.read();
        for (k, v) in headers {
            if metadata.contains_key(k) {
                // Don't overwrite per-request specified headers
                continue;
            }
            if let (Ok(k), Ok(v)) = (MetadataKey::from_str(k), v.parse()) {
                metadata.insert(k, v);
            }
        }
        if !metadata.contains_key("grpc-timeout") {
            request.set_timeout(OTHER_CALL_TIMEOUT);
        }

        Ok(request)
    }
}

/// Aggregates various services exposed by the Temporal server
#[derive(Debug, Clone)]
pub struct TemporalServiceClient<T> {
    svc: T,
    workflow_svc_client: OnceCell<WorkflowServiceClient<T>>,
    operator_svc_client: OnceCell<OperatorServiceClient<T>>,
    test_svc_client: OnceCell<TestServiceClient<T>>,
    health_svc_client: OnceCell<HealthClient<T>>,
}
impl<T> TemporalServiceClient<T>
where
    T: Clone,
    T: GrpcService<BoxBody> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    fn new(svc: T) -> Self {
        Self {
            svc,
            workflow_svc_client: OnceCell::new(),
            operator_svc_client: OnceCell::new(),
            test_svc_client: OnceCell::new(),
            health_svc_client: OnceCell::new(),
        }
    }
    /// Get the underlying workflow service client
    pub fn workflow_svc(&self) -> &WorkflowServiceClient<T> {
        self.workflow_svc_client
            .get_or_init(|| WorkflowServiceClient::new(self.svc.clone()))
    }
    /// Get the underlying operator service client
    pub fn operator_svc(&self) -> &OperatorServiceClient<T> {
        self.operator_svc_client
            .get_or_init(|| OperatorServiceClient::new(self.svc.clone()))
    }
    /// Get the underlying test service client
    pub fn test_svc(&self) -> &TestServiceClient<T> {
        self.test_svc_client
            .get_or_init(|| TestServiceClient::new(self.svc.clone()))
    }
    /// Get the underlying health service client
    pub fn health_svc(&self) -> &HealthClient<T> {
        self.health_svc_client
            .get_or_init(|| HealthClient::new(self.svc.clone()))
    }
    /// Get the underlying workflow service client mutably
    pub fn workflow_svc_mut(&mut self) -> &mut WorkflowServiceClient<T> {
        let _ = self.workflow_svc();
        self.workflow_svc_client.get_mut().unwrap()
    }
    /// Get the underlying operator service client mutably
    pub fn operator_svc_mut(&mut self) -> &mut OperatorServiceClient<T> {
        let _ = self.operator_svc();
        self.operator_svc_client.get_mut().unwrap()
    }
    /// Get the underlying test service client mutably
    pub fn test_svc_mut(&mut self) -> &mut TestServiceClient<T> {
        let _ = self.test_svc();
        self.test_svc_client.get_mut().unwrap()
    }
    /// Get the underlying health service client mutably
    pub fn health_svc_mut(&mut self) -> &mut HealthClient<T> {
        let _ = self.health_svc();
        self.health_svc_client.get_mut().unwrap()
    }
}
/// A [WorkflowServiceClient] with the default interceptors attached.
pub type WorkflowServiceClientWithMetrics = WorkflowServiceClient<InterceptedMetricsSvc>;
/// An [OperatorServiceClient] with the default interceptors attached.
pub type OperatorServiceClientWithMetrics = OperatorServiceClient<InterceptedMetricsSvc>;
/// An [TestServiceClient] with the default interceptors attached.
pub type TestServiceClientWithMetrics = TestServiceClient<InterceptedMetricsSvc>;
/// A [TemporalServiceClient] with the default interceptors attached.
pub type TemporalServiceClientWithMetrics = TemporalServiceClient<InterceptedMetricsSvc>;
type InterceptedMetricsSvc = InterceptedService<GrpcMetricSvc, ServiceCallInterceptor>;

/// Contains an instance of a namespace-bound client for interacting with the Temporal server
#[derive(Debug, Clone)]
pub struct Client {
    /// Client for interacting with workflow service
    inner: ConfiguredClient<TemporalServiceClientWithMetrics>,
    /// The namespace this client interacts with
    namespace: String,
}

impl Client {
    /// Create a new client from an existing configured lower level client and a namespace
    pub fn new(
        client: ConfiguredClient<TemporalServiceClientWithMetrics>,
        namespace: String,
    ) -> Self {
        Client {
            inner: client,
            namespace,
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
        self.inner.workflow_svc()
    }

    /// Return the options this client was initialized with
    pub fn options(&self) -> &ClientOptions {
        &self.inner.options
    }

    /// Return the options this client was initialized with mutably
    pub fn options_mut(&mut self) -> &mut ClientOptions {
        Arc::make_mut(&mut self.inner.options)
    }

    /// Returns a reference to the underlying client
    pub fn inner(&self) -> &ConfiguredClient<TemporalServiceClientWithMetrics> {
        &self.inner
    }

    /// Consumes self and returns the underlying client
    pub fn into_inner(self) -> ConfiguredClient<TemporalServiceClientWithMetrics> {
        self.inner
    }

    fn wf_svc(&self) -> WorkflowServiceClientWithMetrics {
        self.inner.workflow_svc().clone()
    }
}

/// Enum to help reference a namespace by either the namespace name or the namespace id
#[derive(Clone)]
pub enum Namespace {
    /// Namespace name
    Name(String),
    /// Namespace id
    Id(String),
}

impl Namespace {
    fn into_describe_namespace_request(self) -> DescribeNamespaceRequest {
        let (namespace, id) = match self {
            Namespace::Name(n) => (n, "".to_owned()),
            Namespace::Id(n) => ("".to_owned(), n),
        };
        DescribeNamespaceRequest { namespace, id }
    }
}

/// Default workflow execution retention for a Namespace is 3 days
pub const DEFAULT_WORKFLOW_EXECUTION_RETENTION_PERIOD: Duration =
    Duration::from_secs(60 * 60 * 24 * 3);

/// Helper struct for `register_namespace`.
#[derive(Clone, derive_builder::Builder)]
pub struct RegisterNamespaceOptions {
    /// Name (required)
    #[builder(setter(into))]
    pub namespace: String,
    /// Description (required)
    #[builder(setter(into))]
    pub description: String,
    /// Owner's email
    #[builder(setter(into), default)]
    pub owner_email: String,
    /// Workflow execution retention period
    #[builder(default = "DEFAULT_WORKFLOW_EXECUTION_RETENTION_PERIOD")]
    pub workflow_execution_retention_period: Duration,
    /// Cluster settings
    #[builder(setter(strip_option, custom), default)]
    pub clusters: Vec<ClusterReplicationConfig>,
    /// Active cluster name
    #[builder(setter(into), default)]
    pub active_cluster_name: String,
    /// Custom Data
    #[builder(default)]
    pub data: HashMap<String, String>,
    /// Security Token
    #[builder(setter(into), default)]
    pub security_token: String,
    /// Global namespace
    #[builder(default)]
    pub is_global_namespace: bool,
    /// History Archival setting
    #[builder(setter(into), default = "ArchivalState::Unspecified")]
    pub history_archival_state: ArchivalState,
    /// History Archival uri
    #[builder(setter(into), default)]
    pub history_archival_uri: String,
    /// Visibility Archival setting
    #[builder(setter(into), default = "ArchivalState::Unspecified")]
    pub visibility_archival_state: ArchivalState,
    /// Visibility Archival uri
    #[builder(setter(into), default)]
    pub visibility_archival_uri: String,
}

impl RegisterNamespaceOptions {
    /// Builder convenience.  Less `use` imports
    pub fn builder() -> RegisterNamespaceOptionsBuilder {
        Default::default()
    }
}

impl From<RegisterNamespaceOptions> for RegisterNamespaceRequest {
    fn from(val: RegisterNamespaceOptions) -> Self {
        RegisterNamespaceRequest {
            namespace: val.namespace,
            description: val.description,
            owner_email: val.owner_email,
            workflow_execution_retention_period: val
                .workflow_execution_retention_period
                .try_into()
                .ok(),
            clusters: val.clusters,
            active_cluster_name: val.active_cluster_name,
            data: val.data,
            security_token: val.security_token,
            is_global_namespace: val.is_global_namespace,
            history_archival_state: val.history_archival_state as i32,
            history_archival_uri: val.history_archival_uri,
            visibility_archival_state: val.visibility_archival_state as i32,
            visibility_archival_uri: val.visibility_archival_uri,
        }
    }
}

impl RegisterNamespaceOptionsBuilder {
    /// Custum builder function for convenience
    /// Warning: setting cluster_names could blow away any previously set cluster configs
    pub fn cluster_names(&mut self, clusters: Vec<String>) {
        self.clusters = Some(
            clusters
                .into_iter()
                .map(|s| ClusterReplicationConfig { cluster_name: s })
                .collect(),
        );
    }
}

/// Helper struct for `signal_with_start_workflow_execution`.
#[derive(Clone, derive_builder::Builder)]
pub struct SignalWithStartOptions {
    /// Input payload for the workflow run
    #[builder(setter(strip_option), default)]
    pub input: Option<Payloads>,
    /// Task Queue to target (required)
    #[builder(setter(into))]
    pub task_queue: String,
    /// Workflow id for the workflow run
    #[builder(setter(into))]
    pub workflow_id: String,
    /// Workflow type for the workflow run
    #[builder(setter(into))]
    pub workflow_type: String,
    #[builder(setter(strip_option), default)]
    /// Request id for idempotency/deduplication
    pub request_id: Option<String>,
    /// The signal name to send (required)
    #[builder(setter(into))]
    pub signal_name: String,
    /// Payloads for the signal
    #[builder(default)]
    pub signal_input: Option<Payloads>,
    #[builder(setter(strip_option), default)]
    /// Headers for the signal
    pub signal_header: Option<Header>,
}

impl SignalWithStartOptions {
    /// Builder convenience.  Less `use` imports
    pub fn builder() -> SignalWithStartOptionsBuilder {
        Default::default()
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
        request_id: Option<String>,
        options: WorkflowOptions,
    ) -> Result<StartWorkflowExecutionResponse>;

    /// Notifies the server that workflow tasks for a given workflow should be sent to the normal
    /// non-sticky task queue. This normally happens when workflow has been evicted from the cache.
    async fn reset_sticky_task_queue(
        &self,
        workflow_id: String,
        run_id: String,
    ) -> Result<ResetStickyTaskQueueResponse>;

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

    /// Send a signal to a certain workflow instance
    async fn signal_workflow_execution(
        &self,
        workflow_id: String,
        run_id: String,
        signal_name: String,
        payloads: Option<Payloads>,
        request_id: Option<String>,
    ) -> Result<SignalWorkflowExecutionResponse>;

    /// Send signal and start workflow transcationally
    //#TODO maybe lift the Signal type from sdk::workflow_context::options
    #[allow(clippy::too_many_arguments)]
    async fn signal_with_start_workflow_execution(
        &self,
        options: SignalWithStartOptions,
        workflow_options: WorkflowOptions,
    ) -> Result<SignalWithStartWorkflowExecutionResponse>;

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
        request_id: Option<String>,
    ) -> Result<RequestCancelWorkflowExecutionResponse>;

    /// Terminate a currently executing workflow
    async fn terminate_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<TerminateWorkflowExecutionResponse>;

    /// Register a new namespace
    async fn register_namespace(
        &self,
        options: RegisterNamespaceOptions,
    ) -> Result<RegisterNamespaceResponse>;

    /// Lists all available namespaces
    async fn list_namespaces(&self) -> Result<ListNamespacesResponse>;

    /// Query namespace details
    async fn describe_namespace(&self, namespace: Namespace) -> Result<DescribeNamespaceResponse>;

    /// List open workflow executions with Standard Visibility filtering
    async fn list_open_workflow_executions(
        &self,
        max_page_size: i32,
        next_page_token: Vec<u8>,
        start_time_filter: Option<StartTimeFilter>,
        filters: Option<ListOpenFilters>,
    ) -> Result<ListOpenWorkflowExecutionsResponse>;

    /// List closed workflow executions Standard Visibility filtering
    async fn list_closed_workflow_executions(
        &self,
        max_page_size: i32,
        next_page_token: Vec<u8>,
        start_time_filter: Option<StartTimeFilter>,
        filters: Option<ListClosedFilters>,
    ) -> Result<ListClosedWorkflowExecutionsResponse>;

    /// List workflow executions with Advanced Visibility filtering
    async fn list_workflow_executions(
        &self,
        page_size: i32,
        next_page_token: Vec<u8>,
        query: String,
    ) -> Result<ListWorkflowExecutionsResponse>;

    /// List archived workflow executions
    async fn list_archived_workflow_executions(
        &self,
        page_size: i32,
        next_page_token: Vec<u8>,
        query: String,
    ) -> Result<ListArchivedWorkflowExecutionsResponse>;

    /// Get Cluster Search Attributes
    async fn get_search_attributes(&self) -> Result<GetSearchAttributesResponse>;

    /// Returns options that were used to initialize the client
    fn get_options(&self) -> &ClientOptions;

    /// Returns the namespace this client is bound to
    fn namespace(&self) -> &str;
}

/// Optional fields supplied at the start of workflow execution
#[derive(Debug, Clone, Default)]
pub struct WorkflowOptions {
    /// Set the policy for reusing the workflow id
    pub id_reuse_policy: WorkflowIdReusePolicy,

    /// Optionally set the execution timeout for the workflow
    /// <https://docs.temporal.io/workflows/#workflow-execution-timeout>
    pub execution_timeout: Option<Duration>,

    /// Optionally indicates the default run timeout for a workflow run
    pub run_timeout: Option<Duration>,

    /// Optionally indicates the default task timeout for a workflow run
    pub task_timeout: Option<Duration>,

    /// Optionally set a cron schedule for the workflow
    pub cron_schedule: Option<String>,

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
        request_id: Option<String>,
        options: WorkflowOptions,
    ) -> Result<StartWorkflowExecutionResponse> {
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
                    kind: TaskQueueKind::Unspecified as i32,
                    normal_name: "".to_string(),
                }),
                request_id: request_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
                workflow_id_reuse_policy: options.id_reuse_policy as i32,
                workflow_execution_timeout: options
                    .execution_timeout
                    .and_then(|d| d.try_into().ok()),
                workflow_run_timeout: options.execution_timeout.and_then(|d| d.try_into().ok()),
                workflow_task_timeout: options.task_timeout.and_then(|d| d.try_into().ok()),
                search_attributes: options.search_attributes.and_then(|d| d.try_into().ok()),
                cron_schedule: options.cron_schedule.unwrap_or_default(),
                ..Default::default()
            })
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
                worker_version: None,
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
                worker_version: None,
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
                worker_version: None,
            })
            .await?
            .into_inner())
    }

    async fn signal_workflow_execution(
        &self,
        workflow_id: String,
        run_id: String,
        signal_name: String,
        payloads: Option<Payloads>,
        request_id: Option<String>,
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
                request_id: request_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
                ..Default::default()
            })
            .await?
            .into_inner())
    }

    async fn signal_with_start_workflow_execution(
        &self,
        options: SignalWithStartOptions,
        workflow_options: WorkflowOptions,
    ) -> Result<SignalWithStartWorkflowExecutionResponse> {
        Ok(self
            .wf_svc()
            .signal_with_start_workflow_execution(SignalWithStartWorkflowExecutionRequest {
                namespace: self.namespace.clone(),
                workflow_id: options.workflow_id,
                workflow_type: Some(WorkflowType {
                    name: options.workflow_type,
                }),
                task_queue: Some(TaskQueue {
                    name: options.task_queue,
                    kind: TaskQueueKind::Normal as i32,
                    normal_name: "".to_string(),
                }),
                input: options.input,
                signal_name: options.signal_name,
                signal_input: options.signal_input,
                identity: self.inner.options.identity.clone(),
                request_id: options
                    .request_id
                    .unwrap_or_else(|| Uuid::new_v4().to_string()),
                workflow_id_reuse_policy: workflow_options.id_reuse_policy as i32,
                workflow_execution_timeout: workflow_options
                    .execution_timeout
                    .and_then(|d| d.try_into().ok()),
                workflow_run_timeout: workflow_options
                    .execution_timeout
                    .and_then(|d| d.try_into().ok()),
                workflow_task_timeout: workflow_options
                    .task_timeout
                    .and_then(|d| d.try_into().ok()),
                search_attributes: workflow_options
                    .search_attributes
                    .and_then(|d| d.try_into().ok()),
                cron_schedule: workflow_options.cron_schedule.unwrap_or_default(),
                header: options.signal_header,
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
        request_id: Option<String>,
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
                request_id: request_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
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

    async fn register_namespace(
        &self,
        options: RegisterNamespaceOptions,
    ) -> Result<RegisterNamespaceResponse> {
        let req = Into::<RegisterNamespaceRequest>::into(options);
        Ok(self.wf_svc().register_namespace(req).await?.into_inner())
    }

    async fn list_namespaces(&self) -> Result<ListNamespacesResponse> {
        Ok(self
            .wf_svc()
            .list_namespaces(ListNamespacesRequest::default())
            .await?
            .into_inner())
    }

    async fn describe_namespace(&self, namespace: Namespace) -> Result<DescribeNamespaceResponse> {
        Ok(self
            .wf_svc()
            .describe_namespace(namespace.into_describe_namespace_request())
            .await?
            .into_inner())
    }

    async fn list_open_workflow_executions(
        &self,
        maximum_page_size: i32,
        next_page_token: Vec<u8>,
        start_time_filter: Option<StartTimeFilter>,
        filters: Option<ListOpenFilters>,
    ) -> Result<ListOpenWorkflowExecutionsResponse> {
        Ok(self
            .wf_svc()
            .list_open_workflow_executions(ListOpenWorkflowExecutionsRequest {
                namespace: self.namespace.clone(),
                maximum_page_size,
                next_page_token,
                start_time_filter,
                filters,
            })
            .await?
            .into_inner())
    }

    async fn list_closed_workflow_executions(
        &self,
        maximum_page_size: i32,
        next_page_token: Vec<u8>,
        start_time_filter: Option<StartTimeFilter>,
        filters: Option<ListClosedFilters>,
    ) -> Result<ListClosedWorkflowExecutionsResponse> {
        Ok(self
            .wf_svc()
            .list_closed_workflow_executions(ListClosedWorkflowExecutionsRequest {
                namespace: self.namespace.clone(),
                maximum_page_size,
                next_page_token,
                start_time_filter,
                filters,
            })
            .await?
            .into_inner())
    }

    async fn list_workflow_executions(
        &self,
        page_size: i32,
        next_page_token: Vec<u8>,
        query: String,
    ) -> Result<ListWorkflowExecutionsResponse> {
        Ok(self
            .wf_svc()
            .list_workflow_executions(ListWorkflowExecutionsRequest {
                namespace: self.namespace.clone(),
                page_size,
                next_page_token,
                query,
            })
            .await?
            .into_inner())
    }

    async fn list_archived_workflow_executions(
        &self,
        page_size: i32,
        next_page_token: Vec<u8>,
        query: String,
    ) -> Result<ListArchivedWorkflowExecutionsResponse> {
        Ok(self
            .wf_svc()
            .list_archived_workflow_executions(ListArchivedWorkflowExecutionsRequest {
                namespace: self.namespace.clone(),
                page_size,
                next_page_token,
                query,
            })
            .await?
            .into_inner())
    }

    async fn get_search_attributes(&self) -> Result<GetSearchAttributesResponse> {
        Ok(self
            .wf_svc()
            .get_search_attributes(GetSearchAttributesRequest {})
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

    pub trait WfHandleClient:
        WorkflowClientTrait + RawClientLike<SvcType = InterceptedMetricsSvc>
    {
    }
    impl<T> WfHandleClient for T where
        T: WorkflowClientTrait + RawClientLike<SvcType = InterceptedMetricsSvc>
    {
    }
}

/// Additional methods for workflow clients
pub trait WfClientExt: WfHandleClient + Sized + Clone {
    /// Create an untyped handle for a workflow execution, which can be used to do things like
    /// wait for that workflow's result. `run_id` may be left blank to target the latest run.
    fn get_untyped_workflow_handle(
        &self,
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
    ) -> UntypedWorkflowHandle<Self> {
        let rid = run_id.into();
        UntypedWorkflowHandle::new(
            self.clone(),
            WorkflowExecutionInfo {
                namespace: self.namespace().to_string(),
                workflow_id: workflow_id.into(),
                run_id: if rid.is_empty() { None } else { Some(rid) },
            },
        )
    }
}
impl<T> WfClientExt for T where T: WfHandleClient + Clone + Sized {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn respects_per_call_headers() {
        let opts = ClientOptionsBuilder::default()
            .identity("enchicat".to_string())
            .target_url(Url::parse("https://smolkitty").unwrap())
            .client_name("cute-kitty".to_string())
            .client_version("0.1.0".to_string())
            .build()
            .unwrap();

        let mut static_headers = HashMap::new();
        static_headers.insert("enchi".to_string(), "kitty".to_string());
        let mut iceptor = ServiceCallInterceptor {
            opts,
            headers: Arc::new(RwLock::new(static_headers)),
        };
        let mut req = tonic::Request::new(());
        req.metadata_mut().insert("enchi", "cat".parse().unwrap());
        let next_req = iceptor.call(req).unwrap();
        assert_eq!(next_req.metadata().get("enchi").unwrap(), "cat");
    }
}
