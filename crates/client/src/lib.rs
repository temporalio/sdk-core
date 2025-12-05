#![warn(missing_docs)] // error if there are missing docs

//! This crate contains client implementations that can be used to contact the Temporal service.
//!
//! It implements auto-retry behavior and metrics collection.

#[macro_use]
extern crate tracing;

pub mod callback_based;
mod metrics;
/// Visible only for tests
#[doc(hidden)]
pub mod proxy;
mod raw;
mod replaceable;
pub mod request_extensions;
mod retry;
pub mod worker;
mod workflow_handle;

pub use crate::{
    proxy::HttpConnectProxyOptions,
    retry::{CallType, RETRYABLE_ERROR_CODES, RetryClient},
};
pub use metrics::{LONG_REQUEST_LATENCY_HISTOGRAM_NAME, REQUEST_LATENCY_HISTOGRAM_NAME};
pub use raw::{CloudService, HealthService, OperatorService, TestService, WorkflowService};
pub use replaceable::SharedReplaceableClient;
pub use tonic;
pub use workflow_handle::{
    GetWorkflowResultOptions, WorkflowExecutionInfo, WorkflowExecutionResult, WorkflowHandle,
};

use crate::{
    metrics::{ChannelOrGrpcOverride, GrpcMetricSvc, MetricsContext},
    raw::AttachMetricLabels,
    request_extensions::RequestExt,
    sealed::WfHandleClient,
    worker::ClientWorkerSet,
    workflow_handle::UntypedWorkflowHandle,
};
use backoff::{ExponentialBackoff, SystemClock, exponential};
use http::{Uri, uri::InvalidUri};
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    ops::{Deref, DerefMut},
    str::FromStr,
    sync::{Arc, OnceLock},
    time::{Duration, Instant},
};
use temporalio_common::{
    protos::{
        TaskToken,
        coresdk::IntoPayloadsExt,
        grpc::health::v1::health_client::HealthClient,
        temporal::api::{
            cloud::cloudservice::v1::cloud_service_client::CloudServiceClient,
            common::{
                self,
                v1::{Header, Payload, Payloads, RetryPolicy, WorkflowExecution, WorkflowType},
            },
            enums::v1::{
                ArchivalState, TaskQueueKind, WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
            },
            filter::v1::StartTimeFilter,
            operatorservice::v1::operator_service_client::OperatorServiceClient,
            query::v1::WorkflowQuery,
            replication::v1::ClusterReplicationConfig,
            taskqueue::v1::TaskQueue,
            testservice::v1::test_service_client::TestServiceClient,
            update,
            workflowservice::v1::{workflow_service_client::WorkflowServiceClient, *},
        },
    },
    telemetry::metrics::TemporalMeter,
};
use tonic::{
    Code, IntoRequest,
    body::Body,
    client::GrpcService,
    codegen::InterceptedService,
    metadata::{
        AsciiMetadataKey, AsciiMetadataValue, BinaryMetadataKey, BinaryMetadataValue, MetadataMap,
        MetadataValue,
    },
    service::Interceptor,
    transport::{Certificate, Channel, Endpoint, Identity},
};
use tower::ServiceBuilder;
use url::Url;
use uuid::Uuid;

static CLIENT_NAME_HEADER_KEY: &str = "client-name";
static CLIENT_VERSION_HEADER_KEY: &str = "client-version";
static TEMPORAL_NAMESPACE_HEADER_KEY: &str = "temporal-namespace";

/// Key used to communicate when a GRPC message is too large
pub static MESSAGE_TOO_LARGE_KEY: &str = "message-too-large";
/// Key used to indicate a error was returned by the retryer because of the short-circuit predicate
pub static ERROR_RETURNED_DUE_TO_SHORT_CIRCUIT: &str = "short-circuit";

/// The server times out polls after 60 seconds. Set our timeout to be slightly beyond that.
const LONG_POLL_TIMEOUT: Duration = Duration::from_secs(70);
const OTHER_CALL_TIMEOUT: Duration = Duration::from_secs(30);

type Result<T, E = tonic::Status> = std::result::Result<T, E>;

/// Options for the connection to the temporal server. Construct with [ClientOptions::builder]
#[derive(Clone, Debug, bon::Builder)]
#[non_exhaustive]
#[builder(on(String, into), state_mod(vis = "pub"))]
pub struct ClientOptions {
    /// The URL of the Temporal server to connect to
    #[builder(into)]
    pub target_url: Url,

    /// The name of the SDK being implemented on top of core. Is set as `client-name` header in
    /// all RPC calls
    pub client_name: String,

    /// The version of the SDK being implemented on top of core. Is set as `client-version` header
    /// in all RPC calls. The server decides if the client is supported based on this.
    pub client_version: String,

    /// A human-readable string that can identify this process. Defaults to empty string.
    #[builder(default)]
    pub identity: String,

    /// If specified, use TLS as configured by the [TlsOptions] struct. If this is set core will
    /// attempt to use TLS when connecting to the Temporal server. Lang SDK is expected to pass any
    /// certs or keys as bytes, loading them from disk itself if needed.
    pub tls_options: Option<TlsOptions>,

    /// Retry configuration for the server client. Default is [RetryOptions::default]
    #[builder(default)]
    pub retry_options: RetryOptions,

    /// If set, override the origin used when connecting. May be useful in rare situations where tls
    /// verification needs to use a different name from what should be set as the `:authority`
    /// header. If [TlsOptions::domain] is set, and this is not, this will be set to
    /// `https://<domain>`, effectively making the `:authority` header consistent with the domain
    /// override.
    pub override_origin: Option<Uri>,

    /// If set, HTTP2 gRPC keep alive will be enabled.
    /// To enable with default settings, use `.keep_alive(ClientKeepAliveConfig::default())`.
    #[builder(required, default = Some(ClientKeepAliveOptions::default()))]
    pub keep_alive: Option<ClientKeepAliveOptions>,

    /// HTTP headers to include on every RPC call.
    ///
    /// These must be valid gRPC metadata keys, and must not be binary metadata keys (ending in
    /// `-bin). To set binary headers, use [ClientOptions::binary_headers]. Invalid header keys or
    /// values will cause an error to be returned when connecting.
    pub headers: Option<HashMap<String, String>>,

    /// HTTP headers to include on every RPC call as binary gRPC metadata (encoded as base64).
    ///
    /// These must be valid binary gRPC metadata keys (and end with a `-bin` suffix). Invalid
    /// header keys will cause an error to be returned when connecting.
    pub binary_headers: Option<HashMap<String, Vec<u8>>>,

    /// API key which is set as the "Authorization" header with "Bearer " prepended. This will only
    /// be applied if the headers don't already have an "Authorization" header.
    pub api_key: Option<String>,

    /// HTTP CONNECT proxy to use for this client.
    pub http_connect_proxy: Option<HttpConnectProxyOptions>,

    /// If set true, error code labels will not be included on request failure metrics.
    #[builder(default)]
    pub disable_error_code_metric_tags: bool,

    /// If set true, get_system_info will not be called upon connection
    #[builder(default)]
    pub skip_get_system_info: bool,
}

/// Configuration options for TLS
#[derive(Clone, Debug, Default)]
pub struct TlsOptions {
    /// Bytes representing the root CA certificate used by the server. If not set, and the server's
    /// cert is issued by someone the operating system trusts, verification will still work (ex:
    /// Cloud offering).
    pub server_root_ca_cert: Option<Vec<u8>>,
    /// Sets the domain name against which to verify the server's TLS certificate. If not provided,
    /// the domain name will be extracted from the URL used to connect.
    pub domain: Option<String>,
    /// TLS info for the client. If specified, core will attempt to use mTLS.
    pub client_tls_options: Option<ClientTlsOptions>,
}

/// If using mTLS, both the client cert and private key must be specified, this contains them.
#[derive(Clone)]
pub struct ClientTlsOptions {
    /// The certificate for this client, encoded as PEM
    pub client_cert: Vec<u8>,
    /// The private key for this client, encoded as PEM
    pub client_private_key: Vec<u8>,
}

/// Client keep alive configuration.
#[derive(Clone, Debug)]
pub struct ClientKeepAliveOptions {
    /// Interval to send HTTP2 keep alive pings.
    pub interval: Duration,
    /// Timeout that the keep alive must be responded to within or the connection will be closed.
    pub timeout: Duration,
}

impl Default for ClientKeepAliveOptions {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(30),
            timeout: Duration::from_secs(15),
        }
    }
}

/// Configuration for retrying requests to the server
#[derive(Clone, Debug, PartialEq)]
pub struct RetryOptions {
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

impl Default for RetryOptions {
    fn default() -> Self {
        Self {
            initial_interval: Duration::from_millis(100), // 100 ms wait by default.
            randomization_factor: 0.2,                    // +-20% jitter.
            multiplier: 1.7, // each next retry delay will increase by 70%
            max_interval: Duration::from_secs(5), // until it reaches 5 seconds.
            max_elapsed_time: Some(Duration::from_secs(10)), // 10 seconds total allocated time for all retries.
            max_retries: 10,
        }
    }
}

impl RetryOptions {
    pub(crate) const fn task_poll_retry_policy() -> Self {
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

    /// A retry policy that never retires
    pub const fn no_retries() -> Self {
        Self {
            initial_interval: Duration::from_secs(0),
            randomization_factor: 0.0,
            multiplier: 1.0,
            max_interval: Duration::from_secs(0),
            max_elapsed_time: None,
            max_retries: 1,
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

impl From<RetryOptions> for ExponentialBackoff {
    fn from(c: RetryOptions) -> Self {
        c.into_exp_backoff(SystemClock::default())
    }
}

impl Debug for ClientTlsOptions {
    // Intentionally omit details here since they could leak a key if ever printed
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientTlsOptions(..)")
    }
}

/// Errors thrown while attempting to establish a connection to the server
#[derive(thiserror::Error, Debug)]
pub enum ClientInitError {
    /// Invalid URI. Configuration error, fatal.
    #[error("Invalid URI: {0:?}")]
    InvalidUri(#[from] InvalidUri),
    /// Invalid gRPC metadata headers. Configuration error.
    #[error("Invalid headers: {0}")]
    InvalidHeaders(#[from] InvalidHeaderError),
    /// Server connection error. Crashing and restarting the worker is likely best.
    #[error("Server connection error: {0:?}")]
    TonicTransportError(#[from] tonic::transport::Error),
    /// We couldn't successfully make the `get_system_info` call at connection time to establish
    /// server capabilities / verify server is responding.
    #[error("`get_system_info` call error after connection: {0:?}")]
    SystemInfoCallError(tonic::Status),
}

/// Errors thrown when a gRPC metadata header is invalid.
#[derive(thiserror::Error, Debug)]
pub enum InvalidHeaderError {
    /// A binary header key was invalid
    #[error("Invalid binary header key '{key}': {source}")]
    InvalidBinaryHeaderKey {
        /// The invalid key
        key: String,
        /// The source error from tonic
        source: tonic::metadata::errors::InvalidMetadataKey,
    },
    /// An ASCII header key was invalid
    #[error("Invalid ASCII header key '{key}': {source}")]
    InvalidAsciiHeaderKey {
        /// The invalid key
        key: String,
        /// The source error from tonic
        source: tonic::metadata::errors::InvalidMetadataKey,
    },
    /// An ASCII header value was invalid
    #[error("Invalid ASCII header value for key '{key}': {source}")]
    InvalidAsciiHeaderValue {
        /// The key
        key: String,
        /// The invalid value
        value: String,
        /// The source error from tonic
        source: tonic::metadata::errors::InvalidMetadataValue,
    },
}

/// A client with [ClientOptions] attached, which can be passed to initialize workers,
/// or can be used directly. Is cheap to clone.
#[derive(Clone, Debug)]
pub struct ConfiguredClient<C> {
    client: C,
    options: Arc<ClientOptions>,
    headers: Arc<RwLock<ClientHeaders>>,
    /// Capabilities as read from the `get_system_info` RPC call made on client connection
    capabilities: Option<get_system_info_response::Capabilities>,
    workers: Arc<ClientWorkerSet>,
}

impl<C> ConfiguredClient<C> {
    /// Set HTTP request headers overwriting previous headers.
    ///
    /// This will not affect headers set via [ClientOptions::binary_headers].
    ///
    /// # Errors
    ///
    /// Will return an error if any of the provided keys or values are not valid gRPC metadata.
    /// If an error is returned, the previous headers will remain unchanged.
    pub fn set_headers(&self, headers: HashMap<String, String>) -> Result<(), InvalidHeaderError> {
        self.headers.write().user_headers = parse_ascii_headers(headers)?;
        Ok(())
    }

    /// Set binary HTTP request headers overwriting previous headers.
    ///
    /// This will not affect headers set via [ClientOptions::headers].
    ///
    /// # Errors
    ///
    /// Will return an error if any of the provided keys are not valid gRPC binary metadata keys.
    /// If an error is returned, the previous headers will remain unchanged.
    pub fn set_binary_headers(
        &self,
        binary_headers: HashMap<String, Vec<u8>>,
    ) -> Result<(), InvalidHeaderError> {
        self.headers.write().user_binary_headers = parse_binary_headers(binary_headers)?;
        Ok(())
    }

    /// Set API key, overwriting previous
    pub fn set_api_key(&self, api_key: Option<String>) {
        self.headers.write().api_key = api_key;
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

    /// Returns a cloned reference to a registry with workers using this client instance
    pub fn workers(&self) -> Arc<ClientWorkerSet> {
        self.workers.clone()
    }

    /// Returns the worker grouping key, this should be unique across each client
    pub fn worker_grouping_key(&self) -> Uuid {
        self.workers.worker_grouping_key()
    }
}

#[derive(Debug)]
struct ClientHeaders {
    user_headers: HashMap<AsciiMetadataKey, AsciiMetadataValue>,
    user_binary_headers: HashMap<BinaryMetadataKey, BinaryMetadataValue>,
    api_key: Option<String>,
}

impl ClientHeaders {
    fn apply_to_metadata(&self, metadata: &mut MetadataMap) {
        for (key, val) in self.user_headers.iter() {
            // Only if not already present
            if !metadata.contains_key(key) {
                metadata.insert(key, val.clone());
            }
        }
        for (key, val) in self.user_binary_headers.iter() {
            // Only if not already present
            if !metadata.contains_key(key) {
                metadata.insert_bin(key, val.clone());
            }
        }
        if let Some(api_key) = &self.api_key {
            // Only if not already present
            if !metadata.contains_key("authorization")
                && let Ok(val) = format!("Bearer {api_key}").parse()
            {
                metadata.insert("authorization", val);
            }
        }
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
        metrics_meter: Option<TemporalMeter>,
    ) -> Result<RetryClient<Client>, ClientInitError> {
        let client = self.connect_no_namespace(metrics_meter).await?.into_inner();
        let client = Client::new(client, namespace.into());
        let retry_client = RetryClient::new(client, self.retry_options.clone());
        Ok(retry_client)
    }

    /// Attempt to establish a connection to the Temporal server and return a gRPC client which is
    /// intercepted with retry, default headers functionality, and metrics if provided.
    ///
    /// See [RetryClient] for more
    pub async fn connect_no_namespace(
        &self,
        metrics_meter: Option<TemporalMeter>,
    ) -> Result<RetryClient<ConfiguredClient<TemporalServiceClient>>, ClientInitError> {
        self.connect_no_namespace_with_service_override(metrics_meter, None)
            .await
    }

    /// Attempt to establish a connection to the Temporal server and return a gRPC client which is
    /// intercepted with retry, default headers functionality, and metrics if provided. If a
    /// service_override is present, network-specific options are ignored and the callback is
    /// invoked for each gRPC call.
    ///
    /// See [RetryClient] for more
    pub async fn connect_no_namespace_with_service_override(
        &self,
        metrics_meter: Option<TemporalMeter>,
        service_override: Option<callback_based::CallbackBasedGrpcService>,
    ) -> Result<RetryClient<ConfiguredClient<TemporalServiceClient>>, ClientInitError> {
        let service = if let Some(service_override) = service_override {
            GrpcMetricSvc {
                inner: ChannelOrGrpcOverride::GrpcOverride(service_override),
                metrics: metrics_meter.clone().map(MetricsContext::new),
                disable_errcode_label: self.disable_error_code_metric_tags,
            }
        } else {
            let channel = Channel::from_shared(self.target_url.to_string())?;
            let channel = self.add_tls_to_channel(channel).await?;
            let channel = if let Some(keep_alive) = self.keep_alive.as_ref() {
                channel
                    .keep_alive_while_idle(true)
                    .http2_keep_alive_interval(keep_alive.interval)
                    .keep_alive_timeout(keep_alive.timeout)
            } else {
                channel
            };
            let channel = if let Some(origin) = self.override_origin.clone() {
                channel.origin(origin)
            } else {
                channel
            };
            // If there is a proxy, we have to connect that way
            let channel = if let Some(proxy) = self.http_connect_proxy.as_ref() {
                proxy.connect_endpoint(&channel).await?
            } else {
                channel.connect().await?
            };
            ServiceBuilder::new()
                .layer_fn(move |channel| GrpcMetricSvc {
                    inner: ChannelOrGrpcOverride::Channel(channel),
                    metrics: metrics_meter.clone().map(MetricsContext::new),
                    disable_errcode_label: self.disable_error_code_metric_tags,
                })
                .service(channel)
        };

        let headers = Arc::new(RwLock::new(ClientHeaders {
            user_headers: parse_ascii_headers(self.headers.clone().unwrap_or_default())?,
            user_binary_headers: parse_binary_headers(
                self.binary_headers.clone().unwrap_or_default(),
            )?,
            api_key: self.api_key.clone(),
        }));
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
            workers: Arc::new(ClientWorkerSet::new()),
        };
        if !self.skip_get_system_info {
            match client
                .get_system_info(GetSystemInfoRequest::default().into_request())
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
        }
        Ok(RetryClient::new(client, self.retry_options.clone()))
    }

    /// If TLS is configured, set the appropriate options on the provided channel and return it.
    /// Passes it through if TLS options not set.
    async fn add_tls_to_channel(&self, mut channel: Endpoint) -> Result<Endpoint, ClientInitError> {
        if let Some(tls_cfg) = &self.tls_options {
            let mut tls = tonic::transport::ClientTlsConfig::new();

            if let Some(root_cert) = &tls_cfg.server_root_ca_cert {
                let server_root_ca_cert = Certificate::from_pem(root_cert);
                tls = tls.ca_certificate(server_root_ca_cert);
            } else {
                tls = tls.with_native_roots();
            }

            if let Some(domain) = &tls_cfg.domain {
                tls = tls.domain_name(domain);

                // This song and dance ultimately is just to make sure the `:authority` header ends
                // up correct on requests while we use TLS. Setting the header directly in our
                // interceptor doesn't work since seemingly it is overridden at some point by
                // something lower level.
                let uri: Uri = format!("https://{domain}").parse()?;
                channel = channel.origin(uri);
            }

            if let Some(client_opts) = &tls_cfg.client_tls_options {
                let client_identity =
                    Identity::from_pem(&client_opts.client_cert, &client_opts.client_private_key);
                tls = tls.identity(client_identity);
            }

            return channel.tls_config(tls).map_err(Into::into);
        }
        Ok(channel)
    }
}

fn parse_ascii_headers(
    headers: HashMap<String, String>,
) -> Result<HashMap<AsciiMetadataKey, AsciiMetadataValue>, InvalidHeaderError> {
    let mut parsed_headers = HashMap::with_capacity(headers.len());
    for (k, v) in headers.into_iter() {
        let key = match AsciiMetadataKey::from_str(&k) {
            Ok(key) => key,
            Err(err) => {
                return Err(InvalidHeaderError::InvalidAsciiHeaderKey {
                    key: k,
                    source: err,
                });
            }
        };
        let value = match MetadataValue::from_str(&v) {
            Ok(value) => value,
            Err(err) => {
                return Err(InvalidHeaderError::InvalidAsciiHeaderValue {
                    key: k,
                    value: v,
                    source: err,
                });
            }
        };
        parsed_headers.insert(key, value);
    }

    Ok(parsed_headers)
}

fn parse_binary_headers(
    headers: HashMap<String, Vec<u8>>,
) -> Result<HashMap<BinaryMetadataKey, BinaryMetadataValue>, InvalidHeaderError> {
    let mut parsed_headers = HashMap::with_capacity(headers.len());
    for (k, v) in headers.into_iter() {
        let key = match BinaryMetadataKey::from_str(&k) {
            Ok(key) => key,
            Err(err) => {
                return Err(InvalidHeaderError::InvalidBinaryHeaderKey {
                    key: k,
                    source: err,
                });
            }
        };
        let value = BinaryMetadataValue::from_bytes(&v);
        parsed_headers.insert(key, value);
    }

    Ok(parsed_headers)
}

/// Interceptor which attaches common metadata (like "client-name") to every outgoing call
#[derive(Clone)]
pub struct ServiceCallInterceptor {
    opts: ClientOptions,
    /// Only accessed as a reader
    headers: Arc<RwLock<ClientHeaders>>,
}

impl Interceptor for ServiceCallInterceptor {
    /// This function will get called on each outbound request. Returning a `Status` here will
    /// cancel the request and have that status returned to the caller.
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
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
        self.headers.read().apply_to_metadata(metadata);
        request.set_default_timeout(OTHER_CALL_TIMEOUT);

        Ok(request)
    }
}

/// Aggregates various services exposed by the Temporal server
#[derive(Clone)]
pub struct TemporalServiceClient {
    workflow_svc_client: Box<dyn WorkflowService>,
    operator_svc_client: Box<dyn OperatorService>,
    cloud_svc_client: Box<dyn CloudService>,
    test_svc_client: Box<dyn TestService>,
    health_svc_client: Box<dyn HealthService>,
}

/// We up the limit on incoming messages from server from the 4Mb default to 128Mb. If for
/// whatever reason this needs to be changed by the user, we support overriding it via env var.
fn get_decode_max_size() -> usize {
    static _DECODE_MAX_SIZE: OnceLock<usize> = OnceLock::new();
    *_DECODE_MAX_SIZE.get_or_init(|| {
        std::env::var("TEMPORAL_MAX_INCOMING_GRPC_BYTES")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(128 * 1024 * 1024)
    })
}

impl TemporalServiceClient {
    fn new<T>(svc: T) -> Self
    where
        T: GrpcService<Body> + Send + Sync + Clone + 'static,
        T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
        T::Error: Into<tonic::codegen::StdError>,
        <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
        <T as GrpcService<Body>>::Future: Send,
    {
        let workflow_svc_client = Box::new(
            WorkflowServiceClient::new(svc.clone())
                .max_decoding_message_size(get_decode_max_size()),
        );
        let operator_svc_client = Box::new(
            OperatorServiceClient::new(svc.clone())
                .max_decoding_message_size(get_decode_max_size()),
        );
        let cloud_svc_client = Box::new(
            CloudServiceClient::new(svc.clone()).max_decoding_message_size(get_decode_max_size()),
        );
        let test_svc_client = Box::new(
            TestServiceClient::new(svc.clone()).max_decoding_message_size(get_decode_max_size()),
        );
        let health_svc_client = Box::new(
            HealthClient::new(svc.clone()).max_decoding_message_size(get_decode_max_size()),
        );

        Self {
            workflow_svc_client,
            operator_svc_client,
            cloud_svc_client,
            test_svc_client,
            health_svc_client,
        }
    }

    /// Create a service client from implementations of the individual underlying services. Useful
    /// for mocking out service implementations.
    pub fn from_services(
        workflow: Box<dyn WorkflowService>,
        operator: Box<dyn OperatorService>,
        cloud: Box<dyn CloudService>,
        test: Box<dyn TestService>,
        health: Box<dyn HealthService>,
    ) -> Self {
        Self {
            workflow_svc_client: workflow,
            operator_svc_client: operator,
            cloud_svc_client: cloud,
            test_svc_client: test,
            health_svc_client: health,
        }
    }

    /// Get the underlying workflow service client
    pub fn workflow_svc(&self) -> Box<dyn WorkflowService> {
        self.workflow_svc_client.clone()
    }
    /// Get the underlying operator service client
    pub fn operator_svc(&self) -> Box<dyn OperatorService> {
        self.operator_svc_client.clone()
    }
    /// Get the underlying cloud service client
    pub fn cloud_svc(&self) -> Box<dyn CloudService> {
        self.cloud_svc_client.clone()
    }
    /// Get the underlying test service client
    pub fn test_svc(&self) -> Box<dyn TestService> {
        self.test_svc_client.clone()
    }
    /// Get the underlying health service client
    pub fn health_svc(&self) -> Box<dyn HealthService> {
        self.health_svc_client.clone()
    }
}

/// Contains an instance of a namespace-bound client for interacting with the Temporal server
#[derive(Clone)]
pub struct Client {
    /// Client for interacting with workflow service
    inner: ConfiguredClient<TemporalServiceClient>,
    /// The namespace this client interacts with
    namespace: String,
}

impl Client {
    /// Create a new client from an existing configured lower level client and a namespace
    pub fn new(client: ConfiguredClient<TemporalServiceClient>, namespace: String) -> Self {
        Client {
            inner: client,
            namespace,
        }
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
    pub fn inner(&self) -> &ConfiguredClient<TemporalServiceClient> {
        &self.inner
    }

    /// Consumes self and returns the underlying client
    pub fn into_inner(self) -> ConfiguredClient<TemporalServiceClient> {
        self.inner
    }

    /// Returns the client-wide key
    pub fn worker_grouping_key(&self) -> Uuid {
        self.inner.worker_grouping_key()
    }
}

impl NamespacedClient for Client {
    fn namespace(&self) -> String {
        self.namespace.clone()
    }

    fn identity(&self) -> String {
        self.inner.options.identity.clone()
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
    /// Convert into grpc request
    pub fn into_describe_namespace_request(self) -> DescribeNamespaceRequest {
        let (namespace, id) = match self {
            Namespace::Name(n) => (n, "".to_owned()),
            Namespace::Id(n) => ("".to_owned(), n),
        };
        DescribeNamespaceRequest { namespace, id }
    }
}

/// Default workflow execution retention for a Namespace is 3 days
const DEFAULT_WORKFLOW_EXECUTION_RETENTION_PERIOD: Duration = Duration::from_secs(60 * 60 * 24 * 3);

/// Helper struct for `register_namespace`.
#[derive(Clone, bon::Builder)]
#[builder(on(String, into))]
pub struct RegisterNamespaceOptions {
    /// Name (required)
    pub namespace: String,
    /// Description (required)
    pub description: String,
    /// Owner's email
    #[builder(default)]
    pub owner_email: String,
    /// Workflow execution retention period
    #[builder(default = DEFAULT_WORKFLOW_EXECUTION_RETENTION_PERIOD)]
    pub workflow_execution_retention_period: Duration,
    /// Cluster settings
    #[builder(default)]
    pub clusters: Vec<ClusterReplicationConfig>,
    /// Active cluster name
    #[builder(default)]
    pub active_cluster_name: String,
    /// Custom Data
    #[builder(default)]
    pub data: HashMap<String, String>,
    /// Security Token
    #[builder(default)]
    pub security_token: String,
    /// Global namespace
    #[builder(default)]
    pub is_global_namespace: bool,
    /// History Archival setting
    #[builder(default = ArchivalState::Unspecified)]
    pub history_archival_state: ArchivalState,
    /// History Archival uri
    #[builder(default)]
    pub history_archival_uri: String,
    /// Visibility Archival setting
    #[builder(default = ArchivalState::Unspecified)]
    pub visibility_archival_state: ArchivalState,
    /// Visibility Archival uri
    #[builder(default)]
    pub visibility_archival_uri: String,
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

// Note: The cluster_names custom setter from derive_builder is not supported in bon.
// Users should manually construct the clusters vector if needed.

/// Helper struct for `signal_with_start_workflow_execution`.
#[derive(Clone, bon::Builder)]
#[builder(on(String, into))]
pub struct SignalWithStartOptions {
    /// Input payload for the workflow run
    pub input: Option<Payloads>,
    /// Task Queue to target (required)
    pub task_queue: String,
    /// Workflow id for the workflow run
    pub workflow_id: String,
    /// Workflow type for the workflow run
    pub workflow_type: String,
    /// Request id for idempotency/deduplication
    pub request_id: Option<String>,
    /// The signal name to send (required)
    pub signal_name: String,
    /// Payloads for the signal
    pub signal_input: Option<Payloads>,
    /// Headers for the signal
    pub signal_header: Option<Header>,
}

/// This trait provides higher-level friendlier interaction with the server.
/// See the [WorkflowService] trait for a lower-level client.
#[async_trait::async_trait]
pub trait WorkflowClientTrait: NamespacedClient {
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
        filters: Option<list_open_workflow_executions_request::Filters>,
    ) -> Result<ListOpenWorkflowExecutionsResponse>;

    /// List closed workflow executions Standard Visibility filtering
    async fn list_closed_workflow_executions(
        &self,
        max_page_size: i32,
        next_page_token: Vec<u8>,
        start_time_filter: Option<StartTimeFilter>,
        filters: Option<list_closed_workflow_executions_request::Filters>,
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

    /// Send an Update to a workflow execution
    async fn update_workflow_execution(
        &self,
        workflow_id: String,
        run_id: String,
        name: String,
        wait_policy: update::v1::WaitPolicy,
        args: Option<Payloads>,
    ) -> Result<UpdateWorkflowExecutionResponse>;
}

/// A client that is bound to a namespace
pub trait NamespacedClient {
    /// Returns the namespace this client is bound to
    fn namespace(&self) -> String;
    /// Returns the client identity
    fn identity(&self) -> String;
}

/// Optional fields supplied at the start of workflow execution
#[derive(Debug, Clone, Default)]
pub struct WorkflowOptions {
    /// Set the policy for reusing the workflow id
    pub id_reuse_policy: WorkflowIdReusePolicy,

    /// Set the policy for how to resolve conflicts with running policies.
    /// NOTE: This is ignored for child workflows.
    pub id_conflict_policy: WorkflowIdConflictPolicy,

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

    /// Optionally enable Eager Workflow Start, a latency optimization using local workers
    /// NOTE: Experimental
    pub enable_eager_workflow_start: bool,

    /// Optionally set a retry policy for the workflow
    pub retry_policy: Option<RetryPolicy>,

    /// Links to associate with the workflow. Ex: References to a nexus operation.
    pub links: Vec<common::v1::Link>,

    /// Callbacks that will be invoked upon workflow completion. For, ex, completing nexus
    /// operations.
    pub completion_callbacks: Vec<common::v1::Callback>,

    /// Priority for the workflow
    pub priority: Option<Priority>,
}

/// Priority contains metadata that controls relative ordering of task processing
/// when tasks are backlogged in a queue. Initially, Priority will be used in
/// activity and workflow task queues, which are typically where backlogs exist.
/// Other queues in the server (such as transfer and timer queues) and rate
/// limiting decisions do not use Priority, but may in the future.
///
/// Priority is attached to workflows and activities. Activities and child
/// workflows inherit Priority from the workflow that created them, but may
/// override fields when they are started or modified. For each field of a
/// Priority on an activity/workflow, not present or equal to zero/empty string
/// means to inherit the value from the calling workflow, or if there is no
/// calling workflow, then use the default (documented below).
///
/// Despite being named "Priority", this message will also contains fields that
/// control "fairness" mechanisms.
///
/// The overall semantics of Priority are:
/// (more will be added here later)
/// 1. First, consider "priority_key": lower number goes first.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Priority {
    /// Priority key is a positive integer from 1 to n, where smaller integers
    /// correspond to higher priorities (tasks run sooner). In general, tasks in
    /// a queue should be processed in close to priority order, although small
    /// deviations are possible.
    ///
    /// The maximum priority value (minimum priority) is determined by server
    /// configuration, and defaults to 5.
    ///
    /// The default priority is (min+max)/2. With the default max of 5 and min of
    /// 1, that comes out to 3.
    pub priority_key: u32,

    /// Fairness key is a short string that's used as a key for a fairness
    /// balancing mechanism. It may correspond to a tenant id, or to a fixed
    /// string like "high" or "low". The default is the empty string.
    ///
    /// The fairness mechanism attempts to dispatch tasks for a given key in
    /// proportion to its weight. For example, using a thousand distinct tenant
    /// ids, each with a weight of 1.0 (the default) will result in each tenant
    /// getting a roughly equal share of task dispatch throughput.
    ///
    /// (Note: this does not imply equal share of worker capacity! Fairness
    /// decisions are made based on queue statistics, not
    /// current worker load.)
    ///
    /// As another example, using keys "high" and "low" with weight 9.0 and 1.0
    /// respectively will prefer dispatching "high" tasks over "low" tasks at a
    /// 9:1 ratio, while allowing either key to use all worker capacity if the
    /// other is not present.
    ///
    /// All fairness mechanisms, including rate limits, are best-effort and
    /// probabilistic. The results may not match what a "perfect" algorithm with
    /// infinite resources would produce. The more unique keys are used, the less
    /// accurate the results will be.
    ///
    /// Fairness keys are limited to 64 bytes.
    pub fairness_key: String,

    /// Fairness weight for a task can come from multiple sources for
    /// flexibility. From highest to lowest precedence:
    /// 1. Weights for a small set of keys can be overridden in task queue
    ///    configuration with an API.
    /// 2. It can be attached to the workflow/activity in this field.
    /// 3. The default weight of 1.0 will be used.
    ///
    /// Weight values are clamped by the server to the range [0.001, 1000].
    pub fairness_weight: f32,
}

impl From<Priority> for common::v1::Priority {
    fn from(priority: Priority) -> Self {
        common::v1::Priority {
            priority_key: priority.priority_key as i32,
            fairness_key: priority.fairness_key,
            fairness_weight: priority.fairness_weight,
        }
    }
}

impl From<common::v1::Priority> for Priority {
    fn from(priority: common::v1::Priority) -> Self {
        Self {
            priority_key: priority.priority_key as u32,
            fairness_key: priority.fairness_key,
            fairness_weight: priority.fairness_weight,
        }
    }
}

#[async_trait::async_trait]
impl<T> WorkflowClientTrait for T
where
    T: WorkflowService + NamespacedClient + Clone + Send + Sync + 'static,
{
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
            .clone()
            .start_workflow_execution(
                StartWorkflowExecutionRequest {
                    namespace: self.namespace(),
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
                    workflow_id_conflict_policy: options.id_conflict_policy as i32,
                    workflow_execution_timeout: options
                        .execution_timeout
                        .and_then(|d| d.try_into().ok()),
                    workflow_run_timeout: options.run_timeout.and_then(|d| d.try_into().ok()),
                    workflow_task_timeout: options.task_timeout.and_then(|d| d.try_into().ok()),
                    search_attributes: options.search_attributes.map(|d| d.into()),
                    cron_schedule: options.cron_schedule.unwrap_or_default(),
                    request_eager_execution: options.enable_eager_workflow_start,
                    retry_policy: options.retry_policy,
                    links: options.links,
                    completion_callbacks: options.completion_callbacks,
                    priority: options.priority.map(Into::into),
                    ..Default::default()
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn reset_sticky_task_queue(
        &self,
        workflow_id: String,
        run_id: String,
    ) -> Result<ResetStickyTaskQueueResponse> {
        let request = ResetStickyTaskQueueRequest {
            namespace: self.namespace(),
            execution: Some(WorkflowExecution {
                workflow_id,
                run_id,
            }),
        };
        Ok(
            WorkflowService::reset_sticky_task_queue(&mut self.clone(), request.into_request())
                .await?
                .into_inner(),
        )
    }

    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse> {
        Ok(self
            .clone()
            .respond_activity_task_completed(
                RespondActivityTaskCompletedRequest {
                    task_token: task_token.0,
                    result,
                    identity: self.identity(),
                    namespace: self.namespace(),
                    ..Default::default()
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse> {
        Ok(self
            .clone()
            .record_activity_task_heartbeat(
                RecordActivityTaskHeartbeatRequest {
                    task_token: task_token.0,
                    details,
                    identity: self.identity(),
                    namespace: self.namespace(),
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse> {
        Ok(self
            .clone()
            .respond_activity_task_canceled(
                RespondActivityTaskCanceledRequest {
                    task_token: task_token.0,
                    details,
                    identity: self.identity(),
                    namespace: self.namespace(),
                    ..Default::default()
                }
                .into_request(),
            )
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
        Ok(WorkflowService::signal_workflow_execution(
            &mut self.clone(),
            SignalWorkflowExecutionRequest {
                namespace: self.namespace(),
                workflow_execution: Some(WorkflowExecution {
                    workflow_id,
                    run_id,
                }),
                signal_name,
                input: payloads,
                identity: self.identity(),
                request_id: request_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
                ..Default::default()
            }
            .into_request(),
        )
        .await?
        .into_inner())
    }

    async fn signal_with_start_workflow_execution(
        &self,
        options: SignalWithStartOptions,
        workflow_options: WorkflowOptions,
    ) -> Result<SignalWithStartWorkflowExecutionResponse> {
        Ok(WorkflowService::signal_with_start_workflow_execution(
            &mut self.clone(),
            SignalWithStartWorkflowExecutionRequest {
                namespace: self.namespace(),
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
                identity: self.identity(),
                request_id: options
                    .request_id
                    .unwrap_or_else(|| Uuid::new_v4().to_string()),
                workflow_id_reuse_policy: workflow_options.id_reuse_policy as i32,
                workflow_id_conflict_policy: workflow_options.id_conflict_policy as i32,
                workflow_execution_timeout: workflow_options
                    .execution_timeout
                    .and_then(|d| d.try_into().ok()),
                workflow_run_timeout: workflow_options.run_timeout.and_then(|d| d.try_into().ok()),
                workflow_task_timeout: workflow_options
                    .task_timeout
                    .and_then(|d| d.try_into().ok()),
                search_attributes: workflow_options.search_attributes.map(|d| d.into()),
                cron_schedule: workflow_options.cron_schedule.unwrap_or_default(),
                header: options.signal_header,
                ..Default::default()
            }
            .into_request(),
        )
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
            .clone()
            .query_workflow(
                QueryWorkflowRequest {
                    namespace: self.namespace(),
                    execution: Some(WorkflowExecution {
                        workflow_id,
                        run_id,
                    }),
                    query: Some(query),
                    query_reject_condition: 1,
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn describe_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<DescribeWorkflowExecutionResponse> {
        Ok(WorkflowService::describe_workflow_execution(
            &mut self.clone(),
            DescribeWorkflowExecutionRequest {
                namespace: self.namespace(),
                execution: Some(WorkflowExecution {
                    workflow_id,
                    run_id: run_id.unwrap_or_default(),
                }),
            }
            .into_request(),
        )
        .await?
        .into_inner())
    }

    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse> {
        Ok(WorkflowService::get_workflow_execution_history(
            &mut self.clone(),
            GetWorkflowExecutionHistoryRequest {
                namespace: self.namespace(),
                execution: Some(WorkflowExecution {
                    workflow_id,
                    run_id: run_id.unwrap_or_default(),
                }),
                next_page_token: page_token,
                ..Default::default()
            }
            .into_request(),
        )
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
            .clone()
            .request_cancel_workflow_execution(
                RequestCancelWorkflowExecutionRequest {
                    namespace: self.namespace(),
                    workflow_execution: Some(WorkflowExecution {
                        workflow_id,
                        run_id: run_id.unwrap_or_default(),
                    }),
                    identity: self.identity(),
                    request_id: request_id.unwrap_or_else(|| Uuid::new_v4().to_string()),
                    first_execution_run_id: "".to_string(),
                    reason,
                    links: vec![],
                }
                .into_request(),
            )
            .await?
            .into_inner())
    }

    async fn terminate_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<TerminateWorkflowExecutionResponse> {
        Ok(WorkflowService::terminate_workflow_execution(
            &mut self.clone(),
            TerminateWorkflowExecutionRequest {
                namespace: self.namespace(),
                workflow_execution: Some(WorkflowExecution {
                    workflow_id,
                    run_id: run_id.unwrap_or_default(),
                }),
                reason: "".to_string(),
                details: None,
                identity: self.identity(),
                first_execution_run_id: "".to_string(),
                links: vec![],
            }
            .into_request(),
        )
        .await?
        .into_inner())
    }

    async fn register_namespace(
        &self,
        options: RegisterNamespaceOptions,
    ) -> Result<RegisterNamespaceResponse> {
        let req = Into::<RegisterNamespaceRequest>::into(options);
        Ok(
            WorkflowService::register_namespace(&mut self.clone(), req.into_request())
                .await?
                .into_inner(),
        )
    }

    async fn list_namespaces(&self) -> Result<ListNamespacesResponse> {
        Ok(WorkflowService::list_namespaces(
            &mut self.clone(),
            ListNamespacesRequest::default().into_request(),
        )
        .await?
        .into_inner())
    }

    async fn describe_namespace(&self, namespace: Namespace) -> Result<DescribeNamespaceResponse> {
        Ok(WorkflowService::describe_namespace(
            &mut self.clone(),
            namespace.into_describe_namespace_request().into_request(),
        )
        .await?
        .into_inner())
    }

    async fn list_open_workflow_executions(
        &self,
        maximum_page_size: i32,
        next_page_token: Vec<u8>,
        start_time_filter: Option<StartTimeFilter>,
        filters: Option<list_open_workflow_executions_request::Filters>,
    ) -> Result<ListOpenWorkflowExecutionsResponse> {
        Ok(WorkflowService::list_open_workflow_executions(
            &mut self.clone(),
            ListOpenWorkflowExecutionsRequest {
                namespace: self.namespace(),
                maximum_page_size,
                next_page_token,
                start_time_filter,
                filters,
            }
            .into_request(),
        )
        .await?
        .into_inner())
    }

    async fn list_closed_workflow_executions(
        &self,
        maximum_page_size: i32,
        next_page_token: Vec<u8>,
        start_time_filter: Option<StartTimeFilter>,
        filters: Option<list_closed_workflow_executions_request::Filters>,
    ) -> Result<ListClosedWorkflowExecutionsResponse> {
        Ok(WorkflowService::list_closed_workflow_executions(
            &mut self.clone(),
            ListClosedWorkflowExecutionsRequest {
                namespace: self.namespace(),
                maximum_page_size,
                next_page_token,
                start_time_filter,
                filters,
            }
            .into_request(),
        )
        .await?
        .into_inner())
    }

    async fn list_workflow_executions(
        &self,
        page_size: i32,
        next_page_token: Vec<u8>,
        query: String,
    ) -> Result<ListWorkflowExecutionsResponse> {
        Ok(WorkflowService::list_workflow_executions(
            &mut self.clone(),
            ListWorkflowExecutionsRequest {
                namespace: self.namespace(),
                page_size,
                next_page_token,
                query,
            }
            .into_request(),
        )
        .await?
        .into_inner())
    }

    async fn list_archived_workflow_executions(
        &self,
        page_size: i32,
        next_page_token: Vec<u8>,
        query: String,
    ) -> Result<ListArchivedWorkflowExecutionsResponse> {
        Ok(WorkflowService::list_archived_workflow_executions(
            &mut self.clone(),
            ListArchivedWorkflowExecutionsRequest {
                namespace: self.namespace(),
                page_size,
                next_page_token,
                query,
            }
            .into_request(),
        )
        .await?
        .into_inner())
    }

    async fn get_search_attributes(&self) -> Result<GetSearchAttributesResponse> {
        Ok(WorkflowService::get_search_attributes(
            &mut self.clone(),
            GetSearchAttributesRequest {}.into_request(),
        )
        .await?
        .into_inner())
    }

    async fn update_workflow_execution(
        &self,
        workflow_id: String,
        run_id: String,
        name: String,
        wait_policy: update::v1::WaitPolicy,
        args: Option<Payloads>,
    ) -> Result<UpdateWorkflowExecutionResponse> {
        Ok(WorkflowService::update_workflow_execution(
            &mut self.clone(),
            UpdateWorkflowExecutionRequest {
                namespace: self.namespace(),
                workflow_execution: Some(WorkflowExecution {
                    workflow_id,
                    run_id,
                }),
                wait_policy: Some(wait_policy),
                request: Some(update::v1::Request {
                    meta: Some(update::v1::Meta {
                        update_id: "".into(),
                        identity: self.identity(),
                    }),
                    input: Some(update::v1::Input {
                        header: None,
                        name,
                        args,
                    }),
                }),
                ..Default::default()
            }
            .into_request(),
        )
        .await?
        .into_inner())
    }
}

mod sealed {
    use crate::{WorkflowClientTrait, WorkflowService};
    pub trait WfHandleClient: WorkflowClientTrait + WorkflowService {}
    impl<T> WfHandleClient for T where T: WorkflowClientTrait + WorkflowService {}
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
                namespace: self.namespace(),
                workflow_id: workflow_id.into(),
                run_id: if rid.is_empty() { None } else { Some(rid) },
            },
        )
    }
}

impl<T> WfClientExt for T where T: WfHandleClient + Clone + Sized {}

macro_rules! dbg_panic {
  ($($arg:tt)*) => {
      use tracing::error;
      error!($($arg)*);
      debug_assert!(false, $($arg)*);
  };
}
pub(crate) use dbg_panic;

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::Ascii;

    #[test]
    fn applies_headers() {
        let opts = ClientOptions::builder()
            .identity("enchicat".to_string())
            .target_url(Url::parse("https://smolkitty").unwrap())
            .client_name("cute-kitty".to_string())
            .client_version("0.1.0".to_string())
            .build();

        // Initial header set
        let headers = Arc::new(RwLock::new(ClientHeaders {
            user_headers: HashMap::new(),
            user_binary_headers: HashMap::new(),
            api_key: Some("my-api-key".to_owned()),
        }));
        headers.clone().write().user_headers.insert(
            "my-meta-key".parse().unwrap(),
            "my-meta-val".parse().unwrap(),
        );
        headers.clone().write().user_binary_headers.insert(
            "my-bin-meta-key-bin".parse().unwrap(),
            vec![1, 2, 3].try_into().unwrap(),
        );
        let mut interceptor = ServiceCallInterceptor {
            opts,
            headers: headers.clone(),
        };

        // Confirm on metadata
        let req = interceptor.call(tonic::Request::new(())).unwrap();
        assert_eq!(req.metadata().get("my-meta-key").unwrap(), "my-meta-val");
        assert_eq!(
            req.metadata().get("authorization").unwrap(),
            "Bearer my-api-key"
        );
        assert_eq!(
            req.metadata().get_bin("my-bin-meta-key-bin").unwrap(),
            vec![1, 2, 3].as_slice()
        );

        // Overwrite at request time
        let mut req = tonic::Request::new(());
        req.metadata_mut()
            .insert("my-meta-key", "my-meta-val2".parse().unwrap());
        req.metadata_mut()
            .insert("authorization", "my-api-key2".parse().unwrap());
        req.metadata_mut()
            .insert_bin("my-bin-meta-key-bin", vec![4, 5, 6].try_into().unwrap());
        let req = interceptor.call(req).unwrap();
        assert_eq!(req.metadata().get("my-meta-key").unwrap(), "my-meta-val2");
        assert_eq!(req.metadata().get("authorization").unwrap(), "my-api-key2");
        assert_eq!(
            req.metadata().get_bin("my-bin-meta-key-bin").unwrap(),
            vec![4, 5, 6].as_slice()
        );

        // Overwrite auth on header
        headers.clone().write().user_headers.insert(
            "authorization".parse().unwrap(),
            "my-api-key3".parse().unwrap(),
        );
        let req = interceptor.call(tonic::Request::new(())).unwrap();
        assert_eq!(req.metadata().get("my-meta-key").unwrap(), "my-meta-val");
        assert_eq!(req.metadata().get("authorization").unwrap(), "my-api-key3");

        // Remove headers and auth and confirm gone
        headers.clone().write().user_headers.clear();
        headers.clone().write().user_binary_headers.clear();
        headers.clone().write().api_key.take();
        let req = interceptor.call(tonic::Request::new(())).unwrap();
        assert!(!req.metadata().contains_key("my-meta-key"));
        assert!(!req.metadata().contains_key("authorization"));
        assert!(!req.metadata().contains_key("my-bin-meta-key-bin"));

        // Timeout header not overriden
        let mut req = tonic::Request::new(());
        req.metadata_mut()
            .insert("grpc-timeout", "1S".parse().unwrap());
        let req = interceptor.call(req).unwrap();
        assert_eq!(
            req.metadata().get("grpc-timeout").unwrap(),
            "1S".parse::<MetadataValue<Ascii>>().unwrap()
        );
    }

    #[test]
    fn invalid_ascii_header_key() {
        let invalid_headers = {
            let mut h = HashMap::new();
            h.insert("x-binary-key-bin".to_owned(), "value".to_owned());
            h
        };

        let result = parse_ascii_headers(invalid_headers);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Invalid ASCII header key 'x-binary-key-bin': invalid gRPC metadata key name"
        );
    }

    #[test]
    fn invalid_ascii_header_value() {
        let invalid_headers = {
            let mut h = HashMap::new();
            // Nul bytes are valid UTF-8, but not valid ascii gRPC headers:
            h.insert("x-ascii-key".to_owned(), "\x00value".to_owned());
            h
        };

        let result = parse_ascii_headers(invalid_headers);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Invalid ASCII header value for key 'x-ascii-key': failed to parse metadata value"
        );
    }

    #[test]
    fn invalid_binary_header_key() {
        let invalid_headers = {
            let mut h = HashMap::new();
            h.insert("x-ascii-key".to_owned(), vec![1, 2, 3]);
            h
        };

        let result = parse_binary_headers(invalid_headers);
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Invalid binary header key 'x-ascii-key': invalid gRPC metadata key name"
        );
    }

    #[test]
    fn keep_alive_defaults() {
        let opts = ClientOptions::builder()
            .identity("enchicat".to_string())
            .target_url(Url::parse("https://smolkitty").unwrap())
            .client_name("cute-kitty".to_string())
            .client_version("0.1.0".to_string())
            .build();
        assert_eq!(
            opts.keep_alive.clone().unwrap().interval,
            ClientKeepAliveOptions::default().interval
        );
        assert_eq!(
            opts.keep_alive.clone().unwrap().timeout,
            ClientKeepAliveOptions::default().timeout
        );

        // Can be explicitly set to None
        let opts = ClientOptions::builder()
            .identity("enchicat".to_string())
            .target_url(Url::parse("https://smolkitty").unwrap())
            .client_name("cute-kitty".to_string())
            .client_version("0.1.0".to_string())
            .keep_alive(None)
            .build();
        dbg!(&opts.keep_alive);
        assert!(opts.keep_alive.is_none());
    }
}
