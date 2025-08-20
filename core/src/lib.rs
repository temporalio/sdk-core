#![warn(missing_docs)] // error if there are missing docs
#![allow(clippy::upper_case_acronyms)]

//! This crate provides a basis for creating new Temporal SDKs without completely starting from
//! scratch

#[cfg(test)]
#[macro_use]
pub extern crate assert_matches;
#[macro_use]
extern crate tracing;
extern crate core;

mod abstractions;
#[cfg(feature = "debug-plugin")]
pub mod debug_client;
#[cfg(feature = "ephemeral-server")]
pub mod ephemeral_server;
mod internal_flags;
mod pollers;
mod protosext;
pub mod replay;
pub(crate) mod retry_logic;
pub mod telemetry;
mod worker;

#[cfg(test)]
mod core_tests;
#[cfg(any(feature = "test-utilities", test))]
#[macro_use]
pub mod test_help;

use std::collections::HashMap;
pub(crate) use temporal_sdk_core_api::errors;

pub use pollers::{
    Client, ClientOptions, ClientOptionsBuilder, ClientTlsConfig, RetryClient, RetryConfig,
    TlsConfig, WorkflowClientTrait,
};
pub use temporal_sdk_core_api as api;
pub use temporal_sdk_core_protos as protos;
pub use temporal_sdk_core_protos::TaskToken;
pub use url::Url;
pub use worker::{
    FixedSizeSlotSupplier, RealSysInfo, ResourceBasedSlotsOptions,
    ResourceBasedSlotsOptionsBuilder, ResourceBasedTuner, ResourceSlotOptions, SlotSupplierOptions,
    TunerBuilder, TunerHolder, TunerHolderOptions, TunerHolderOptionsBuilder, Worker, WorkerConfig,
    WorkerConfigBuilder,
};

use crate::abstractions::dbg_panic;
use crate::worker::client::WorkerClientWithHeartbeat;
/// Expose [WorkerClient] symbols
pub use crate::worker::client::{
    PollActivityOptions, PollOptions, PollWorkflowOptions, WorkerClient, WorkflowTaskCompletion,
};
use crate::worker::heartbeat::{ClientIdentity, HeartbeatFn, SharedNamespaceWorker};
use crate::{
    replay::{HistoryForReplay, ReplayWorkerInput},
    telemetry::{
        TelemetryInstance, metrics::MetricsContext, remove_trace_subscriber_for_current_thread,
        set_trace_subscriber_for_current_thread, telemetry_init,
    },
    worker::client::WorkerClientBag,
};
use anyhow::bail;
use futures_util::Stream;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::Duration;
use temporal_client::{ConfiguredClient, NamespacedClient, TemporalServiceClientWithMetrics};
use temporal_sdk_core_api::{
    Worker as WorkerTrait,
    errors::{CompleteActivityError, PollError},
    telemetry::TelemetryOptions,
};
use temporal_sdk_core_protos::coresdk::ActivityHeartbeat;
use uuid::Uuid;

/// Initialize a worker bound to a task queue.
///
/// You will need to have already initialized a [CoreRuntime] which will be used for this worker.
/// After the worker is initialized, you should use [CoreRuntime::tokio_handle] to run the worker's
/// async functions.
///
/// Lang implementations may pass in a [ConfiguredClient] directly (or a
/// [RetryClient] wrapping one, or a handful of other variants of the same idea). When they do so,
/// this function will always overwrite the client retry configuration, force the client to use the
/// namespace defined in the worker config, and set the client identity appropriately. IE: Use
/// [ClientOptions::connect_no_namespace], not [ClientOptions::connect].
pub fn init_worker<CT>(
    runtime: &CoreRuntime,
    worker_config: WorkerConfig,
    client: CT,
) -> Result<Worker, anyhow::Error>
where
    CT: Into<sealed::AnyClient>,
{
    let client_inner = *client.into().into_inner();
    let endpoint = client_inner.options().clone().target_url;
    let client = init_worker_client(
        worker_config.namespace.clone(),
        worker_config.client_identity_override.clone(),
        client_inner,
    );
    let namespace = worker_config.namespace.clone();
    if client.namespace() != namespace {
        bail!("Passed in client is not bound to the same namespace as the worker");
    }
    if client.namespace() == "" {
        bail!("Client namespace cannot be empty");
    }
    let client_ident = client.get_identity().to_owned();
    let sticky_q = sticky_q_name_for_worker(&client_ident, worker_config.max_cached_workflows);

    if client_ident.is_empty() {
        bail!("Client identity cannot be empty. Either lang or user should be setting this value");
    }

    let client_bag = Arc::new(WorkerClientBag::new(
        client,
        namespace.clone(),
        client_ident,
        worker_config.versioning_strategy.clone(),
    ));

    let mut worker = Worker::new(
        worker_config,
        sticky_q,
        client_bag.clone(),
        Some(&runtime.telemetry),
        false,
    );

    if runtime.heartbeat_interval.is_some() {
        let worker_instance_key = worker.worker_instance_key();
        let heartbeat_callback = worker.get_heartbeat_callback();
        if let Some(instance_key) = worker_instance_key
            && let Some(callback) = heartbeat_callback
        {
            let remove_worker_callback = runtime.register_heartbeat_callback(
                instance_key,
                ClientIdentity {
                    endpoint: endpoint.to_string(),
                    namespace: namespace.clone(),
                    task_queue: format!(
                        "temporal-sys/worker-commands/{}/{}",
                        namespace,
                        runtime.task_queue_key()
                    ),
                },
                callback,
                client_bag,
            );
            worker.register_heartbeat_shutdown_callback(remove_worker_callback);
        } else {
            dbg_panic!(
                "Failed to register worker with runtime, worker instance key or heartbeat callback missing for non-shared namespace worker"
            );
        }
    }

    Ok(worker)
}

/// Create a worker for replaying one or more existing histories. It will auto-shutdown as soon as
/// all histories have finished being replayed.
///
/// You do not necessarily need a [CoreRuntime] for replay workers, but it's advisable to create
/// one and use it to run the replay worker's async functions the same way you would for a normal
/// worker.
pub fn init_replay_worker<I>(rwi: ReplayWorkerInput<I>) -> Result<Worker, anyhow::Error>
where
    I: Stream<Item = HistoryForReplay> + Send + 'static,
{
    info!(
        task_queue = rwi.config.task_queue.as_str(),
        "Registering replay worker"
    );
    rwi.into_core_worker()
}

pub(crate) fn init_worker_client(
    namespace: String,
    client_identity_override: Option<String>,
    client: ConfiguredClient<TemporalServiceClientWithMetrics>,
) -> RetryClient<Client> {
    let mut client = Client::new(client, namespace);
    if let Some(ref id_override) = client_identity_override {
        client.options_mut().identity.clone_from(id_override);
    }
    RetryClient::new(client, RetryConfig::default())
}

/// Creates a unique sticky queue name for a worker, iff the config allows for 1 or more cached
/// workflows.
pub(crate) fn sticky_q_name_for_worker(
    process_identity: &str,
    max_cached_workflows: usize,
) -> Option<String> {
    if max_cached_workflows > 0 {
        Some(format!(
            "{}-{}",
            &process_identity,
            uuid::Uuid::new_v4().simple()
        ))
    } else {
        None
    }
}

mod sealed {
    use super::*;

    /// Allows passing different kinds of clients into things that want to be flexible. Motivating
    /// use-case was worker initialization.
    ///
    /// Needs to exist in this crate to avoid blanket impl conflicts.
    pub struct AnyClient {
        pub(crate) inner: Box<ConfiguredClient<TemporalServiceClientWithMetrics>>,
    }
    impl AnyClient {
        pub(crate) fn into_inner(self) -> Box<ConfiguredClient<TemporalServiceClientWithMetrics>> {
            self.inner
        }
    }

    impl From<RetryClient<ConfiguredClient<TemporalServiceClientWithMetrics>>> for AnyClient {
        fn from(c: RetryClient<ConfiguredClient<TemporalServiceClientWithMetrics>>) -> Self {
            Self {
                inner: Box::new(c.into_inner()),
            }
        }
    }
    impl From<RetryClient<Client>> for AnyClient {
        fn from(c: RetryClient<Client>) -> Self {
            Self {
                inner: Box::new(c.into_inner().into_inner()),
            }
        }
    }
    impl From<Arc<RetryClient<Client>>> for AnyClient {
        fn from(c: Arc<RetryClient<Client>>) -> Self {
            Self {
                inner: Box::new(c.get_client().inner().clone()),
            }
        }
    }
    impl From<ConfiguredClient<TemporalServiceClientWithMetrics>> for AnyClient {
        fn from(c: ConfiguredClient<TemporalServiceClientWithMetrics>) -> Self {
            Self { inner: Box::new(c) }
        }
    }
}

/// Holds shared state/components needed to back instances of workers and clients. More than one
/// may be instantiated, but typically only one is needed. More than one runtime instance may be
/// useful if multiple different telemetry settings are required.
pub struct CoreRuntime {
    telemetry: TelemetryInstance,
    runtime: Option<tokio::runtime::Runtime>,
    runtime_handle: tokio::runtime::Handle,
    shared_namespace_map: Arc<Mutex<HashMap<ClientIdentity, SharedNamespaceWorker>>>,
    task_queue_key: Uuid,
    heartbeat_interval: Option<Duration>,
}

/// Holds telemetry options, as well as worker heartbeat_interval. Construct with [RuntimeOptionsBuilder]
#[derive(derive_builder::Builder)]
#[non_exhaustive]
#[derive(Default)]
pub struct RuntimeOptions {
    /// Telemetry configuration options.
    #[builder(default)]
    telemetry_options: TelemetryOptions,
    /// Optional worker heartbeat interval - This configures the heartbeat setting of all
    /// workers created using this runtime.
    #[builder(default)]
    heartbeat_interval: Option<Duration>,
}

impl RuntimeOptions {
    /// Creates new runtime options.
    pub fn new(telemetry_options: TelemetryOptions, heartbeat_interval: Option<Duration>) -> Self {
        Self {
            telemetry_options,
            heartbeat_interval,
        }
    }
}

/// Wraps a [tokio::runtime::Builder] to allow layering multiple on_thread_start functions
pub struct TokioRuntimeBuilder<F> {
    /// The underlying tokio runtime builder
    pub inner: tokio::runtime::Builder,
    /// A function to be called when setting the runtime builder's on thread start
    pub lang_on_thread_start: Option<F>,
}

impl Default for TokioRuntimeBuilder<Box<dyn Fn() + Send + Sync>> {
    fn default() -> Self {
        TokioRuntimeBuilder {
            inner: tokio::runtime::Builder::new_multi_thread(),
            lang_on_thread_start: None,
        }
    }
}

impl CoreRuntime {
    /// Create a new core runtime with the provided telemetry options and tokio runtime builder.
    /// Also initialize telemetry for the thread this is being called on.
    ///
    /// Note that this function will call the [tokio::runtime::Builder::enable_all] builder option
    /// on the Tokio runtime builder, and will call [tokio::runtime::Builder::on_thread_start] to
    /// ensure telemetry subscribers are set on every tokio thread.
    ///
    /// **Important**: You need to call this *before* calling any async functions on workers or
    /// clients, otherwise the tracing subscribers will not be properly attached.
    ///
    /// # Panics
    /// If a tokio runtime has already been initialized. To re-use an existing runtime, call
    /// [CoreRuntime::new_assume_tokio].
    pub fn new<F>(
        runtime_options: RuntimeOptions,
        mut tokio_builder: TokioRuntimeBuilder<F>,
    ) -> Result<Self, anyhow::Error>
    where
        F: Fn() + Send + Sync + 'static,
    {
        let telemetry = telemetry_init(runtime_options.telemetry_options)?;
        let subscriber = telemetry.trace_subscriber();
        let runtime = tokio_builder
            .inner
            .enable_all()
            .on_thread_start(move || {
                if let Some(sub) = subscriber.as_ref() {
                    set_trace_subscriber_for_current_thread(sub.clone());
                }
                if let Some(lang_on_thread_start) = tokio_builder.lang_on_thread_start.as_ref() {
                    lang_on_thread_start();
                }
            })
            .build()?;
        let _rg = runtime.enter();
        let mut me =
            Self::new_assume_tokio_initialized_telem(telemetry, runtime_options.heartbeat_interval);
        me.runtime = Some(runtime);
        Ok(me)
    }

    /// Initialize telemetry for the thread this is being called on, assuming a tokio runtime is
    /// already active and this call exists in its context. See [Self::new] for more.
    ///
    /// # Panics
    /// If there is no currently active Tokio runtime
    pub fn new_assume_tokio(runtime_options: RuntimeOptions) -> Result<Self, anyhow::Error> {
        let telemetry = telemetry_init(runtime_options.telemetry_options)?;
        Ok(Self::new_assume_tokio_initialized_telem(
            telemetry,
            runtime_options.heartbeat_interval,
        ))
    }

    /// Construct a runtime from an already-initialized telemetry instance, assuming a tokio runtime
    /// is already active and this call exists in its context. See [Self::new] for more.
    ///
    /// # Panics
    /// If there is no currently active Tokio runtime
    pub fn new_assume_tokio_initialized_telem(
        telemetry: TelemetryInstance,
        heartbeat_interval: Option<Duration>,
    ) -> Self {
        let runtime_handle = tokio::runtime::Handle::current();
        if let Some(sub) = telemetry.trace_subscriber() {
            set_trace_subscriber_for_current_thread(sub);
        }
        Self {
            telemetry,
            runtime: None,
            runtime_handle,
            shared_namespace_map: Arc::new(Mutex::new(HashMap::new())),
            task_queue_key: Uuid::new_v4(),
            heartbeat_interval,
        }
    }

    /// Get a handle to the tokio runtime used by this Core runtime.
    pub fn tokio_handle(&self) -> tokio::runtime::Handle {
        self.runtime_handle.clone()
    }

    /// Return a reference to the owned [TelemetryInstance]
    pub fn telemetry(&self) -> &TelemetryInstance {
        &self.telemetry
    }

    /// Return a mutable reference to the owned [TelemetryInstance]
    pub fn telemetry_mut(&mut self) -> &mut TelemetryInstance {
        &mut self.telemetry
    }

    fn register_heartbeat_callback(
        &self,
        worker_instance_key: String,
        client_identity: ClientIdentity,
        heartbeat_callback: HeartbeatFn,
        client: Arc<dyn WorkerClientWithHeartbeat>,
    ) -> Arc<dyn Fn() + Send + Sync> {
        if let Some(ref heartbeat_interval) = self.heartbeat_interval {
            let mut shared_namespace_map = self.shared_namespace_map.lock();
            let worker = shared_namespace_map
                .entry(client_identity.clone())
                .or_insert_with(|| {
                    let namespace_map = self.shared_namespace_map.clone();
                    let client_identity_clone = client_identity.clone();
                    let remove_namespace_worker_callback = Arc::new(move || {
                        namespace_map.lock().remove(&client_identity_clone);
                    });

                    // The first client of a ClientIdentity is the client used for the
                    // SharedNamespaceWorker, so we need that client's heartbeat_map to store
                    // heartbeat callbacks
                    let heartbeat_map = client.get_heartbeat_map();

                    SharedNamespaceWorker::new(
                        client,
                        client_identity,
                        *heartbeat_interval,
                        Some(&self.telemetry),
                        remove_namespace_worker_callback,
                        heartbeat_map,
                    )
                });
            worker.register_callback(worker_instance_key, heartbeat_callback)
        } else {
            dbg_panic!("Worker heartbeat disabled for this runtime");
            Arc::new(|| {})
        }
    }

    fn task_queue_key(&self) -> Uuid {
        self.task_queue_key
    }
}

impl Drop for CoreRuntime {
    fn drop(&mut self) {
        remove_trace_subscriber_for_current_thread();
    }
}
