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
#[cfg(feature = "antithesis_assertions")]
mod antithesis;
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

pub(crate) use temporalio_common::errors;

pub use pollers::{
    Client, ClientOptions, ClientOptionsBuilder, ClientTlsConfig, RetryClient, RetryConfig,
    TlsConfig, WorkflowClientTrait,
};
pub use temporalio_common::protos::TaskToken;
pub use url::Url;
pub use worker::{
    FixedSizeSlotSupplier, ResourceBasedSlotsOptions, ResourceBasedSlotsOptionsBuilder,
    ResourceBasedTuner, ResourceSlotOptions, SlotSupplierOptions, TunerBuilder, TunerHolder,
    TunerHolderOptions, TunerHolderOptionsBuilder, Worker, WorkerConfig, WorkerConfigBuilder,
};

/// Expose [WorkerClient] symbols
pub use crate::worker::client::{
    PollActivityOptions, PollOptions, PollWorkflowOptions, WorkerClient, WorkflowTaskCompletion,
};
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
use std::{sync::Arc, time::Duration};
use temporalio_client::{ConfiguredClient, NamespacedClient, SharedReplaceableClient};
use temporalio_common::{
    Worker as WorkerTrait,
    errors::{CompleteActivityError, PollError},
    protos::coresdk::ActivityHeartbeat,
    telemetry::TelemetryOptions,
};

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
    let namespace = worker_config.namespace.clone();
    if namespace.is_empty() {
        bail!("Worker namespace cannot be empty");
    }

    let client = RetryClient::new(
        SharedReplaceableClient::new(init_worker_client(
            worker_config.namespace.clone(),
            worker_config.client_identity_override.clone(),
            client,
        )),
        RetryConfig::default(),
    );
    let client_ident = client.identity();
    let sticky_q = sticky_q_name_for_worker(&client_ident, worker_config.max_cached_workflows);

    if client_ident.is_empty() {
        bail!("Client identity cannot be empty. Either lang or user should be setting this value");
    }

    let client_bag = Arc::new(WorkerClientBag::new(
        client,
        namespace.clone(),
        client_ident.clone(),
        worker_config.versioning_strategy.clone(),
    ));

    Worker::new(
        worker_config.clone(),
        sticky_q,
        client_bag.clone(),
        Some(&runtime.telemetry),
        runtime.heartbeat_interval,
    )
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

pub(crate) fn init_worker_client<CT>(
    namespace: String,
    client_identity_override: Option<String>,
    client: CT,
) -> Client
where
    CT: Into<sealed::AnyClient>,
{
    let mut client = Client::new(*client.into().into_inner(), namespace.clone());
    if let Some(ref id_override) = client_identity_override {
        client.options_mut().identity.clone_from(id_override);
    }
    client
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
    use temporalio_client::{SharedReplaceableClient, TemporalServiceClient};

    /// Allows passing different kinds of clients into things that want to be flexible. Motivating
    /// use-case was worker initialization.
    ///
    /// Needs to exist in this crate to avoid blanket impl conflicts.
    pub struct AnyClient {
        pub(crate) inner: Box<ConfiguredClient<TemporalServiceClient>>,
    }
    impl AnyClient {
        pub(crate) fn into_inner(self) -> Box<ConfiguredClient<TemporalServiceClient>> {
            self.inner
        }
    }

    impl From<ConfiguredClient<TemporalServiceClient>> for AnyClient {
        fn from(c: ConfiguredClient<TemporalServiceClient>) -> Self {
            Self { inner: Box::new(c) }
        }
    }

    impl From<Client> for AnyClient {
        fn from(c: Client) -> Self {
            c.into_inner().into()
        }
    }

    impl<T> From<RetryClient<T>> for AnyClient
    where
        T: Into<AnyClient>,
    {
        fn from(c: RetryClient<T>) -> Self {
            c.into_inner().into()
        }
    }

    impl<T> From<SharedReplaceableClient<T>> for AnyClient
    where
        T: Into<AnyClient> + Clone + Send + Sync,
    {
        fn from(c: SharedReplaceableClient<T>) -> Self {
            c.inner_clone().into()
        }
    }

    impl<T> From<Arc<T>> for AnyClient
    where
        T: Into<AnyClient> + Clone,
    {
        fn from(c: Arc<T>) -> Self {
            Arc::unwrap_or_clone(c).into()
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
    heartbeat_interval: Option<Duration>,
}

/// Holds telemetry options, as well as worker heartbeat_interval. Construct with [RuntimeOptionsBuilder]
#[derive(Default, derive_builder::Builder)]
#[builder(build_fn(validate = "Self::validate"))]
#[non_exhaustive]
pub struct RuntimeOptions {
    /// Telemetry configuration options.
    #[builder(default)]
    telemetry_options: TelemetryOptions,
    /// Optional worker heartbeat interval - This configures the heartbeat setting of all
    /// workers created using this runtime.
    ///
    /// Interval must be between 1s and 60s, inclusive.
    #[builder(default = "Some(Duration::from_secs(60))")]
    heartbeat_interval: Option<Duration>,
}

impl RuntimeOptionsBuilder {
    fn validate(&self) -> Result<(), String> {
        if let Some(Some(interval)) = self.heartbeat_interval
            && (interval < Duration::from_secs(1) || interval > Duration::from_secs(60))
        {
            return Err(format!(
                "heartbeat_interval ({interval:?}) must be between 1s and 60s",
            ));
        }

        Ok(())
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
}

impl Drop for CoreRuntime {
    fn drop(&mut self) {
        remove_trace_subscriber_for_current_thread();
    }
}
