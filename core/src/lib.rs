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
pub mod ephemeral_server;
mod pollers;
mod protosext;
pub mod replay;
pub(crate) mod retry_logic;
pub mod telemetry;
mod worker;

#[cfg(test)]
mod core_tests;
#[cfg(test)]
#[macro_use]
mod test_help;

pub(crate) use temporal_sdk_core_api::errors;

pub use pollers::{
    Client, ClientOptions, ClientOptionsBuilder, ClientTlsConfig, RetryClient, RetryConfig,
    TlsConfig, WorkflowClientTrait,
};
pub use temporal_sdk_core_api as api;
pub use temporal_sdk_core_protos as protos;
pub use temporal_sdk_core_protos::TaskToken;
pub use url::Url;
pub use worker::{Worker, WorkerConfig, WorkerConfigBuilder};

use crate::{
    replay::{mock_client_from_histories, Historator, HistoryForReplay},
    telemetry::{
        metrics::MetricsContext, remove_trace_subscriber_for_current_thread,
        set_trace_subscriber_for_current_thread, telemetry_init, TelemetryInstance,
    },
    worker::client::WorkerClientBag,
};
use futures::Stream;
use std::sync::Arc;
use temporal_client::{ConfiguredClient, TemporalServiceClientWithMetrics};
use temporal_sdk_core_api::{
    errors::{CompleteActivityError, PollActivityError, PollWfError},
    telemetry::{CoreTelemetry, TelemetryOptions},
    Worker as WorkerTrait,
};
use temporal_sdk_core_protos::coresdk::ActivityHeartbeat;

/// Initialize a worker bound to a task queue.
///
/// You will need to have already initialized a [CoreRuntime] which will be used for this worker.
/// After the worker is initialized, you should use [CoreRuntime::tokio_handle] to run the worker's
/// async functions.
///
/// Lang implementations may pass in a [temporal_client::ConfiguredClient] directly (or a
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
    let client = {
        let ll = client.into().into_inner();
        let mut client = Client::new(*ll, worker_config.namespace.clone());
        client.set_worker_build_id(worker_config.worker_build_id.clone());
        if let Some(ref id_override) = worker_config.client_identity_override {
            client.options_mut().identity = id_override.clone();
        }
        RetryClient::new(client, RetryConfig::default())
    };
    if client.namespace() != worker_config.namespace {
        panic!("Passed in client is not bound to the same namespace as the worker");
    }
    let client_ident = client.get_options().identity.clone();
    let sticky_q = sticky_q_name_for_worker(&client_ident, &worker_config);
    let client_bag = Arc::new(WorkerClientBag::new(
        client,
        worker_config.namespace.clone(),
        client_ident,
        worker_config.worker_build_id.clone(),
        worker_config.use_worker_versioning,
    ));

    let metrics = MetricsContext::top_level(worker_config.namespace.clone(), &runtime.telemetry)
        .with_task_q(worker_config.task_queue.clone());
    Ok(Worker::new(worker_config, sticky_q, client_bag, metrics))
}

/// Create a worker for replaying a specific history. It will auto-shutdown as soon as the history
/// has finished being replayed.
///
/// You do not necessarily need a [CoreRuntime] for replay workers, but it's advisable to create
/// one and use it to run the replay worker's async functions the same way you would for a normal
/// worker.
pub fn init_replay_worker<I>(
    mut config: WorkerConfig,
    histories: I,
) -> Result<Worker, anyhow::Error>
where
    I: Stream<Item = HistoryForReplay> + Send + 'static,
{
    info!(
        task_queue = config.task_queue.as_str(),
        "Registering replay worker"
    );
    config.max_cached_workflows = 1;
    config.max_concurrent_wft_polls = 1;
    config.no_remote_activities = true;
    let historator = Historator::new(histories);
    let post_activate = historator.get_post_activate_hook();
    let shutdown_tok = historator.get_shutdown_setter();
    let client = mock_client_from_histories(historator);
    let mut worker = Worker::new(config, None, Arc::new(client), MetricsContext::no_op());
    worker.set_post_activate_hook(post_activate);
    shutdown_tok(worker.shutdown_token());
    Ok(worker)
}

/// Creates a unique sticky queue name for a worker, iff the config allows for 1 or more cached
/// workflows.
pub(crate) fn sticky_q_name_for_worker(
    process_identity: &str,
    config: &WorkerConfig,
) -> Option<String> {
    if config.max_cached_workflows > 0 {
        Some(format!(
            "{}-{}-{}",
            &process_identity,
            &config.task_queue,
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
    pub struct AnyClient(Box<ConfiguredClient<TemporalServiceClientWithMetrics>>);
    impl AnyClient {
        pub(crate) fn into_inner(self) -> Box<ConfiguredClient<TemporalServiceClientWithMetrics>> {
            self.0
        }
    }

    impl From<RetryClient<ConfiguredClient<TemporalServiceClientWithMetrics>>> for AnyClient {
        fn from(c: RetryClient<ConfiguredClient<TemporalServiceClientWithMetrics>>) -> Self {
            Self(Box::new(c.into_inner()))
        }
    }
    impl From<RetryClient<Client>> for AnyClient {
        fn from(c: RetryClient<Client>) -> Self {
            Self(Box::new(c.into_inner().into_inner()))
        }
    }
    impl From<Arc<RetryClient<Client>>> for AnyClient {
        fn from(c: Arc<RetryClient<Client>>) -> Self {
            Self(Box::new(c.get_client().inner().clone()))
        }
    }
    impl From<ConfiguredClient<TemporalServiceClientWithMetrics>> for AnyClient {
        fn from(c: ConfiguredClient<TemporalServiceClientWithMetrics>) -> Self {
            Self(Box::new(c))
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
    pub fn new(
        telemetry_options: TelemetryOptions,
        mut tokio_builder: tokio::runtime::Builder,
    ) -> Result<Self, anyhow::Error> {
        let telemetry = telemetry_init(telemetry_options)?;
        let subscriber = telemetry.trace_subscriber();
        let runtime = tokio_builder
            .enable_all()
            .on_thread_start(move || {
                set_trace_subscriber_for_current_thread(subscriber.clone());
            })
            .build()?;
        let _rg = runtime.enter();
        let mut me = Self::new_assume_tokio_initialized_telem(telemetry);
        me.runtime = Some(runtime);
        Ok(me)
    }

    /// Initialize telemetry for the thread this is being called on, assuming a tokio runtime is
    /// already active and this call exists in its context. See [Self::new] for more.
    ///
    /// # Panics
    /// If there is no currently active Tokio runtime
    pub fn new_assume_tokio(telemetry_options: TelemetryOptions) -> Result<Self, anyhow::Error> {
        let telemetry = telemetry_init(telemetry_options)?;
        Ok(Self::new_assume_tokio_initialized_telem(telemetry))
    }

    /// Construct a runtime from an already-initialized telemetry instance, assuming a tokio runtime
    /// is already active and this call exists in its context. See [Self::new] for more.
    ///
    /// # Panics
    /// If there is no currently active Tokio runtime
    pub fn new_assume_tokio_initialized_telem(telemetry: TelemetryInstance) -> Self {
        let runtime_handle = tokio::runtime::Handle::current();
        set_trace_subscriber_for_current_thread(telemetry.trace_subscriber());
        Self {
            telemetry,
            runtime: None,
            runtime_handle,
        }
    }

    /// Get a handle to the tokio runtime used by this Core runtime.
    pub fn tokio_handle(&self) -> tokio::runtime::Handle {
        self.runtime_handle.clone()
    }

    /// Returns the metric meter used for recording metrics, if they were enabled.
    pub fn metric_meter(&self) -> Option<&opentelemetry::metrics::Meter> {
        self.telemetry.get_metric_meter()
    }

    /// Return the trace subscriber associated with the telemetry options/instance. Can be used
    /// to manually set the default for a thread or globally using the `tracing` crate, or with
    /// [set_trace_subscriber_for_current_thread]
    pub fn trace_subscriber(&self) -> Arc<dyn tracing::Subscriber + Send + Sync> {
        self.telemetry.trace_subscriber()
    }

    /// Return a reference to the owned [TelemetryInstance]
    pub fn telemetry(&self) -> &TelemetryInstance {
        &self.telemetry
    }
}

impl Drop for CoreRuntime {
    fn drop(&mut self) {
        remove_trace_subscriber_for_current_thread();
    }
}
