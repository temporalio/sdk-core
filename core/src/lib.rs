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
mod log_export;
mod pollers;
mod protosext;
pub mod replay;
pub(crate) mod retry_logic;
pub(crate) mod telemetry;
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
pub use telemetry::{
    fetch_global_buffered_logs, telemetry_init, Logger, MetricsExporter, OtelCollectorOptions,
    TelemetryOptions, TelemetryOptionsBuilder, TraceExporter,
};
pub use temporal_sdk_core_api as api;
pub use temporal_sdk_core_protos as protos;
pub use temporal_sdk_core_protos::TaskToken;
pub use url::Url;
pub use worker::{Worker, WorkerConfig, WorkerConfigBuilder};

use crate::{
    replay::mock_client_from_history,
    telemetry::metrics::{MetricsContext, METRIC_METER},
    worker::client::WorkerClientBag,
};
use std::sync::Arc;
use temporal_client::AnyClient;
use temporal_sdk_core_api::{
    errors::{CompleteActivityError, PollActivityError, PollWfError},
    CoreLog, Worker as WorkerTrait,
};
use temporal_sdk_core_protos::{coresdk::ActivityHeartbeat, temporal::api::history::v1::History};

lazy_static::lazy_static! {
    /// A process-wide unique string, which will be different on every startup
    static ref PROCCESS_UNIQ_ID: String = {
        uuid::Uuid::new_v4().to_simple().to_string()
    };
}

/// Initialize a worker bound to a task queue.
///
/// Lang implementations should pass in a a [temporal_client::ConfiguredClient] directly (or a
/// [RetryClient] wrapping one). When they do so, this function will always overwrite the client
/// retry configuration, force the client to use the namespace defined in the worker config, and set
/// the client identity appropriately. IE: Use [ClientOptions::connect_no_namespace], not
/// [ClientOptions::connect].
///
/// It is also possible to pass in a [WorkflowClientTrait] implementor, but this largely exists to
/// support testing and mocking. Lang impls should not operate that way, as it may result in
/// improper retry behavior for a worker.
pub fn init_worker<CT>(worker_config: WorkerConfig, client: CT) -> Worker
where
    CT: Into<AnyClient>,
{
    let as_enum = client.into();
    let client = match as_enum {
        AnyClient::HighLevel(ac) => ac,
        AnyClient::LowLevel(ll) => {
            let mut client = Client::new(*ll, worker_config.namespace.clone());
            client.set_worker_build_id(worker_config.worker_build_id.clone());
            if let Some(ref id_override) = worker_config.client_identity_override {
                client.options_mut().identity = id_override.clone();
            }
            let retry_client = RetryClient::new(client, Default::default());
            Arc::new(retry_client)
        }
    };
    if client.namespace() != worker_config.namespace {
        panic!("Passed in client is not bound to the same namespace as the worker");
    }
    let sticky_q = sticky_q_name_for_worker(&client.get_options().identity, &worker_config);
    let client_bag = Arc::new(WorkerClientBag::new(
        Box::new(client),
        worker_config.namespace.clone(),
    ));
    let metrics = MetricsContext::top_level(worker_config.namespace.clone())
        .with_task_q(worker_config.task_queue.clone());
    Worker::new(worker_config, sticky_q, client_bag, metrics)
}

/// Create a worker for replaying a specific history. It will auto-shutdown as soon as the history
/// has finished being replayed. The provided client should be a mock, and this should only be used
/// for workflow testing purposes.
pub fn init_replay_worker(
    mut config: WorkerConfig,
    history: &History,
) -> Result<Worker, anyhow::Error> {
    info!(
        task_queue = config.task_queue.as_str(),
        "Registering replay worker"
    );
    config.max_cached_workflows = 1;
    config.max_concurrent_wft_polls = 1;
    config.no_remote_activities = true;
    // Could possibly just use mocked pollers here?
    let client = mock_client_from_history(history, &config.task_queue);
    let run_id = history.extract_run_id_from_start()?.to_string();
    let last_event = history.last_event_id();
    let mut worker = Worker::new(config, None, Arc::new(client), MetricsContext::default());
    worker.set_shutdown_on_run_reaches_event(run_id, last_event);
    Ok(worker)
}

pub(crate) fn sticky_q_name_for_worker(
    process_identity: &str,
    config: &WorkerConfig,
) -> Option<String> {
    if config.max_cached_workflows > 0 {
        Some(format!(
            "{}-{}-{}",
            &process_identity, &config.task_queue, *PROCCESS_UNIQ_ID
        ))
    } else {
        None
    }
}
