#![warn(missing_docs)] // error if there are missing docs
#![allow(clippy::upper_case_acronyms)]

//! This crate provides a basis for creating new Temporal SDKs without completely starting from
//! scratch

#[cfg(test)]
#[macro_use]
pub extern crate assert_matches;
#[macro_use]
extern crate tracing;

mod abstractions;
mod log_export;
mod pending_activations;
mod pollers;
mod protosext;
pub mod replay;
pub(crate) mod retry_logic;
pub(crate) mod telemetry;
mod worker;
mod workflow;

#[cfg(test)]
mod core_tests;
#[cfg(test)]
#[macro_use]
mod test_help;

pub(crate) use temporal_sdk_core_api::errors;

pub use pollers::{
    ClientOptions, ClientOptionsBuilder, ClientTlsConfig, RetryClient, RetryConfig, ServerGateway,
    TlsConfig, WorkflowClientTrait,
};
pub use telemetry::{
    fetch_global_buffered_logs, telemetry_init, TelemetryOptions, TelemetryOptionsBuilder,
};
pub use temporal_sdk_core_api as api;
pub use temporal_sdk_core_protos as protos;
pub use temporal_sdk_core_protos::TaskToken;
pub use url::Url;
pub use worker::{Worker, WorkerConfig, WorkerConfigBuilder};

use crate::telemetry::metrics::{MetricsContext, METRIC_METER};
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

/// Initialize a worker bound to a task queue
pub fn init_worker<CT>(worker_config: WorkerConfig, client: CT) -> Worker
where
    CT: Into<AnyClient>,
{
    let as_enum = client.into();
    // TODO: Assert namespaces match
    let client = match as_enum {
        AnyClient::HighLevel(ac) => ac,
        AnyClient::LowLevel(ll) => {
            let (c, opts) = ll.into_parts();
            let gateway = ServerGateway::new(c, opts, worker_config.namespace.to_owned());
            let retry_gateway = RetryClient::new(gateway, RetryConfig::default());
            Arc::new(retry_gateway)
        }
    };
    let sticky_q = sticky_q_name_for_worker(&client.get_options().identity, &worker_config);
    let metrics = MetricsContext::top_level(worker_config.namespace.clone())
        .with_task_q(worker_config.task_queue.clone());
    Worker::new(worker_config, sticky_q, client, metrics)
}

/// Create a worker for replaying a specific history. It will auto-shutdown as soon as the history
/// has finished being replayed. The provided gateway should be a mock, and this should only be used
/// for workflow testing purposes.
pub fn init_replay_worker(
    mut config: WorkerConfig,
    gateway: Arc<dyn WorkflowClientTrait + Send + Sync>,
    history: &History,
) -> Result<Worker, anyhow::Error> {
    info!(
        task_queue = config.task_queue.as_str(),
        "Registering replay worker"
    );
    config.max_cached_workflows = 1;
    config.max_concurrent_wft_polls = 1;
    config.no_remote_activities = true;
    // Could possibly just use mocked pollers here, but they'd need to be un-test-moded
    let run_id = history.extract_run_id_from_start()?.to_string();
    let last_event = history.last_event_id();
    let mut worker = Worker::new(config, None, gateway, MetricsContext::default());
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
