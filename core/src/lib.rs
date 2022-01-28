#![warn(missing_docs)] // error if there are missing docs
#![allow(clippy::upper_case_acronyms)]

//! This crate provides a basis for creating new Temporal SDKs without completely starting from
//! scratch

#[cfg(test)]
#[macro_use]
pub extern crate assert_matches;
#[macro_use]
extern crate tracing;

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
    ClientTlsConfig, RetryConfig, RetryGateway, ServerGateway, ServerGatewayApis,
    ServerGatewayOptions, ServerGatewayOptionsBuilder, TlsConfig,
};
pub use telemetry::{telemetry_init, TelemetryOptions, TelemetryOptionsBuilder};
pub use temporal_sdk_core_api as api;
pub use temporal_sdk_core_protos as protos;
pub use temporal_sdk_core_protos::TaskToken;
pub use url::Url;
pub use worker::{WorkerConfig, WorkerConfigBuilder};

use crate::{telemetry::metrics::METRIC_METER, worker::Worker};
use std::sync::Arc;
use temporal_sdk_core_api::{
    errors::{CompleteActivityError, CompleteWfError, PollActivityError, PollWfError},
    CoreLog, Worker as WorkerTrait,
};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_task::ActivityTask,
        workflow_activation::{remove_from_cache::EvictionReason, WorkflowActivation},
        workflow_completion::WorkflowActivationCompletion,
        ActivityHeartbeat, ActivityTaskCompletion,
    },
    temporal::api::history::v1::History,
};

#[cfg(test)]
use crate::test_help::MockWorker;

lazy_static::lazy_static! {
    /// A process-wide unique string, which will be different on every startup
    static ref PROCCESS_UNIQ_ID: String = {
        uuid::Uuid::new_v4().to_simple().to_string()
    };
}

/// Holds various configuration information required to call [init]
#[derive(Debug, Clone, derive_builder::Builder)]
#[builder(setter(into))]
#[non_exhaustive]
pub struct CoreInitOptions {
    /// Options for the connection to the temporal server
    pub gateway_opts: ServerGatewayOptions,
    /// Options for telemetry (traces and metrics)
    #[builder(default)]
    pub telemetry_opts: TelemetryOptions,
}

/// Initialize a worker bound to a task queue
pub fn init_worker<SG: ServerGatewayApis + Send + Sync + 'static>(
    _worker_config: WorkerConfig,
    _client: SG,
) -> Worker {
    todo!()
}

/// Create a worker for replaying a specific history. It will auto-shutdown as soon as the history
/// has finished being replayed. The provided gateway should be a mock, and this should only be used
/// for workflow testing purposes.
pub fn init_replay_worker(
    config: WorkerConfig,
    gateway: Arc<dyn ServerGatewayApis + Send + Sync>,
    history: &History,
) -> Result<Worker, anyhow::Error> {
    info!(
        task_queue = config.task_queue.as_str(),
        "Registering replay worker"
    );
    // Could possibly just use mocked pollers here, but they'd need to be un-test-moded
    let run_id = history.extract_run_id_from_start()?.to_string();
    let last_event = history.last_event_id();
    let mut worker = Worker::new(config, None, gateway, todo!());
    worker.set_post_activate_hook(move |worker| {
        if worker
            .wft_manager
            .most_recently_processed_event(&run_id)
            .unwrap_or_default()
            >= last_event
        {
            worker.initiate_shutdown();
        }
    });
    Ok(worker)
}

#[async_trait::async_trait]
impl WorkerTrait for Worker {
    #[instrument(level = "debug", skip(self), fields(run_id))]
    async fn poll_workflow_activation(&self) -> Result<WorkflowActivation, PollWfError> {
        self.next_workflow_activation().await
    }

    #[instrument(level = "debug", skip(self))]
    async fn poll_activity_task(&self) -> Result<ActivityTask, PollActivityError> {
        loop {
            match self.activity_poll().await.transpose() {
                Some(r) => break r,
                None => {
                    tokio::task::yield_now().await;
                    continue;
                }
            }
        }
    }

    #[instrument(level = "debug", skip(self, completion),
      fields(completion=%&completion, run_id=%completion.run_id))]
    async fn complete_workflow_activation(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<(), CompleteWfError> {
        self.complete_workflow_activation(completion).await
    }

    #[instrument(level = "debug", skip(self))]
    async fn complete_activity_task(
        &self,
        completion: ActivityTaskCompletion,
    ) -> Result<(), CompleteActivityError> {
        let task_token = TaskToken(completion.task_token);
        let status = if let Some(s) = completion.result.and_then(|r| r.status) {
            s
        } else {
            return Err(CompleteActivityError::MalformedActivityCompletion {
                reason: "Activity completion had empty result/status field".to_owned(),
                completion: None,
            });
        };

        self.complete_activity(task_token, status).await
    }

    fn record_activity_heartbeat(&self, details: ActivityHeartbeat) {
        self.record_heartbeat(details);
    }

    fn request_workflow_eviction(&self, run_id: &str) {
        self.request_wf_eviction(
            run_id,
            "Eviction explicitly requested by lang",
            EvictionReason::LangRequested,
        );
    }

    fn server_gateway(&self) -> Arc<dyn ServerGatewayApis + Send + Sync> {
        self.server_gateway.clone()
    }

    async fn shutdown(&self) {
        self.shutdown().await;
    }
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

/// Allow construction of workers with mocked poll responses during testing
// #[cfg(test)]
// pub(crate) fn reg_worker_sync(&self, worker: MockWorker) {
//     let sticky_q = self.get_sticky_q_name_for_worker(&worker.config);
//     let tq = worker.config.task_queue.clone();
//     let worker = Worker::new_with_pollers(
//         worker.config,
//         sticky_q,
//         self.server_gateway.clone(),
//         worker.wf_poller,
//         worker.act_poller,
//         self.metrics.clone(),
//     );
//     self.workers.set_worker_for_task_queue(tq, worker).unwrap();
// }

#[derive(Debug)]
enum WorkerLookupErr {
    Shutdown(String),
    NoWorker(String),
}

impl From<WorkerLookupErr> for PollWfError {
    fn from(e: WorkerLookupErr) -> Self {
        match e {
            WorkerLookupErr::Shutdown(_) => Self::ShutDown,
            WorkerLookupErr::NoWorker(s) => Self::NoWorkerForQueue(s),
        }
    }
}

impl From<WorkerLookupErr> for PollActivityError {
    fn from(e: WorkerLookupErr) -> Self {
        match e {
            WorkerLookupErr::Shutdown(_) => Self::ShutDown,
            WorkerLookupErr::NoWorker(s) => Self::NoWorkerForQueue(s),
        }
    }
}

impl From<WorkerLookupErr> for CompleteWfError {
    fn from(e: WorkerLookupErr) -> Self {
        match e {
            WorkerLookupErr::Shutdown(s) | WorkerLookupErr::NoWorker(s) => {
                Self::NoWorkerForQueue(s)
            }
        }
    }
}

impl From<WorkerLookupErr> for CompleteActivityError {
    fn from(e: WorkerLookupErr) -> Self {
        match e {
            WorkerLookupErr::Shutdown(s) | WorkerLookupErr::NoWorker(s) => {
                Self::NoWorkerForQueue(s)
            }
        }
    }
}
