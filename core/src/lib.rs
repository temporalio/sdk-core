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
pub use telemetry::{TelemetryOptions, TelemetryOptionsBuilder};
pub use temporal_sdk_core_protos::TaskToken;
pub use url::Url;
pub use worker::{WorkerConfig, WorkerConfigBuilder};

use crate::{
    pollers::GatewayRef,
    telemetry::{fetch_global_buffered_logs, telemetry_init},
    worker::{Worker, WorkerDispatcher},
};
use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use temporal_sdk_core_api::{
    errors::{
        CompleteActivityError, CompleteWfError, CoreInitError, PollActivityError, PollWfError,
        WorkerRegistrationError,
    },
    Core, CoreLog,
};
use temporal_sdk_core_protos::coresdk::{
    activity_task::ActivityTask, workflow_activation::WfActivation,
    workflow_completion::WfActivationCompletion, ActivityHeartbeat, ActivityTaskCompletion,
};

use crate::telemetry::metrics::{MetricsContext, METRIC_METER};
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

/// Initializes an instance of the core sdk and establishes a connection to the temporal server.
///
/// Note: Also creates a tokio runtime that will be used for all client-server interactions.
///
/// # Panics
/// * Will panic if called from within an async context, as it will construct a runtime and you
///   cannot construct a runtime from within a runtime.
pub async fn init(opts: CoreInitOptions) -> Result<impl Core, CoreInitError> {
    telemetry_init(&opts.telemetry_opts).map_err(CoreInitError::TelemetryInitError)?;
    // Initialize server client
    let server_gateway = opts.gateway_opts.connect(Some(&METRIC_METER)).await?;

    Ok(CoreSDK::new(server_gateway, opts))
}

struct CoreSDK {
    /// Options provided at initialization time
    init_options: CoreInitOptions,
    /// Provides work in the form of responses the server would send from polling task Qs
    server_gateway: Arc<GatewayRef>,
    /// Controls access to workers
    workers: WorkerDispatcher,
    /// Has shutdown been called and all workers drained of tasks?
    whole_core_shutdown: AtomicBool,
    /// Top-level metrics context
    metrics: MetricsContext,
}

#[async_trait::async_trait]
impl Core for CoreSDK {
    async fn register_worker(&self, config: WorkerConfig) -> Result<(), WorkerRegistrationError> {
        info!(
            task_queue = config.task_queue.as_str(),
            "Registering worker"
        );
        let sticky_q = self.get_sticky_q_name_for_worker(&config);
        self.workers
            .new_worker(
                config,
                sticky_q,
                self.server_gateway.clone(),
                self.metrics.clone(),
            )
            .await
    }

    #[instrument(level = "debug", skip(self), fields(run_id))]
    async fn poll_workflow_activation(
        &self,
        task_queue: &str,
    ) -> Result<WfActivation, PollWfError> {
        let worker = self.worker(task_queue)?;
        worker.next_workflow_activation().await
    }

    #[instrument(level = "debug", skip(self))]
    async fn poll_activity_task(
        &self,
        task_queue: &str,
    ) -> Result<ActivityTask, PollActivityError> {
        loop {
            if self.whole_core_shutdown.load(Ordering::Relaxed) {
                return Err(PollActivityError::ShutDown);
            }
            let worker = self.worker(task_queue)?;
            match worker.activity_poll().await.transpose() {
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
        completion: WfActivationCompletion,
    ) -> Result<(), CompleteWfError> {
        let worker = self.worker(&completion.task_queue)?;
        worker.complete_workflow_activation(completion).await
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

        let worker = self.worker(&completion.task_queue)?;
        worker.complete_activity(task_token, status).await
    }

    fn record_activity_heartbeat(&self, details: ActivityHeartbeat) {
        if let Ok(w) = self.worker(&details.task_queue) {
            w.record_heartbeat(details);
        }
    }

    fn request_workflow_eviction(&self, task_queue: &str, run_id: &str) {
        if let Ok(w) = self.worker(task_queue) {
            w.request_wf_eviction(run_id, "Eviction explicitly requested by lang");
        }
    }

    fn server_gateway(&self) -> Arc<dyn ServerGatewayApis + Send + Sync> {
        self.server_gateway.gw.clone()
    }

    async fn shutdown(&self) {
        self.workers.shutdown_all().await;
        self.whole_core_shutdown.store(true, Ordering::Relaxed);
    }

    async fn shutdown_worker(&self, task_queue: &str) {
        self.workers.shutdown_one(task_queue).await;
    }

    fn fetch_buffered_logs(&self) -> Vec<CoreLog> {
        fetch_global_buffered_logs()
    }
}

impl CoreSDK {
    pub(crate) fn new<SG: ServerGatewayApis + Send + Sync + 'static>(
        server_gateway: SG,
        init_options: CoreInitOptions,
    ) -> Self {
        let sg = GatewayRef::new(Arc::new(server_gateway), init_options.gateway_opts.clone());
        let workers = WorkerDispatcher::default();
        Self {
            workers,
            init_options,
            whole_core_shutdown: AtomicBool::new(false),
            metrics: MetricsContext::top_level(sg.options.namespace.clone()),
            server_gateway: Arc::new(sg),
        }
    }

    /// Allow construction of workers with mocked poll responses during testing
    #[cfg(test)]
    pub(crate) fn reg_worker_sync(&mut self, worker: MockWorker) {
        let sticky_q = self.get_sticky_q_name_for_worker(&worker.config);
        let tq = worker.config.task_queue.clone();
        let worker = Worker::new_with_pollers(
            worker.config,
            sticky_q,
            self.server_gateway.clone(),
            worker.wf_poller,
            worker.act_poller,
            self.metrics.clone(),
        );
        self.workers.set_worker_for_task_queue(tq, worker).unwrap();
    }

    #[cfg(test)]
    pub(crate) fn outstanding_wfts(&self, tq: &str) -> usize {
        self.worker(tq).unwrap().outstanding_workflow_tasks()
    }
    #[cfg(test)]
    pub(crate) fn available_wft_permits(&self, tq: &str) -> usize {
        self.worker(tq).unwrap().available_wft_permits()
    }

    fn get_sticky_q_name_for_worker(&self, config: &WorkerConfig) -> Option<String> {
        if config.max_cached_workflows > 0 {
            Some(format!(
                "{}-{}-{}",
                &self.init_options.gateway_opts.identity, &config.task_queue, *PROCCESS_UNIQ_ID
            ))
        } else {
            None
        }
    }

    fn worker(&self, tq: &str) -> Result<impl Deref<Target = Worker>, WorkerLookupErr> {
        let worker = self.workers.get(tq);
        if worker.is_err() && self.whole_core_shutdown.load(Ordering::Relaxed) {
            return Err(WorkerLookupErr::Shutdown(tq.to_owned()));
        }
        worker
    }
}

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
