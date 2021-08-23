#![warn(missing_docs)] // error if there are missing docs
#![allow(clippy::upper_case_acronyms)]

//! This crate provides a basis for creating new Temporal SDKs without completely starting from
//! scratch

#[cfg(test)]
#[macro_use]
pub extern crate assert_matches;
#[macro_use]
extern crate tracing;

pub mod errors;
pub mod protos;
pub mod prototype_rust_sdk;

pub(crate) mod core_tracing;
mod machines;
mod pending_activations;
mod pollers;
mod protosext;
pub(crate) mod task_token;
mod worker;
mod workflow;

#[cfg(test)]
mod core_tests;
#[cfg(test)]
#[macro_use]
mod test_help;

pub use core_tracing::tracing_init;
pub use pollers::{
    ClientTlsConfig, RetryConfig, ServerGateway, ServerGatewayApis, ServerGatewayOptions, TlsConfig,
};
pub use protosext::IntoCompletion;
pub use url::Url;
pub use worker::{WorkerConfig, WorkerConfigBuilder};

use crate::{
    errors::{
        ActivityHeartbeatError, CompleteActivityError, CompleteWfError, CoreInitError,
        PollActivityError, PollWfError, WorkerRegistrationError,
    },
    protos::coresdk::{
        activity_task::ActivityTask, workflow_activation::WfActivation,
        workflow_completion::WfActivationCompletion, ActivityHeartbeat, ActivityTaskCompletion,
    },
    task_token::TaskToken,
    worker::WorkerDispatcher,
};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

#[cfg(test)]
use crate::test_help::MockWorker;
use crate::worker::Worker;
use std::ops::Deref;

lazy_static::lazy_static! {
    /// A process-wide unique string, which will be different on every startup
    static ref PROCCESS_UNIQ_ID: String = {
        uuid::Uuid::new_v4().to_simple().to_string()
    };
}

/// This trait is the primary way by which language specific SDKs interact with the core SDK. It is
/// expected that only one instance of an implementation will exist for the lifetime of the
/// worker(s) using it.
#[async_trait::async_trait]
pub trait Core: Send + Sync {
    /// Register a worker with core. Workers poll on a specific task queue, and when calling core's
    /// poll functions, you must provide a task queue name. If there was already a worker registered
    /// with the same task queue name, it will be shut down and a new one will be created.
    async fn register_worker(&self, config: WorkerConfig) -> Result<(), WorkerRegistrationError>;

    /// Ask the core for some work, returning a [WfActivation]. It is then the language SDK's
    /// responsibility to call the appropriate workflow code with the provided inputs. Blocks
    /// indefinitely until such work is available or [Core::shutdown] is called.
    ///
    /// The returned activation is guaranteed to be for the same task queue / worker which was
    /// provided as the `task_queue` argument.
    ///
    /// It is important to understand that all activations must be responded to. There can only
    /// be one outstanding activation for a particular run of a workflow at any time. If an
    /// activation is not responded to, it will cause that workflow to become stuck forever.
    ///
    /// Activations that contain only a `remove_from_cache` job should not cause the workflow code
    /// to be invoked and may be responded to with an empty command list. Eviction jobs may also
    /// appear with other jobs, but will always appear last in the job list. In this case it is
    /// expected that the workflow code will be invoked, and the response produced as normal, but
    /// the caller should evict the run after doing so.
    ///
    /// It is rarely a good idea to call poll concurrently. It handles polling the server
    /// concurrently internally.
    ///
    /// TODO: Examples
    async fn poll_workflow_activation(&self, task_queue: &str)
        -> Result<WfActivation, PollWfError>;

    /// Ask the core for some work, returning an [ActivityTask]. It is then the language SDK's
    /// responsibility to call the appropriate activity code with the provided inputs. Blocks
    /// indefinitely until such work is available or [Core::shutdown] is called.
    ///
    /// The returned activation is guaranteed to be for the same task queue / worker which was
    /// provided as the `task_queue` argument.
    ///
    /// It is rarely a good idea to call poll concurrently. It handles polling the server
    /// concurrently internally.
    ///
    /// TODO: Examples
    async fn poll_activity_task(&self, task_queue: &str)
        -> Result<ActivityTask, PollActivityError>;

    /// Tell the core that a workflow activation has completed. May be freely called concurrently.
    async fn complete_workflow_activation(
        &self,
        completion: WfActivationCompletion,
    ) -> Result<(), CompleteWfError>;

    /// Tell the core that an activity has finished executing. May be freely called concurrently.
    async fn complete_activity_task(
        &self,
        completion: ActivityTaskCompletion,
    ) -> Result<(), CompleteActivityError>;

    /// Notify workflow that an activity is still alive. Long running activities that take longer
    /// than `activity_heartbeat_timeout` to finish must call this function in order to report
    /// progress, otherwise the activity will timeout and a new attempt will be scheduled.
    ///
    /// The first heartbeat request will be sent immediately, subsequent rapid calls to this
    /// function will result in heartbeat requests being aggregated and the last one received during
    /// the aggregation period will be sent to the server, where that period is defined as half the
    /// heartbeat timeout.
    ///
    /// Unlike java/go SDKs we do not return cancellation status as part of heartbeat response and
    /// instead send it as a separate activity task to the lang, decoupling heartbeat and
    /// cancellation processing.
    ///
    /// For now activity still need to send heartbeats if they want to receive cancellation
    /// requests. In the future we will change this and will dispatch cancellations more
    /// proactively. Note that this function does not block on the server call and returns
    /// immediately. Underlying validation errors are swallowed and logged, this has been agreed to
    /// be optimal behavior for the user as we don't want to break activity execution due to badly
    /// configured heartbeat options.
    fn record_activity_heartbeat(&self, details: ActivityHeartbeat);

    /// Request that a workflow be evicted by its run id. This will generate a workflow activation
    /// with the eviction job inside it to be eventually returned by
    /// [Core::poll_workflow_activation]. If the workflow had any existing outstanding activations,
    /// such activations are invalidated and subsequent completions of them will do nothing and log
    /// a warning.
    fn request_workflow_eviction(&self, task_queue: &str, run_id: &str);

    /// Returns core's instance of the [ServerGatewayApis] implementor it is using.
    fn server_gateway(&self) -> Arc<dyn ServerGatewayApis>;

    /// Initiates async shutdown procedure, eventually ceases all polling of the server and shuts
    /// down all registered workers. [Core::poll_workflow_activation] should be called until it
    /// returns [PollWfError::ShutDown] to ensure that any workflows which are still undergoing
    /// replay have an opportunity to finish. This means that the lang sdk will need to call
    /// [Core::complete_workflow_activation] for those workflows until they are done. At that point,
    /// the lang SDK can end the process, or drop the [Core] instance, which will close the
    /// connection.
    async fn shutdown(&self);

    /// Shut down a specific worker. Will cease all polling on the task queue and future attempts
    /// to poll that queue will return [PollWfError::NoWorkerForQueue].
    async fn shutdown_worker(&self, task_queue: &str);
}

/// Holds various configuration information required to call [init]
#[derive(Debug, Clone, derive_builder::Builder)]
#[builder(setter(into))]
pub struct CoreInitOptions {
    /// Options for the connection to the temporal server
    pub gateway_opts: ServerGatewayOptions,
}

/// Initializes an instance of the core sdk and establishes a connection to the temporal server.
///
/// Note: Also creates a tokio runtime that will be used for all client-server interactions.  
///
/// # Panics
/// * Will panic if called from within an async context, as it will construct a runtime and you
///   cannot construct a runtime from within a runtime.
pub async fn init(opts: CoreInitOptions) -> Result<impl Core, CoreInitError> {
    // Initialize server client
    let server_gateway = opts.gateway_opts.connect().await?;

    Ok(CoreSDK::new(server_gateway, opts))
}

struct CoreSDK<SG> {
    /// Options provided at initialization time
    init_options: CoreInitOptions,
    /// Provides work in the form of responses the server would send from polling task Qs
    server_gateway: Arc<SG>,
    /// Controls access to workers
    workers: WorkerDispatcher,
    /// Has shutdown been called?
    shutdown_requested: AtomicBool,
}

#[async_trait::async_trait]
impl<WP> Core for CoreSDK<WP>
where
    WP: ServerGatewayApis + Send + Sync + 'static,
{
    async fn register_worker(&self, config: WorkerConfig) -> Result<(), WorkerRegistrationError> {
        let sticky_q = self.get_sticky_q_name_for_worker(&config);
        self.workers
            .new_worker(config, sticky_q, self.server_gateway.clone())
            .await
    }

    #[instrument(skip(self))]
    async fn poll_workflow_activation(
        &self,
        task_queue: &str,
    ) -> Result<WfActivation, PollWfError> {
        let worker = self.worker(task_queue)?;
        worker.next_workflow_activation().await
    }

    #[instrument(skip(self))]
    async fn poll_activity_task(
        &self,
        task_queue: &str,
    ) -> Result<ActivityTask, PollActivityError> {
        loop {
            if self.shutdown_requested.load(Ordering::Relaxed) {
                return Err(PollActivityError::ShutDown);
            }
            let worker = self.worker(task_queue)?;
            match worker.activity_poll().await.transpose() {
                Some(r) => break r,
                None => continue,
            }
        }
    }

    #[instrument(skip(self, completion), fields(completion=%&completion))]
    async fn complete_workflow_activation(
        &self,
        completion: WfActivationCompletion,
    ) -> Result<(), CompleteWfError> {
        let worker = self.worker(&completion.task_queue)?;
        worker.complete_workflow_activation(completion).await
    }

    #[instrument(skip(self))]
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
            w.request_wf_eviction(run_id);
        }
    }

    fn server_gateway(&self) -> Arc<dyn ServerGatewayApis> {
        self.server_gateway.clone()
    }

    async fn shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        self.workers.shutdown_all().await;
    }

    async fn shutdown_worker(&self, task_queue: &str) {
        self.workers.shutdown_one(task_queue).await;
    }
}

impl<SG: ServerGatewayApis + Send + Sync + 'static> CoreSDK<SG> {
    pub(crate) fn new(server_gateway: SG, init_options: CoreInitOptions) -> Self {
        let sg = Arc::new(server_gateway);
        let workers = WorkerDispatcher::default();
        Self {
            server_gateway: sg,
            workers,
            init_options,
            shutdown_requested: AtomicBool::new(false),
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
        );
        self.workers.set_worker_for_task_queue(tq, worker).unwrap();
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
        if worker.is_none() && self.shutdown_requested.load(Ordering::Relaxed) {
            return Err(WorkerLookupErr::Shutdown(tq.to_owned()));
        }
        let worker = worker.ok_or_else(|| WorkerLookupErr::NoWorker(tq.to_owned()))?;
        Ok(worker)
    }
}

enum WorkerLookupErr {
    Shutdown(String),
    NoWorker(String),
}
