#![warn(missing_docs)] // error if there are missing docs
#![allow(clippy::upper_case_acronyms)]

//! This crate provides a basis for creating new Temporal SDKs without completely starting from
//! scratch

#[cfg(test)]
#[macro_use]
pub extern crate assert_matches;
#[macro_use]
extern crate tracing;

pub mod protos;
pub mod test_workflow_driver;

mod activity;
pub(crate) mod core_tracing;
mod errors;
mod machines;
mod pending_activations;
mod pollers;
mod protosext;
pub(crate) mod task_token;
mod worker;
mod workflow;
mod workflow_tasks;

#[cfg(test)]
mod core_tests;
#[cfg(test)]
mod test_help;

pub use crate::errors::{
    ActivityHeartbeatError, CompleteActivityError, CompleteWfError, CoreInitError,
    PollActivityError, PollWfError,
};
pub use core_tracing::tracing_init;
pub use pollers::{
    ClientTlsConfig, ServerGateway, ServerGatewayApis, ServerGatewayOptions, TlsConfig,
};
pub use protosext::IntoCompletion;
pub use url::Url;
pub use worker::{WorkerConfig, WorkerConfigBuilder};

use crate::{
    activity::ActivityTaskManager,
    errors::ShutdownErr,
    machines::EmptyWorkflowCommandErr,
    protos::{
        coresdk::{
            activity_result::{self as ar, activity_result},
            activity_task::ActivityTask,
            workflow_activation::WfActivation,
            workflow_completion::{self, wf_activation_completion, WfActivationCompletion},
            ActivityHeartbeat, ActivityTaskCompletion,
        },
        temporal::api::enums::v1::WorkflowTaskFailedCause,
    },
    protosext::ValidPollWFTQResponse,
    task_token::TaskToken,
    worker::Worker,
    workflow::WorkflowCachingPolicy,
    workflow_tasks::{FailedActivationOutcome, NewWfTaskOutcome, WorkflowTaskManager},
};
use dashmap::DashMap;
use futures::{FutureExt, TryFutureExt};
use std::{
    convert::TryInto,
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver},
    Mutex, Notify,
};

/// This trait is the primary way by which language specific SDKs interact with the core SDK. It is
/// expected that only one instance of an implementation will exist for the lifetime of the
/// worker(s) using it.
#[async_trait::async_trait]
pub trait Core: Send + Sync {
    /// Register a worker with core. Workers poll on a specific task queue, and when calling core's
    /// poll functions, you must provide a task queue name. Returned activations will include that
    /// task queue name as well, so that lang may route them to the appropriate place.
    fn register_worker(&self, config: WorkerConfig);

    /// Ask the core for some work, returning a [WfActivation]. It is then the language SDK's
    /// responsibility to call the appropriate workflow code with the provided inputs. Blocks
    /// indefinitely until such work is available or [Core::shutdown] is called.
    ///
    /// TODO: Examples
    async fn poll_workflow_task(&self, task_queue: &str) -> Result<WfActivation, PollWfError>;

    /// Ask the core for some work, returning an [ActivityTask]. It is then the language SDK's
    /// responsibility to call the appropriate activity code with the provided inputs. Blocks
    /// indefinitely until such work is available or [Core::shutdown] is called.
    ///
    /// TODO: Examples
    async fn poll_activity_task(&self, task_queue: &str)
        -> Result<ActivityTask, PollActivityError>;

    /// Tell the core that a workflow activation has completed
    async fn complete_workflow_task(
        &self,
        completion: WfActivationCompletion,
    ) -> Result<(), CompleteWfError>;

    /// Tell the core that an activity has finished executing
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
    /// with the eviction job inside it to be eventually returned by [Core::poll_workflow_task]. If
    /// the workflow had any existing outstanding activations, such activations are invalidated and
    /// subsequent completions of them will do nothing and log a warning.
    fn request_workflow_eviction(&self, run_id: &str);

    /// Returns core's instance of the [ServerGatewayApis] implementor it is using.
    fn server_gateway(&self) -> Arc<dyn ServerGatewayApis>;

    /// Initiates async shutdown procedure, eventually ceases all polling of the server.
    /// [Core::poll_workflow_task] should be called until it returns [PollWfError::ShutDown]
    /// to ensure that any workflows which are still undergoing replay have an opportunity to finish.
    /// This means that the lang sdk will need to call [Core::complete_workflow_task] for those
    /// workflows until they are done. At that point, the lang SDK can end the process,
    /// or drop the [Core] instance, which will close the connection.
    async fn shutdown(&self);
}

/// Holds various configuration information required to call [init]
#[derive(Debug, Clone, derive_builder::Builder)]
#[builder(setter(into))]
pub struct CoreInitOptions {
    /// Options for the connection to the temporal server
    pub gateway_opts: ServerGatewayOptions,
    /// If set nonzero, workflows will be cached and sticky task queues will be used, meaning that
    /// history updates are applied incrementally to suspended instances of workflow execution.
    /// Workflows are evicted according to a least-recently-used policy one the cache maximum is
    /// reached. Workflows may also be explicitly evicted at any time, or as a result of errors
    /// or failures.
    #[builder(default = "0")]
    pub max_cached_workflows: usize,
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
    let work_provider = opts.gateway_opts.connect().await?;

    Ok(CoreSDK::new(work_provider, opts))
}

struct CoreSDK<WP> {
    /// Options provided at initialization time
    _init_options: CoreInitOptions,
    /// Provides work in the form of responses the server would send from polling task Qs
    server_gateway: Arc<WP>,
    /// Maps task queue names to workers
    workers: DashMap<String, Worker>,
    /// Workflow task management
    wft_manager: WorkflowTaskManager,
    /// Activity task management
    act_manager: ActivityTaskManager,
    /// Has shutdown been called?
    shutdown_requested: AtomicBool,
    /// Used to wake up future which checks shutdown state
    shutdown_notify: Notify,
    /// Used to wake blocked workflow task polling when there is some change to workflow activations
    /// that should cause us to restart the loop
    workflow_activations_update: Mutex<UnboundedReceiver<WfActivationUpdate>>,
}

enum WfActivationUpdate {
    NewPendingActivation,
    WorkflowTaskComplete { task_queue: String },
}

#[async_trait::async_trait]
impl<WP> Core for CoreSDK<WP>
where
    WP: ServerGatewayApis + Send + Sync + 'static,
{
    fn register_worker(&self, config: WorkerConfig) {
        let tq = config.task_queue.clone();
        let worker = Worker::new(config, self.server_gateway.clone());
        self.workers.insert(tq, worker);
    }

    #[instrument(skip(self))]
    async fn poll_workflow_task(&self, task_queue: &str) -> Result<WfActivation, PollWfError> {
        // The poll needs to be in a loop because we can't guarantee tail call optimization in Rust
        // (simply) and we really, really need that for long-poll retries.
        loop {
            // We must first check if there are pending workflow activations for workflows that are
            // currently replaying or otherwise need immediate jobs, and issue those before
            // bothering the server.
            if let Some(pa) = self.wft_manager.next_pending_activation() {
                debug!(activation=%pa, "Sending pending activation to lang");
                return Ok(pa);
            }

            if self.shutdown_requested.load(Ordering::Relaxed) {
                return Err(PollWfError::ShutDown);
            }

            // Apply any buffered poll responses from the server. Must come after pending
            // activations, since there may be an eviction etc for whatever run is popped here.
            if let Some(buff_wft) = self.wft_manager.next_buffered_poll() {
                match self.apply_server_work(buff_wft).await? {
                    Some(a) => return Ok(a),
                    None => continue,
                }
            }

            let activation_update_fut = self
                .workflow_activations_update
                .lock()
                .then(|mut l| async move { l.recv().await });
            let worker = self
                .workers
                .get(task_queue)
                .ok_or_else(|| PollWfError::NoWorkerForQueue(task_queue.to_owned()))?;
            let poll_result_future =
                self.shutdownable_fut(worker.workflow_poll().map_err(Into::into));
            let selected_f = tokio::select! {
                biased;

                // If a task is completed while we are waiting on polling, we need to restart the
                // loop right away to provide any potential new pending activation
                update = activation_update_fut => {
                    // If the update indicates a task completed, free a slot in the worker
                    if let Some(WfActivationUpdate::WorkflowTaskComplete {task_queue}) = update {
                        if let Some(w) = self.workers.get(&task_queue) {
                            w.workflow_task_done();
                        }
                    }
                    continue;
                }
                r = poll_result_future => r
            };

            match selected_f {
                Ok(None) => {
                    debug!("Poll wft timeout");
                    continue;
                }
                Ok(Some(work)) => match self.apply_server_work(work).await? {
                    Some(a) => return Ok(a),
                    None => continue,
                },
                // Drain pending activations in case of shutdown.
                Err(PollWfError::ShutDown) => continue,
                Err(e) => return Err(e),
            }
        }
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

            let worker = self
                .workers
                .get(task_queue)
                .ok_or_else(|| PollActivityError::NoWorkerForQueue(task_queue.to_owned()))?;

            tokio::select! {
                biased;

                r = self.act_manager.poll(&worker) => {
                    match r.transpose() {
                        None => continue,
                        Some(r) => return r
                    }
                }
                _ = self.shutdown_notifier() => {
                    return Err(PollActivityError::ShutDown);
                }
            }
        }
    }

    #[instrument(skip(self, completion), fields(completion=%&completion))]
    async fn complete_workflow_task(
        &self,
        completion: WfActivationCompletion,
    ) -> Result<(), CompleteWfError> {
        let wfstatus = completion.status;

        match wfstatus {
            Some(wf_activation_completion::Status::Successful(success)) => {
                self.wf_activation_success(&completion.run_id, success)
                    .await
            }
            Some(wf_activation_completion::Status::Failed(failure)) => {
                self.wf_activation_failed(&completion.run_id, failure).await
            }
            None => Err(CompleteWfError::MalformedWorkflowCompletion {
                reason: "Workflow completion had empty status field".to_owned(),
                completion: None,
            }),
        }
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
        let tt = task_token.clone();
        let maybe_net_err = match status {
            activity_result::Status::Completed(ar::Success { result }) => self
                .server_gateway
                .complete_activity_task(task_token, result.map(Into::into))
                .await
                .err(),
            activity_result::Status::Failed(ar::Failure { failure }) => self
                .server_gateway
                .fail_activity_task(task_token, failure.map(Into::into))
                .await
                .err(),
            activity_result::Status::Canceled(ar::Cancelation { details }) => self
                .server_gateway
                .cancel_activity_task(task_token, details.map(Into::into))
                .await
                .err(),
        };
        let (res, should_remove) = match maybe_net_err {
            Some(e) if e.code() == tonic::Code::NotFound => {
                warn!(task_token = ?tt, details = ?e, "Activity not found on completion.\
                 This may happen if the activity has already been cancelled but completed anyway.");
                (Ok(()), true)
            }
            Some(err) => (Err(err), false),
            None => (Ok(()), true),
        };
        if should_remove {
            // Remove the activity from tracking and tell the worker a slot is free
            if let Some(deets) = self.act_manager.mark_complete(&tt) {
                if let Some(worker) = self.workers.get(&deets.task_queue) {
                    worker.activity_done();
                }
            }
        }
        Ok(res?)
    }

    fn record_activity_heartbeat(&self, details: ActivityHeartbeat) {
        let tt = details.task_token.clone();
        if let Err(e) = self.act_manager.record_activity_heartbeat(details) {
            warn!(task_token = ?tt, details = ?e, "Activity heartbeat failed.")
        }
    }

    fn request_workflow_eviction(&self, run_id: &str) {
        self.wft_manager.evict_run(run_id);
    }

    fn server_gateway(&self) -> Arc<dyn ServerGatewayApis> {
        self.server_gateway.clone()
    }

    async fn shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();
        for entry in self.workers.iter() {
            entry.shutdown();
        }
        self.wft_manager.shutdown();
        self.act_manager.shutdown().await;
    }
}

impl<SG: ServerGatewayApis + Send + Sync + 'static> CoreSDK<SG> {
    pub(crate) fn new(server_gateway: SG, init_options: CoreInitOptions) -> Self {
        let sg = Arc::new(server_gateway);
        let cache_policy = if init_options.max_cached_workflows == 0 {
            WorkflowCachingPolicy::NonSticky
        } else {
            WorkflowCachingPolicy::Sticky {
                max_cached_workflows: init_options.max_cached_workflows,
            }
        };
        let (wau_tx, wau_rx) = unbounded_channel();
        Self {
            wft_manager: WorkflowTaskManager::new(wau_tx, cache_policy),
            act_manager: ActivityTaskManager::new(sg.clone()),
            server_gateway: sg,
            workers: DashMap::new(),
            _init_options: init_options,
            shutdown_requested: AtomicBool::new(false),
            shutdown_notify: Notify::new(),
            workflow_activations_update: Mutex::new(wau_rx),
        }
    }

    /// Apply validated WFTs from the server. Returns an activation if one should be issued to
    /// lang, or returns `None` in which case the polling loop should be restarted.
    async fn apply_server_work(
        &self,
        work: ValidPollWFTQResponse,
    ) -> Result<Option<WfActivation>, CompleteWfError> {
        let we = work.workflow_execution.clone();
        match self.wft_manager.apply_new_wft(work)? {
            NewWfTaskOutcome::IssueActivation(a) => {
                debug!(activation=%a, "Sending activation to lang");
                return Ok(Some(a));
            }
            NewWfTaskOutcome::RestartPollLoop => Ok(None),
            NewWfTaskOutcome::Autocomplete => {
                debug!(workflow_execution=?we,
                       "No work for lang to perform after polling server. Sending autocomplete.");
                self.complete_workflow_task(WfActivationCompletion {
                    run_id: we.run_id,
                    status: Some(workflow_completion::Success::from_variants(vec![]).into()),
                })
                .await?;
                Ok(None)
            }
        }
    }

    /// Handle a successful workflow completion
    async fn wf_activation_success(
        &self,
        run_id: &str,
        success: workflow_completion::Success,
    ) -> Result<(), CompleteWfError> {
        // Convert to wf commands
        let cmds = success
            .commands
            .into_iter()
            .map(|c| c.try_into())
            .collect::<Result<Vec<_>, EmptyWorkflowCommandErr>>()
            .map_err(|_| CompleteWfError::MalformedWorkflowCompletion {
                reason: "At least one workflow command in the completion \
                                contained an empty variant"
                    .to_owned(),
                completion: None,
            })?;

        if let Some(server_cmds) = self.wft_manager.successful_activation(run_id, cmds)? {
            debug!("Sending commands to server: {:?}", &server_cmds.commands);
            let res = self
                .server_gateway
                .complete_workflow_task(server_cmds.task_token, server_cmds.commands)
                .await;
            if let Err(ts) = res {
                let should_evict = self.handle_wft_complete_errs(ts)?;
                if should_evict {
                    self.wft_manager.evict_run(run_id);
                }
            }
        }
        Ok(())
    }

    /// Handle a failed workflow completion
    async fn wf_activation_failed(
        &self,
        run_id: &str,
        failure: workflow_completion::Failure,
    ) -> Result<(), CompleteWfError> {
        if let FailedActivationOutcome::Report(tt) = self.wft_manager.failed_activation(run_id) {
            self.server_gateway
                .fail_workflow_task(
                    tt,
                    WorkflowTaskFailedCause::Unspecified,
                    failure.failure.map(Into::into),
                )
                .await?;
        }
        Ok(())
    }

    /// A future that resolves when the shutdown flag has been set to true
    async fn shutdown_notifier(&self) {
        loop {
            if self.shutdown_requested.load(Ordering::Relaxed) {
                break;
            }
            self.shutdown_notify.notified().await;
        }
    }

    /// Wrap a future, making it return early with a shutdown error in the event the shutdown
    /// flag has been set
    async fn shutdownable_fut<FOut, FErr>(
        &self,
        wrap_this: impl Future<Output = Result<FOut, FErr>>,
    ) -> Result<FOut, FErr>
    where
        FErr: From<ShutdownErr>,
    {
        tokio::select! {
            biased;

            _ = self.shutdown_notifier() => {
                Err(ShutdownErr.into())
            }
            r = wrap_this => r,
        }
    }

    /// Handle server errors from either completing or failing a workflow task
    ///
    /// Returns `Ok(true)` if the workflow should be evicted, `Err(_)` if the error should be
    /// propagated, and `Ok(false)` if it is safe to ignore entirely.
    fn handle_wft_complete_errs(&self, err: tonic::Status) -> Result<bool, CompleteWfError> {
        match err.code() {
            // Silence unhandled command errors since the lang SDK cannot do anything about them
            // besides poll again, which it will do anyway.
            tonic::Code::InvalidArgument if err.message() == "UnhandledCommand" => {
                warn!("Unhandled command response when completing");
                Ok(false)
            }
            tonic::Code::NotFound => {
                warn!("Task not found when completing");
                Ok(true)
            }
            _ => Err(err.into()),
        }
    }
}
