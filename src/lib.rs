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
    PollActivityError, PollWfError, WorkerRegistrationError,
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
    protosext::{ValidPollWFTQResponse, WorkflowTaskCompletion},
    task_token::TaskToken,
    worker::Worker,
    workflow::WorkflowCachingPolicy,
    workflow_tasks::{
        ActivationAction, FailedActivationOutcome, NewWfTaskOutcome,
        ServerCommandsWithWorkflowInfo, WorkflowTaskManager,
    },
};
use futures::{FutureExt, TryFutureExt};
use std::{
    collections::HashMap,
    convert::TryInto,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver},
    Mutex, Notify, RwLock,
};

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
    /// TODO: Examples
    async fn poll_workflow_task(&self, task_queue: &str) -> Result<WfActivation, PollWfError>;

    /// Ask the core for some work, returning an [ActivityTask]. It is then the language SDK's
    /// responsibility to call the appropriate activity code with the provided inputs. Blocks
    /// indefinitely until such work is available or [Core::shutdown] is called.
    ///
    /// The returned activation is guaranteed to be for the same task queue / worker which was
    /// provided as the `task_queue` argument.
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

    /// Initiates async shutdown procedure, eventually ceases all polling of the server and shuts
    /// down all registered workers. [Core::poll_workflow_task] should be called until it returns
    /// [PollWfError::ShutDown] to ensure that any workflows which are still undergoing replay have
    /// an opportunity to finish. This means that the lang sdk will need to call
    /// [Core::complete_workflow_task] for those workflows until they are done. At that point, the
    /// lang SDK can end the process, or drop the [Core] instance, which will close the connection.
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
    init_options: CoreInitOptions,
    /// Provides work in the form of responses the server would send from polling task Qs
    server_gateway: Arc<WP>,
    /// Maps task queue names to workers
    workers: RwLock<HashMap<String, WorkerStatus>>,
    /// Workflow task management
    wft_manager: WorkflowTaskManager,
    /// Activity task management
    act_manager: ActivityTaskManager,
    /// Has shutdown been called?
    shutdown_requested: AtomicBool,
    /// Used to wake up future which checks shutdown state
    poll_loop_notify: Notify,
    /// Used to wake blocked workflow task polling when there is some change to workflow activations
    /// that should cause us to restart the loop
    workflow_activations_update: Mutex<UnboundedReceiver<WfActivationUpdate>>,
}

enum WfActivationUpdate {
    NewPendingActivation,
    WorkflowTaskComplete { task_queue: String },
}

enum WorkerStatus {
    Live(Worker),
    Shutdown,
}

impl WorkerStatus {
    #[cfg(test)]
    pub fn unwrap(&self) -> &Worker {
        match self {
            WorkerStatus::Live(w) => w,
            WorkerStatus::Shutdown => panic!("Worker not present"),
        }
    }
}

#[async_trait::async_trait]
impl<WP> Core for CoreSDK<WP>
where
    WP: ServerGatewayApis + Send + Sync + 'static,
{
    async fn register_worker(&self, config: WorkerConfig) -> Result<(), WorkerRegistrationError> {
        if let Some(WorkerStatus::Live(_)) = self.workers.read().await.get(&config.task_queue) {
            return Err(WorkerRegistrationError::WorkerAlreadyRegisteredForQueue(
                config.task_queue,
            ));
        }
        let (tq, worker) = self.build_worker(config);
        self.workers
            .write()
            .await
            .insert(tq, WorkerStatus::Live(worker));
        Ok(())
    }

    #[instrument(skip(self))]
    async fn poll_workflow_task(&self, task_queue: &str) -> Result<WfActivation, PollWfError> {
        // The poll needs to be in a loop because we can't guarantee tail call optimization in Rust
        // (simply) and we really, really need that for long-poll retries.
        loop {
            // We must first check if there are pending workflow activations for workflows that are
            // currently replaying or otherwise need immediate jobs, and issue those before
            // bothering the server.
            if let Some(pa) = self.wft_manager.next_pending_activation(task_queue) {
                debug!(activation=%pa, "Sending pending activation to lang");
                return Ok(pa);
            }

            if self.shutdown_requested.load(Ordering::Relaxed) {
                return Err(PollWfError::ShutDown);
            }

            // Apply any buffered poll responses from the server. Must come after pending
            // activations, since there may be an eviction etc for whatever run is popped here.
            if let Some(buff_wft) = self.wft_manager.next_buffered_poll(task_queue) {
                match self.apply_server_work(buff_wft).await? {
                    Some(a) => return Ok(a),
                    None => continue,
                }
            }

            let activation_update_fut = self
                .workflow_activations_update
                .lock()
                .then(|mut l| async move { l.recv().await });
            let workers_rg = self.workers.read().await;
            let worker = workers_rg
                .get(task_queue)
                .ok_or_else(|| PollWfError::NoWorkerForQueue(task_queue.to_owned()))?;
            let worker = if let WorkerStatus::Live(w) = worker {
                w
            } else {
                return Err(PollWfError::ShutDown);
            };
            let poll_result_future = worker.workflow_poll().map_err(Into::into);
            let selected_f = tokio::select! {
                biased;

                // If a task is completed while we are waiting on polling, we need to restart the
                // loop right away to provide any potential new pending activation
                update = activation_update_fut => {
                    // If the update indicates a task completed, free a slot in the worker
                    if let Some(WfActivationUpdate::WorkflowTaskComplete {task_queue}) = update {
                        if let Some(WorkerStatus::Live(w)) = workers_rg.get(&task_queue) {
                            w.workflow_task_done();
                        }
                    }
                    continue;
                }
                r = poll_result_future => {r},
                shutdown = self.shutdown_or_continue_notifier() => {
                    if shutdown {
                        return Err(PollWfError::ShutDown);
                    } else {
                        continue;
                    }
                }
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

            let workers_rg = self.workers.read().await;
            let worker = workers_rg
                .get(task_queue)
                .ok_or_else(|| PollActivityError::NoWorkerForQueue(task_queue.to_owned()))?;
            let worker = if let WorkerStatus::Live(w) = worker {
                w
            } else {
                return Err(PollActivityError::ShutDown);
            };

            tokio::select! {
                biased;

                r = self.act_manager.poll(&worker) => {
                    match r.transpose() {
                        None => continue,
                        Some(r) => return r
                    }
                }
                shutdown = self.shutdown_or_continue_notifier() => {
                    if shutdown {
                        return Err(PollActivityError::ShutDown);
                    }
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
        let r = match wfstatus {
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
        };

        self.wft_manager.activation_done(&completion.run_id);
        r
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
                if let Some(WorkerStatus::Live(worker)) =
                    self.workers.read().await.get(&deets.task_queue)
                {
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
        self.poll_loop_notify.notify_waiters();
        for (_, w) in self.workers.write().await.drain() {
            if let WorkerStatus::Live(w) = w {
                w.shutdown_complete().await;
            }
        }
        self.wft_manager.shutdown();
        self.act_manager.shutdown().await;
    }

    async fn shutdown_worker(&self, task_queue: &str) {
        info!("Shutting down worker on queue {}", task_queue);
        if let Some(WorkerStatus::Live(w)) = self.workers.read().await.get(task_queue) {
            w.notify_shutdown();
        }
        let mut workers = match self.workers.try_write() {
            Ok(wg) => wg,
            Err(_) => {
                self.poll_loop_notify.notify_waiters();
                self.workers.write().await
            }
        };
        if let Some(WorkerStatus::Live(w)) = workers.remove(task_queue) {
            workers.insert(task_queue.to_owned(), WorkerStatus::Shutdown);
            drop(workers);
            w.shutdown_complete().await;
        }
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
            workers: RwLock::new(HashMap::new()),
            init_options,
            shutdown_requested: AtomicBool::new(false),
            poll_loop_notify: Notify::new(),
            workflow_activations_update: Mutex::new(wau_rx),
        }
    }

    /// Internal convenience function to avoid needing async when caller has mutable access to core
    #[cfg(test)]
    fn reg_worker_sync(&mut self, config: WorkerConfig) {
        let (tq, worker) = self.build_worker(config);
        self.workers
            .get_mut()
            .insert(tq, WorkerStatus::Live(worker));
    }

    /// From a worker config, return a (task queue, Worker) pair
    fn build_worker(&self, config: WorkerConfig) -> (String, Worker) {
        let tq = config.task_queue.clone();
        let use_sticky = if self.init_options.max_cached_workflows > 0 {
            Some(format!(
                "{}-{}-{}",
                &self.init_options.gateway_opts.identity, &tq, *PROCCESS_UNIQ_ID
            ))
        } else {
            None
        };
        (
            tq,
            Worker::new(config, use_sticky, self.server_gateway.clone()),
        )
    }

    /// Apply validated poll responses from the server. Returns an activation if one should be
    /// issued to lang, or returns `None` in which case the polling loop should be restarted.
    async fn apply_server_work(
        &self,
        work: ValidPollWFTQResponse,
    ) -> Result<Option<WfActivation>, CompleteWfError> {
        let we = work.workflow_execution.clone();
        match self.wft_manager.apply_new_poll_resp(work)? {
            NewWfTaskOutcome::IssueActivation(a) => {
                debug!(activation=%a, "Sending activation to lang");
                Ok(Some(a))
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
                reason:
                    "At least one workflow command in the completion contained an empty variant"
                        .to_owned(),
                completion: None,
            })?;

        match self.wft_manager.successful_activation(run_id, cmds)? {
            Some(ServerCommandsWithWorkflowInfo {
                task_token,
                task_queue,
                action:
                    ActivationAction::WftComplete {
                        commands,
                        query_responses,
                    },
            }) => {
                debug!("Sending commands to server: {:?}", &commands);
                let mut completion = WorkflowTaskCompletion {
                    task_token,
                    commands,
                    query_responses,
                    sticky_attributes: None,
                    return_new_workflow_task: false,
                    force_create_new_workflow_task: false,
                };
                match self.workers.read().await.get(&task_queue) {
                    Some(WorkerStatus::Live(worker)) => {
                        let sticky_attrs = worker.get_sticky_attrs();
                        completion.sticky_attributes = sticky_attrs;
                        // TODO: Do this in shutdown branch as well?
                        let res = self.server_gateway.complete_workflow_task(completion).await;
                        if let Err(ts) = res {
                            let should_evict = self.handle_wft_complete_errs(ts)?;
                            if should_evict {
                                self.wft_manager.evict_run(run_id);
                            }
                        }
                    }
                    Some(WorkerStatus::Shutdown) => {
                        // If the worker is shutdown we still want to be able to complete the WF, but
                        // sticky queue no longer makes sense, so we complete directly through gateway
                        // with no sticky info
                        self.server_gateway
                            .complete_workflow_task(completion)
                            .await?;
                    }
                    None => {
                        return Err(CompleteWfError::NoWorkerForQueue(task_queue));
                    }
                }
            }
            Some(ServerCommandsWithWorkflowInfo {
                task_token,
                action: ActivationAction::RespondLegacyQuery { result },
                ..
            }) => {
                self.server_gateway
                    .respond_legacy_query(task_token, result)
                    .await?;
            }
            None => {}
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

    /// A future that resolves to true the shutdown flag has been set to true, false is simply
    /// a signal that a poll loop should be restarted. Only meant to be called from polling funcs.
    async fn shutdown_or_continue_notifier(&self) -> bool {
        if self.shutdown_requested.load(Ordering::Relaxed) {
            return true;
        }
        self.poll_loop_notify.notified().await;
        self.shutdown_requested.load(Ordering::Relaxed)
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
