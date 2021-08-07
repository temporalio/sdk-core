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
    ClientTlsConfig, ServerGateway, ServerGatewayApis, ServerGatewayOptions, TlsConfig,
};
pub use protosext::IntoCompletion;
pub use url::Url;
pub use worker::{WorkerConfig, WorkerConfigBuilder};

use crate::{
    errors::{
        ActivityHeartbeatError, CompleteActivityError, CompleteWfError, CoreInitError,
        PollActivityError, PollWfError, WorkerRegistrationError,
    },
    machines::EmptyWorkflowCommandErr,
    protos::{
        coresdk::{
            activity_task::ActivityTask,
            workflow_activation::WfActivation,
            workflow_commands::QueryResult,
            workflow_completion::{self, wf_activation_completion, WfActivationCompletion},
            ActivityHeartbeat, ActivityTaskCompletion,
        },
        temporal::api::enums::v1::WorkflowTaskFailedCause,
        temporal::api::failure::v1::Failure,
    },
    protosext::{ValidPollWFTQResponse, WorkflowTaskCompletion},
    task_token::TaskToken,
    worker::{WorkerDispatcher, WorkerStatus},
    workflow::{
        workflow_tasks::{
            ActivationAction, FailedActivationOutcome, NewWfTaskOutcome,
            ServerCommandsWithWorkflowInfo, WorkflowTaskManager,
        },
        WorkflowCachingPolicy,
    },
};
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

#[cfg(test)]
use crate::test_help::MockWorker;

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
    async fn poll_workflow_task(&self, task_queue: &str) -> Result<WfActivation, PollWfError>;

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
    async fn complete_workflow_task(
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
    /// Controls access to workers
    workers: Arc<WorkerDispatcher>,
    /// Workflow task management
    wft_manager: WorkflowTaskManager,
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

#[async_trait::async_trait]
impl<WP> Core for CoreSDK<WP>
where
    WP: ServerGatewayApis + Send + Sync + 'static,
{
    async fn register_worker(&self, config: WorkerConfig) -> Result<(), WorkerRegistrationError> {
        let sticky_q = self.get_sticky_q_name_for_worker(&config);
        self.workers
            .store_worker(config, sticky_q, self.server_gateway.clone())
            .await
    }

    #[instrument(skip(self))]
    async fn poll_workflow_task(&self, task_queue: &str) -> Result<WfActivation, PollWfError> {
        // The poll needs to be in a loop because we can't guarantee tail call optimization in Rust
        // (simply) and we really, really need that for long-poll retries.
        loop {
            // We must first check if there are pending workflow activations for workflows that are
            // currently replaying or otherwise need immediate jobs, and issue those before
            // bothering the server.
            if let Some(pa) = self.wft_manager.next_pending_activation(task_queue)? {
                debug!(activation=%pa, "Sending pending activation to lang");
                return Ok(pa);
            }

            // Apply any buffered poll responses from the server. Must come after pending
            // activations, since there may be an eviction etc for whatever run is popped here.
            if let Some(buff_wft) = self.wft_manager.next_buffered_poll(task_queue) {
                match self.apply_server_work(buff_wft).await? {
                    NewWfTaskOutcome::IssueActivation(a) => return Ok(a),
                    _ => continue,
                }
            }

            if self.shutdown_requested.load(Ordering::Relaxed) {
                return Err(PollWfError::ShutDown);
            }

            // Optimization avoiding worker access if we know we need to handle update immediately
            if let Some(update) = self
                .workflow_activations_update
                .lock()
                .await
                .recv()
                .now_or_never()
            {
                self.maybe_mark_task_done(update).await;
                continue;
            }

            let worker = self.workers.get(task_queue).await;
            let worker = worker
                .as_deref()
                .ok_or_else(|| PollWfError::NoWorkerForQueue(task_queue.to_owned()))?;

            let worker = if let WorkerStatus::Live(w) = worker {
                w
            } else {
                return Err(PollWfError::ShutDown);
            };

            let activation_update_fut = self
                .workflow_activations_update
                .lock()
                .then(|mut l| async move { l.recv().await });
            let poll_result_future = worker.workflow_poll().map_err(Into::into);
            let selected_f = tokio::select! {
                biased;

                // If a task is completed while we are waiting on polling, we need to restart the
                // loop right away to provide any potential new pending activation
                update = activation_update_fut => {
                    self.maybe_mark_task_done(update).await;
                    continue;
                }
                r = poll_result_future => r,
                shutdown = self.shutdown_or_continue_notifier() => {
                    if shutdown {
                        return Err(PollWfError::ShutDown);
                    } else {
                        continue;
                    }
                }
            };

            match selected_f {
                Ok(Some(work)) => match self.apply_server_work(work).await? {
                    NewWfTaskOutcome::IssueActivation(a) => return Ok(a),
                    NewWfTaskOutcome::TaskBuffered => {
                        // If the task was buffered, it's not actually outstanding, so we can
                        // immediately return a permit.
                        worker.return_workflow_task_permit();
                    }
                    _ => {}
                },
                Ok(None) => {
                    debug!("Poll wft timeout");
                }
                // Drain pending activations in case of shutdown.
                Err(PollWfError::ShutDown) => {}
                Err(e) => return Err(e),
            }

            // Make sure that polling looping doesn't hog up the whole scheduler. Realistically
            // this probably only happens when mock responses return at full speed.
            tokio::task::yield_now().await;
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
            let worker = self.workers.get(task_queue).await;
            let worker = worker
                .as_deref()
                .ok_or_else(|| PollActivityError::NoWorkerForQueue(task_queue.to_owned()))?;
            let worker = if let WorkerStatus::Live(w) = worker {
                w
            } else {
                return Err(PollActivityError::ShutDown);
            };

            tokio::select! {
                biased;

                r = worker.activity_poll() => {
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

        self.wft_manager.on_activation_done(&completion.run_id);
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

        let worker = self.workers.get(&completion.task_queue).await;
        let worker = if let Some(WorkerStatus::Live(w)) = worker.as_deref() {
            w
        } else {
            return Err(CompleteActivityError::NoWorkerForQueue(
                completion.task_queue,
            ));
        };

        worker
            .complete_activity(task_token, status, self.server_gateway.as_ref())
            .await
    }

    fn record_activity_heartbeat(&self, details: ActivityHeartbeat) {
        let workers = self.workers.clone();
        // TODO: Ugh. Can we avoid making this async in a better way?
        let _ = tokio::task::spawn(async move {
            if let Some(WorkerStatus::Live(w)) = workers.get(&details.task_queue).await.as_deref() {
                w.record_heartbeat(details)
            }
        });
    }

    fn request_workflow_eviction(&self, run_id: &str) {
        self.wft_manager.request_eviction(run_id);
    }

    fn server_gateway(&self) -> Arc<dyn ServerGatewayApis> {
        self.server_gateway.clone()
    }

    async fn shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        self.poll_loop_notify.notify_waiters();
        self.workers.shutdown_all().await;
    }

    async fn shutdown_worker(&self, task_queue: &str) {
        self.workers
            .shutdown_one(task_queue, &self.poll_loop_notify)
            .await;
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
        let workers = Arc::new(WorkerDispatcher::default());
        Self {
            server_gateway: sg,
            wft_manager: WorkflowTaskManager::new(wau_tx, cache_policy),
            workers,
            init_options,
            shutdown_requested: AtomicBool::new(false),
            poll_loop_notify: Notify::new(),
            workflow_activations_update: Mutex::new(wau_rx),
        }
    }

    /// Allow construction of workers with mocked poll responses during testing
    #[cfg(test)]
    pub(crate) fn reg_worker_sync(&mut self, worker: MockWorker) {
        use crate::worker::Worker;

        let sticky_q = self.get_sticky_q_name_for_worker(&worker.config);
        let tq = worker.config.task_queue.clone();
        let worker = Worker::new_with_pollers(
            worker.config,
            sticky_q,
            self.server_gateway.clone(),
            worker.wf_poller,
            worker.act_poller,
        );
        Arc::get_mut(&mut self.workers)
            .expect("No other worker dispatch yet")
            .store_worker_mut(tq, worker);
    }

    fn get_sticky_q_name_for_worker(&self, config: &WorkerConfig) -> Option<String> {
        if self.init_options.max_cached_workflows > 0 {
            Some(format!(
                "{}-{}-{}",
                &self.init_options.gateway_opts.identity, &config.task_queue, *PROCCESS_UNIQ_ID
            ))
        } else {
            None
        }
    }

    /// Apply validated poll responses from the server. Returns an activation if one should be
    /// issued to lang, or returns `None` in which case the polling loop should be restarted
    /// (ex: Got a new workflow task for a run but lang is already handling an activation for that
    /// same run)
    async fn apply_server_work(
        &self,
        work: ValidPollWFTQResponse,
    ) -> Result<NewWfTaskOutcome, PollWfError> {
        let we = work.workflow_execution.clone();
        let tt = work.task_token.clone();
        let res = self
            .wft_manager
            .apply_new_poll_resp(work, self.server_gateway.clone())
            .await?;
        match &res {
            NewWfTaskOutcome::IssueActivation(a) => {
                debug!(activation=%a, "Sending activation to lang");
            }
            NewWfTaskOutcome::TaskBuffered => {}
            NewWfTaskOutcome::Autocomplete => {
                debug!(workflow_execution=?we,
                       "No work for lang to perform after polling server. Sending autocomplete.");
                self.complete_workflow_task(WfActivationCompletion {
                    run_id: we.run_id,
                    status: Some(workflow_completion::Success::from_variants(vec![]).into()),
                })
                .await?;
            }
            NewWfTaskOutcome::CacheMiss => {
                debug!(workflow_execution=?we, "Unable to process workflow task with partial \
                history because workflow cache does not contain workflow anymore.");
                self.server_gateway
                    .fail_workflow_task(
                        tt,
                        WorkflowTaskFailedCause::ResetStickyTaskQueue,
                        Some(Failure {
                            message: "Unable to process workflow task with partial history \
                                      because workflow cache does not contain workflow anymore."
                                .to_string(),
                            ..Default::default()
                        }),
                    )
                    .await?;
            }
        };
        Ok(res)
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

        match self.wft_manager.successful_activation(run_id, cmds).await? {
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
                match self.workers.get(&task_queue).await.as_deref() {
                    Some(WorkerStatus::Live(worker)) => {
                        let sticky_attrs = worker.get_sticky_attrs();
                        completion.sticky_attributes = sticky_attrs;
                        self.handle_wft_complete_errs(run_id, || async {
                            self.server_gateway.complete_workflow_task(completion).await
                        })
                        .await?;
                    }
                    Some(WorkerStatus::Shutdown) => {
                        // If the worker is shutdown we still want to be able to complete the WF,
                        // but sticky queue no longer makes sense, so we complete directly through
                        // gateway with no sticky info
                        self.handle_wft_complete_errs(run_id, || async {
                            self.server_gateway.complete_workflow_task(completion).await
                        })
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

        self.wft_manager.after_wft_report(run_id)?;
        Ok(())
    }

    /// Handle a failed workflow completion
    async fn wf_activation_failed(
        &self,
        run_id: &str,
        failure: workflow_completion::Failure,
    ) -> Result<(), CompleteWfError> {
        match self.wft_manager.failed_activation(run_id)? {
            FailedActivationOutcome::Report(tt) => {
                self.handle_wft_complete_errs(run_id, || async {
                    self.server_gateway
                        .fail_workflow_task(
                            tt,
                            WorkflowTaskFailedCause::Unspecified,
                            failure.failure.map(Into::into),
                        )
                        .await
                })
                .await?;
            }
            FailedActivationOutcome::ReportLegacyQueryFailure(task_token) => {
                self.server_gateway
                    .respond_legacy_query(task_token, QueryResult::legacy_failure(failure))
                    .await?;
            }
            _ => {}
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

    /// Handle server errors from either completing or failing a workflow task. Returns any errors
    /// that can't be automatically handled.
    async fn handle_wft_complete_errs<T, Fut>(
        &self,
        run_id: &str,
        completer: impl FnOnce() -> Fut,
    ) -> Result<(), CompleteWfError>
    where
        Fut: Future<Output = Result<T, tonic::Status>>,
    {
        let mut should_evict = false;
        let res = match completer().await {
            Err(err) => {
                match err.code() {
                    // Silence unhandled command errors since the lang SDK cannot do anything about
                    // them besides poll again, which it will do anyway.
                    tonic::Code::InvalidArgument if err.message() == "UnhandledCommand" => {
                        warn!("Unhandled command response when completing: {}", err);
                        Ok(())
                    }
                    tonic::Code::NotFound => {
                        warn!("Task not found when completing: {}", err);
                        should_evict = true;
                        Ok(())
                    }
                    _ => Err(err),
                }
            }
            _ => Ok(()),
        };
        if should_evict {
            self.wft_manager.request_eviction(run_id);
        }
        res.map_err(Into::into)
    }

    async fn maybe_mark_task_done(&self, update: Option<WfActivationUpdate>) {
        // If the update indicates a task completed, free a slot in the worker
        if let Some(WfActivationUpdate::WorkflowTaskComplete { task_queue }) = update {
            if let Some(WorkerStatus::Live(w)) = self.workers.get(&task_queue).await.as_deref() {
                w.return_workflow_task_permit();
            }
        }
    }
}
