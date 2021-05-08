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
pub use pollers::{PollTaskRequest, ServerGateway, ServerGatewayApis, ServerGatewayOptions};
pub use protosext::IntoCompletion;
pub use url::Url;

use crate::{
    activity::{ActivityHeartbeatManager, ActivityHeartbeatManagerHandle, InflightActivityDetails},
    errors::ShutdownErr,
    machines::EmptyWorkflowCommandErr,
    pollers::{
        new_activity_task_buffer, new_workflow_task_buffer, PollActivityTaskBuffer,
        PollWorkflowTaskBuffer,
    },
    protos::{
        coresdk::{
            activity_result::{self as ar, activity_result},
            activity_task::ActivityTask,
            workflow_activation::WfActivation,
            workflow_completion::{self, wf_activation_completion, WfActivationCompletion},
            ActivityHeartbeat, ActivityTaskCompletion,
        },
        temporal::api::{
            enums::v1::WorkflowTaskFailedCause,
            workflowservice::v1::{PollActivityTaskQueueResponse, PollWorkflowTaskQueueResponse},
        },
    },
    protosext::ValidPollWFTQResponse,
    task_token::TaskToken,
    workflow_tasks::{NewWfTaskOutcome, WorkflowTaskManager},
};
use dashmap::DashMap;
use futures::TryFutureExt;
use std::{
    convert::TryInto,
    future::Future,
    ops::Div,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time,
};
use tokio::sync::Notify;

/// This trait is the primary way by which language specific SDKs interact with the core SDK. It is
/// expected that only one instance of an implementation will exist for the lifetime of the
/// worker(s) using it.
#[async_trait::async_trait]
pub trait Core: Send + Sync {
    /// Ask the core for some work, returning a [WfActivation]. It is then the language SDK's
    /// responsibility to call the appropriate workflow code with the provided inputs. Blocks
    /// indefinitely until such work is available or [Core::shutdown] is called.
    ///
    /// TODO: Examples
    async fn poll_workflow_task(&self) -> Result<WfActivation, PollWfError>;

    /// Ask the core for some work, returning an [ActivityTask]. It is then the language SDK's
    /// responsibility to call the appropriate activity code with the provided inputs. Blocks
    /// indefinitely until such work is available or [Core::shutdown] is called.
    ///
    /// TODO: Examples
    async fn poll_activity_task(&self) -> Result<ActivityTask, PollActivityError>;

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
#[derive(Debug, Clone)]
pub struct CoreInitOptions {
    /// Options for the connection to the temporal server
    pub gateway_opts: ServerGatewayOptions,
    /// If set to true (which should be the default choice until sticky task queues are implemented)
    /// workflows are evicted after they no longer have any pending activations. IE: After they
    /// have sent new commands to the server.
    pub evict_after_pending_cleared: bool,
    /// The maximum allowed number of workflow tasks that will ever be given to lang at one
    /// time. Note that one workflow task may require multiple activations - so the WFT counts as
    /// "outstanding" until all activations it requires have been completed.
    pub max_outstanding_workflow_tasks: usize,
    /// The maximum allowed number of activity tasks that will ever be given to lang at one time.
    pub max_outstanding_activities: usize,
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
    /// Workflow task management
    wft_manager: WorkflowTaskManager,

    /// Buffers workflow task polling in the event we need to return a pending activation while
    /// a poll is ongoing
    wf_task_poll_buffer: PollWorkflowTaskBuffer,
    /// Buffers workflow task polling in the event we need to return a pending activation while
    /// a poll is ongoing
    at_task_poll_buffer: PollActivityTaskBuffer,

    activity_heartbeat_manager_handle: ActivityHeartbeatManagerHandle,
    /// Activities that have been issued to lang but not yet completed
    outstanding_activity_tasks: DashMap<TaskToken, InflightActivityDetails>,
    /// Has shutdown been called?
    shutdown_requested: AtomicBool,
    /// Used to wake up future which checks shutdown state
    shutdown_notify: Notify,
    /// Used to wake blocked workflow task polling when tasks complete
    workflow_task_complete_notify: Arc<Notify>,
    /// Used to wake blocked activity task polling when tasks complete
    activity_task_complete_notify: Notify,
}

#[async_trait::async_trait]
impl<WP> Core for CoreSDK<WP>
where
    WP: ServerGatewayApis + Send + Sync + 'static,
{
    #[instrument(skip(self))]
    async fn poll_workflow_task(&self) -> Result<WfActivation, PollWfError> {
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

            if self.shutdown_requested.load(Ordering::SeqCst) {
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

            // Do not proceed to poll unless we are below the outstanding WFT limit
            if self.wft_manager.outstanding_wft()
                >= self.init_options.max_outstanding_workflow_tasks
            {
                self.workflow_task_complete_notify.notified().await;
                continue;
            }

            debug!("Polling server");

            let task_complete_fut = self.workflow_task_complete_notify.notified();
            let poll_result_future =
                self.shutdownable_fut(self.wf_task_poll_buffer.poll().map_err(Into::into));
            let selected_f = tokio::select! {
                biased;

                // If a task is completed while we are waiting on polling, we need to restart the
                // loop right away to provide any potential new pending activation
                _ = task_complete_fut => {
                    continue;
                }
                r = poll_result_future => r
            };

            match selected_f {
                Ok(work) => {
                    if work == PollWorkflowTaskQueueResponse::default() {
                        // We get the default proto in the event that the long poll times out.
                        debug!("Poll wft timeout");
                        continue;
                    }

                    let work: ValidPollWFTQResponse = work
                        .try_into()
                        .map_err(PollWfError::BadPollResponseFromServer)?;

                    match self.apply_server_work(work).await? {
                        Some(a) => return Ok(a),
                        None => continue,
                    }
                }
                // Drain pending activations in case of shutdown.
                Err(PollWfError::ShutDown) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    #[instrument(skip(self))]
    async fn poll_activity_task(&self) -> Result<ActivityTask, PollActivityError> {
        loop {
            if self.shutdown_requested.load(Ordering::Relaxed) {
                return Err(PollActivityError::ShutDown);
            }

            tokio::select! {
                biased;

                maybe_tt = self.activity_heartbeat_manager_handle.next_pending_cancel() => {
                    // Issue cancellations for anything we noticed was cancelled during heartbeating
                    if let Some(task_token) = maybe_tt {
                        // It's possible that activity has been completed and we no longer have an
                        // outstanding activity task. This is fine because it means that we no
                        // longer need to cancel this activity, so we'll just ignore such orphaned
                        // cancellations.
                        if let Some(mut details) =
                            self.outstanding_activity_tasks.get_mut(&task_token) {
                                if details.issued_cancel_to_lang {
                                    // Don't double-issue cancellations
                                    continue;
                                }
                                details.issued_cancel_to_lang = true;
                                return Ok(ActivityTask::cancel_from_ids(
                                    task_token,
                                    details.activity_id.clone(),
                                ));
                        } else {
                            warn!(task_token = ?task_token,
                                  "Unknown activity task when issuing cancel");
                            // If we can't find the activity here, it's already been completed,
                            // in which case issuing a cancel again is pointless.
                            continue;
                        }
                    }
                    // The only situation where the next cancel would return none is if the manager
                    // was dropped, which can only happen on shutdown.
                    return Err(PollActivityError::ShutDown);
                }
                _ = self.shutdown_notifier() => {
                    return Err(PollActivityError::ShutDown);
                }
                r = self.do_activity_poll() => {
                    match r.transpose() {
                        None => continue,
                        Some(r) => return r
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
        let task_token = TaskToken(completion.task_token);
        let wfstatus = completion.status;
        let run_id = self
            .wft_manager
            .run_id_for_task(&task_token)
            .ok_or_else(|| CompleteWfError::MalformedWorkflowCompletion {
                reason: format!(
                    "Task token {} had no workflow run associated with it",
                    &task_token
                ),
                completion: None,
            })?;

        match wfstatus {
            Some(wf_activation_completion::Status::Successful(success)) => {
                self.wf_activation_success(task_token.clone(), &run_id, success)
                    .await
            }
            Some(wf_activation_completion::Status::Failed(failure)) => {
                self.wf_activation_failed(task_token.clone(), &run_id, failure)
                    .await
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
            self.outstanding_activity_tasks.remove(&tt);
        }
        self.activity_task_complete_notify.notify_waiters();
        Ok(res?)
    }

    fn record_activity_heartbeat(&self, details: ActivityHeartbeat) {
        let tt = details.task_token.clone();
        if let Err(e) = self.record_activity_heartbeat_with_errors(details) {
            warn!(task_token = ?tt, details = ?e, "Activity heartbeat failed.")
        }
    }

    fn server_gateway(&self) -> Arc<dyn ServerGatewayApis> {
        self.server_gateway.clone()
    }

    async fn shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();
        self.wft_manager.shutdown();
        self.wf_task_poll_buffer.shutdown();
        self.at_task_poll_buffer.shutdown();
        self.activity_heartbeat_manager_handle.shutdown().await;
    }
}

impl<SG: ServerGatewayApis + Send + Sync + 'static> CoreSDK<SG> {
    pub(crate) fn new(server_gateway: SG, init_options: CoreInitOptions) -> Self {
        let sg = Arc::new(server_gateway);
        let workflow_task_complete_notify = Arc::new(Notify::new());
        Self {
            wft_manager: WorkflowTaskManager::new(
                workflow_task_complete_notify.clone(),
                init_options.evict_after_pending_cleared,
            ),
            init_options,
            server_gateway: sg.clone(),
            // TODO: Make poll options configurable, use builder for init options.
            wf_task_poll_buffer: new_workflow_task_buffer(sg.clone(), 10, 100),
            at_task_poll_buffer: new_activity_task_buffer(sg.clone(), 10, 100),
            outstanding_activity_tasks: Default::default(),
            shutdown_requested: AtomicBool::new(false),
            shutdown_notify: Notify::new(),
            workflow_task_complete_notify,
            activity_task_complete_notify: Notify::new(),
            activity_heartbeat_manager_handle: ActivityHeartbeatManager::new(sg),
        }
    }

    /// Wait until not at the outstanding activity limit, and then poll for new activities.
    ///
    /// Returns Ok(None) if the long poll timeout is hit
    async fn do_activity_poll(&self) -> Result<Option<ActivityTask>, PollActivityError> {
        while self.outstanding_activity_tasks.len() >= self.init_options.max_outstanding_activities
        {
            self.activity_task_complete_notify.notified().await
        }

        match self
            .shutdownable_fut(self.at_task_poll_buffer.poll().map_err(Into::into))
            .await
        {
            Ok(work) => {
                if work == PollActivityTaskQueueResponse::default() {
                    return Ok(None);
                }
                let task_token = TaskToken(work.task_token.clone());
                self.outstanding_activity_tasks.insert(
                    task_token.clone(),
                    InflightActivityDetails::new(
                        work.activity_id.clone(),
                        work.heartbeat_timeout.clone(),
                        false,
                    ),
                );
                Ok(Some(ActivityTask::start_from_poll_resp(work, task_token)))
            }
            Err(e) => Err(e),
        }
    }

    /// Apply validated WFTs from the server. Returns an activation if one should be issued to
    /// lang, or returns `None` in which case the polling loop should be restarted.
    async fn apply_server_work(
        &self,
        work: ValidPollWFTQResponse,
    ) -> Result<Option<WfActivation>, CompleteWfError> {
        let tt = work.task_token.clone();
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
                    task_token: tt.0,
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
        task_token: TaskToken,
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

        if let Some(server_cmds) =
            self.wft_manager
                .successful_activation(run_id, task_token.clone(), cmds)?
        {
            debug!("Sending commands to server: {:?}", &server_cmds.commands);
            let res = self
                .server_gateway
                .complete_workflow_task(task_token.clone(), server_cmds.commands)
                .await;
            if let Err(ts) = res {
                let should_evict = self.handle_wft_complete_errs(ts)?;
                if should_evict {
                    self.wft_manager.evict_run(&task_token)
                }
            }
        }
        Ok(())
    }

    /// Handle a failed workflow completion
    async fn wf_activation_failed(
        &self,
        task_token: TaskToken,
        run_id: &str,
        failure: workflow_completion::Failure,
    ) -> Result<(), CompleteWfError> {
        if self.wft_manager.failed_activation(run_id, &task_token) {
            // TODO: Handle errors
            self.server_gateway
                .fail_workflow_task(
                    task_token,
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
            r = wrap_this => r,
            _ = self.shutdown_notifier() => {
                Err(ShutdownErr.into())
            }
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

    fn record_activity_heartbeat_with_errors(
        &self,
        details: ActivityHeartbeat,
    ) -> Result<(), ActivityHeartbeatError> {
        let t: time::Duration = self
            .outstanding_activity_tasks
            .get(&TaskToken(details.task_token.clone()))
            .ok_or(ActivityHeartbeatError::UnknownActivity)?
            .heartbeat_timeout
            .clone()
            .ok_or(ActivityHeartbeatError::HeartbeatTimeoutNotSet)?
            .try_into()
            .or(Err(ActivityHeartbeatError::InvalidHeartbeatTimeout))?;
        // There is a bug in the server that translates non-set heartbeat timeouts into 0 duration.
        // That's why we treat 0 the same way as None, otherwise we wouldn't know which aggregation
        // delay to use, and using 0 is not a good idea as SDK would hammer the server too hard.
        if t.as_millis() == 0 {
            return Err(ActivityHeartbeatError::HeartbeatTimeoutNotSet);
        }
        self.activity_heartbeat_manager_handle
            .record(details, t.div(2))
    }
}
