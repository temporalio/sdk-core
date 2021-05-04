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

mod activity;
pub(crate) mod core_tracing;
mod errors;
mod machines;
mod pending_activations;
mod pollers;
mod protosext;
pub(crate) mod task_token;
mod workflow;

#[cfg(test)]
mod test_help;

pub use crate::errors::{
    ActivityHeartbeatError, CompleteActivityError, CompleteWfError, CoreInitError,
    PollActivityError, PollWfError,
};
pub use core_tracing::tracing_init;
pub use pollers::{PollTaskRequest, ServerGateway, ServerGatewayApis, ServerGatewayOptions};
pub use url::Url;

use crate::protosext::ValidPollWFTQResponse;
use crate::{
    activity::{ActivityHeartbeatManager, ActivityHeartbeatManagerHandle, InflightActivityDetails},
    errors::{ShutdownErr, WorkflowUpdateError},
    machines::{EmptyWorkflowCommandErr, WFCommand},
    pending_activations::PendingActivations,
    pollers::{
        new_activity_task_buffer, new_workflow_task_buffer, PollActivityTaskBuffer,
        PollWorkflowTaskBuffer,
    },
    protos::{
        coresdk::{
            activity_result::{self as ar, activity_result},
            activity_task::ActivityTask,
            workflow_activation::{create_evict_activation, WfActivation},
            workflow_completion::{self, wf_activation_completion, WfActivationCompletion},
            ActivityHeartbeat, ActivityTaskCompletion,
        },
        temporal::api::{
            enums::v1::WorkflowTaskFailedCause,
            workflowservice::v1::{PollActivityTaskQueueResponse, PollWorkflowTaskQueueResponse},
        },
    },
    task_token::TaskToken,
    workflow::{NextWfActivation, WorkflowConcurrencyManager, WorkflowError, WorkflowManager},
};
use crossbeam::queue::SegQueue;
use dashmap::{DashMap, DashSet};
use futures::TryFutureExt;
use std::{
    convert::TryInto,
    fmt::Debug,
    future::Future,
    ops::Div,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time,
};
use tokio::sync::Notify;
use tracing::Span;

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

    /// Notify workflow that activity is still alive. Long running activities that take longer than
    /// `activity_heartbeat_timeout` to finish must call this function in order to report progress,
    /// otherwise activity will timeout and new attempt will be scheduled.
    /// `result` contains latest known activity cancelation status.
    /// First heartbeat request will be sent immediately, sub-sequent rapid calls to this
    /// function would result in heartbeat requests being aggregated and the last one received during
    /// the aggregation period will be sent to the server.
    /// Unlike java/go SDKs we are not going to return cancellation status as part of heartbeat response
    /// and instead will send it as a separate activity task to the lang, decoupling heartbeat and
    /// cancellation processing.
    /// For now activity still needs to heartbeat if it wants to receive cancellation requests.
    /// In the future we are going to change this and will dispatch cancellations more proactively.
    /// Note that this function is not blocking on the server call and will return Ok immediately,
    /// underlying validation errors are swallowed and logged, this has been agreed to be optimal
    /// behavior for the user as we don't want to break activity execution due to badly configured heartbeat options.
    async fn record_activity_heartbeat(&self, details: ActivityHeartbeat) -> Result<(), ()>;

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
    /// Key is run id
    workflow_machines: WorkflowConcurrencyManager,
    // TODO: move all workflow stuff inside a wft manager
    /// Maps task tokens to workflow run ids
    workflow_task_tokens: DashMap<TaskToken, String>,
    /// Workflows (by run id) for which the last task completion we sent was a failure
    workflows_last_task_failed: DashSet<String>,
    /// Used to track which workflows (by run id) have outstanding workflow tasks. Additionally,
    /// if the value is set, it indicates there is a buffered poll response from the server that
    /// applies to this run. This can happen when lang takes too long to complete a task and
    /// the task times out, for example. Upon next completion, the buffered response will be
    /// removed and pushed into [ready_buffered_wft].
    ///
    /// Only the most recent such reply from the server for a given run is kept.
    outstanding_workflow_tasks: DashMap<String, Option<ValidPollWFTQResponse>>,
    /// Holds poll wft responses from the server that need to be applied
    ready_buffered_wft: SegQueue<ValidPollWFTQResponse>,

    /// Buffers workflow task polling in the event we need to return a pending activation while
    /// a poll is ongoing
    wf_task_poll_buffer: PollWorkflowTaskBuffer,
    /// Buffers workflow task polling in the event we need to return a pending activation while
    /// a poll is ongoing
    at_task_poll_buffer: PollActivityTaskBuffer,

    /// Workflows may generate new activations immediately upon completion (ex: while replaying,
    /// or when cancelling an activity in try-cancel/abandon mode). They queue here.
    pending_activations: PendingActivations,

    activity_heartbeat_manager_handle: ActivityHeartbeatManagerHandle,
    /// Activities that have been issued to lang but not yet completed
    outstanding_activity_tasks: DashMap<TaskToken, InflightActivityDetails>,
    /// Has shutdown been called?
    shutdown_requested: AtomicBool,
    /// Used to wake up future which checks shutdown state
    shutdown_notify: Notify,
    /// Used to wake blocked workflow task polling when tasks complete
    workflow_task_complete_notify: Notify,
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
            if let Some(pa) = self.pending_activations.pop() {
                debug!(activation=%pa, "Sending pending activation to lang");
                return Ok(pa);
            }

            if self.shutdown_requested.load(Ordering::SeqCst) {
                return Err(PollWfError::ShutDown);
            }

            // Apply any buffered poll responses from the server. Must come after pending
            // activations, since there may be an eviction etc for whatever run is popped here.
            if let Some(buff_wft) = self.ready_buffered_wft.pop() {
                match self.apply_server_work(buff_wft).await? {
                    ActivtionOrContinue::Activation(a) => return Ok(a),
                    ActivtionOrContinue::Continue => continue,
                }
            }

            // Do not proceed to poll unless we are below the outstanding WFT limit
            if self.outstanding_workflow_tasks.len()
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

                    if let Some(mut outstanding_entry) = self
                        .outstanding_workflow_tasks
                        .get_mut(&work.workflow_execution.run_id)
                    {
                        debug!("Got new WFT for a run with one outstanding");
                        *outstanding_entry.value_mut() = Some(work);
                        continue;
                    }

                    debug!(history_length = %work.history.events.len(), "Got new work from server");
                    match self.apply_server_work(work).await? {
                        ActivtionOrContinue::Activation(a) => return Ok(a),
                        ActivtionOrContinue::Continue => continue,
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
            .workflow_task_tokens
            .get(&task_token)
            .map(|x| x.value().clone())
            .ok_or_else(|| CompleteWfError::MalformedWorkflowCompletion {
                reason: format!(
                    "Task token {} had no workflow run associated with it",
                    &task_token
                ),
                completion: None,
            })?;
        let res = match wfstatus {
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
        };

        // Workflows with no more pending activations (IE: They have completed a WFT) must be
        // removed from the outstanding tasks map
        if !self.pending_activations.has_pending(&run_id) {
            self.outstanding_workflow_tasks.remove(&run_id);

            // Blow them up if we're in non-sticky mode as well
            if self.init_options.evict_after_pending_cleared {
                self.evict_run(&task_token);
            }

            // The evict may or may not have already done this, but even when we aren't evicting
            // we want to remove from the run id mapping if we completed the wft.
            self.workflow_task_tokens.remove(&task_token);
        }
        self.workflow_task_complete_notify.notify_waiters();
        res
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

    async fn record_activity_heartbeat(&self, details: ActivityHeartbeat) -> Result<(), ()> {
        let tt = details.task_token.clone();
        if let Err(e) = self.record_activity_heartbeat_with_errors(details) {
            warn!(task_token = ?tt, details = ?e, "Activity heartbeat failed.")
        }
        Ok(())
    }

    fn server_gateway(&self) -> Arc<dyn ServerGatewayApis> {
        self.server_gateway.clone()
    }

    async fn shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();
        self.workflow_machines.shutdown();
        self.activity_heartbeat_manager_handle.shutdown().await;
    }
}

#[derive(Debug, derive_more::From)]
enum ActivtionOrContinue {
    Activation(WfActivation),
    Continue,
}

impl<WP: ServerGatewayApis + Send + Sync + 'static> CoreSDK<WP> {
    pub(crate) fn new(wp: WP, init_options: CoreInitOptions) -> Self {
        let sg = Arc::new(wp);
        Self {
            init_options,
            server_gateway: sg.clone(),
            workflow_machines: WorkflowConcurrencyManager::new(),
            workflow_task_tokens: Default::default(),
            workflows_last_task_failed: Default::default(),
            outstanding_workflow_tasks: Default::default(),
            ready_buffered_wft: Default::default(),
            wf_task_poll_buffer: new_workflow_task_buffer(sg.clone()),
            at_task_poll_buffer: new_activity_task_buffer(sg.clone()),
            pending_activations: Default::default(),
            outstanding_activity_tasks: Default::default(),
            shutdown_requested: AtomicBool::new(false),
            shutdown_notify: Notify::new(),
            workflow_task_complete_notify: Notify::new(),
            activity_task_complete_notify: Notify::new(),
            activity_heartbeat_manager_handle: ActivityHeartbeatManager::new(sg),
        }
    }

    /// Evict a workflow from the cache by its task token
    ///
    /// TODO: Very likely needs to be in Core public api
    pub(crate) fn evict_run(&self, task_token: &TaskToken) -> Option<ValidPollWFTQResponse> {
        if let Some((_, run_id)) = self.workflow_task_tokens.remove(task_token) {
            debug!(run_id=%run_id, "Evicting run");
            let maybe_buffered = self.outstanding_workflow_tasks.remove(&run_id);
            self.workflow_machines.evict(&run_id);
            self.pending_activations.remove_all_with_run_id(&run_id);
            // Queue up an eviction activation
            self.pending_activations
                .push(create_evict_activation(task_token.to_owned(), run_id));
            self.workflow_task_complete_notify.notify_waiters();
            return maybe_buffered.and_then(|mb| mb.1);
        }
        None
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

    /// Prepare an activation we've just pulled out of a workflow machines instance to be shipped
    /// to the lang sdk
    fn finalize_next_activation(
        &self,
        next_a: NextWfActivation,
        task_token: TaskToken,
    ) -> WfActivation {
        next_a.finalize(task_token)
    }

    /// Given an already validated workflow task from the server, apply it to the associated
    /// workflow machines and return an activation if appropriate, or possibly a signal that
    /// the poll loop should be restarted.
    async fn apply_server_work(
        &self,
        work: ValidPollWFTQResponse,
    ) -> Result<ActivtionOrContinue, CompleteWfError> {
        let tt = work.task_token.clone();
        if !work.next_page_token.is_empty() {
            // TODO: Support history pagination
            unimplemented!("History pagination not yet implemented");
        }
        let we = work.workflow_execution.clone();
        if let Some(activation) = self.prepare_new_activation(work)? {
            self.outstanding_workflow_tasks
                .insert(activation.run_id.clone(), None);
            debug!(activation=%activation, "Sending activation to lang");
            Ok(activation.into())
        } else {
            // This can be triggered when an activity is completed that we already
            // canceled, for example. There is no work for lang to do so we autocomplete
            // the task
            debug!(workflow_execution=?we,
                   "No work for lang to perform after polling server. Sending autocomplete.");
            self.complete_workflow_task(WfActivationCompletion {
                task_token: tt.0,
                status: Some(workflow_completion::Success::from_cmds(vec![]).into()),
            })
            .await?;
            Ok(ActivtionOrContinue::Continue)
        }
    }

    /// Given a wf task from the server, prepare an activation (if there is one) to be sent to lang
    fn prepare_new_activation(
        &self,
        work: ValidPollWFTQResponse,
    ) -> Result<Option<WfActivation>, WorkflowUpdateError> {
        debug!(
            task_token = %&work.task_token,
            "Received workflow task from server"
        );

        let task_token = work.task_token.clone();
        let next_activation = self.instantiate_or_update_workflow(work)?;

        if let Some(na) = next_activation {
            return Ok(Some(self.finalize_next_activation(na, task_token)));
        }
        Ok(None)
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
        self.push_lang_commands(run_id, cmds)?;
        self.enqueue_next_activation_if_needed(run_id, task_token.clone())?;
        // We want to fetch the outgoing commands only after any new activation has been queued,
        // as doing so may have altered the outgoing commands.
        let server_cmds = self.access_wf_machine(run_id, |w| Ok(w.get_server_commands()))?;
        // We only actually want to send commands back to the server if there are no more pending
        // activations and we are at the final workflow task (IE: the lang SDK has caught up on
        // replay)
        if !self.pending_activations.has_pending(run_id) && server_cmds.at_final_workflow_task {
            // Since we're telling the server about a wft success, we can remove it from the
            // last failed map (if it was present)
            self.workflows_last_task_failed.remove(run_id);
            debug!("Sending commands to server: {:?}", &server_cmds.commands);
            let res = self
                .server_gateway
                .complete_workflow_task(task_token.clone(), server_cmds.commands)
                .await;
            if let Err(ts) = res {
                let should_evict = self.handle_wft_complete_errs(ts)?;
                if should_evict {
                    if let Some(maybe_buffered_work) = self.evict_run(&task_token) {
                        self.ready_buffered_wft.push(maybe_buffered_work);
                    }
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
        // Blow up any cached data associated with the workflow
        self.evict_run(&task_token);

        // TODO: Handle errors
        if !self.workflows_last_task_failed.contains(run_id) {
            self.server_gateway
                .fail_workflow_task(
                    task_token,
                    WorkflowTaskFailedCause::Unspecified,
                    failure.failure.map(Into::into),
                )
                .await?;
            self.workflows_last_task_failed.insert(run_id.to_owned());
        }

        Ok(())
    }

    /// Will create a new workflow manager if needed for the workflow activation, if not, it will
    /// feed the existing manager the updated history we received from the server.
    ///
    /// Also updates [CoreSDK::workflow_task_tokens] and validates the
    /// [PollWorkflowTaskQueueResponse]
    ///
    /// Returns the next workflow activation and the workflow's run id
    fn instantiate_or_update_workflow(
        &self,
        poll_wf_resp: ValidPollWFTQResponse,
    ) -> Result<Option<NextWfActivation>, WorkflowUpdateError> {
        let run_id = poll_wf_resp.workflow_execution.run_id.clone();
        // Correlate task token w/ run ID
        self.workflow_task_tokens
            .insert(poll_wf_resp.task_token, run_id.clone());

        match self.workflow_machines.create_or_update(
            &run_id,
            poll_wf_resp.history,
            poll_wf_resp.workflow_execution,
        ) {
            Ok(activation) => Ok(activation),
            Err(source) => Err(WorkflowUpdateError { source, run_id }),
        }
    }

    /// Feed commands from the lang sdk into appropriate workflow manager which will iterate
    /// the state machines and return commands ready to be sent to the server
    fn push_lang_commands(
        &self,
        run_id: &str,
        cmds: Vec<WFCommand>,
    ) -> Result<(), WorkflowUpdateError> {
        self.access_wf_machine(run_id, move |mgr| mgr.push_commands(cmds))
    }

    /// Wraps access to `self.workflow_machines.access`, properly passing in the current tracing
    /// span to the wf machines thread.
    fn access_wf_machine<F, Fout>(
        &self,
        run_id: &str,
        mutator: F,
    ) -> Result<Fout, WorkflowUpdateError>
    where
        F: FnOnce(&mut WorkflowManager) -> Result<Fout, WorkflowError> + Send + 'static,
        Fout: Send + Debug + 'static,
    {
        let curspan = Span::current();
        let mutator = move |wfm: &mut WorkflowManager| {
            let _e = curspan.enter();
            mutator(wfm)
        };
        self.workflow_machines
            .access(run_id, mutator)
            .map_err(|source| WorkflowUpdateError {
                source,
                run_id: run_id.to_owned(),
            })
    }

    async fn shutdown_notifier(&self) {
        loop {
            self.shutdown_notify.notified().await;
            if self.shutdown_requested.load(Ordering::SeqCst) {
                break;
            }
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

    /// Check if the machine needs another activation and queue it up if there is one
    fn enqueue_next_activation_if_needed(
        &self,
        run_id: &str,
        task_token: TaskToken,
    ) -> Result<bool, CompleteWfError> {
        let mut new_activation = false;
        if let Some(next_activation) =
            self.access_wf_machine(run_id, move |mgr| mgr.get_next_activation())?
        {
            self.pending_activations
                .push(self.finalize_next_activation(next_activation, task_token));
            new_activation = true;
        }
        self.workflow_task_complete_notify.notify_waiters();
        Ok(new_activation)
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

#[cfg(test)]
mod test {
    use super::*;
    use crate::machines::test_help::mock_core;
    use crate::{
        machines::test_help::{
            build_fake_core, build_multihist_mock_sg, fake_core_from_mock_sg, fake_sg_opts,
            gen_assert_and_fail, gen_assert_and_reply, hist_to_poll_resp, poll_and_reply,
            EvictionMode, FakeCore, FakeWfResponses, TestHistoryBuilder,
        },
        pollers::{MockManualGateway, MockServerGatewayApis},
        protos::{
            coresdk::{
                activity_result::ActivityResult,
                activity_task::activity_task,
                common::UserCodeFailure,
                workflow_activation::{
                    wf_activation_job, FireTimer, ResolveActivity, StartWorkflow, UpdateRandomSeed,
                    WfActivationJob,
                },
                workflow_commands::{
                    ActivityCancellationType, CancelTimer, CompleteWorkflowExecution,
                    FailWorkflowExecution, RequestCancelActivity, ScheduleActivity, StartTimer,
                },
            },
            temporal::api::workflowservice::v1::{
                PollActivityTaskQueueResponse, RecordActivityTaskHeartbeatResponse,
                RespondActivityTaskCompletedResponse,
            },
            temporal::api::{
                enums::v1::EventType, workflowservice::v1::RespondWorkflowTaskCompletedResponse,
            },
        },
        test_help::canned_histories,
    };
    use futures::FutureExt;
    use rstest::{fixture, rstest};
    use std::{
        collections::{hash_map::Entry, HashMap, VecDeque},
        sync::atomic::{AtomicU64, AtomicUsize},
        time::Duration,
    };
    use tokio::time::sleep;

    #[fixture(hist_batches = &[])]
    fn single_timer_setup(hist_batches: &[usize]) -> FakeCore {
        let wfid = "fake_wf_id";

        let t = canned_histories::single_timer("fake_timer");
        build_fake_core(wfid, t, hist_batches)
    }

    #[fixture(hist_batches = &[])]
    fn single_activity_setup(hist_batches: &[usize]) -> FakeCore {
        let wfid = "fake_wf_id";

        let t = canned_histories::single_activity("fake_activity");
        build_fake_core(wfid, t, hist_batches)
    }

    #[fixture(hist_batches = &[])]
    fn single_activity_failure_setup(hist_batches: &[usize]) -> FakeCore {
        let wfid = "fake_wf_id";

        let t = canned_histories::single_failed_activity("fake_activity");
        build_fake_core(wfid, t, hist_batches)
    }

    #[rstest]
    #[case::incremental(single_timer_setup(&[1, 2]), EvictionMode::NotSticky)]
    #[case::replay(single_timer_setup(&[2]), EvictionMode::NotSticky)]
    #[case::incremental_evict(single_timer_setup(&[1, 2]), EvictionMode::AfterEveryReply)]
    #[case::replay_evict(single_timer_setup(&[2, 2]), EvictionMode::AfterEveryReply)]
    #[tokio::test]
    async fn single_timer_test_across_wf_bridge(
        #[case] core: FakeCore,
        #[case] evict: EvictionMode,
    ) {
        poll_and_reply(
            &core,
            evict,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    vec![StartTimer {
                        timer_id: "fake_timer".to_string(),
                        ..Default::default()
                    }
                    .into()],
                ),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::FireTimer(_)),
                    vec![CompleteWorkflowExecution { result: None }.into()],
                ),
            ],
        )
        .await;
    }

    #[rstest(core,
        case::incremental(single_activity_setup(&[1, 2])),
        case::incremental_activity_failure(single_activity_failure_setup(&[1, 2])),
        case::replay(single_activity_setup(&[2])),
        case::replay_activity_failure(single_activity_failure_setup(&[2]))
    )]
    #[tokio::test]
    async fn single_activity_completion(core: FakeCore) {
        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    vec![ScheduleActivity {
                        activity_id: "fake_activity".to_string(),
                        ..Default::default()
                    }
                    .into()],
                ),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::ResolveActivity(_)),
                    vec![CompleteWorkflowExecution { result: None }.into()],
                ),
            ],
        )
        .await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    #[tokio::test]
    async fn parallel_timer_test_across_wf_bridge(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let timer_1_id = "timer1";
        let timer_2_id = "timer2";

        let t = canned_histories::parallel_timer(timer_1_id, timer_2_id);
        let core = build_fake_core(wfid, t, hist_batches);

        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    vec![
                        StartTimer {
                            timer_id: timer_1_id.to_string(),
                            ..Default::default()
                        }
                        .into(),
                        StartTimer {
                            timer_id: timer_2_id.to_string(),
                            ..Default::default()
                        }
                        .into(),
                    ],
                ),
                gen_assert_and_reply(
                    &|res| {
                        assert_matches!(
                            res.jobs.as_slice(),
                            [
                                WfActivationJob {
                                    variant: Some(wf_activation_job::Variant::FireTimer(
                                        FireTimer { timer_id: t1_id }
                                    )),
                                },
                                WfActivationJob {
                                    variant: Some(wf_activation_job::Variant::FireTimer(
                                        FireTimer { timer_id: t2_id }
                                    )),
                                }
                            ] => {
                                assert_eq!(t1_id, &timer_1_id);
                                assert_eq!(t2_id, &timer_2_id);
                            }
                        );
                    },
                    vec![CompleteWorkflowExecution { result: None }.into()],
                ),
            ],
        )
        .await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    #[tokio::test]
    async fn timer_cancel_test_across_wf_bridge(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let timer_id = "wait_timer";
        let cancel_timer_id = "cancel_timer";

        let t = canned_histories::cancel_timer(timer_id, cancel_timer_id);
        let core = build_fake_core(wfid, t, hist_batches);

        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    vec![
                        StartTimer {
                            timer_id: cancel_timer_id.to_string(),
                            ..Default::default()
                        }
                        .into(),
                        StartTimer {
                            timer_id: timer_id.to_string(),
                            ..Default::default()
                        }
                        .into(),
                    ],
                ),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::FireTimer(_)),
                    vec![
                        CancelTimer {
                            timer_id: cancel_timer_id.to_string(),
                        }
                        .into(),
                        CompleteWorkflowExecution { result: None }.into(),
                    ],
                ),
            ],
        )
        .await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    #[tokio::test]
    async fn scheduled_activity_cancellation_try_cancel(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let activity_id = "fake_activity";
        let signal_id = "signal";

        let t = canned_histories::cancel_scheduled_activity(activity_id, signal_id);
        let core = build_fake_core(wfid, t, hist_batches);

        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    vec![ScheduleActivity {
                        activity_id: activity_id.to_string(),
                        cancellation_type: ActivityCancellationType::TryCancel as i32,
                        ..Default::default()
                    }
                    .into()],
                ),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::SignalWorkflow(_)),
                    vec![RequestCancelActivity {
                        activity_id: activity_id.to_string(),
                        ..Default::default()
                    }
                    .into()],
                ),
                // Activity is getting resolved right away as we are in the TryCancel mode.
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::ResolveActivity(_)),
                    vec![CompleteWorkflowExecution { result: None }.into()],
                ),
            ],
        )
        .await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    #[tokio::test]
    async fn scheduled_activity_timeout(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let activity_id = "fake_activity";

        let t = canned_histories::scheduled_activity_timeout(activity_id);
        let core = build_fake_core(wfid, t, hist_batches);
        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    vec![ScheduleActivity {
                        activity_id: activity_id.to_string(),
                        ..Default::default()
                    }
                    .into()],
                ),
                // Activity is getting resolved right away as it has been timed out.
                gen_assert_and_reply(
                    &|res| {
                        assert_matches!(
                                res.jobs.as_slice(),
                                [
                                    WfActivationJob {
                                        variant: Some(wf_activation_job::Variant::ResolveActivity(
                                            ResolveActivity {
                                                activity_id: aid,
                                                result: Some(ActivityResult {
                                                    status: Some(activity_result::Status::Failed(ar::Failure {
                                                        failure: Some(failure)
                                                    })),
                                                })
                                            }
                                        )),
                                    }
                                ] => {
                                    assert_eq!(failure.message, "Activity task timed out".to_string());
                                    assert_eq!(aid, &activity_id.to_string());
                                }
                            );
                    },
                    vec![CompleteWorkflowExecution { result: None }.into()],
                ),
            ],
        )
        .await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    #[tokio::test]
    async fn started_activity_timeout(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let activity_id = "fake_activity";

        let t = canned_histories::started_activity_timeout(activity_id);
        let core = build_fake_core(wfid, t, hist_batches);

        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    vec![ScheduleActivity {
                        activity_id: activity_id.to_string(),
                        ..Default::default()
                    }
                    .into()],
                ),
                // Activity is getting resolved right away as it has been timed out.
                gen_assert_and_reply(
                    &|res| {
                        assert_matches!(
                                res.jobs.as_slice(),
                                [
                                    WfActivationJob {
                                        variant: Some(wf_activation_job::Variant::ResolveActivity(
                                            ResolveActivity {
                                                activity_id: aid,
                                                result: Some(ActivityResult {
                                                    status: Some(activity_result::Status::Failed(ar::Failure {
                                                        failure: Some(failure)
                                                    })),
                                                })
                                            }
                                        )),
                                    }
                                ] => {
                                    assert_eq!(failure.message, "Activity task timed out".to_string());
                                    assert_eq!(aid, &activity_id.to_string());
                                }
                            );
                    },
                    vec![CompleteWorkflowExecution { result: None }.into()],
                ),
            ],
        )
        .await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 3]), case::replay(&[3]))]
    #[tokio::test]
    async fn cancelled_activity_timeout(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let activity_id = "fake_activity";
        let signal_id = "signal";

        let t = canned_histories::scheduled_cancelled_activity_timeout(activity_id, signal_id);
        let core = build_fake_core(wfid, t, hist_batches);

        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    vec![ScheduleActivity {
                        activity_id: activity_id.to_string(),
                        ..Default::default()
                    }
                    .into()],
                ),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::SignalWorkflow(_)),
                    vec![RequestCancelActivity {
                        activity_id: activity_id.to_string(),
                        ..Default::default()
                    }
                    .into()],
                ),
                // Activity is getting resolved right away as it has been timed out.
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::ResolveActivity(
                        ResolveActivity {
                            activity_id: _,
                            result: Some(ActivityResult {
                                status: Some(activity_result::Status::Canceled(..)),
                            })
                        }
                    )),
                    vec![CompleteWorkflowExecution { result: None }.into()],
                ),
            ],
        )
        .await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    #[tokio::test]
    async fn scheduled_activity_cancellation_abandon(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let activity_id = "fake_activity";
        let signal_id = "signal";

        let t = canned_histories::cancel_scheduled_activity_abandon(activity_id, signal_id);
        let core = build_fake_core(wfid, t, hist_batches);

        verify_activity_cancellation_abandon(&activity_id, &core).await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    #[tokio::test]
    async fn started_activity_cancellation_abandon(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let activity_id = "fake_activity";
        let signal_id = "signal";

        let t = canned_histories::cancel_started_activity_abandon(activity_id, signal_id);
        let core = build_fake_core(wfid, t, hist_batches);

        verify_activity_cancellation_abandon(&activity_id, &core).await;
    }

    async fn verify_activity_cancellation_abandon(activity_id: &&str, core: &FakeCore) {
        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    vec![ScheduleActivity {
                        activity_id: activity_id.to_string(),
                        cancellation_type: ActivityCancellationType::Abandon as i32,
                        ..Default::default()
                    }
                    .into()],
                ),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::SignalWorkflow(_)),
                    vec![RequestCancelActivity {
                        activity_id: activity_id.to_string(),
                        ..Default::default()
                    }
                    .into()],
                ),
                // Activity is getting resolved right away as we are in the Abandon mode.
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::ResolveActivity(
                        ResolveActivity {
                            activity_id: _,
                            result: Some(ActivityResult {
                                status: Some(activity_result::Status::Canceled(..)),
                            })
                        }
                    )),
                    vec![CompleteWorkflowExecution { result: None }.into()],
                ),
            ],
        )
        .await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 2, 3, 4]), case::replay(&[4]))]
    #[tokio::test]
    async fn scheduled_activity_cancellation_wait_for_cancellation(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let activity_id = "fake_activity";
        let signal_id = "signal";

        let t = canned_histories::cancel_scheduled_activity_with_signal_and_activity_task_cancel(
            activity_id,
            signal_id,
        );
        let core = build_fake_core(wfid, t, hist_batches);

        verify_activity_cancellation_wait_for_cancellation(activity_id, &core).await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 2, 3, 4]), case::replay(&[4]))]
    #[tokio::test]
    async fn started_activity_cancellation_wait_for_cancellation(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let activity_id = "fake_activity";
        let signal_id = "signal";

        let t = canned_histories::cancel_started_activity_with_signal_and_activity_task_cancel(
            activity_id,
            signal_id,
        );
        let core = build_fake_core(wfid, t, hist_batches);

        verify_activity_cancellation_wait_for_cancellation(activity_id, &core).await;
    }

    async fn verify_activity_cancellation_wait_for_cancellation(
        activity_id: &str,
        core: &FakeCore,
    ) {
        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    vec![ScheduleActivity {
                        activity_id: activity_id.to_string(),
                        cancellation_type: ActivityCancellationType::WaitCancellationCompleted
                            as i32,
                        ..Default::default()
                    }
                    .into()],
                ),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::SignalWorkflow(_)),
                    vec![RequestCancelActivity {
                        activity_id: activity_id.to_string(),
                        ..Default::default()
                    }
                    .into()],
                ),
                // Making sure that activity is not resolved until it's cancelled.
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::SignalWorkflow(_)),
                    vec![],
                ),
                // Now ActivityTaskCanceled has been processed and activity can be resolved.
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::ResolveActivity(
                        ResolveActivity {
                            activity_id: _,
                            result: Some(ActivityResult {
                                status: Some(activity_result::Status::Canceled(..)),
                            })
                        }
                    )),
                    vec![CompleteWorkflowExecution { result: None }.into()],
                ),
            ],
        )
        .await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 3]), case::replay(&[3]))]
    #[tokio::test]
    async fn scheduled_activity_cancellation_try_cancel_task_canceled(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let activity_id = "fake_activity";
        let signal_id = "signal";

        let t = canned_histories::cancel_scheduled_activity_with_activity_task_cancel(
            activity_id,
            signal_id,
        );
        let core = build_fake_core(wfid, t, hist_batches);

        verify_activity_cancellation_try_cancel_task_canceled(&activity_id, &core).await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 3]), case::replay(&[3]))]
    #[tokio::test]
    async fn started_activity_cancellation_try_cancel_task_canceled(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let activity_id = "fake_activity";
        let signal_id = "signal";

        let t = canned_histories::cancel_started_activity_with_activity_task_cancel(
            activity_id,
            signal_id,
        );
        let core = build_fake_core(wfid, t, hist_batches);

        verify_activity_cancellation_try_cancel_task_canceled(&activity_id, &core).await;
    }

    async fn verify_activity_cancellation_try_cancel_task_canceled(
        activity_id: &&str,
        core: &FakeCore,
    ) {
        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    vec![ScheduleActivity {
                        activity_id: activity_id.to_string(),
                        cancellation_type: ActivityCancellationType::TryCancel as i32,
                        ..Default::default()
                    }
                    .into()],
                ),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::SignalWorkflow(_)),
                    vec![RequestCancelActivity {
                        activity_id: activity_id.to_string(),
                        ..Default::default()
                    }
                    .into()],
                ),
                // Making sure that activity is not resolved until it's cancelled.
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::ResolveActivity(
                        ResolveActivity {
                            activity_id: _,
                            result: Some(ActivityResult {
                                status: Some(activity_result::Status::Canceled(..)),
                            })
                        }
                    )),
                    vec![CompleteWorkflowExecution { result: None }.into()],
                ),
            ],
        )
        .await;
    }

    #[rstest(single_timer_setup(&[1]))]
    #[tokio::test]
    async fn after_shutdown_server_is_not_polled(single_timer_setup: FakeCore) {
        let res = single_timer_setup.inner.poll_workflow_task().await.unwrap();
        assert_eq!(res.jobs.len(), 1);

        single_timer_setup.inner.shutdown().await;
        assert_matches!(
            single_timer_setup
                .inner
                .poll_workflow_task()
                .await
                .unwrap_err(),
            PollWfError::ShutDown
        );
    }

    #[tokio::test]
    async fn workflow_update_random_seed_on_workflow_reset() {
        let wfid = "fake_wf_id";
        let new_run_id = "86E39A5F-AE31-4626-BDFE-398EE072D156";
        let timer_1_id = "timer1";
        let randomness_seed_from_start = AtomicU64::new(0);

        let t = canned_histories::workflow_fails_with_reset_after_timer(timer_1_id, new_run_id);
        let core = build_fake_core(wfid, t, &[2]);

        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &|res| {
                        assert_matches!(
                            res.jobs.as_slice(),
                            [WfActivationJob {
                                variant: Some(wf_activation_job::Variant::StartWorkflow(
                                StartWorkflow{randomness_seed, ..}
                                )),
                            }] => {
                            randomness_seed_from_start.store(*randomness_seed, Ordering::SeqCst);
                            }
                        );
                    },
                    vec![StartTimer {
                        timer_id: timer_1_id.to_string(),
                        ..Default::default()
                    }
                    .into()],
                ),
                gen_assert_and_reply(
                    &|res| {
                        assert_matches!(
                            res.jobs.as_slice(),
                            [WfActivationJob {
                                variant: Some(wf_activation_job::Variant::FireTimer(_),),
                            },
                            WfActivationJob {
                                variant: Some(wf_activation_job::Variant::UpdateRandomSeed(
                                    UpdateRandomSeed{randomness_seed})),
                            }] => {
                                assert_ne!(randomness_seed_from_start.load(Ordering::SeqCst),
                                          *randomness_seed)
                            }
                        )
                    },
                    vec![CompleteWorkflowExecution { result: None }.into()],
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn cancel_timer_before_sent_wf_bridge() {
        let wfid = "fake_wf_id";
        let cancel_timer_id = "cancel_timer";

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let core = build_fake_core(wfid, t, &[1]);

        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![
                    StartTimer {
                        timer_id: cancel_timer_id.to_string(),
                        ..Default::default()
                    }
                    .into(),
                    CancelTimer {
                        timer_id: cancel_timer_id.to_string(),
                    }
                    .into(),
                    CompleteWorkflowExecution { result: None }.into(),
                ],
            )],
        )
        .await;
    }

    #[rstest]
    #[case::no_evict_inc(&[1, 2, 2], EvictionMode::NotSticky)]
    #[case::no_evict(&[2, 2], EvictionMode::NotSticky)]
    #[case::evict(&[1, 2, 2, 2], EvictionMode::AfterEveryReply)]
    #[tokio::test]
    async fn complete_activation_with_failure(
        #[case] batches: &[usize],
        #[case] evict: EvictionMode,
    ) {
        let wfid = "fake_wf_id";
        let timer_id = "timer";

        let hist = canned_histories::workflow_fails_with_failure_after_timer(timer_id);
        let mock_sg = build_multihist_mock_sg(
            vec![FakeWfResponses {
                wf_id: wfid.to_string(),
                hist,
                response_batches: batches.to_vec(),
            }],
            true,
            Some(1),
        );
        let core = fake_core_from_mock_sg(mock_sg);

        poll_and_reply(
            &core,
            evict,
            &[
                gen_assert_and_reply(
                    &|_| {},
                    vec![StartTimer {
                        timer_id: timer_id.to_owned(),
                        ..Default::default()
                    }
                    .into()],
                ),
                gen_assert_and_fail(&|_| {}),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::FireTimer(_)),
                    vec![CompleteWorkflowExecution { result: None }.into()],
                ),
            ],
        )
        .await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    #[tokio::test]
    async fn simple_timer_fail_wf_execution(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let timer_id = "timer1";

        let t = canned_histories::single_timer(timer_id);
        let core = build_fake_core(wfid, t, hist_batches);

        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    vec![StartTimer {
                        timer_id: timer_id.to_string(),
                        ..Default::default()
                    }
                    .into()],
                ),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::FireTimer(_)),
                    vec![FailWorkflowExecution {
                        failure: Some(UserCodeFailure {
                            message: "I'm ded".to_string(),
                            ..Default::default()
                        }),
                    }
                    .into()],
                ),
            ],
        )
        .await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    #[tokio::test]
    async fn two_signals(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";

        let t = canned_histories::two_signals("sig1", "sig2");
        let core = build_fake_core(wfid, t, hist_batches);

        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    // Task is completed with no commands
                    vec![],
                ),
                gen_assert_and_reply(
                    &job_assert!(
                        wf_activation_job::Variant::SignalWorkflow(_),
                        wf_activation_job::Variant::SignalWorkflow(_)
                    ),
                    vec![],
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn workflow_failures_only_reported_once() {
        let wfid = "fake_wf_id";
        let timer_1 = "timer1";
        let timer_2 = "timer2";

        let hist =
            canned_histories::workflow_fails_with_failure_two_different_points(timer_1, timer_2);
        let response_batches = vec![
            1, 2, // Start then first good reply
            2, 2, 2, // Poll for every failure
            // Poll again after evicting after second good reply, then two more fails
            3, 3, 3,
        ];
        let mock_sg = build_multihist_mock_sg(
            vec![FakeWfResponses {
                wf_id: wfid.to_string(),
                hist,
                response_batches,
            }],
            true,
            // We should only call the server to say we failed twice (once after each success)
            Some(2),
        );
        let core = fake_core_from_mock_sg(mock_sg);

        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &|_| {},
                    vec![StartTimer {
                        timer_id: timer_1.to_owned(),
                        ..Default::default()
                    }
                    .into()],
                ),
                // Fail a few times in a row (only one of which should be reported)
                gen_assert_and_fail(&|_| {}),
                gen_assert_and_fail(&|_| {}),
                gen_assert_and_fail(&|_| {}),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::FireTimer(_)),
                    vec![StartTimer {
                        timer_id: timer_2.to_string(),
                        ..Default::default()
                    }
                    .into()],
                ),
                // Again (a new fail should be reported here)
                gen_assert_and_fail(&|_| {}),
                gen_assert_and_fail(&|_| {}),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::FireTimer(_)),
                    vec![CompleteWorkflowExecution { result: None }.into()],
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn max_concurrent_wft_respected() {
        // Create long histories for three workflows
        let t1 = canned_histories::long_sequential_timers(20);
        let t2 = canned_histories::long_sequential_timers(20);
        let mut tasks = VecDeque::from(vec![
            hist_to_poll_resp(&t1, "wf1".to_owned(), 100),
            hist_to_poll_resp(&t2, "wf2".to_owned(), 100),
        ]);
        // Limit the core to two outstanding workflow tasks, hence we should only see polling
        // happen twice, since we will not actually finish the two workflows
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_poll_workflow_task()
            .times(2)
            .returning(move || Ok(tasks.pop_front().unwrap()));
        // Response not really important here
        mock_gateway
            .expect_complete_workflow_task()
            .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));

        let core = CoreSDK::new(
            mock_gateway,
            CoreInitOptions {
                gateway_opts: fake_sg_opts(),
                evict_after_pending_cleared: true,
                max_outstanding_workflow_tasks: 2,
                max_outstanding_activities: 1,
            },
        );

        // Poll twice in a row before completing -- we should be at limit
        let r1 = core.poll_workflow_task().await.unwrap();
        let _r2 = core.poll_workflow_task().await.unwrap();
        // Now we immediately poll for new work, and complete one of the existing activations. The
        // poll must not unblock until the completion goes through.
        let last_finisher = AtomicUsize::new(0);
        let (_, mut r1) = tokio::join! {
            async {
                core.complete_workflow_task(WfActivationCompletion::from_status(
                    r1.task_token,
                    workflow_completion::Success::from_cmds(vec![StartTimer {
                        timer_id: "timer-1".to_string(),
                        ..Default::default()
                    }
                    .into()]).into()
                )).await.unwrap();
                last_finisher.store(1, Ordering::SeqCst);
            },
            async {
                let r = core.poll_workflow_task().await.unwrap();
                last_finisher.store(2, Ordering::SeqCst);
                r
            }
        };
        // So that we know we blocked
        assert_eq!(last_finisher.load(Ordering::Acquire), 2);

        // Since we never did anything with r2, all subsequent activations should be for wf1
        for i in 2..19 {
            core.complete_workflow_task(WfActivationCompletion::from_status(
                r1.task_token,
                workflow_completion::Success::from_cmds(vec![StartTimer {
                    timer_id: format!("timer-{}", i),
                    ..Default::default()
                }
                .into()])
                .into(),
            ))
            .await
            .unwrap();
            r1 = core.poll_workflow_task().await.unwrap();
        }
    }

    #[tokio::test]
    async fn max_activites_respected() {
        let mut tasks = VecDeque::from(vec![
            PollActivityTaskQueueResponse {
                task_token: vec![1],
                activity_id: "act1".to_string(),
                ..Default::default()
            },
            PollActivityTaskQueueResponse {
                task_token: vec![2],
                activity_id: "act2".to_string(),
                ..Default::default()
            },
            PollActivityTaskQueueResponse {
                task_token: vec![3],
                activity_id: "act3".to_string(),
                ..Default::default()
            },
        ]);
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_poll_activity_task()
            .times(3)
            .returning(move || Ok(tasks.pop_front().unwrap()));
        mock_gateway
            .expect_complete_activity_task()
            .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));

        let core = CoreSDK::new(
            mock_gateway,
            CoreInitOptions {
                gateway_opts: fake_sg_opts(),
                evict_after_pending_cleared: true,
                max_outstanding_workflow_tasks: 1,
                max_outstanding_activities: 2,
            },
        );

        // We allow two outstanding activities, therefore first two polls should return right away
        let r1 = core.poll_activity_task().await.unwrap();
        let _r2 = core.poll_activity_task().await.unwrap();
        // Third should block until we complete one of the first two
        let last_finisher = AtomicUsize::new(0);
        tokio::join! {
            async {
                core.complete_activity_task(ActivityTaskCompletion {
                    task_token: r1.task_token,
                    result: Some(ActivityResult::ok(vec![1].into()))
                }).await.unwrap();
                last_finisher.store(1, Ordering::SeqCst);
            },
            async {
                core.poll_activity_task().await.unwrap();
                last_finisher.store(2, Ordering::SeqCst);
            }
        };
        // So that we know we blocked
        assert_eq!(last_finisher.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn heartbeats_report_cancels() {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_poll_activity_task()
            .times(1)
            .returning(|| {
                Ok(PollActivityTaskQueueResponse {
                    task_token: vec![1],
                    activity_id: "act1".to_string(),
                    heartbeat_timeout: Some(Duration::from_secs(1).into()),
                    ..Default::default()
                })
            });
        mock_gateway
            .expect_record_activity_heartbeat()
            .times(1)
            .returning(|_, _| {
                Ok(RecordActivityTaskHeartbeatResponse {
                    cancel_requested: true,
                })
            });

        let core = CoreSDK::new(
            mock_gateway,
            CoreInitOptions {
                gateway_opts: fake_sg_opts(),
                evict_after_pending_cleared: true,
                max_outstanding_workflow_tasks: 5,
                max_outstanding_activities: 5,
            },
        );

        let act = core.poll_activity_task().await.unwrap();
        core.record_activity_heartbeat(ActivityHeartbeat {
            task_token: act.task_token,
            details: vec![vec![1u8, 2, 3].into()],
        })
        .await
        .unwrap();
        // We have to wait a beat for the heartbeat to be processed
        sleep(Duration::from_millis(50)).await;
        let act = core.poll_activity_task().await.unwrap();
        assert_matches!(
            act,
            ActivityTask {
                task_token,
                variant: Some(activity_task::Variant::Cancel(_)),
                ..
            } => { task_token == vec![1] }
        );
    }

    #[tokio::test]
    async fn activity_not_found_returns_ok() {
        let mut mock_gateway = MockServerGatewayApis::new();
        mock_gateway
            .expect_complete_activity_task()
            .times(1)
            .returning(|_, _| Err(tonic::Status::not_found("unimportant")));

        let core = CoreSDK::new(
            mock_gateway,
            CoreInitOptions {
                gateway_opts: fake_sg_opts(),
                evict_after_pending_cleared: true,
                max_outstanding_workflow_tasks: 5,
                max_outstanding_activities: 5,
            },
        );

        core.complete_activity_task(ActivityTaskCompletion {
            task_token: vec![1],
            result: Some(ActivityResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn activity_cancel_interrupts_poll() {
        let mut mock_gateway = MockManualGateway::new();
        let mut poll_resps = VecDeque::from(vec![
            async {
                Ok(PollActivityTaskQueueResponse {
                    task_token: vec![1],
                    heartbeat_timeout: Some(Duration::from_secs(1).into()),
                    ..Default::default()
                })
            }
            .boxed(),
            async {
                tokio::time::sleep(Duration::from_millis(500)).await;
                Ok(Default::default())
            }
            .boxed(),
        ]);
        mock_gateway
            .expect_poll_activity_task()
            .times(2)
            .returning(move || poll_resps.pop_front().unwrap());
        mock_gateway
            .expect_record_activity_heartbeat()
            .times(1)
            .returning(|_, _| {
                async {
                    Ok(RecordActivityTaskHeartbeatResponse {
                        cancel_requested: true,
                    })
                }
                .boxed()
            });

        let core = CoreSDK::new(
            mock_gateway,
            CoreInitOptions {
                gateway_opts: fake_sg_opts(),
                evict_after_pending_cleared: true,
                max_outstanding_workflow_tasks: 5,
                max_outstanding_activities: 5,
            },
        );
        let last_finisher = AtomicUsize::new(0);
        // Perform first poll to get the activity registered
        let act = core.poll_activity_task().await.unwrap();
        // Poll should block until heartbeat is sent, issuing the cancel, and interrupting the poll
        tokio::join! {
            async {
                core.record_activity_heartbeat(ActivityHeartbeat {
                    task_token: act.task_token,
                    details: vec![vec![1u8, 2, 3].into()],
                })
                .await
                .unwrap();
                last_finisher.store(1, Ordering::SeqCst);
            },
            async {
                core.poll_activity_task().await.unwrap();
                last_finisher.store(2, Ordering::SeqCst);
            }
        };
        // So that we know we blocked
        assert_eq!(last_finisher.load(Ordering::Acquire), 2);
    }

    #[tokio::test]
    async fn shutdown_interrupts_both_polls() {
        let mut mock_gateway = MockManualGateway::new();
        mock_gateway
            .expect_poll_activity_task()
            .times(1)
            .returning(move || {
                async move {
                    sleep(Duration::from_secs(1)).await;
                    Ok(PollActivityTaskQueueResponse {
                        task_token: vec![1],
                        heartbeat_timeout: Some(Duration::from_secs(1).into()),
                        ..Default::default()
                    })
                }
                .boxed()
            });
        mock_gateway
            .expect_poll_workflow_task()
            .times(1)
            .returning(move || {
                async move {
                    let t = canned_histories::single_timer("hi");
                    sleep(Duration::from_secs(1)).await;
                    Ok(hist_to_poll_resp(&t, "wf".to_string(), 100))
                }
                .boxed()
            });

        let core = CoreSDK::new(
            mock_gateway,
            CoreInitOptions {
                gateway_opts: fake_sg_opts(),
                evict_after_pending_cleared: true,
                max_outstanding_workflow_tasks: 5,
                max_outstanding_activities: 5,
            },
        );
        tokio::join! {
            async {
                assert_matches!(core.poll_activity_task().await.unwrap_err(),
                                PollActivityError::ShutDown);
            },
            async {
                assert_matches!(core.poll_workflow_task().await.unwrap_err(),
                                PollWfError::ShutDown);
            },
            async {
                // Give polling a bit to get stuck, then shutdown
                sleep(Duration::from_millis(200)).await;
                core.shutdown().await;
            }
        };
    }

    #[tokio::test]
    async fn activity_poll_timeout_retries() {
        let mut mock_gateway = MockServerGatewayApis::new();
        let mut calls = 0;
        mock_gateway
            .expect_poll_activity_task()
            .times(3)
            .returning(move || {
                calls += 1;
                if calls <= 2 {
                    Ok(PollActivityTaskQueueResponse::default())
                } else {
                    Err(tonic::Status::unknown("Test done"))
                }
            });
        let core = mock_core(mock_gateway);
        let r = core.poll_activity_task().await;
        assert_matches!(r.unwrap_err(), PollActivityError::TonicError(_));
    }

    // TODO: Test doesn't fail when mock not called enough times (even though it panics??), but it
    //   actually only should be called two times (with 1,2,3)
    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[3]))]
    #[tokio::test]
    async fn activity_not_canceled_on_replay_repro(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let t = canned_histories::unsent_at_cancel_repro();
        let core = build_fake_core(wfid, t, hist_batches);
        let activity_id = "act-1";

        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    // Start timer and activity
                    vec![
                        ScheduleActivity {
                            activity_id: activity_id.to_string(),
                            cancellation_type: ActivityCancellationType::TryCancel as i32,
                            ..Default::default()
                        }
                        .into(),
                        StartTimer {
                            timer_id: "timer-1".to_owned(),
                            ..Default::default()
                        }
                        .into(),
                    ],
                ),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::FireTimer(_)),
                    vec![RequestCancelActivity {
                        activity_id: activity_id.to_string(),
                        ..Default::default()
                    }
                    .into()],
                ),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::ResolveActivity(
                        ResolveActivity {
                            result: Some(ActivityResult {
                                status: Some(activity_result::Status::Canceled(..)),
                            }),
                            ..
                        }
                    )),
                    vec![StartTimer {
                        timer_id: "timer-2".to_owned(),
                        ..Default::default()
                    }
                    .into()],
                ),
            ],
        )
        .await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[3]))]
    #[tokio::test]
    async fn activity_not_canceled_when_also_completed_repro(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let t = canned_histories::cancel_not_sent_when_also_complete_repro();
        let core = build_fake_core(wfid, t, hist_batches);
        let activity_id = "act-1";

        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    // Start timer and activity
                    vec![ScheduleActivity {
                        activity_id: activity_id.to_string(),
                        cancellation_type: ActivityCancellationType::TryCancel as i32,
                        ..Default::default()
                    }
                    .into()],
                ),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::SignalWorkflow(_)),
                    vec![
                        RequestCancelActivity {
                            activity_id: activity_id.to_string(),
                            ..Default::default()
                        }
                        .into(),
                        StartTimer {
                            timer_id: "timer-1".to_owned(),
                            ..Default::default()
                        }
                        .into(),
                    ],
                ),
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::ResolveActivity(
                        ResolveActivity {
                            result: Some(ActivityResult {
                                status: Some(activity_result::Status::Canceled(..)),
                            }),
                            ..
                        }
                    )),
                    vec![CompleteWorkflowExecution { result: None }.into()],
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn many_concurrent_heartbeat_cancels() {
        // Run a whole bunch of activities in parallel, having the server return cancellations for
        // them after a few successful heartbeats
        const CONCURRENCY_NUM: u32 = 1000;

        let mut mock_gateway = MockManualGateway::new();
        let mut poll_resps = VecDeque::from(
            (0..CONCURRENCY_NUM)
                .map(|i| {
                    async move {
                        Ok(PollActivityTaskQueueResponse {
                            task_token: i.to_be_bytes().to_vec(),
                            heartbeat_timeout: Some(Duration::from_millis(500).into()),
                            ..Default::default()
                        })
                    }
                    .boxed()
                })
                .collect::<Vec<_>>(),
        );
        // Because the mock is so fast, it's possible it can return before the cancel channel in
        // the activity task poll selector. So, the final poll when there are no more tasks must
        // take a while.
        poll_resps.push_back(
            async {
                sleep(Duration::from_secs(10)).await;
                unreachable!("Long poll")
            }
            .boxed(),
        );
        let mut calls_map = HashMap::<_, i32>::new();
        mock_gateway
            .expect_poll_activity_task()
            .returning(move || poll_resps.pop_front().unwrap());
        mock_gateway
            .expect_cancel_activity_task()
            .returning(move |_, _| async move { Ok(Default::default()) }.boxed());
        mock_gateway
            .expect_record_activity_heartbeat()
            .returning(move |tt, _| {
                let calls = match calls_map.entry(tt) {
                    Entry::Occupied(mut e) => {
                        *e.get_mut() += 1;
                        *e.get()
                    }
                    Entry::Vacant(v) => *v.insert(1),
                };
                async move {
                    if calls < 5 {
                        Ok(RecordActivityTaskHeartbeatResponse {
                            cancel_requested: false,
                        })
                    } else {
                        Ok(RecordActivityTaskHeartbeatResponse {
                            cancel_requested: true,
                        })
                    }
                }
                .boxed()
            });

        let core = CoreSDK::new(
            mock_gateway,
            CoreInitOptions {
                gateway_opts: fake_sg_opts(),
                evict_after_pending_cleared: true,
                max_outstanding_workflow_tasks: 5,
                max_outstanding_activities: CONCURRENCY_NUM as usize,
            },
        );
        let core = Arc::new(core);

        // Poll all activities first so they are registered
        for _ in 0..CONCURRENCY_NUM {
            core.poll_activity_task().await.unwrap();
        }

        // Spawn "activities"
        let mut handles = vec![];
        for i in 0..CONCURRENCY_NUM {
            let core = core.clone();
            let jh = tokio::spawn(async move {
                for _ in 0..10 {
                    core.record_activity_heartbeat(ActivityHeartbeat {
                        task_token: i.to_be_bytes().to_vec(),
                        details: vec![],
                    })
                    .await
                    .unwrap();
                    sleep(Duration::from_millis(100)).await;
                }
            });
            handles.push(jh);
        }

        // Wait for all the heartbeating to finish, which should result in a bunch of pending
        // cancellations
        for h in handles.drain(..) {
            h.await.unwrap()
        }
        let mut handles = vec![];

        // Read all the cancellations and reply to them concurrently
        for _ in 0..CONCURRENCY_NUM {
            let core = core.clone();
            let jh = tokio::spawn(async move {
                let r = core.poll_activity_task().await.unwrap();
                assert_matches!(
                    r,
                    ActivityTask {
                        variant: Some(activity_task::Variant::Cancel(_)),
                        ..
                    }
                );
                core.complete_activity_task(ActivityTaskCompletion {
                    task_token: r.task_token.clone(),
                    result: Some(ActivityResult::cancel_from_details(None)),
                })
                .await
                .unwrap();
            });
            handles.push(jh);
        }

        for h in handles.drain(..) {
            h.await.unwrap();
        }
        assert!(core.outstanding_activity_tasks.is_empty());
    }

    #[tokio::test]
    async fn lots_of_workflows() {
        let hists = (0..500).into_iter().map(|i| {
            let wf_id = format!("fake-wf-{}", i);
            let hist = canned_histories::single_timer("fake_timer");
            FakeWfResponses {
                wf_id,
                hist,
                response_batches: vec![1, 2],
            }
        });

        let mock = build_multihist_mock_sg(hists, false, None);
        let core = CoreSDK::new(
            mock.sg,
            CoreInitOptions {
                gateway_opts: fake_sg_opts(),
                evict_after_pending_cleared: true,
                max_outstanding_workflow_tasks: 20,
                max_outstanding_activities: 5,
            },
        );

        let core = Arc::new(core);
        let mut handles = vec![];
        for _ in 0..5 {
            let core = core.clone();
            let h = tokio::spawn(async move {
                while let Ok(wft) = core.poll_workflow_task().await {
                    let job = &wft.jobs[0];
                    let reply = match job.variant {
                        Some(wf_activation_job::Variant::StartWorkflow(_)) => StartTimer {
                            timer_id: "fake_timer".to_string(),
                            ..Default::default()
                        }
                        .into(),
                        Some(wf_activation_job::Variant::RemoveFromCache(_)) => continue,
                        _ => CompleteWorkflowExecution { result: None }.into(),
                    };
                    core.complete_workflow_task(WfActivationCompletion::from_status(
                        wft.task_token,
                        workflow_completion::Success::from_cmds(vec![reply]).into(),
                    ))
                    .await
                    .unwrap();
                }
            });
            handles.push(h);
        }
        for h in handles {
            h.await.unwrap()
        }
        assert!(core.outstanding_workflow_tasks.is_empty());
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    #[tokio::test]
    async fn wft_timeout_repro(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let t = canned_histories::wft_timeout_repro();
        let core = build_fake_core(wfid, t, hist_batches);
        let activity_id = "act-1";

        poll_and_reply(
            &core,
            EvictionMode::NotSticky,
            &[
                gen_assert_and_reply(
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    // Start timer and activity
                    vec![ScheduleActivity {
                        activity_id: activity_id.to_string(),
                        cancellation_type: ActivityCancellationType::TryCancel as i32,
                        ..Default::default()
                    }
                    .into()],
                ),
                gen_assert_and_reply(
                    &job_assert!(
                        wf_activation_job::Variant::SignalWorkflow(_),
                        wf_activation_job::Variant::SignalWorkflow(_),
                        wf_activation_job::Variant::ResolveActivity(ResolveActivity {
                            result: Some(ActivityResult {
                                status: Some(activity_result::Status::Completed(..)),
                            }),
                            ..
                        })
                    ),
                    vec![CompleteWorkflowExecution { result: None }.into()],
                ),
            ],
        )
        .await;
    }
}
