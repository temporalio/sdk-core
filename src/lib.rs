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

pub(crate) mod core_tracing;
mod errors;
mod machines;
mod pending_activations;
mod pollers;
mod protosext;
mod workflow;

#[cfg(test)]
mod test_help;

pub use crate::errors::{
    CompleteActivityError, CompleteWfError, CoreInitError, PollActivityError, PollWfError,
};
pub use core_tracing::tracing_init;
pub use pollers::{PollTaskRequest, ServerGateway, ServerGatewayApis, ServerGatewayOptions};
pub use url::Url;

use crate::{
    errors::{ShutdownErr, WorkflowUpdateError},
    machines::{EmptyWorkflowCommandErr, WFCommand},
    pending_activations::{PendingActivation, PendingActivations},
    protos::{
        coresdk::{
            activity_result::{self as ar, activity_result},
            activity_task::ActivityTask,
            workflow_activation::WfActivation,
            workflow_completion::{self, wf_activation_completion, WfActivationCompletion},
            ActivityHeartbeat, ActivityTaskCompletion,
        },
        temporal::api::{
            enums::v1::WorkflowTaskFailedCause, workflowservice::v1::PollWorkflowTaskQueueResponse,
        },
    },
    protosext::fmt_task_token,
    workflow::{
        NextWfActivation, PushCommandsResult, WorkflowConcurrencyManager, WorkflowError,
        WorkflowManager,
    },
};
use dashmap::DashMap;
use futures::TryFutureExt;
use std::{
    convert::TryInto,
    fmt::Debug,
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
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
    /// Note that it is possible to receive work from a task queue that isn't the argument you
    /// passed if you previously polled that task queue and there is remaining work that the lang
    /// side must do before core can respond to the server.
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

    /// Indicate that a long running activity is still making progress
    async fn send_activity_heartbeat(&self, task_token: ActivityHeartbeat) -> Result<(), ()>;

    /// Returns core's instance of the [ServerGatewayApis] implementor it is using.
    fn server_gateway(&self) -> Arc<dyn ServerGatewayApis>;

    /// Eventually ceases all polling of the server. [Core::poll_workflow_task] should be called
    /// until it returns [PollWfError::ShutDown] to ensure that any workflows which are still
    /// undergoing replay have an opportunity to finish. This means that the lang sdk will need to
    /// call [Core::complete_workflow_task] for those workflows until they are done. At that point,
    /// the lang SDK can end the process, or drop the [Core] instance, which will close the
    /// connection.
    fn shutdown(&self);
}

/// Holds various configuration information required to call [init]
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
    let work_provider = opts.gateway_opts.connect().await?;

    Ok(CoreSDK::new(work_provider))
}

struct CoreSDK<WP> {
    /// Provides work in the form of responses the server would send from polling task Qs
    server_gateway: Arc<WP>,
    /// Key is run id
    workflow_machines: WorkflowConcurrencyManager,
    /// Maps task tokens to workflow run ids
    workflow_task_tokens: DashMap<Vec<u8>, String>,

    /// Workflows that are currently under replay will queue here, indicating that there are more
    /// workflow tasks / activations to be performed.
    pending_activations: PendingActivations,

    /// Has shutdown been called?
    shutdown_requested: AtomicBool,
    /// Used to wake up future which checks shutdown state
    shutdown_notify: Notify,
    /// Used to interrupt workflow task polling if there is a new pending activation
    pending_activation_notify: Notify,
}

#[async_trait::async_trait]
impl<WP> Core for CoreSDK<WP>
where
    WP: ServerGatewayApis + Send + Sync + 'static,
{
    #[instrument(skip(self))]
    async fn poll_workflow_task(&self, task_queue: &str) -> Result<WfActivation, PollWfError> {
        // The poll needs to be in a loop because we can't guarantee tail call optimization in Rust
        // (simply) and we really, really need that for long-poll retries.
        loop {
            // We must first check if there are pending workflow tasks for workflows that are
            // currently replaying, and issue those tasks before bothering the server.
            if let Some(pa) = self
                .pending_activations
                .pop()
                .and_then(|p| self.prepare_pending_activation(p).transpose())
            {
                return pa;
            }

            if self.shutdown_requested.load(Ordering::SeqCst) {
                return Err(PollWfError::ShutDown);
            }

            let pa_fut = self.pending_activation_notify.notified();
            let poll_result_future = self.shutdownable_fut(
                self.server_gateway
                    .poll_workflow_task(task_queue.to_owned())
                    .map_err(Into::into),
            );
            debug!("Polling server");
            let selected_f = tokio::select! {
                // If a pending activation comes in while we are waiting on polling, we need to
                // restart the loop right away to provide it
                _ = pa_fut => {
                    debug!("Poll interrupted by new pending activation");
                    continue;
                }
                r = poll_result_future => r
            };

            match selected_f {
                Ok(work) => {
                    if let Some(activation) = self.prepare_new_activation(work)? {
                        return Ok(activation);
                    }
                }
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
        if self.shutdown_requested.load(Ordering::SeqCst) {
            return Err(PollActivityError::ShutDown);
        }

        match self
            .shutdownable_fut(
                self.server_gateway
                    .poll_activity_task(task_queue.to_owned())
                    .map_err(Into::into),
            )
            .await
        {
            Ok(work) => {
                let task_token = work.task_token.clone();
                Ok(ActivityTask::start_from_poll_resp(work, task_token))
            }
            Err(e) => Err(e),
        }
    }

    #[instrument(skip(self))]
    async fn complete_workflow_task(
        &self,
        completion: WfActivationCompletion,
    ) -> Result<(), CompleteWfError> {
        let task_token = completion.task_token;
        let wfstatus = completion.status;
        let run_id = self
            .workflow_task_tokens
            .get(&task_token)
            .map(|x| x.value().clone())
            .ok_or_else(|| CompleteWfError::MalformedWorkflowCompletion {
                reason: format!(
                    "Task token {} had no workflow run associated with it",
                    fmt_task_token(&task_token)
                ),
                completion: None,
            })?;
        let res = match wfstatus {
            Some(wf_activation_completion::Status::Successful(success)) => {
                self.wf_activation_success(task_token, &run_id, success)
                    .await
            }
            Some(wf_activation_completion::Status::Failed(failure)) => {
                self.wf_activation_failed(task_token, &run_id, failure)
                    .await
            }
            None => Err(CompleteWfError::MalformedWorkflowCompletion {
                reason: "Workflow completion had empty status field".to_owned(),
                completion: None,
            }),
        };

        // Blow up workflows with no more pending activations (IE: They have completed a WFT)
        if !self.pending_activations.has_pending(&run_id) {
            warn!("Evicting");
            self.evict_run(&run_id);
        }
        res
    }

    #[instrument(skip(self))]
    async fn complete_activity_task(
        &self,
        completion: ActivityTaskCompletion,
    ) -> Result<(), CompleteActivityError> {
        let task_token = completion.task_token;
        let status = if let Some(s) = completion.result.and_then(|r| r.status) {
            s
        } else {
            return Err(CompleteActivityError::MalformedActivityCompletion {
                reason: "Activity completion had empty result/status field".to_owned(),
                completion: None,
            });
        };
        match status {
            activity_result::Status::Completed(ar::Success { result }) => {
                self.server_gateway
                    .complete_activity_task(task_token, result.map(Into::into))
                    .await?;
            }
            activity_result::Status::Failed(ar::Failure { failure }) => {
                self.server_gateway
                    .fail_activity_task(task_token, failure.map(Into::into))
                    .await?;
            }
            activity_result::Status::Canceled(ar::Cancelation { details }) => {
                self.server_gateway
                    .cancel_activity_task(task_token, details.map(Into::into))
                    .await?;
            }
        }
        Ok(())
    }

    async fn send_activity_heartbeat(&self, _task_token: ActivityHeartbeat) -> Result<(), ()> {
        unimplemented!()
    }

    fn server_gateway(&self) -> Arc<dyn ServerGatewayApis> {
        self.server_gateway.clone()
    }

    fn shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_one();
        self.workflow_machines.shutdown();
    }
}

impl<WP: ServerGatewayApis> CoreSDK<WP> {
    pub(crate) fn new(wp: WP) -> Self {
        Self {
            server_gateway: Arc::new(wp),
            workflow_machines: WorkflowConcurrencyManager::new(),
            workflow_task_tokens: Default::default(),
            pending_activations: Default::default(),
            shutdown_requested: AtomicBool::new(false),
            shutdown_notify: Notify::new(),
            pending_activation_notify: Notify::new(),
        }
    }

    /// Evict a workflow from the cache by it's run id
    ///
    /// TODO: Very likely needs to be in Core public api
    pub(crate) fn evict_run(&self, run_id: &str) {
        self.workflow_machines.evict(run_id);
        self.pending_activations.remove_all_with_run_id(run_id);
    }

    /// Given a pending activation, prepare it to be sent to lang
    #[instrument(skip(self))]
    fn prepare_pending_activation(
        &self,
        pa: PendingActivation,
    ) -> Result<Option<WfActivation>, PollWfError> {
        if let Some(next_activation) =
            self.access_wf_machine(&pa.run_id, move |mgr| mgr.get_next_activation())?
        {
            return Ok(Some(self.finalize_next_activation(
                next_activation,
                pa.task_token,
                true,
            )));
        }
        Ok(None)
    }

    /// Prepare an activation we've just pulled out of a workflow machines instance to be shipped
    /// to the lang sdk
    fn finalize_next_activation(
        &self,
        next_a: NextWfActivation,
        task_token: Vec<u8>,
        from_pending: bool,
    ) -> WfActivation {
        if next_a.more_activations_needed {
            self.add_pending_activation(PendingActivation {
                run_id: next_a.get_run_id().to_owned(),
                task_token: task_token.clone(),
            });
        }
        next_a.finalize(task_token, from_pending)
    }

    /// Given a wf task from the server, prepare an activation (if there is one) to be sent to lang
    fn prepare_new_activation(
        &self,
        work: PollWorkflowTaskQueueResponse,
    ) -> Result<Option<WfActivation>, PollWfError> {
        if work == PollWorkflowTaskQueueResponse::default() {
            // We get the default proto in the event that the long poll times out.
            return Ok(None);
        }
        let task_token = work.task_token.clone();
        debug!(
            task_token = %fmt_task_token(&task_token),
            "Received workflow task from server"
        );

        let next_activation = self.instantiate_or_update_workflow(work)?;

        if let Some(na) = next_activation {
            return Ok(Some(self.finalize_next_activation(na, task_token, false)));
        }
        Ok(None)
    }

    /// Handle a successful workflow completion
    async fn wf_activation_success(
        &self,
        task_token: Vec<u8>,
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
        let push_result = self.push_lang_commands(&run_id, cmds)?;
        if push_result.has_new_lang_jobs {
            self.add_pending_activation(PendingActivation {
                run_id: run_id.to_string(),
                task_token: task_token.clone(),
            });
        }
        // We only actually want to send commands back to the server if there are
        // no more pending activations -- in other words the lang SDK has caught
        // up on replay.
        if !self.pending_activations.has_pending(&run_id) {
            self.server_gateway
                .complete_workflow_task(task_token, push_result.server_commands)
                .await
                .map_err(|ts| {
                    if ts.code() == tonic::Code::InvalidArgument
                        && ts.message() == "UnhandledCommand"
                    {
                        CompleteWfError::UnhandledCommandWhenCompleting
                    } else {
                        ts.into()
                    }
                })?;
        }
        Ok(())
    }

    /// Handle a failed workflow completion
    async fn wf_activation_failed(
        &self,
        task_token: Vec<u8>,
        run_id: &str,
        failure: workflow_completion::Failure,
    ) -> Result<(), CompleteWfError> {
        warn!("Failed!");
        // Blow up any cached data associated with the workflow
        self.evict_run(&run_id);

        self.server_gateway
            .fail_workflow_task(
                task_token,
                WorkflowTaskFailedCause::Unspecified,
                failure.failure.map(Into::into),
            )
            .await?;
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
        poll_wf_resp: PollWorkflowTaskQueueResponse,
    ) -> Result<Option<NextWfActivation>, PollWfError> {
        match poll_wf_resp {
            PollWorkflowTaskQueueResponse {
                task_token,
                workflow_execution: Some(workflow_execution),
                history: Some(history),
                ..
            } => {
                let run_id = workflow_execution.run_id.clone();
                // Correlate task token w/ run ID
                self.workflow_task_tokens.insert(task_token, run_id.clone());

                match self
                    .workflow_machines
                    .create_or_update(&run_id, history, workflow_execution)
                {
                    Ok(activation) => Ok(activation),
                    Err(source) => Err(PollWfError::WorkflowUpdateError { source, run_id }),
                }
            }
            p => Err(PollWfError::BadPollResponseFromServer(p)),
        }
    }

    /// Feed commands from the lang sdk into appropriate workflow manager which will iterate
    /// the state machines and return commands ready to be sent to the server
    fn push_lang_commands(
        &self,
        run_id: &str,
        cmds: Vec<WFCommand>,
    ) -> Result<PushCommandsResult, WorkflowUpdateError> {
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

    /// Wrap a future, making it return early with a shutdown error in the event the shutdown
    /// flag has been set
    async fn shutdownable_fut<FOut, FErr>(
        &self,
        wrap_this: impl Future<Output = Result<FOut, FErr>>,
    ) -> Result<FOut, FErr>
    where
        FErr: From<ShutdownErr>,
    {
        let shutdownfut = async {
            loop {
                self.shutdown_notify.notified().await;
                if self.shutdown_requested.load(Ordering::SeqCst) {
                    break;
                }
            }
        };
        tokio::select! {
            _ = shutdownfut => {
                Err(ShutdownErr.into())
            }
            r = wrap_this => r
        }
    }

    /// Enqueue a new pending activation, and notify ourselves
    fn add_pending_activation(&self, pa: PendingActivation) {
        self.pending_activations.push(pa);
        self.pending_activation_notify.notify_waiters();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::machines::test_help::EvictionMode;
    use crate::protos::coresdk::activity_result::ActivityResult;
    use crate::protos::coresdk::workflow_activation::ResolveActivity;
    use crate::protos::coresdk::workflow_commands::{
        ActivityCancellationType, RequestCancelActivity,
    };
    use crate::{
        machines::test_help::{
            build_fake_core, gen_assert_and_fail, gen_assert_and_reply, poll_and_reply, FakeCore,
            TestHistoryBuilder,
        },
        protos::{
            coresdk::{
                common::UserCodeFailure,
                workflow_activation::{
                    wf_activation_job, FireTimer, StartWorkflow, UpdateRandomSeed, WfActivationJob,
                },
                workflow_commands::{
                    CancelTimer, CompleteWorkflowExecution, FailWorkflowExecution,
                    ScheduleActivity, StartTimer,
                },
            },
            temporal::api::{
                enums::v1::EventType, workflowservice::v1::RespondWorkflowTaskFailedResponse,
            },
        },
        test_help::canned_histories,
    };
    use rstest::{fixture, rstest};
    use std::sync::atomic::AtomicU64;

    const TASK_Q: &str = "test-task-queue";

    #[fixture(hist_batches = &[])]
    fn single_timer_setup(hist_batches: &[usize]) -> FakeCore {
        let wfid = "fake_wf_id";

        let mut t = canned_histories::single_timer("fake_timer");
        build_fake_core(wfid, &mut t, hist_batches)
    }

    #[fixture(hist_batches = &[])]
    fn single_activity_setup(hist_batches: &[usize]) -> FakeCore {
        let wfid = "fake_wf_id";

        let mut t = canned_histories::single_activity("fake_activity");
        build_fake_core(wfid, &mut t, hist_batches)
    }

    #[fixture(hist_batches = &[])]
    fn single_activity_failure_setup(hist_batches: &[usize]) -> FakeCore {
        let wfid = "fake_wf_id";

        let mut t = canned_histories::single_failed_activity("fake_activity");
        build_fake_core(wfid, &mut t, hist_batches)
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
            TASK_Q,
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
            TASK_Q,
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

        let mut t = canned_histories::parallel_timer(timer_1_id, timer_2_id);
        let core = build_fake_core(wfid, &mut t, hist_batches);

        poll_and_reply(
            &core,
            TASK_Q,
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

        let mut t = canned_histories::cancel_timer(timer_id, cancel_timer_id);
        let core = build_fake_core(wfid, &mut t, hist_batches);

        poll_and_reply(
            &core,
            TASK_Q,
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

        let mut t = canned_histories::cancel_scheduled_activity(activity_id, signal_id);
        let core = build_fake_core(wfid, &mut t, hist_batches);

        poll_and_reply(
            &core,
            TASK_Q,
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

        let mut t = canned_histories::scheduled_activity_timeout(activity_id);
        let core = build_fake_core(wfid, &mut t, hist_batches);
        poll_and_reply(
            &core,
            TASK_Q,
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

        let mut t = canned_histories::started_activity_timeout(activity_id);
        let core = build_fake_core(wfid, &mut t, hist_batches);

        poll_and_reply(
            &core,
            TASK_Q,
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

        let mut t = canned_histories::scheduled_cancelled_activity_timeout(activity_id, signal_id);
        let core = build_fake_core(wfid, &mut t, hist_batches);

        poll_and_reply(
            &core,
            TASK_Q,
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

        let mut t = canned_histories::cancel_scheduled_activity_abandon(activity_id, signal_id);
        let core = build_fake_core(wfid, &mut t, hist_batches);

        verify_activity_cancellation_abandon(&activity_id, &core).await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    #[tokio::test]
    async fn started_activity_cancellation_abandon(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let activity_id = "fake_activity";
        let signal_id = "signal";

        let mut t = canned_histories::cancel_started_activity_abandon(activity_id, signal_id);
        let core = build_fake_core(wfid, &mut t, hist_batches);

        verify_activity_cancellation_abandon(&activity_id, &core).await;
    }

    async fn verify_activity_cancellation_abandon(activity_id: &&str, core: &FakeCore) {
        poll_and_reply(
            &core,
            TASK_Q,
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

        let mut t =
            canned_histories::cancel_scheduled_activity_with_signal_and_activity_task_cancel(
                activity_id,
                signal_id,
            );
        let core = build_fake_core(wfid, &mut t, hist_batches);

        verify_activity_cancellation_wait_for_cancellation(activity_id, &core).await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 2, 3, 4]), case::replay(&[4]))]
    #[tokio::test]
    async fn started_activity_cancellation_wait_for_cancellation(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let activity_id = "fake_activity";
        let signal_id = "signal";

        let mut t = canned_histories::cancel_started_activity_with_signal_and_activity_task_cancel(
            activity_id,
            signal_id,
        );
        let core = build_fake_core(wfid, &mut t, hist_batches);

        verify_activity_cancellation_wait_for_cancellation(activity_id, &core).await;
    }

    async fn verify_activity_cancellation_wait_for_cancellation(
        activity_id: &str,
        core: &FakeCore,
    ) {
        poll_and_reply(
            &core,
            TASK_Q,
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

        let mut t = canned_histories::cancel_scheduled_activity_with_activity_task_cancel(
            activity_id,
            signal_id,
        );
        let core = build_fake_core(wfid, &mut t, hist_batches);

        verify_activity_cancellation_try_cancel_task_canceled(&activity_id, &core).await;
    }

    #[rstest(hist_batches, case::incremental(&[1, 3]), case::replay(&[3]))]
    #[tokio::test]
    async fn started_activity_cancellation_try_cancel_task_canceled(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let activity_id = "fake_activity";
        let signal_id = "signal";

        let mut t = canned_histories::cancel_started_activity_with_activity_task_cancel(
            activity_id,
            signal_id,
        );
        let core = build_fake_core(wfid, &mut t, hist_batches);

        verify_activity_cancellation_try_cancel_task_canceled(&activity_id, &core).await;
    }

    async fn verify_activity_cancellation_try_cancel_task_canceled(
        activity_id: &&str,
        core: &FakeCore,
    ) {
        poll_and_reply(
            &core,
            TASK_Q,
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
        let res = single_timer_setup
            .inner
            .poll_workflow_task(TASK_Q)
            .await
            .unwrap();
        assert_eq!(res.jobs.len(), 1);

        single_timer_setup.inner.shutdown();
        assert_matches!(
            single_timer_setup
                .inner
                .poll_workflow_task(TASK_Q)
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

        let mut t = canned_histories::workflow_fails_with_reset_after_timer(timer_1_id, new_run_id);
        let core = build_fake_core(wfid, &mut t, &[2]);

        poll_and_reply(
            &core,
            TASK_Q,
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

        let core = build_fake_core(wfid, &mut t, &[1]);

        poll_and_reply(
            &core,
            TASK_Q,
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
    #[case::evict(&[1, 2, 2, 2, 2], EvictionMode::AfterEveryReply)]
    #[tokio::test]
    async fn complete_activation_with_failure(
        #[case] batches: &[usize],
        #[case] evict: EvictionMode,
    ) {
        let wfid = "fake_wf_id";
        let timer_id = "timer";

        let mut t = canned_histories::workflow_fails_with_failure_after_timer(timer_id);
        let mut core = build_fake_core(wfid, &mut t, batches);
        // Need to create an expectation that we will call a failure completion
        Arc::get_mut(&mut core.inner.server_gateway)
            .unwrap()
            .expect_fail_workflow_task()
            // TODO: Re-enable this once we turn on only-fail-wft-once?
            // .times(1)
            .returning(|_, _, _| Ok(RespondWorkflowTaskFailedResponse {}));

        poll_and_reply(
            &core,
            TASK_Q,
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
                    &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                    // Need to re-issue the start timer command (we are replaying)
                    vec![StartTimer {
                        timer_id: timer_id.to_string(),
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

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    #[tokio::test]
    async fn simple_timer_fail_wf_execution(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let timer_id = "timer1";

        let mut t = canned_histories::single_timer(timer_id);
        let core = build_fake_core(wfid, &mut t, hist_batches);

        poll_and_reply(
            &core,
            TASK_Q,
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

        let mut t = canned_histories::two_signals("sig1", "sig2");
        let core = build_fake_core(wfid, &mut t, hist_batches);

        poll_and_reply(
            &core,
            TASK_Q,
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
}
