#![warn(missing_docs)] // error if there are missing docs

//! This crate provides a basis for creating new Temporal SDKs without completely starting from
//! scratch

#[cfg(test)]
#[macro_use]
pub extern crate assert_matches;
#[macro_use]
extern crate tracing;

pub mod protos;

pub(crate) mod core_tracing;
mod machines;
mod pending_activations;
mod pollers;
mod protosext;
mod workflow;

#[cfg(test)]
mod test_help;

pub use core_tracing::tracing_init;
pub use pollers::{PollTaskRequest, ServerGateway, ServerGatewayApis, ServerGatewayOptions};
pub use url::Url;

use crate::{
    machines::{EmptyWorkflowCommandErr, ProtoCommand},
    pending_activations::{PendingActivation, PendingActivations},
    protos::{
        coresdk::{
            activity_result::{self as ar, activity_result, ActivityResult},
            task_completion,
            workflow_completion::{self, wf_activation_completion, WfActivationCompletion},
            ActivityHeartbeat, PayloadsExt, PollTaskReq, Task, TaskCompletion,
        },
        temporal::api::{
            enums::v1::WorkflowTaskFailedCause, workflowservice::v1::PollWorkflowTaskQueueResponse,
        },
    },
    protosext::fmt_task_token,
    workflow::{NextWfActivation, WorkflowConcurrencyManager, WorkflowError, WorkflowManager},
};
use dashmap::DashMap;
use std::{
    convert::TryInto,
    fmt::Debug,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::runtime::Runtime;
use tonic::codegen::http::uri::InvalidUri;
use tracing::Span;

/// A result alias having [CoreError] as the error type
pub type Result<T, E = CoreError> = std::result::Result<T, E>;

/// This trait is the primary way by which language specific SDKs interact with the core SDK. It is
/// expected that only one instance of an implementation will exist for the lifetime of the
/// worker(s) using it.
pub trait Core: Send + Sync {
    /// Ask the core for some work, returning a [Task]. It is then the language SDK's responsibility
    /// to call the appropriate code with the provided inputs. Blocks indefinitely until such work
    /// is available or [shutdown] is called.
    ///
    /// TODO: Examples
    fn poll_task(&self, task_queue: PollTaskReq) -> Result<Task>;

    /// Tell the core that some work has been completed - whether as a result of running workflow
    /// code or executing an activity.
    fn complete_task(&self, req: TaskCompletion) -> Result<()>;

    /// Indicate that a long running activity is still making progress
    fn send_activity_heartbeat(&self, task_token: ActivityHeartbeat) -> Result<()>;

    /// Returns core's instance of the [ServerGatewayApis] implementor it is using.
    fn server_gateway(&self) -> Arc<dyn ServerGatewayApis>;

    /// Eventually ceases all polling of the server. [Core::poll_task] should be called until it
    /// returns [CoreError::ShuttingDown] to ensure that any workflows which are still undergoing
    /// replay have an opportunity to finish. This means that the lang sdk will need to call
    /// [Core::complete_task] for those workflows until they are done. At that point, the lang
    /// SDK can end the process, or drop the [Core] instance, which will close the connection.
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
pub fn init(opts: CoreInitOptions) -> Result<impl Core> {
    let runtime = Runtime::new().map_err(CoreError::TokioInitError)?;
    // Initialize server client
    let work_provider = runtime.block_on(opts.gateway_opts.connect())?;

    Ok(CoreSDK {
        runtime,
        server_gateway: Arc::new(work_provider),
        workflow_machines: WorkflowConcurrencyManager::new(),
        workflow_task_tokens: Default::default(),
        pending_activations: Default::default(),
        shutdown_requested: AtomicBool::new(false),
    })
}

struct CoreSDK<WP> {
    runtime: Runtime,
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
}

/// Can be used inside the CoreSDK impl to block on any method that polls the server until it
/// responds, or until the shutdown flag is set (aborting the poll)
macro_rules! abort_on_shutdown {
    ($self:ident, $gateway_fn:tt, $poll_arg:expr) => {
        $self.runtime.block_on(async {
            let shutdownfut = async {
                loop {
                    if $self.shutdown_requested.load(Ordering::Relaxed) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            };
            let poll_result_future = $self.server_gateway.$gateway_fn($poll_arg);
            tokio::select! {
                _ = shutdownfut => {
                    Err(CoreError::ShuttingDown)
                }
                r = poll_result_future => r
            }
        })
    };
}

impl<WP> Core for CoreSDK<WP>
where
    WP: ServerGatewayApis + Send + Sync + 'static,
{
    #[instrument(skip(self), fields(pending_activation))]
    fn poll_task(&self, req: PollTaskReq) -> Result<Task> {
        let task_queue = req.task_queue;
        // The poll needs to be in a loop because we can't guarantee tail call optimization in Rust
        // (simply) and we really, really need that for long-poll retries.
        loop {
            if req.workflows {
                // We must first check if there are pending workflow tasks for workflows that are
                // currently replaying, and issue those tasks before bothering the server.
                if let Some(pa) = self.pending_activations.pop() {
                    Span::current().record("pending_activation", &format!("{}", &pa).as_str());

                    let next_activation =
                        self.access_wf_machine(&pa.run_id, move |mgr| mgr.get_next_activation())?;
                    let task_token = pa.task_token.clone();
                    if next_activation.more_activations_needed {
                        self.pending_activations.push(pa);
                    }
                    return Ok(Task {
                        task_token,
                        variant: next_activation.activation.map(Into::into),
                    });
                }
            }

            if self.shutdown_requested.load(Ordering::SeqCst) {
                return Err(CoreError::ShuttingDown);
            }

            // TODO: There's an interesting question here about whether activities or workflows
            //  should take priority in the event that the caller is OK with either.

            // This will block until the long poll timeout (unless interrupted by shutdown) in the
            // event there is no work from the server
            match abort_on_shutdown!(self, poll_workflow_task, task_queue.to_owned()) {
                Ok(work) => {
                    if work == PollWorkflowTaskQueueResponse::default() {
                        // We get the default proto in the event that the long poll times out.
                        continue;
                    }
                    let task_token = work.task_token.clone();
                    debug!(
                        task_token = %fmt_task_token(&task_token),
                        "Received workflow task from server"
                    );

                    let (next_activation, run_id) = self.instantiate_or_update_workflow(work)?;

                    if next_activation.more_activations_needed {
                        self.pending_activations.push(PendingActivation {
                            run_id,
                            task_token: task_token.clone(),
                        });
                    }

                    return Ok(Task {
                        task_token,
                        variant: next_activation.activation.map(Into::into),
                    });
                }
                // Drain pending activations in case of shutdown.
                Err(CoreError::ShuttingDown) => continue,
                Err(e) => return Err(e),
            }
        }
    }

    #[instrument(skip(self))]
    fn complete_task(&self, req: TaskCompletion) -> Result<()> {
        match req {
            TaskCompletion {
                task_token,
                variant:
                    Some(task_completion::Variant::Workflow(WfActivationCompletion {
                        status: Some(wfstatus),
                    })),
            } => {
                let run_id = self
                    .workflow_task_tokens
                    .get(&task_token)
                    .map(|x| x.value().clone())
                    .ok_or_else(|| CoreError::MalformedCompletion {
                        reason: format!(
                            "Task token {} had no workflow run associated with it",
                            fmt_task_token(&task_token)
                        ),
                        completion: None,
                    })?;
                match wfstatus {
                    wf_activation_completion::Status::Successful(success) => {
                        let commands = self.push_lang_commands(&run_id, success)?;
                        // We only actually want to send commands back to the server if there are
                        // no more pending activations -- in other words the lang SDK has caught
                        // up on replay.
                        if !self.pending_activations.has_pending(&run_id) {
                            self.runtime.block_on(
                                self.server_gateway
                                    .complete_workflow_task(task_token, commands),
                            )?;
                        }
                    }
                    wf_activation_completion::Status::Failed(failure) => {
                        // Blow up any cached data associated with the workflow
                        self.evict_run(&run_id);

                        self.runtime
                            .block_on(self.server_gateway.fail_workflow_task(
                                task_token,
                                WorkflowTaskFailedCause::Unspecified,
                                failure.failure.map(Into::into),
                            ))?;
                    }
                }
                Ok(())
            }
            TaskCompletion {
                task_token,
                variant:
                    Some(task_completion::Variant::Activity(ActivityResult {
                        status: Some(status),
                    })),
            } => {
                match status {
                    activity_result::Status::Completed(ar::Success { result }) => {
                        self.runtime.block_on(
                            self.server_gateway
                                .complete_activity_task(task_token, result.into_payloads()),
                        )?;
                    }
                    activity_result::Status::Failed(ar::Failure { failure }) => {
                        self.runtime.block_on(
                            self.server_gateway
                                .fail_activity_task(task_token, failure.map(Into::into)),
                        )?;
                    }
                    activity_result::Status::Canceled(ar::Cancelation { details }) => {
                        self.runtime.block_on(
                            self.server_gateway
                                .cancel_activity_task(task_token, details.into_payloads()),
                        )?;
                    }
                }
                Ok(())
            }
            _ => Err(CoreError::MalformedCompletion {
                completion: Some(req),
                reason: "TaskCompletion didn't have a workflow or activity completion".to_owned(),
            }),
        }
    }

    fn send_activity_heartbeat(&self, _task_token: ActivityHeartbeat) -> Result<()> {
        unimplemented!()
    }

    fn server_gateway(&self) -> Arc<dyn ServerGatewayApis> {
        self.server_gateway.clone()
    }

    fn shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        self.workflow_machines.shutdown();
    }
}

impl<WP: ServerGatewayApis> CoreSDK<WP> {
    /// Will create a new workflow manager if needed for the workflow task, if not, it will
    /// feed the existing manager the updated history we received from the server.
    ///
    /// Also updates [CoreSDK::workflow_task_tokens] and validates the
    /// [PollWorkflowTaskQueueResponse]
    ///
    /// Returns the next workflow activation and the workflow's run id
    fn instantiate_or_update_workflow(
        &self,
        poll_wf_resp: PollWorkflowTaskQueueResponse,
    ) -> Result<(NextWfActivation, String)> {
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
                    Ok(activation) => Ok((activation, run_id)),
                    Err(source) => Err(CoreError::WorkflowError { source, run_id }),
                }
            }
            p => Err(CoreError::BadDataFromWorkProvider(p)),
        }
    }

    /// Feed commands from the lang sdk into appropriate workflow manager which will iterate
    /// the state machines and return commands ready to be sent to the server
    fn push_lang_commands(
        &self,
        run_id: &str,
        success: workflow_completion::Success,
    ) -> Result<Vec<ProtoCommand>> {
        // Convert to wf commands
        let cmds = success
            .commands
            .into_iter()
            .map(|c| c.try_into().map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        Ok(self.access_wf_machine(run_id, move |mgr| mgr.push_commands(cmds))?)
    }

    /// Remove a workflow run from the cache entirely
    fn evict_run(&self, run_id: &str) {
        self.workflow_machines.evict(run_id);
    }

    /// Wraps access to `self.workflow_machines.access`, properly passing in the current tracing
    /// span to the wf machines thread.
    fn access_wf_machine<F, Fout>(&self, run_id: &str, mutator: F) -> Result<Fout, CoreError>
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
            .map_err(|source| CoreError::WorkflowError {
                source,
                run_id: run_id.to_owned(),
            })
    }
}

/// The error type returned by interactions with [Core]
#[derive(thiserror::Error, Debug, displaydoc::Display)]
#[allow(clippy::large_enum_variant)]
// NOTE: Docstrings take the place of #[error("xxxx")] here b/c of displaydoc
pub enum CoreError {
    /// [Core::shutdown] was called, and there are no more replay tasks to be handled. You must
    /// call [Core::complete_task] for any remaining tasks, and then may exit.
    ShuttingDown,
    /// Poll response from server was malformed: {0:?}
    BadDataFromWorkProvider(PollWorkflowTaskQueueResponse),
    /// Lang SDK sent us a malformed completion ({reason}): {completion:?}
    MalformedCompletion {
        reason: String,
        completion: Option<TaskCompletion>,
    },
    /// There was an error specific to a workflow instance with id ({run_id}): {source:?}
    WorkflowError {
        /// Underlying workflow error
        source: WorkflowError,
        /// The run id of the erring workflow
        run_id: String,
    },
    /// Land SDK sent us an empty workflow command (no variant)
    UninterpretableCommand(#[from] EmptyWorkflowCommandErr),
    /// Error calling the service: {0:?}
    TonicError(#[from] tonic::Status),
    /// Server connection error: {0:?}
    TonicTransportError(#[from] tonic::transport::Error),
    /// Failed to initialize tokio runtime: {0:?}
    TokioInitError(std::io::Error),
    /// Invalid URI: {0:?}
    InvalidUri(#[from] InvalidUri),
    /// State machines are missing for the workflow with run id {0}!
    MissingMachines(String),
    /// There exists a pending command in this workflow's history which has not yet been handled.
    /// When thrown from complete_task, it means you should poll for a new task, receive a new
    /// task token, and complete that task.
    UnhandledCommandWhenCompleting,
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        machines::test_help::{build_fake_core, FakeCore, TestHistoryBuilder},
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
                IntoTaskQExt, TaskCompletion,
            },
            temporal::api::{
                enums::v1::EventType, workflowservice::v1::RespondWorkflowTaskFailedResponse,
            },
        },
        test_help::canned_histories,
    };
    use rstest::{fixture, rstest};

    const TASK_Q: &str = "test-task-queue";
    const RUN_ID: &str = "fake_run_id";

    #[fixture(hist_batches = &[])]
    fn single_timer_setup(hist_batches: &[usize]) -> FakeCore {
        let wfid = "fake_wf_id";

        let mut t = canned_histories::single_timer("fake_timer");
        build_fake_core(wfid, RUN_ID, &mut t, hist_batches)
    }

    #[fixture(hist_batches = &[])]
    fn single_activity_setup(hist_batches: &[usize]) -> FakeCore {
        let wfid = "fake_wf_id";

        let mut t = canned_histories::single_activity("fake_activity");
        build_fake_core(wfid, RUN_ID, &mut t, hist_batches)
    }

    #[rstest(core,
    case::incremental(single_timer_setup(&[1, 2])),
    case::replay(single_timer_setup(&[2]))
    )]
    fn single_timer_test_across_wf_bridge(core: FakeCore) {
        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::StartWorkflow(_)),
            }]
        );
        assert!(core.workflow_machines.exists(RUN_ID));

        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![StartTimer {
                timer_id: "fake_timer".to_string(),
                ..Default::default()
            }
            .into()],
            task_tok,
        ))
        .unwrap();

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(_)),
            }]
        );
        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![CompleteWorkflowExecution { result: vec![] }.into()],
            task_tok,
        ))
        .unwrap();
    }

    #[rstest(core,
    case::incremental(single_activity_setup(&[1, 2])),
    case::replay(single_activity_setup(&[2]))
    )]
    fn single_activity_completion(core: FakeCore) {
        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::StartWorkflow(_)),
            }]
        );
        assert!(core.workflow_machines.exists(RUN_ID));

        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![ScheduleActivity {
                activity_id: "fake_activity".to_string(),
                ..Default::default()
            }
            .into()],
            task_tok,
        ))
        .unwrap();

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![CompleteWorkflowExecution { result: vec![] }.into()],
            task_tok,
        ))
        .unwrap();
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    fn parallel_timer_test_across_wf_bridge(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let run_id = "fake_run_id";
        let timer_1_id = "timer1";
        let timer_2_id = "timer2";

        let mut t = canned_histories::parallel_timer(timer_1_id, timer_2_id);
        let core = build_fake_core(wfid, run_id, &mut t, hist_batches);

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::StartWorkflow(_)),
            }]
        );
        assert!(core.workflow_machines.exists(run_id));

        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
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
            task_tok,
        ))
        .unwrap();

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
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
        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![CompleteWorkflowExecution { result: vec![] }.into()],
            task_tok,
        ))
        .unwrap();
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    fn timer_cancel_test_across_wf_bridge(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let run_id = "fake_run_id";
        let timer_id = "wait_timer";
        let cancel_timer_id = "cancel_timer";

        let mut t = canned_histories::cancel_timer(timer_id, cancel_timer_id);
        let core = build_fake_core(wfid, run_id, &mut t, hist_batches);

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::StartWorkflow(_)),
            }]
        );
        assert!(core.workflow_machines.exists(run_id));

        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
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
            task_tok,
        ))
        .unwrap();

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(_)),
            }]
        );
        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![
                CancelTimer {
                    timer_id: cancel_timer_id.to_string(),
                }
                .into(),
                CompleteWorkflowExecution { result: vec![] }.into(),
            ],
            task_tok,
        ))
        .unwrap();
    }

    #[rstest(single_timer_setup(&[1]))]
    fn after_shutdown_server_is_not_polled(single_timer_setup: FakeCore) {
        let res = single_timer_setup.poll_task(TASK_Q.into_wf_poll()).unwrap();
        assert_eq!(res.get_wf_jobs().len(), 1);

        single_timer_setup.shutdown();
        assert_matches!(
            single_timer_setup
                .poll_task(TASK_Q.into_wf_poll())
                .unwrap_err(),
            CoreError::ShuttingDown
        );
    }

    #[test]
    fn workflow_update_random_seed_on_workflow_reset() {
        let wfid = "fake_wf_id";
        let run_id = "CA733AB0-8133-45F6-A4C1-8D375F61AE8B";
        let original_run_id = "86E39A5F-AE31-4626-BDFE-398EE072D156";
        let timer_1_id = "timer1";

        let mut t =
            canned_histories::workflow_fails_with_reset_after_timer(timer_1_id, original_run_id);
        let core = build_fake_core(wfid, run_id, &mut t, &[2]);

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        let randomness_seed_from_start: u64;
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::StartWorkflow(
                StartWorkflow{randomness_seed, ..}
                )),
            }] => {
            randomness_seed_from_start = *randomness_seed;
            }
        );
        assert!(core.workflow_machines.exists(run_id));

        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![StartTimer {
                timer_id: timer_1_id.to_string(),
                ..Default::default()
            }
            .into()],
            task_tok,
        ))
        .unwrap();

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(_),),
            },
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::UpdateRandomSeed(UpdateRandomSeed{randomness_seed})),
            }] => {
                assert_ne!(randomness_seed_from_start, *randomness_seed)
            }
        );
        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![CompleteWorkflowExecution { result: vec![] }.into()],
            task_tok,
        ))
        .unwrap();
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    fn cancel_timer_before_sent_wf_bridge(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let run_id = "fake_run_id";
        let cancel_timer_id = "cancel_timer";

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let core = build_fake_core(wfid, run_id, &mut t, hist_batches);

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::StartWorkflow(_)),
            }]
        );

        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
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
                CompleteWorkflowExecution { result: vec![] }.into(),
            ],
            task_tok,
        ))
        .unwrap();
        if hist_batches.len() > 1 {
            core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        }
    }

    #[test]
    fn complete_activation_with_failure() {
        let wfid = "fake_wf_id";
        let timer_id = "timer";

        let mut t = canned_histories::workflow_fails_with_failure_after_timer(timer_id);
        let mut core = build_fake_core(wfid, RUN_ID, &mut t, &[2, 3]);
        // Need to create an expectation that we will call a failure completion
        Arc::get_mut(&mut core.server_gateway)
            .unwrap()
            .expect_fail_workflow_task()
            .times(1)
            .returning(|_, _, _| Ok(RespondWorkflowTaskFailedResponse {}));

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![StartTimer {
                timer_id: timer_id.to_string(),
                ..Default::default()
            }
            .into()],
            res.task_token,
        ))
        .unwrap();

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        core.complete_task(TaskCompletion::fail(
            res.task_token,
            UserCodeFailure {
                message: "oh noooooooo".to_string(),
                ..Default::default()
            },
        ))
        .unwrap();

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::StartWorkflow(_)),
            }]
        );
        // Need to re-issue the start timer command (we are replaying)
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![StartTimer {
                timer_id: timer_id.to_string(),
                ..Default::default()
            }
            .into()],
            res.task_token,
        ))
        .unwrap();
        // Now we may complete the workflow
        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(_)),
            }]
        );
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![CompleteWorkflowExecution { result: vec![] }.into()],
            res.task_token,
        ))
        .unwrap();
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    fn simple_timer_fail_wf_execution(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let run_id = "fake_run_id";
        let timer_id = "timer1";

        let mut t = canned_histories::single_timer(timer_id);
        let core = build_fake_core(wfid, run_id, &mut t, hist_batches);

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![StartTimer {
                timer_id: timer_id.to_string(),
                ..Default::default()
            }
            .into()],
            res.task_token,
        ))
        .unwrap();

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![FailWorkflowExecution {
                failure: Some(UserCodeFailure {
                    message: "I'm ded".to_string(),
                    ..Default::default()
                }),
            }
            .into()],
            res.task_token,
        ))
        .unwrap();
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    fn two_signals(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let run_id = "fake_run_id";

        let mut t = canned_histories::two_signals("sig1", "sig2");
        let core = build_fake_core(wfid, run_id, &mut t, hist_batches);

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        // Task is completed with no commands
        core.complete_task(TaskCompletion::ok_from_api_attrs(vec![], res.task_token))
            .unwrap();

        let res = core.poll_task(TASK_Q.into_wf_poll()).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
                },
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
                }
            ]
        );
    }
}
