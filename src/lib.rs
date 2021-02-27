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
mod pollers;
mod protosext;
mod workflow;

#[cfg(test)]
mod test_help;

pub use pollers::{ServerGateway, ServerGatewayApis, ServerGatewayOptions};
pub use url::Url;

use crate::machines::WFMachinesError;
use crate::{
    machines::{InconvertibleCommandError, WFCommand},
    protos::{
        coresdk::{
            task_completion, wf_activation_completion::Status, Task, TaskCompletion,
            WfActivationCompletion, WfActivationSuccess,
        },
        temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse,
    },
    protosext::HistoryInfoError,
    workflow::{NextWfActivation, WorkflowConcurrencyManager},
};
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use std::{
    convert::TryInto,
    fmt::Debug,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::SendError,
        Arc,
    },
};
use tokio::runtime::Runtime;
use tonic::codegen::http::uri::InvalidUri;
use tracing::Level;

/// A result alias having [CoreError] as the error type
pub type Result<T, E = CoreError> = std::result::Result<T, E>;

/// This trait is the primary way by which language specific SDKs interact with the core SDK. It is
/// expected that only one instance of an implementation will exist for the lifetime of the
/// worker(s) using it.
pub trait Core: Send + Sync {
    /// Ask the core for some work, returning a [Task], which will eventually contain either a
    /// [protos::coresdk::WfActivation] or an [protos::coresdk::ActivityTask]. It is then the
    /// language SDK's responsibility to call the appropriate code with the provided inputs.
    ///
    /// TODO: Examples
    fn poll_task(&self, task_queue: &str) -> Result<Task>;

    /// Tell the core that some work has been completed - whether as a result of running workflow
    /// code or executing an activity.
    fn complete_task(&self, req: TaskCompletion) -> Result<()>;

    /// Returns an instance of ServerGateway.
    fn server_gateway(&self) -> Result<Arc<dyn ServerGatewayApis>>;

    /// Eventually ceases all polling of the server. [Core::poll_task] should be called until it
    /// returns [CoreError::ShuttingDown] to ensure that any workflows which are still undergoing
    /// replay have an opportunity to finish. This means that the lang sdk will need to call
    /// [Core::complete_task] for those workflows until they are done. At that point, the lang
    /// SDK can end the process, or drop the [Core] instance, which will close the connection.
    fn shutdown(&self) -> Result<()>;
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
    core_tracing::tracing_init();
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

/// Type of task queue to poll.
pub enum TaskQueue {
    /// Workflow task
    Workflow(String),
    /// Activity task
    _Activity(String),
}

struct CoreSDK<WP>
where
    WP: ServerGatewayApis + 'static,
{
    runtime: Runtime,
    /// Provides work in the form of responses the server would send from polling task Qs
    server_gateway: Arc<WP>,
    /// Key is run id
    workflow_machines: WorkflowConcurrencyManager,
    /// Maps task tokens to workflow run ids
    workflow_task_tokens: DashMap<Vec<u8>, String>,

    /// Workflows that are currently under replay will queue their run ID here, indicating that
    /// there are more workflow tasks / activations to be performed.
    pending_activations: SegQueue<PendingActivation>,

    /// Has shutdown been called?
    shutdown_requested: AtomicBool,
}

#[derive(Debug)]
struct PendingActivation {
    run_id: String,
    task_token: Vec<u8>,
}

impl<WP> Core for CoreSDK<WP>
where
    WP: ServerGatewayApis + Send + Sync,
{
    #[instrument(skip(self))]
    fn poll_task(&self, task_queue: &str) -> Result<Task> {
        // We must first check if there are pending workflow tasks for workflows that are currently
        // replaying, and issue those tasks before bothering the server.
        if let Some(pa) = self.pending_activations.pop() {
            event!(Level::DEBUG, msg = "Applying pending activations", ?pa);
            let next_activation = self
                .workflow_machines
                .access(&pa.run_id, |mgr| mgr.get_next_activation())?;
            let task_token = pa.task_token.clone();
            if next_activation.more_activations_needed {
                self.pending_activations.push(pa);
            }
            return Ok(Task {
                task_token,
                variant: next_activation.activation.map(Into::into),
            });
        }

        if self.shutdown_requested.load(Ordering::SeqCst) {
            return Err(CoreError::ShuttingDown);
        }

        // This will block forever in the event there is no work from the server
        let work = self
            .runtime
            .block_on(self.server_gateway.poll_workflow_task(task_queue))?;
        let task_token = work.task_token.clone();
        event!(
            Level::DEBUG,
            msg = "Received workflow task from server",
            ?task_token
        );

        let (next_activation, run_id) = self.instantiate_or_update_workflow(work)?;

        if next_activation.more_activations_needed {
            self.pending_activations.push(PendingActivation {
                run_id,
                task_token: task_token.clone(),
            });
        }

        Ok(Task {
            task_token,
            variant: next_activation.activation.map(Into::into),
        })
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
                    .ok_or_else(|| CoreError::NothingFoundForTaskToken(task_token.clone()))?;
                match wfstatus {
                    Status::Successful(success) => {
                        self.push_lang_commands(&run_id, success)?;
                        let commands = self
                            .workflow_machines
                            .access(&run_id, |mgr| Ok(mgr.machines.get_commands()))?;
                        self.runtime.block_on(
                            self.server_gateway
                                .complete_workflow_task(task_token, commands),
                        )?;
                    }
                    Status::Failed(_) => {}
                }
                Ok(())
            }
            TaskCompletion {
                variant: Some(task_completion::Variant::Activity(_)),
                ..
            } => unimplemented!(),
            _ => Err(CoreError::MalformedCompletion(req)),
        }
    }

    fn server_gateway(&self) -> Result<Arc<dyn ServerGatewayApis>> {
        Ok(self.server_gateway.clone())
    }

    fn shutdown(&self) -> Result<(), CoreError> {
        self.shutdown_requested.store(true, Ordering::SeqCst);
        self.workflow_machines.shutdown();
        Ok(())
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
        if let PollWorkflowTaskQueueResponse {
            task_token,
            workflow_execution: Some(workflow_execution),
            ..
        } = &poll_wf_resp
        {
            let run_id = workflow_execution.run_id.clone();
            // Correlate task token w/ run ID
            self.workflow_task_tokens
                .insert(task_token.clone(), run_id.clone());

            let activation = self
                .workflow_machines
                .create_or_update(&run_id, poll_wf_resp)?;
            Ok((activation, run_id))
        } else {
            Err(CoreError::BadDataFromWorkProvider(poll_wf_resp))
        }
    }

    /// Feed commands from the lang sdk into the appropriate workflow bridge
    fn push_lang_commands(&self, run_id: &str, success: WfActivationSuccess) -> Result<()> {
        // Convert to wf commands
        let cmds = success
            .commands
            .into_iter()
            .map(|c| c.try_into().map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        self.workflow_machines.access(run_id, |mgr| {
            mgr.command_sink.send(cmds)?;
            mgr.machines.iterate_machines()?;
            Ok(())
        })?;
        Ok(())
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
    /// Lang SDK sent us a malformed completion: {0:?}
    MalformedCompletion(TaskCompletion),
    /// Error buffering commands
    CantSendCommands(#[from] SendError<Vec<WFCommand>>),
    /// Couldn't interpret command from <lang>
    UninterpretableCommand(#[from] InconvertibleCommandError),
    /// Underlying error in history processing
    UnderlyingHistError(#[from] HistoryInfoError),
    /// Underlying error in state machines
    UnderlyingMachinesError(#[from] WFMachinesError),
    /// Task token had nothing associated with it: {0:?}
    NothingFoundForTaskToken(Vec<u8>),
    /// Error calling the service: {0:?}
    TonicError(#[from] tonic::Status),
    /// Server connection error: {0:?}
    TonicTransportError(#[from] tonic::transport::Error),
    /// Failed to initialize tokio runtime: {0:?}
    TokioInitError(std::io::Error),
    /// Invalid URI: {0:?}
    InvalidUri(#[from] InvalidUri),
    /// A mutex was poisoned: {0:?}
    LockPoisoned(String),
    /// State machines are missing for the workflow with run id {0}!
    MissingMachines(String),
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::machines::test_help::TestHistoryBuilder;
    use crate::protos::temporal::api::enums::v1::EventType;
    use crate::{
        machines::test_help::{build_fake_core, FakeCore},
        protos::{
            coresdk::{
                wf_activation_job, RandomSeedUpdatedAttributes, StartWorkflowTaskAttributes,
                TaskCompletion, TimerFiredTaskAttributes, WfActivationJob,
            },
            temporal::api::command::v1::{
                CancelTimerCommandAttributes, CompleteWorkflowExecutionCommandAttributes,
                StartTimerCommandAttributes,
            },
        },
        test_help::canned_histories,
    };
    use rstest::{fixture, rstest};

    const TASK_Q: &str = "test-task-queue";
    const RUN_ID: &str = "fake_run_id";

    #[fixture(hist_batches=&[])]
    fn single_timer_setup(hist_batches: &[usize]) -> FakeCore {
        let wfid = "fake_wf_id";

        let mut t = canned_histories::single_timer("fake_timer");
        let core = build_fake_core(wfid, RUN_ID, &mut t, hist_batches);
        core
    }

    #[rstest(core,
              case::incremental(single_timer_setup(&[1, 2])),
              case::replay(single_timer_setup(&[2]))
    )]
    fn single_timer_test_across_wf_bridge(core: FakeCore) {
        let res = core.poll_task(TASK_Q).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                attributes: Some(wf_activation_job::Attributes::StartWorkflow(_)),
            }]
        );
        assert!(core.workflow_machines.exists(RUN_ID));

        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![StartTimerCommandAttributes {
                timer_id: "fake_timer".to_string(),
                ..Default::default()
            }
            .into()],
            task_tok,
        ))
        .unwrap();

        let res = core.poll_task(TASK_Q).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                attributes: Some(wf_activation_job::Attributes::TimerFired(_)),
            }]
        );
        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![CompleteWorkflowExecutionCommandAttributes { result: None }.into()],
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
        let task_queue = "test-task-queue";

        let mut t = canned_histories::parallel_timer(timer_1_id, timer_2_id);
        let core = build_fake_core(wfid, run_id, &mut t, hist_batches);

        let res = core.poll_task(task_queue).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                attributes: Some(wf_activation_job::Attributes::StartWorkflow(_)),
            }]
        );
        assert!(core.workflow_machines.exists(run_id));

        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![
                StartTimerCommandAttributes {
                    timer_id: timer_1_id.to_string(),
                    ..Default::default()
                }
                .into(),
                StartTimerCommandAttributes {
                    timer_id: timer_2_id.to_string(),
                    ..Default::default()
                }
                .into(),
            ],
            task_tok,
        ))
        .unwrap();

        let res = core.poll_task(task_queue).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [
                WfActivationJob {
                    attributes: Some(wf_activation_job::Attributes::TimerFired(
                        TimerFiredTaskAttributes { timer_id: t1_id }
                    )),
                },
                WfActivationJob {
                    attributes: Some(wf_activation_job::Attributes::TimerFired(
                        TimerFiredTaskAttributes { timer_id: t2_id }
                    )),
                }
            ] => {
                assert_eq!(t1_id, &timer_1_id);
                assert_eq!(t2_id, &timer_2_id);
            }
        );
        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![CompleteWorkflowExecutionCommandAttributes { result: None }.into()],
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
        let task_queue = "test-task-queue";

        let mut t = canned_histories::cancel_timer(timer_id, cancel_timer_id);
        let core = build_fake_core(wfid, run_id, &mut t, hist_batches);

        let res = core.poll_task(task_queue).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                attributes: Some(wf_activation_job::Attributes::StartWorkflow(_)),
            }]
        );
        assert!(core.workflow_machines.exists(run_id));

        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![
                StartTimerCommandAttributes {
                    timer_id: cancel_timer_id.to_string(),
                    ..Default::default()
                }
                .into(),
                StartTimerCommandAttributes {
                    timer_id: timer_id.to_string(),
                    ..Default::default()
                }
                .into(),
            ],
            task_tok,
        ))
        .unwrap();

        let res = core.poll_task(task_queue).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                attributes: Some(wf_activation_job::Attributes::TimerFired(_)),
            }]
        );
        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![
                CancelTimerCommandAttributes {
                    timer_id: cancel_timer_id.to_string(),
                }
                .into(),
                CompleteWorkflowExecutionCommandAttributes { result: None }.into(),
            ],
            task_tok,
        ))
        .unwrap();
    }

    #[rstest(single_timer_setup(&[1]))]
    fn after_shutdown_server_is_not_polled(single_timer_setup: FakeCore) {
        let res = single_timer_setup.poll_task(TASK_Q).unwrap();
        assert_eq!(res.get_wf_jobs().len(), 1);

        single_timer_setup.shutdown().unwrap();
        assert_matches!(
            single_timer_setup.poll_task(TASK_Q).unwrap_err(),
            CoreError::ShuttingDown
        );
    }

    #[test]
    fn workflow_update_random_seed_on_workflow_reset() {
        let s = span!(Level::DEBUG, "Test start", t = "bridge");
        let _enter = s.enter();

        let wfid = "fake_wf_id";
        let run_id = "CA733AB0-8133-45F6-A4C1-8D375F61AE8B";
        let original_run_id = "86E39A5F-AE31-4626-BDFE-398EE072D156";
        let timer_1_id = "timer1";
        let task_queue = "test-task-queue";

        let mut t = canned_histories::workflow_fails_after_timer(timer_1_id, original_run_id);
        let core = build_fake_core(wfid, run_id, &mut t, &[2]);

        let res = core.poll_task(task_queue).unwrap();
        let randomness_seed_from_start: u64;
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                attributes: Some(wf_activation_job::Attributes::StartWorkflow(
                StartWorkflowTaskAttributes{randomness_seed, ..}
                )),
            }] => {
            randomness_seed_from_start = *randomness_seed;
            }
        );
        assert!(core.workflow_machines.exists(run_id));

        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![StartTimerCommandAttributes {
                timer_id: timer_1_id.to_string(),
                ..Default::default()
            }
            .into()],
            task_tok,
        ))
        .unwrap();

        let res = core.poll_task(task_queue).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                attributes: Some(wf_activation_job::Attributes::TimerFired(_),),
            },
            WfActivationJob {
                attributes: Some(wf_activation_job::Attributes::RandomSeedUpdated(RandomSeedUpdatedAttributes{randomness_seed})),
            }] => {
                assert_ne!(randomness_seed_from_start, *randomness_seed)
            }
        );
        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![CompleteWorkflowExecutionCommandAttributes { result: None }.into()],
            task_tok,
        ))
        .unwrap();
    }

    #[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
    fn cancel_timer_before_sent_wf_bridge(hist_batches: &[usize]) {
        let wfid = "fake_wf_id";
        let run_id = "fake_run_id";
        let cancel_timer_id = "cancel_timer";
        let task_queue = "test-task-queue";

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let core = build_fake_core(wfid, run_id, &mut t, hist_batches);

        let res = core.poll_task(task_queue).unwrap();
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                attributes: Some(wf_activation_job::Attributes::StartWorkflow(_)),
            }]
        );

        let task_tok = res.task_token;
        core.complete_task(TaskCompletion::ok_from_api_attrs(
            vec![
                StartTimerCommandAttributes {
                    timer_id: cancel_timer_id.to_string(),
                    ..Default::default()
                }
                .into(),
                CancelTimerCommandAttributes {
                    timer_id: cancel_timer_id.to_string(),
                    ..Default::default()
                }
                .into(),
                CompleteWorkflowExecutionCommandAttributes { result: None }.into(),
            ],
            task_tok,
        ))
        .unwrap();
        if hist_batches.len() > 1 {
            core.poll_task(task_queue).unwrap();
        }
    }
}
