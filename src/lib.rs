#![warn(missing_docs)] // error if there are missing docs

//! This crate provides a basis for creating new Temporal SDKs without completely starting from
//! scratch

#[cfg(test)]
#[macro_use]
pub extern crate assert_matches;
#[macro_use]
extern crate tracing;

pub mod protos;

mod machines;
mod pollers;
mod protosext;
mod workflow;

pub use pollers::{ServerGateway, ServerGatewayApis, ServerGatewayOptions};
pub use url::Url;

use crate::{
    machines::{InconvertibleCommandError, WFCommand},
    protos::{
        coresdk::{
            complete_task_req::Completion, wf_activation_completion::Status, CompleteTaskReq, Task,
            WfActivationCompletion, WfActivationSuccess,
        },
        temporal::api::{
            common::v1::WorkflowExecution, workflowservice::v1::PollWorkflowTaskQueueResponse,
        },
    },
    protosext::HistoryInfoError,
    workflow::{WfManagerProtected, WorkflowManager},
};
use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use std::{convert::TryInto, sync::mpsc::SendError, sync::Arc};
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
    fn complete_task(&self, req: CompleteTaskReq) -> Result<()>;

    /// Returns an instance of ServerGateway.
    fn server_gateway(&self) -> Result<Arc<dyn ServerGatewayApis>>;
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
    let _ = env_logger::try_init();
    let runtime = Runtime::new().map_err(CoreError::TokioInitError)?;
    // Initialize server client
    let work_provider = runtime.block_on(opts.gateway_opts.connect())?;

    Ok(CoreSDK {
        runtime,
        server_gateway: Arc::new(work_provider),
        workflow_machines: Default::default(),
        workflow_task_tokens: Default::default(),
        pending_activations: Default::default(),
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
    workflow_machines: DashMap<String, WorkflowManager>,
    /// Maps task tokens to workflow run ids
    workflow_task_tokens: DashMap<Vec<u8>, String>,

    /// Workflows that are currently under replay will queue their run ID here, indicating that
    /// there are more workflow tasks / activations to be performed.
    pending_activations: SegQueue<String>,
}

impl<WP> Core for CoreSDK<WP>
where
    WP: ServerGatewayApis + Send + Sync,
{
    #[instrument(skip(self))]
    fn poll_task(&self, task_queue: &str) -> Result<Task> {
        // We must first check if there are pending workflow tasks for workflows that are currently
        // replaying
        if let Some(run_id) = self.pending_activations.pop() {
            dbg!(&run_id);
            let (activation, more_tasks) =
                self.access_machine(&run_id, |mgr| mgr.get_next_activation())?;
            if more_tasks {
                self.pending_activations.push(run_id);
            }
            return Ok(Task {
                // TODO: Set this properly
                task_token: vec![],
                variant: activation.map(Into::into),
            });
        }

        // This will block forever in the event there is no work from the server
        let work = self
            .runtime
            .block_on(self.server_gateway.poll_workflow_task(task_queue))?;
        let run_id = match &work.workflow_execution {
            Some(we) => {
                self.instantiate_workflow_if_needed(we);
                we.run_id.clone()
            }
            None => return Err(CoreError::BadDataFromWorkProvider(work)),
        };
        let history = if let Some(hist) = work.history {
            hist
        } else {
            return Err(CoreError::BadDataFromWorkProvider(work));
        };

        event!(
            Level::DEBUG,
            msg = "Received workflow task from server",
            ?work.task_token
        );

        // Correlate task token w/ run ID
        self.workflow_task_tokens
            .insert(work.task_token.clone(), run_id.clone());

        let (activation, more_tasks) =
            self.access_machine(&run_id, |mgr| mgr.feed_history_from_server(history))?;

        if more_tasks {
            dbg!("More tasks!");
            self.pending_activations.push(run_id);
        }

        Ok(Task {
            task_token: work.task_token,
            variant: activation.map(Into::into),
        })
    }

    #[instrument(skip(self))]
    fn complete_task(&self, req: CompleteTaskReq) -> Result<()> {
        match req {
            CompleteTaskReq {
                task_token,
                completion:
                    Some(Completion::Workflow(WfActivationCompletion {
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
                        self.access_machine(&run_id, |mgr| {
                            let commands = mgr.machines.get_commands();
                            self.runtime.block_on(
                                self.server_gateway
                                    .complete_workflow_task(task_token, commands),
                            )
                        })?;
                    }
                    Status::Failed(_) => {}
                }
                Ok(())
            }
            CompleteTaskReq {
                completion: Some(Completion::Activity(_)),
                ..
            } => {
                unimplemented!()
            }
            _ => Err(CoreError::MalformedCompletion(req)),
        }
    }

    fn server_gateway(&self) -> Result<Arc<dyn ServerGatewayApis>> {
        Ok(self.server_gateway.clone())
    }
}

impl<WP: ServerGatewayApis> CoreSDK<WP> {
    fn instantiate_workflow_if_needed(&self, workflow_execution: &WorkflowExecution) {
        if self
            .workflow_machines
            .contains_key(&workflow_execution.run_id)
        {
            return;
        }
        self.workflow_machines.insert(
            workflow_execution.run_id.clone(),
            WorkflowManager::new(workflow_execution),
        );
    }

    /// Feed commands from the lang sdk into the appropriate workflow bridge
    fn push_lang_commands(&self, run_id: &str, success: WfActivationSuccess) -> Result<()> {
        // Convert to wf commands
        let cmds = success
            .commands
            .into_iter()
            .map(|c| c.try_into().map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        self.access_machine(run_id, |mgr| {
            mgr.command_sink.send(cmds)?;
            mgr.machines.event_loop();
            Ok(())
        })?;
        Ok(())
    }

    /// Use a closure to access the machines for a workflow run, handles locking and missing
    /// machines.
    fn access_machine<F, Fout>(&self, run_id: &str, mutator: F) -> Result<Fout>
    where
        F: FnOnce(&mut WfManagerProtected) -> Result<Fout>,
    {
        if let Some(mut machines) = self.workflow_machines.get_mut(run_id) {
            let mut mgr = machines.value_mut().lock()?;
            mutator(&mut mgr)
        } else {
            Err(CoreError::MissingMachines(run_id.to_string()))
        }
    }
}

/// The error type returned by interactions with [Core]
#[derive(thiserror::Error, Debug, displaydoc::Display)]
#[allow(clippy::large_enum_variant)]
// NOTE: Docstrings take the place of #[error("xxxx")] here b/c of displaydoc
pub enum CoreError {
    /// Unknown service error
    Unknown,
    /// No tasks to perform for now
    NoWork,
    /// Poll response from server was malformed: {0:?}
    BadDataFromWorkProvider(PollWorkflowTaskQueueResponse),
    /// Lang SDK sent us a malformed completion: {0:?}
    MalformedCompletion(CompleteTaskReq),
    /// Error buffering commands
    CantSendCommands(#[from] SendError<Vec<WFCommand>>),
    /// Couldn't interpret command from <lang>
    UninterprableCommand(#[from] InconvertibleCommandError),
    /// Underlying error in history processing
    UnderlyingHistError(#[from] HistoryInfoError),
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
    use crate::{
        machines::test_help::{build_fake_core, TestHistoryBuilder},
        protos::{
            coresdk::{wf_activation_job, TimerFiredTaskAttributes, WfActivationJob},
            temporal::api::{
                command::v1::{
                    CompleteWorkflowExecutionCommandAttributes, StartTimerCommandAttributes,
                },
                enums::v1::EventType,
                history::v1::{history_event, TimerFiredEventAttributes},
            },
        },
    };

    #[test]
    fn timer_test_across_wf_bridge() {
        let wfid = "fake_wf_id";
        let run_id = "fake_run_id";
        let timer_id = "fake_timer".to_string();
        let task_queue = "test-task-queue";

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_workflow_task();
        let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
        t.add(
            EventType::TimerFired,
            history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
                started_event_id: timer_started_event_id,
                timer_id: timer_id.clone(),
            }),
        );
        t.add_workflow_task_scheduled_and_started();
        /*
           1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
           2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
           3: EVENT_TYPE_WORKFLOW_TASK_STARTED
           ---
           4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
           5: EVENT_TYPE_TIMER_STARTED
           6: EVENT_TYPE_TIMER_FIRED
           7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
           8: EVENT_TYPE_WORKFLOW_TASK_STARTED
           ---
        */
        let core = build_fake_core(wfid, run_id, &mut t, &[1, 2]);

        let res = core.poll_task(task_queue).unwrap();
        // TODO: uggo
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                attributes: Some(wf_activation_job::Attributes::StartWorkflow(_)),
            }]
        );
        assert!(core.workflow_machines.get(run_id).is_some());

        let task_tok = res.task_token;
        core.complete_task(CompleteTaskReq::ok_from_api_attrs(
            vec![StartTimerCommandAttributes {
                timer_id,
                ..Default::default()
            }
            .into()],
            task_tok,
        ))
        .unwrap();

        dbg!("Second poll");
        let res = core.poll_task(task_queue).unwrap();
        // TODO: uggo
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                attributes: Some(wf_activation_job::Attributes::TimerFired(_)),
            }]
        );
        let task_tok = res.task_token;
        core.complete_task(CompleteTaskReq::ok_from_api_attrs(
            vec![CompleteWorkflowExecutionCommandAttributes { result: None }.into()],
            task_tok,
        ))
        .unwrap();
    }

    #[test]
    fn parallel_timer_test_across_wf_bridge() {
        let wfid = "fake_wf_id";
        let run_id = "fake_run_id";
        let timer_1_id = "timer1".to_string();
        let timer_2_id = "timer2".to_string();
        let task_queue = "test-task-queue";

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_workflow_task();
        let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
        let timer_2_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
        t.add(
            EventType::TimerFired,
            history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
                started_event_id: timer_started_event_id,
                timer_id: timer_1_id.clone(),
            }),
        );
        t.add(
            EventType::TimerFired,
            history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
                started_event_id: timer_2_started_event_id,
                timer_id: timer_2_id.clone(),
            }),
        );
        t.add_workflow_task_scheduled_and_started();
        /*
           1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
           2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
           3: EVENT_TYPE_WORKFLOW_TASK_STARTED
           ---
           4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
           5: EVENT_TYPE_TIMER_STARTED
           6: EVENT_TYPE_TIMER_STARTED
           7: EVENT_TYPE_TIMER_FIRED
           8: EVENT_TYPE_TIMER_FIRED
           9: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
           10: EVENT_TYPE_WORKFLOW_TASK_STARTED
           ---
        */
        let core = build_fake_core(wfid, run_id, &mut t, &[1, 2]);

        let res = core.poll_task(task_queue).unwrap();
        // TODO: uggo
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                attributes: Some(wf_activation_job::Attributes::StartWorkflow(_)),
            }]
        );
        assert!(core.workflow_machines.get(run_id).is_some());

        let task_tok = res.task_token;
        core.complete_task(CompleteTaskReq::ok_from_api_attrs(
            vec![
                StartTimerCommandAttributes {
                    timer_id: timer_1_id.clone(),
                    ..Default::default()
                }
                .into(),
                StartTimerCommandAttributes {
                    timer_id: timer_2_id.clone(),
                    ..Default::default()
                }
                .into(),
            ],
            task_tok,
        ))
        .unwrap();

        let res = core.poll_task(task_queue).unwrap();
        // TODO: uggo
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
        core.complete_task(CompleteTaskReq::ok_from_api_attrs(
            vec![CompleteWorkflowExecutionCommandAttributes { result: None }.into()],
            task_tok,
        ))
        .unwrap();
    }

    #[test]
    fn single_timer_whole_replay_test_across_wf_bridge() {
        let s = span!(Level::DEBUG, "Test start", t = "bridge");
        let _enter = s.enter();

        let wfid = "fake_wf_id";
        let run_id = "fake_run_id";
        let timer_1_id = "timer1".to_string();
        let task_queue = "test-task-queue";

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_workflow_task();
        let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
        t.add(
            EventType::TimerFired,
            history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
                started_event_id: timer_started_event_id,
                timer_id: timer_1_id.clone(),
            }),
        );
        t.add_workflow_task_scheduled_and_started();
        // NOTE! What makes this a replay test is the server only responds with *one* batch here.
        // So, server is polled once, but lang->core interactions look just like non-replay test.
        let core = build_fake_core(wfid, run_id, &mut t, &[2]);

        let res = core.poll_task(task_queue).unwrap();
        // TODO: Not the right expectation -- is timer fired?
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                attributes: Some(wf_activation_job::Attributes::StartWorkflow(_)),
            }]
        );
        assert!(core.workflow_machines.get(run_id).is_some());

        let task_tok = res.task_token;
        core.complete_task(CompleteTaskReq::ok_from_api_attrs(
            vec![StartTimerCommandAttributes {
                timer_id: timer_1_id,
                ..Default::default()
            }
            .into()],
            task_tok,
        ))
        .unwrap();

        let res = core.poll_task(task_queue).unwrap();
        // TODO: uggo
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                attributes: Some(wf_activation_job::Attributes::TimerFired(_)),
            }]
        );
        let task_tok = res.task_token;
        core.complete_task(CompleteTaskReq::ok_from_api_attrs(
            vec![CompleteWorkflowExecutionCommandAttributes { result: None }.into()],
            task_tok,
        ))
        .unwrap();
    }
}
