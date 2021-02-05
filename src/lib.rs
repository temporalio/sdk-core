#![warn(missing_docs)] // error if there are missing docs

//! This crate provides a basis for creating new Temporal SDKs without completely starting from
//! scratch

#[cfg(test)]
#[macro_use]
pub extern crate assert_matches;
#[macro_use]
extern crate tracing;

mod machines;
mod pollers;
pub mod protos;
mod protosext;

pub use pollers::{ServerGateway, ServerGatewayOptions};
pub use url::Url;

use crate::pollers::ServerGatewayApis;
use crate::protos::temporal::api::workflowservice::v1::StartWorkflowExecutionResponse;
use crate::{
    machines::{
        ActivationListener, DrivenWorkflow, InconvertibleCommandError, ProtoCommand, WFCommand,
        WorkflowMachines,
    },
    protos::{
        coresdk::{
            complete_task_req::Completion, wf_activation_completion::Status, CompleteTaskReq, Task,
            WfActivationCompletion, WfActivationSuccess,
        },
        temporal::api::{
            common::v1::WorkflowExecution,
            history::v1::{
                WorkflowExecutionCanceledEventAttributes, WorkflowExecutionSignaledEventAttributes,
                WorkflowExecutionStartedEventAttributes,
            },
            workflowservice::v1::{
                PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedResponse,
            },
        },
    },
    protosext::{HistoryInfo, HistoryInfoError},
};
use dashmap::DashMap;
use std::sync::Arc;
use std::{
    convert::TryInto,
    sync::mpsc::{self, Receiver, SendError, Sender},
};
use tokio::runtime::Runtime;
use tonic::codegen::http::uri::InvalidUri;

/// A result alias having [CoreError] as the error type
pub type Result<T, E = CoreError> = std::result::Result<T, E>;

/// This trait is the primary way by which language specific SDKs interact with the core SDK. It is
/// expected that only one instance of an implementation will exist for the lifetime of the
/// worker(s) using it.
pub trait Core {
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
    let runtime = Runtime::new().map_err(CoreError::TokioInitError)?;
    // Initialize server client
    let work_provider = runtime.block_on(opts.gateway_opts.connect())?;

    Ok(CoreSDK {
        runtime,
        server_gateway: Arc::new(work_provider),
        workflow_machines: Default::default(),
        workflow_task_tokens: Default::default(),
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
    workflow_machines: DashMap<String, (WorkflowMachines, Sender<Vec<WFCommand>>)>,
    /// Maps task tokens to workflow run ids
    workflow_task_tokens: DashMap<Vec<u8>, String>,
}

impl<WP> Core for CoreSDK<WP>
where
    WP: ServerGatewayApis,
{
    #[instrument(skip(self))]
    fn poll_task(&self, task_queue: &str) -> Result<Task, CoreError> {
        // This will block forever in the event there is no work from the server
        let work = self
            .runtime
            .block_on(self.server_gateway.poll_workflow_task(task_queue))?;
        let run_id = match &work.workflow_execution {
            Some(we) => {
                self.instantiate_workflow_if_needed(we);
                we.run_id.clone()
            }
            // TODO: Appropriate error
            None => return Err(CoreError::Unknown),
        };
        let history = if let Some(hist) = work.history {
            hist
        } else {
            return Err(CoreError::BadDataFromWorkProvider(work));
        };

        // Correlate task token w/ run ID
        self.workflow_task_tokens
            .insert(work.task_token.clone(), run_id.clone());

        // We pass none since we want to apply all the history we just got.
        // Will need to change a bit once we impl caching.
        let hist_info = HistoryInfo::new_from_history(&history, None)?;
        let activation = if let Some(mut machines) = self.workflow_machines.get_mut(&run_id) {
            hist_info.apply_history_events(&mut machines.value_mut().0)?;
            machines.0.get_wf_activation()
        } else {
            //err
            unimplemented!()
        };

        Ok(Task {
            task_token: work.task_token,
            variant: activation.map(Into::into),
        })
    }

    #[instrument(skip(self))]
    fn complete_task(&self, req: CompleteTaskReq) -> Result<(), CoreError> {
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
                        if let Some(mut machines) = self.workflow_machines.get_mut(&run_id) {
                            let commands = machines.0.get_commands();
                            self.runtime.block_on(
                                self.server_gateway
                                    .complete_workflow_task(task_token, commands),
                            )?;
                        }
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
        let (wfb, cmd_sink) = WorkflowBridge::new();
        let state_machines = WorkflowMachines::new(
            workflow_execution.workflow_id.clone(),
            workflow_execution.run_id.clone(),
            Box::new(wfb),
        );
        self.workflow_machines.insert(
            workflow_execution.run_id.clone(),
            (state_machines, cmd_sink),
        );
    }

    /// Feed commands from the lang sdk into the appropriate workflow bridge
    fn push_lang_commands(
        &self,
        run_id: &str,
        success: WfActivationSuccess,
    ) -> Result<(), CoreError> {
        // Convert to wf commands
        let cmds = success
            .commands
            .into_iter()
            .map(|c| c.try_into().map_err(Into::into))
            .collect::<Result<Vec<_>>>()?;
        if let Some(mut machine) = self.workflow_machines.get_mut(run_id) {
            machine.1.send(cmds)?;
            machine.0.event_loop();
        } else {
            // TODO: Error
        }
        Ok(())
    }
}

/// Implementors can provide new work to the SDK. The connection to the server is the real
/// implementor.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait PollWorkflowTaskQueueApi {
    /// Fetch new work. Should block indefinitely if there is no work.
    async fn poll_workflow_task(&self, task_queue: &str) -> Result<PollWorkflowTaskQueueResponse>;
}

/// Implementors can complete tasks as would've been issued by [Core::poll]. The real implementor
/// sends the completed tasks to the server.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait RespondWorkflowTaskCompletedApi {
    /// Complete a task by sending it to the server. `task_token` is the task token that would've
    /// been received from [PollWorkflowTaskQueueApi::poll]. `commands` is a list of new commands
    /// to send to the server, such as starting a timer.
    async fn complete_workflow_task(
        &self,
        task_token: Vec<u8>,
        commands: Vec<ProtoCommand>,
    ) -> Result<RespondWorkflowTaskCompletedResponse>;
}

/// Implementors should send StartWorkflowExecutionRequest to the server and pass the response back.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait StartWorkflowExecutionApi {
    /// Starts workflow execution.
    async fn start_workflow(
        &self,
        namespace: &str,
        task_queue: &str,
        workflow_id: &str,
        workflow_type: &str,
    ) -> Result<StartWorkflowExecutionResponse>;
}

/// The [DrivenWorkflow] trait expects to be called to make progress, but the [CoreSDKService]
/// expects to be polled by the lang sdk. This struct acts as the bridge between the two, buffering
/// output from calls to [DrivenWorkflow] and offering them to [CoreSDKService]
#[derive(Debug)]
pub(crate) struct WorkflowBridge {
    // does wf id belong in here?
    started_attrs: Option<WorkflowExecutionStartedEventAttributes>,
    incoming_commands: Receiver<Vec<WFCommand>>,
}

impl WorkflowBridge {
    /// Create a new bridge, returning it and the sink used to send commands to it.
    pub(crate) fn new() -> (Self, Sender<Vec<WFCommand>>) {
        let (tx, rx) = mpsc::channel();
        (
            Self {
                started_attrs: None,
                incoming_commands: rx,
            },
            tx,
        )
    }
}

impl DrivenWorkflow for WorkflowBridge {
    #[instrument]
    fn start(&mut self, attribs: WorkflowExecutionStartedEventAttributes) -> Vec<WFCommand> {
        self.started_attrs = Some(attribs);
        vec![]
    }

    #[instrument]
    fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand> {
        self.incoming_commands
            .try_recv()
            .unwrap_or_else(|_| vec![WFCommand::NoCommandsFromLang])
    }

    fn signal(&mut self, _attribs: WorkflowExecutionSignaledEventAttributes) {
        unimplemented!()
    }

    fn cancel(&mut self, _attribs: WorkflowExecutionCanceledEventAttributes) {
        unimplemented!()
    }
}

// Real bridge doesn't actually need to listen
impl ActivationListener for WorkflowBridge {}

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
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        machines::test_help::TestHistoryBuilder,
        pollers::MockServerGateway,
        protos::{
            coresdk::{wf_activation_job, WfActivationJob},
            temporal::api::{
                command::v1::{
                    CompleteWorkflowExecutionCommandAttributes, StartTimerCommandAttributes,
                },
                enums::v1::EventType,
                history::v1::{history_event, History, TimerFiredEventAttributes},
            },
        },
    };
    use std::collections::VecDeque;
    use tracing::Level;

    #[test]
    fn workflow_bridge() {
        let s = span!(Level::DEBUG, "Test start");
        let _enter = s.enter();

        let wfid = "fake_wf_id";
        let run_id = "fake_run_id";
        let timer_id = "fake_timer";
        let task_queue = "test-task-queue";

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_workflow_task();
        let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
        t.add(
            EventType::TimerFired,
            history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
                started_event_id: timer_started_event_id,
                timer_id: "timer1".to_string(),
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
        let events_first_batch = t.get_history_info(1).unwrap().events;
        let wf = Some(WorkflowExecution {
            workflow_id: wfid.to_string(),
            run_id: run_id.to_string(),
        });
        let first_response = PollWorkflowTaskQueueResponse {
            history: Some(History {
                events: events_first_batch,
            }),
            workflow_execution: wf.clone(),
            ..Default::default()
        };
        let events_second_batch = t.get_history_info(2).unwrap().events;
        let second_response = PollWorkflowTaskQueueResponse {
            history: Some(History {
                events: events_second_batch,
            }),
            workflow_execution: wf,
            ..Default::default()
        };
        let responses = vec![first_response, second_response];

        let mut tasks = VecDeque::from(responses);
        let mut mock_gateway = MockServerGateway::new();
        mock_gateway
            .expect_poll()
            .returning(move |_| Ok(tasks.pop_front().unwrap()));
        // Response not really important here
        mock_gateway
            .expect_complete()
            .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));

        let runtime = Runtime::new().unwrap();
        let core = CoreSDK {
            runtime,
            server_gateway: Arc::new(mock_gateway),
            workflow_machines: DashMap::new(),
            workflow_task_tokens: DashMap::new(),
        };

        let res = dbg!(core.poll_task(task_queue).unwrap());
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
            StartTimerCommandAttributes {
                timer_id: timer_id.to_string(),
                ..Default::default()
            }
            .into(),
            task_tok,
        ))
        .unwrap();
        dbg!("sent completion w/ start timer");

        let res = dbg!(core.poll_task(task_queue).unwrap());
        // TODO: uggo
        assert_matches!(
            res.get_wf_jobs().as_slice(),
            [WfActivationJob {
                attributes: Some(wf_activation_job::Attributes::TimerFired(_)),
            }]
        );
        let task_tok = res.task_token;
        core.complete_task(CompleteTaskReq::ok_from_api_attrs(
            CompleteWorkflowExecutionCommandAttributes { result: None }.into(),
            task_tok,
        ))
        .unwrap();
        dbg!("sent workflow done");
    }
}
