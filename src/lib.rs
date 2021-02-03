#[macro_use]
extern crate tracing;
#[cfg(test)]
#[macro_use]
extern crate assert_matches;

mod machines;
mod pollers;
pub mod protos;
mod protosext;

pub use protosext::HistoryInfo;

use crate::{
    machines::{
        ActivationListener, DrivenWorkflow, InconvertibleCommandError, WFCommand, WorkflowMachines,
    },
    pollers::ServerGatewayOptions,
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
            workflowservice::v1::PollWorkflowTaskQueueResponse,
        },
    },
    protosext::HistoryInfoError,
};
use anyhow::Error;
use dashmap::DashMap;
use std::{
    convert::TryInto,
    sync::mpsc::{self, Receiver, SendError, Sender},
};
use tokio::runtime::Runtime;
use url::Url;

pub type Result<T, E = CoreError> = std::result::Result<T, E>;

// TODO: Do we actually need this to be send+sync? Probably, but there's also no reason to have
//   any given WorfklowMachines instance accessed on more than one thread. Ideally this trait can
//   be accessed from many threads, but each workflow is pinned to one thread (possibly with many
//   sharing the same thread). IE: WorkflowMachines should be Send but not Sync, and this should
//   be both, ideally.
pub trait Core {
    fn poll_task(&self) -> Result<Task>;
    fn complete_task(&self, req: CompleteTaskReq) -> Result<()>;
}

pub struct CoreInitOptions {
    target_url: Url,
    namespace: String,
    _task_queue: Vec<String>,
    identity: String,
    binary_checksum: String,
}

/// Initializes instance of the core sdk and establishes connection to the temporal server.
/// Creates tokio runtime that will be used for all client-server interactions.  
pub fn init(opts: CoreInitOptions) -> Result<impl Core> {
    let runtime = Runtime::new().map_err(CoreError::TokioInitError)?;
    let gateway_opts = ServerGatewayOptions {
        namespace: opts.namespace,
        identity: opts.identity,
        binary_checksum: opts.binary_checksum,
    };
    // Initialize server client
    let work_provider = runtime.block_on(gateway_opts.connect(opts.target_url))?;

    Ok(CoreSDK {
        runtime,
        work_provider,
        workflow_machines: Default::default(),
        workflow_task_tokens: Default::default(),
    })
}

pub struct CoreSDK<WP> {
    runtime: Runtime,
    /// Provides work in the form of responses the server would send from polling task Qs
    work_provider: WP,
    /// Key is run id
    workflow_machines: DashMap<String, (WorkflowMachines, Sender<Vec<WFCommand>>)>,
    /// Maps task tokens to workflow run ids
    workflow_task_tokens: DashMap<Vec<u8>, String>,
}

impl<WP> Core for CoreSDK<WP>
where
    WP: WorkflowTaskProvider,
{
    #[instrument(skip(self))]
    fn poll_task(&self) -> Result<Task, CoreError> {
        // This will block forever in the event there is no work from the server
        let work = self
            .runtime
            .block_on(self.work_provider.get_work("TODO: Real task queue"))?;
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
                let wf_run_id = self
                    .workflow_task_tokens
                    .get(&task_token)
                    .map(|x| x.value().clone())
                    .ok_or(CoreError::NothingFoundForTaskToken(task_token))?;
                match wfstatus {
                    Status::Successful(success) => self.push_lang_commands(&wf_run_id, success)?,
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
        // TODO: Get fsm commands and send them to server (get_commands)
    }
}

impl<WP> CoreSDK<WP>
where
    WP: WorkflowTaskProvider,
{
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
        self.workflow_machines
            .get_mut(run_id)
            .unwrap()
            .1
            .send(cmds)?;
        Ok(())
    }
}

/// Implementors can provide new work to the SDK. The connection to the server is the real
/// implementor.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait WorkflowTaskProvider {
    /// Fetch new work. Should block indefinitely if there is no work.
    async fn get_work(&self, task_queue: &str) -> Result<PollWorkflowTaskQueueResponse>;
}

/// The [DrivenWorkflow] trait expects to be called to make progress, but the [CoreSDKService]
/// expects to be polled by the lang sdk. This struct acts as the bridge between the two, buffering
/// output from calls to [DrivenWorkflow] and offering them to [CoreSDKService]
#[derive(Debug)]
pub struct WorkflowBridge {
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
    fn start(
        &mut self,
        attribs: WorkflowExecutionStartedEventAttributes,
    ) -> Result<Vec<WFCommand>, Error> {
        self.started_attrs = Some(attribs);
        Ok(vec![])
    }

    #[instrument]
    fn iterate_wf(&mut self) -> Result<Vec<WFCommand>, Error> {
        Ok(self
            .incoming_commands
            .try_recv()
            .unwrap_or_else(|_| vec![WFCommand::NoCommandsFromLang]))
    }

    fn signal(&mut self, _attribs: WorkflowExecutionSignaledEventAttributes) -> Result<(), Error> {
        unimplemented!()
    }

    fn cancel(&mut self, _attribs: WorkflowExecutionCanceledEventAttributes) -> Result<(), Error> {
        unimplemented!()
    }
}

// Real bridge doesn't actually need to listen
impl ActivationListener for WorkflowBridge {}

#[derive(thiserror::Error, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum CoreError {
    #[error("Unknown service error")]
    Unknown,
    #[error("No tasks to perform for now")]
    NoWork,
    #[error("Poll response from server was malformed: {0:?}")]
    BadDataFromWorkProvider(PollWorkflowTaskQueueResponse),
    #[error("Lang SDK sent us a malformed completion: {0:?}")]
    MalformedCompletion(CompleteTaskReq),
    #[error("Error buffering commands")]
    CantSendCommands(#[from] SendError<Vec<WFCommand>>),
    #[error("Couldn't interpret command from <lang>")]
    UninterprableCommand(#[from] InconvertibleCommandError),
    #[error("Underlying error in history processing")]
    UnderlyingHistError(#[from] HistoryInfoError),
    #[error("Task token had nothing associated with it: {0:?}")]
    NothingFoundForTaskToken(Vec<u8>),
    #[error("Error calling the service: {0:?}")]
    TonicError(#[from] tonic::Status),
    #[error("Server connection error: {0:?}")]
    TonicTransportError(#[from] tonic::transport::Error),
    #[error("Failed to initialize tokio runtime: {0:?}")]
    TokioInitError(std::io::Error),
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        machines::test_help::TestHistoryBuilder,
        protos::{
            coresdk::{task, wf_activation, WfActivation},
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
        let mut mock_provider = MockWorkflowTaskProvider::new();
        mock_provider
            .expect_get_work()
            .returning(move |_| Ok(tasks.pop_front().unwrap()));

        let runtime = Runtime::new().unwrap();
        let core = CoreSDK {
            runtime,
            work_provider: mock_provider,
            workflow_machines: DashMap::new(),
            workflow_task_tokens: DashMap::new(),
        };

        let res = dbg!(core.poll_task().unwrap());
        // TODO: uggo
        assert_matches!(
            res,
            Task {
                variant: Some(task::Variant::Workflow(WfActivation {
                    attributes: Some(wf_activation::Attributes::StartWorkflow(_)),
                    ..
                })),
                ..
            }
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

        let res = dbg!(core.poll_task().unwrap());
        // TODO: uggo
        assert_matches!(
            res,
            Task {
                variant: Some(task::Variant::Workflow(WfActivation {
                    attributes: Some(wf_activation::Attributes::UnblockTimer(_)),
                    ..
                })),
                ..
            }
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
