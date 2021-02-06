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
    protosext::{HistoryInfo, HistoryInfoError},
    workflow::{WfManagerProtected, WorkflowManager},
};
use dashmap::DashMap;
use std::{convert::TryInto, sync::mpsc::SendError, sync::Arc};
use tokio::runtime::Runtime;
use tonic::codegen::http::uri::InvalidUri;

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
    workflow_machines: DashMap<String, WorkflowManager>,
    /// Maps task tokens to workflow run ids
    workflow_task_tokens: DashMap<Vec<u8>, String>,
}

impl<WP> Core for CoreSDK<WP>
where
    WP: ServerGatewayApis + Send + Sync,
{
    #[instrument(skip(self))]
    fn poll_task(&self, task_queue: &str) -> Result<Task> {
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

        // Correlate task token w/ run ID
        self.workflow_task_tokens
            .insert(work.task_token.clone(), run_id.clone());

        // We pass none since we want to apply all the history we just got.
        // Will need to change a bit once we impl caching.
        let hist_info = HistoryInfo::new_from_history(&history, None)?;
        let activation = self.access_machine(&run_id, |mgr| {
            let machines = &mut mgr.machines;
            hist_info.apply_history_events(machines)?;
            Ok(machines.get_wf_activation())
        })?;

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
                workflowservice::v1::RespondWorkflowTaskCompletedResponse,
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
            .expect_poll_workflow_task()
            .returning(move |_| Ok(tasks.pop_front().unwrap()));
        // Response not really important here
        mock_gateway
            .expect_complete_workflow_task()
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
