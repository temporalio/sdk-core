#[macro_use]
extern crate tracing;

mod machines;
mod pollers;
pub mod protos;

use crate::{
    machines::{DrivenWorkflow, InconvertibleCommandError, WFCommand, WorkflowMachines},
    protos::{
        coresdk::{
            complete_task_req::Completion, workflow_task_completion::Status, CompleteTaskReq,
            RegistrationReq, Task, WorkflowTaskCompletion,
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
};
use anyhow::Error;
use dashmap::DashMap;
use std::{
    convert::TryInto,
    sync::mpsc::{self, Receiver, SendError, Sender},
};

pub type Result<T, E = CoreError> = std::result::Result<T, E>;

// TODO: Do we actually need this to be send+sync? Probably, but there's also no reason to have
//   any given WorfklowMachines instance accessed on more than one thread. Ideally this trait can
//   be accessed from many threads, but each workflow is pinned to one thread (possibly with many
//   sharing the same thread). IE: WorkflowMachines should be Send but not Sync, and this should
//   be both, ideally.
pub trait Core {
    fn poll_task(&self) -> Result<Task>;
    fn complete_task(&self, req: CompleteTaskReq) -> Result<()>;
    fn register_implementations(&self, req: RegistrationReq) -> Result<()>;
}

pub struct CoreInitOptions {
    _queue_name: String,
    _max_concurrent_workflow_executions: u32,
    _max_concurrent_activity_executions: u32,
}

pub fn init(_opts: CoreInitOptions) -> Result<Box<dyn Core>> {
    Err(CoreError::Unknown {})
}

pub struct CoreSDK<WP> {
    work_provider: WP,
    /// Key is workflow id
    workflow_machines: DashMap<String, (WorkflowMachines, Sender<Vec<WFCommand>>)>,
}

impl<WP> CoreSDK<WP>
where
    WP: WorkProvider,
{
    fn new_workflow(&self, id: String) {
        let (wfb, cmd_sink) = WorkflowBridge::new();
        let state_machines = WorkflowMachines::new(Box::new(wfb));
        self.workflow_machines
            .insert(id, (state_machines, cmd_sink));
    }
}

/// Implementors can provide new work to the SDK. The connection to the server is the real
/// implementor.
#[cfg_attr(test, mockall::automock)]
pub trait WorkProvider {
    /// Fetch new work. Should block indefinitely if there is no work.
    fn get_work(&self, task_queue: &str) -> Result<PollWorkflowTaskQueueResponse>;
}

impl<WP> Core for CoreSDK<WP>
where
    WP: WorkProvider,
{
    fn poll_task(&self) -> Result<Task, CoreError> {
        // This will block forever in the event there is no work from the server
        let work = self.work_provider.get_work("TODO: Real task queue")?;
        match &work.workflow_execution {
            Some(WorkflowExecution { workflow_id, .. }) => {
                // TODO: Only do if it doesn't exist yet
                self.new_workflow(workflow_id.to_string());
            }
            // TODO: Appropriate error
            None => return Err(CoreError::Unknown),
        }
        // TODO: Apply history to machines, get commands out, convert them to task
        Ok(Task {
            task_token: work.task_token,
            variant: None,
        })
    }

    fn complete_task(&self, req: CompleteTaskReq) -> Result<(), CoreError> {
        match req.completion {
            Some(Completion::Workflow(WorkflowTaskCompletion {
                status: Some(wfstatus),
            })) => {
                match wfstatus {
                    Status::Successful(success) => {
                        // Convert to wf commands
                        let cmds = success
                            .commands
                            .into_iter()
                            .map(|c| c.try_into().map_err(Into::into))
                            .collect::<Result<Vec<_>>>()?;
                        // TODO: Need to use task token or wfid etc
                        self.workflow_machines
                            .get_mut("fake_wf_id")
                            .unwrap()
                            .1
                            .send(cmds)?;
                    }
                    Status::Failed(_) => {}
                }
                Ok(())
            }
            Some(Completion::Activity(_)) => {
                unimplemented!()
            }
            _ => Err(CoreError::MalformedCompletion(req)),
        }
    }

    fn register_implementations(&self, _req: RegistrationReq) -> Result<(), CoreError> {
        unimplemented!()
    }
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
    fn start(
        &mut self,
        attribs: WorkflowExecutionStartedEventAttributes,
    ) -> Result<Vec<WFCommand>, Error> {
        self.started_attrs = Some(attribs);
        Ok(vec![])
    }

    fn iterate_wf(&mut self) -> Result<Vec<WFCommand>, Error> {
        Ok(self.incoming_commands.try_recv().unwrap_or_default())
    }

    fn signal(&mut self, _attribs: WorkflowExecutionSignaledEventAttributes) -> Result<(), Error> {
        unimplemented!()
    }

    fn cancel(&mut self, _attribs: WorkflowExecutionCanceledEventAttributes) -> Result<(), Error> {
        unimplemented!()
    }
}

#[derive(thiserror::Error, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum CoreError {
    #[error("Unknown service error")]
    Unknown,
    #[error("No tasks to perform for now")]
    NoWork,
    #[error("Lang SDK sent us a malformed completion: {0:?}")]
    MalformedCompletion(CompleteTaskReq),
    #[error("Error buffering commands")]
    CantSendCommands(#[from] SendError<Vec<WFCommand>>),
    #[error("Couldn't interpret command from <lang>")]
    UninterprableCommand(#[from] InconvertibleCommandError),
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        machines::test_help::TestHistoryBuilder,
        protos::{
            coresdk::{self, command::Variant, WorkflowTaskSuccess},
            temporal::api::{
                command::v1::{command, Command, StartTimerCommandAttributes},
                enums::v1::EventType,
                history::v1::{history_event, TimerFiredEventAttributes},
            },
        },
    };
    use std::{
        collections::VecDeque,
        sync::{atomic::AtomicBool, Arc},
    };

    #[test]
    fn workflow_bridge() {
        let wfid = "fake_wf_id";
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
                ..Default::default()
            }),
        );
        t.add_workflow_task_scheduled_and_started();

        // TODO: Use test history builder to issue appropriate histories
        let mut tasks = VecDeque::from(vec![]);
        let mut mock_provider = MockWorkProvider::new();
        mock_provider
            .expect_get_work()
            .returning(move |_| Ok(tasks.pop_front().unwrap()));

        let core = CoreSDK {
            workflow_machines: DashMap::new(),
            work_provider: mock_provider,
        };
        core.new_workflow(wfid.to_string());

        // TODO: These are what poll_task should end up returning
        //             SdkwfTask {
        //                 timestamp: None,
        //                 workflow_id: wfid.to_string(),
        //                 task: Some(
        //                     StartWorkflowTaskAttributes {
        //                         namespace: "namespace".to_string(),
        //                         name: "wf name?".to_string(),
        //                         arguments: None,
        //                     }
        //                         .into(),
        //                 ),
        //             }
        //                 .into(),
        //             SdkwfTask {
        //                 timestamp: None,
        //                 workflow_id: wfid.to_string(),
        //                 task: Some(
        //                     UnblockTimerTaskAttibutes {
        //                         timer_id: timer_id.to_string(),
        //                     }
        //                         .into(),
        //                 ),
        //             }
        //                 .into(),

        let res = core.poll_task().unwrap();
        let task_tok = res.task_token;
        let timer_atom = Arc::new(AtomicBool::new(false));
        let cmd: command::Attributes = StartTimerCommandAttributes {
            timer_id: timer_id.to_string(),
            ..Default::default()
        }
        .into();
        let cmd: Command = cmd.into();
        let success = WorkflowTaskSuccess {
            commands: vec![coresdk::Command {
                variant: Some(Variant::Api(cmd)),
            }],
        };
        core.complete_task(CompleteTaskReq {
            task_token: task_tok,
            completion: Some(Completion::Workflow(WorkflowTaskCompletion {
                status: Some(Status::Successful(success)),
            })),
        })
        .unwrap();

        // SDK commands should be StartTimer followed by Complete WE
    }
}
