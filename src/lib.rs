#[macro_use]
extern crate tracing;

mod machines;
mod pollers;
pub mod protos;

use crate::machines::WorkflowMachines;
use crate::protos::coresdk::poll_sdk_task_resp;
use crate::protos::coresdk::poll_sdk_task_resp::Task;
use crate::protos::temporal::api::history::v1::HistoryEvent;
use crate::{
    machines::{DrivenWorkflow, WFCommand},
    protos::{
        coresdk::{
            complete_sdk_task_req::Completion, sdkwf_task_completion::Status, CompleteSdkTaskReq,
            PollSdkTaskResp, RegistrationReq, SdkwfTask, SdkwfTaskCompletion,
        },
        temporal::api::history::v1::{
            WorkflowExecutionCanceledEventAttributes, WorkflowExecutionSignaledEventAttributes,
            WorkflowExecutionStartedEventAttributes,
        },
    },
};
use anyhow::Error;
use dashmap::DashMap;
use std::collections::VecDeque;

pub type Result<T, E = SDKServiceError> = std::result::Result<T, E>;

// TODO: Do we actually need this to be send+sync? Probably, but there's also no reason to have
//   any given WorfklowMachines instance accessed on more than one thread. Ideally this trait can
//   be accessed from many threads, but each workflow is pinned to one thread (possibly with many
//   sharing the same thread). IE: WorkflowMachines should be Send but not Sync, and this should
//   be both, ideally.
pub trait CoreSDKService {
    fn poll_sdk_task(&self) -> Result<PollSdkTaskResp>;
    fn complete_sdk_task(&self, req: CompleteSdkTaskReq) -> Result<()>;
    fn register_implementations(&self, req: RegistrationReq) -> Result<()>;
}

pub struct CoreSDKInitOptions {
    _queue_name: String,
    _max_concurrent_workflow_executions: u32,
    _max_concurrent_activity_executions: u32,
}

pub fn init_sdk(_opts: CoreSDKInitOptions) -> Result<Box<dyn CoreSDKService>> {
    Err(SDKServiceError::Unknown {})
}

pub struct CoreSDK<WP> {
    work_provider: WP,
    /// Key is workflow id
    workflows: DashMap<String, WorkflowMachines>,
}

// impl<WP> CoreSDK<WP> where WP: WorkProvider {
//     fn apply_hist_event(event: &HistoryEvent, has_next_event)
// }

/// Implementors can provide new work to the SDK. The connection to the server is the real
/// implementor, which adapts workflow tasks from the server to sdk workflow tasks.
#[cfg_attr(test, mockall::automock)]
pub trait WorkProvider {
    // TODO: Should actually likely return server tasks directly, so coresdk can do things like
    //  apply history events that don't need workflow to actually do anything, like schedule.
    /// Fetch new work. Should block indefinitely if there is no work.
    fn get_work(&self, task_queue: &str) -> Result<poll_sdk_task_resp::Task>;
}

impl<WP> CoreSDKService for CoreSDK<WP>
where
    WP: WorkProvider,
{
    fn poll_sdk_task(&self) -> Result<PollSdkTaskResp, SDKServiceError> {
        // This will block forever in the event there is no work from the server
        let work = self.work_provider.get_work("TODO: Real task queue")?;
        match work {
            Task::WfTask(t) => return Ok(PollSdkTaskResp::from_wf_task(b"token".to_vec(), t)),
            Task::ActivityTask(_) => {
                unimplemented!()
            }
        }
    }

    fn complete_sdk_task(&self, req: CompleteSdkTaskReq) -> Result<(), SDKServiceError> {
        match &req.completion {
            Some(Completion::Workflow(SdkwfTaskCompletion {
                status: Some(wfstatus),
            })) => {
                match wfstatus {
                    Status::Successful(success) => {
                        // let cmds = success.commands.iter().map(|_c| {
                        //     // TODO: Make it go
                        //     vec![]
                        // });
                        // TODO: Need to use task token or wfid etc
                        // self.workflows
                        //     .get_mut("ID")
                        //     .unwrap()
                        //     .incoming_commands
                        //     .extend(cmds);
                    }
                    Status::Failed(_) => {}
                }
                Ok(())
            }
            Some(Completion::Activity(_)) => {
                unimplemented!()
            }
            _ => Err(SDKServiceError::MalformedCompletion(req)),
        }
    }

    fn register_implementations(&self, _req: RegistrationReq) -> Result<(), SDKServiceError> {
        unimplemented!()
    }
}

/// The [DrivenWorkflow] trait expects to be called to make progress, but the [CoreSDKService]
/// expects to be polled by the lang sdk. This struct acts as the bridge between the two, buffering
/// output from calls to [DrivenWorkflow] and offering them to [CoreSDKService]
#[derive(Debug, Default)]
pub struct WorkflowBridge {
    // does wf id belong in here?
    started_attrs: Option<WorkflowExecutionStartedEventAttributes>,
    outgoing_tasks: VecDeque<SdkwfTask>,
    incoming_commands: VecDeque<Vec<WFCommand>>,
}

impl WorkflowBridge {
    pub fn get_next_task(&mut self) -> Option<SdkwfTask> {
        self.outgoing_tasks.pop_front()
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
        Ok(self.incoming_commands.pop_front().unwrap_or_default())
    }

    fn signal(&mut self, _attribs: WorkflowExecutionSignaledEventAttributes) -> Result<(), Error> {
        unimplemented!()
    }

    fn cancel(&mut self, _attribs: WorkflowExecutionCanceledEventAttributes) -> Result<(), Error> {
        unimplemented!()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum SDKServiceError {
    #[error("Unknown service error")]
    Unknown,
    #[error("No tasks to perform for now")]
    NoWork,
    #[error("Lang SDK sent us a malformed completion: {0:?}")]
    MalformedCompletion(CompleteSdkTaskReq),
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        machines::{test_help::TestHistoryBuilder, WorkflowMachines},
        protos::{
            coresdk::{CompleteTimerTaskAttributes, StartWorkflowTaskAttributes, WfTaskType},
            temporal::api::{
                enums::v1::EventType,
                history::v1::{history_event, TimerFiredEventAttributes},
            },
        },
    };

    #[test]
    fn workflow_bridge() {
        let wfid = "fake_wf_id";
        let mut tasks = VecDeque::from(vec![
            SdkwfTask {
                r#type: WfTaskType::StartWorkflow as i32,
                timestamp: None,
                workflow_id: wfid.to_string(),
                attributes: Some(
                    StartWorkflowTaskAttributes {
                        namespace: "namespace".to_string(),
                        name: "wf name?".to_string(),
                        arguments: None,
                    }
                    .into(),
                ),
            }
            .into(),
            SdkwfTask {
                r#type: WfTaskType::CompleteTimer as i32,
                timestamp: None,
                workflow_id: wfid.to_string(),
                attributes: Some(
                    CompleteTimerTaskAttributes {
                        timer_id: "timer".to_string(),
                    }
                    .into(),
                ),
            }
            .into(),
        ]);

        let mut mock_provider = MockWorkProvider::new();
        mock_provider
            .expect_get_work()
            .returning(move |_| Ok(tasks.pop_front().unwrap()));

        let mut wfb = WorkflowBridge::default();
        let state_machines = WorkflowMachines::new(Box::new(wfb));
        let core = CoreSDK {
            workflows: DashMap::new(),
            work_provider: mock_provider,
        };
        core.workflows.insert(wfid.to_string(), state_machines);

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

        let res = core.poll_sdk_task();
        dbg!(res);
    }
}
