#[macro_use]
extern crate tracing;

mod machines;
mod pollers;
pub mod protos;

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
use prost::alloc::collections::VecDeque;

pub type Result<T, E = SDKServiceError> = std::result::Result<T, E>;

pub trait CoreSDKService: Send + Sync {
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

pub struct CoreSDK {
    /// Key is workflow id
    workflows: DashMap<String, WorkflowBridge>,
}

impl CoreSDKService for CoreSDK {
    fn poll_sdk_task(&self) -> Result<PollSdkTaskResp, SDKServiceError> {
        for mut wfb in self.workflows.iter_mut() {
            if let Some(task) = wfb.value_mut().get_next_task() {
                return Ok(PollSdkTaskResp {
                    task_token: b"TODO: Something real!".to_vec(),
                    task: Some(task.into()),
                });
            }
        }
        // Block thread instead? Or return optional task?
        Err(SDKServiceError::NoWork)
    }

    fn complete_sdk_task(&self, req: CompleteSdkTaskReq) -> Result<(), SDKServiceError> {
        match &req.completion {
            Some(Completion::Workflow(SdkwfTaskCompletion {
                status: Some(wfstatus),
            })) => {
                match wfstatus {
                    Status::Successful(success) => {
                        let cmds = success.commands.iter().map(|_c| {
                            // TODO: Make it go
                            vec![]
                        });
                        // TODO: Need to use task token or wfid etc
                        self.workflows
                            .get_mut("ID")
                            .unwrap()
                            .incoming_commands
                            .extend(cmds);
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
pub struct WorkflowBridge {
    started_attrs: WorkflowExecutionStartedEventAttributes,
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
        self.started_attrs = attribs;
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
    #[test]
    fn foo() {}
}
