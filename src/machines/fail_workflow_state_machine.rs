use rustfsm::{fsm, TransitionResult};

fsm! {
    name FailWorkflowMachine; command FailWorkflowCommand; error FailWorkflowMachineError;

    Created --(Schedule, on_schedule) --> FailWorkflowCommandCreated;

    FailWorkflowCommandCreated --(CommandFailWorkflowExecution) --> FailWorkflowCommandCreated;
    FailWorkflowCommandCreated --(WorkflowExecutionFailed, on_workflow_execution_failed) --> FailWorkflowCommandRecorded;
}

#[derive(thiserror::Error, Debug)]
pub enum FailWorkflowMachineError {}

pub enum FailWorkflowCommand {}

#[derive(Default)]
pub struct Created {}

impl Created {
    pub fn on_schedule(self) -> FailWorkflowMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct FailWorkflowCommandCreated {}

impl FailWorkflowCommandCreated {
    pub fn on_workflow_execution_failed(self) -> FailWorkflowMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct FailWorkflowCommandRecorded {}
