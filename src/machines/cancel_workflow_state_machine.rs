use rustfsm::{fsm, TransitionResult};

fsm! {
    name CancelWorkflowMachine; command CancelWorkflowCommand; error CancelWorkflowMachineError;

    CancelWorkflowCommandCreated --(CommandCancelWorkflowExecution) --> CancelWorkflowCommandCreated;
    CancelWorkflowCommandCreated --(WorkflowExecutionCanceled, on_workflow_execution_canceled) --> CancelWorkflowCommandRecorded;

    Created --(Schedule, on_schedule) --> CancelWorkflowCommandCreated;
}

#[derive(thiserror::Error, Debug)]
pub enum CancelWorkflowMachineError {}

pub enum CancelWorkflowCommand {}

#[derive(Default, Clone)]
pub struct CancelWorkflowCommandCreated {}

impl CancelWorkflowCommandCreated {
    pub fn on_workflow_execution_canceled(self) -> CancelWorkflowMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct CancelWorkflowCommandRecorded {}

#[derive(Default, Clone)]
pub struct Created {}

impl Created {
    pub fn on_schedule(self) -> CancelWorkflowMachineTransition {
        unimplemented!()
    }
}
