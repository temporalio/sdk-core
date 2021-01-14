use rustfsm::{fsm, TransitionResult};

fsm! {
    CancelWorkflowMachine, CancelWorkflowCommand, CancelWorkflowMachineError

    CancelWorkflowCommandCreated --(CommandCancelWorkflowExecution) --> CancelWorkflowCommandCreated;
    CancelWorkflowCommandCreated --(WorkflowExecutionCanceled, on_workflow_execution_canceled) --> CancelWorkflowCommandRecorded;

    Created --(Schedule, on_schedule) --> CancelWorkflowCommandCreated;
}

#[derive(thiserror::Error, Debug)]
pub enum CancelWorkflowMachineError {}

pub enum CancelWorkflowCommand {}

#[derive(Default)]
pub struct CancelWorkflowCommandCreated {}

impl CancelWorkflowCommandCreated {
    pub fn on_workflow_execution_canceled(self) -> CancelWorkflowMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct CancelWorkflowCommandRecorded {}

#[derive(Default)]
pub struct Created {}

impl Created {
    pub fn on_schedule(self) -> CancelWorkflowMachineTransition {
        unimplemented!()
    }
}
