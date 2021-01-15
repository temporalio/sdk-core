use rustfsm::{fsm, TransitionResult};

fsm! {
    name ContinueAsNewWorkflowMachine; command ContinueAsNewWorkflowCommand; error ContinueAsNewWorkflowMachineError;

    ContinueAsNewWorkflowCommandCreated --(CommandContinueAsNewWorkflowExecution) --> ContinueAsNewWorkflowCommandCreated;
    ContinueAsNewWorkflowCommandCreated --(WorkflowExecutionContinuedAsNew, on_workflow_execution_continued_as_new) --> ContinueAsNewWorkflowCommandRecorded;

    Created --(Schedule, on_schedule) --> ContinueAsNewWorkflowCommandCreated;
}

#[derive(thiserror::Error, Debug)]
pub enum ContinueAsNewWorkflowMachineError {}

pub enum ContinueAsNewWorkflowCommand {}

#[derive(Default, Clone)]
pub struct ContinueAsNewWorkflowCommandCreated {}

impl ContinueAsNewWorkflowCommandCreated {
    pub fn on_workflow_execution_continued_as_new(self) -> ContinueAsNewWorkflowMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct ContinueAsNewWorkflowCommandRecorded {}

#[derive(Default, Clone)]
pub struct Created {}

impl Created {
    pub fn on_schedule(self) -> ContinueAsNewWorkflowMachineTransition {
        unimplemented!()
    }
}
