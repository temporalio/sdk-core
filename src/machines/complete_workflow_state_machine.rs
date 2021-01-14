use rustfsm::{fsm, TransitionResult};

fsm! {
    name CompleteWorkflowMachine; command CompleteWorkflowCommand; error CompleteWorkflowMachineError;

    CompleteWorkflowCommandCreated --(CommandCompleteWorkflowExecution) --> CompleteWorkflowCommandCreated;
    CompleteWorkflowCommandCreated --(WorkflowExecutionCompleted, on_workflow_execution_completed) --> CompleteWorkflowCommandRecorded;

    Created --(Schedule, on_schedule) --> CompleteWorkflowCommandCreated;
}

#[derive(thiserror::Error, Debug)]
pub enum CompleteWorkflowMachineError {}

pub enum CompleteWorkflowCommand {}

#[derive(Default)]
pub struct CompleteWorkflowCommandCreated {}

impl CompleteWorkflowCommandCreated {
    pub fn on_workflow_execution_completed(self) -> CompleteWorkflowMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct CompleteWorkflowCommandRecorded {}

#[derive(Default)]
pub struct Created {}

impl Created {
    pub fn on_schedule(self) -> CompleteWorkflowMachineTransition {
        unimplemented!()
    }
}
