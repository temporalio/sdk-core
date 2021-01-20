use rustfsm::{fsm, TransitionResult};

fsm! {
    pub(super) name CompleteWorkflowMachine; command CompleteWorkflowCommand; error CompleteWorkflowMachineError;

    CompleteWorkflowCommandCreated --(CommandCompleteWorkflowExecution) --> CompleteWorkflowCommandCreated;
    CompleteWorkflowCommandCreated --(WorkflowExecutionCompleted, on_workflow_execution_completed) --> CompleteWorkflowCommandRecorded;

    Created --(Schedule, on_schedule) --> CompleteWorkflowCommandCreated;
}

#[derive(thiserror::Error, Debug)]
pub(super) enum CompleteWorkflowMachineError {}

pub(super) enum CompleteWorkflowCommand {}

#[derive(Default, Clone)]
pub(super) struct CompleteWorkflowCommandCreated {}

impl CompleteWorkflowCommandCreated {
    pub(super) fn on_workflow_execution_completed(self) -> CompleteWorkflowMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct CompleteWorkflowCommandRecorded {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(self) -> CompleteWorkflowMachineTransition {
        unimplemented!()
    }
}
