use rustfsm::{fsm, TransitionResult};

fsm! {
    pub(super) name FailWorkflowMachine; command FailWorkflowCommand; error FailWorkflowMachineError;

    Created --(Schedule, on_schedule) --> FailWorkflowCommandCreated;

    FailWorkflowCommandCreated --(CommandFailWorkflowExecution) --> FailWorkflowCommandCreated;
    FailWorkflowCommandCreated --(WorkflowExecutionFailed, on_workflow_execution_failed) --> FailWorkflowCommandRecorded;
}

#[derive(thiserror::Error, Debug)]
pub(super) enum FailWorkflowMachineError {}

pub(super) enum FailWorkflowCommand {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(self) -> FailWorkflowMachineTransition<FailWorkflowCommandCreated> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct FailWorkflowCommandCreated {}

impl FailWorkflowCommandCreated {
    pub(super) fn on_workflow_execution_failed(
        self,
    ) -> FailWorkflowMachineTransition<FailWorkflowCommandRecorded> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct FailWorkflowCommandRecorded {}
