use rustfsm::{fsm, TransitionResult};

fsm! {
    pub(super) name ContinueAsNewWorkflowMachine; command ContinueAsNewWorkflowCommand; error ContinueAsNewWorkflowMachineError;

    ContinueAsNewWorkflowCommandCreated --(CommandContinueAsNewWorkflowExecution) --> ContinueAsNewWorkflowCommandCreated;
    ContinueAsNewWorkflowCommandCreated --(WorkflowExecutionContinuedAsNew, on_workflow_execution_continued_as_new) --> ContinueAsNewWorkflowCommandRecorded;

    Created --(Schedule, on_schedule) --> ContinueAsNewWorkflowCommandCreated;
}

#[derive(thiserror::Error, Debug)]
pub(super) enum ContinueAsNewWorkflowMachineError {}

pub(super) enum ContinueAsNewWorkflowCommand {}

#[derive(Default, Clone)]
pub(super) struct ContinueAsNewWorkflowCommandCreated {}

impl ContinueAsNewWorkflowCommandCreated {
    pub(super) fn on_workflow_execution_continued_as_new(
        self,
    ) -> ContinueAsNewWorkflowMachineTransition<ContinueAsNewWorkflowCommandRecorded> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct ContinueAsNewWorkflowCommandRecorded {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> ContinueAsNewWorkflowMachineTransition<ContinueAsNewWorkflowCommandCreated> {
        unimplemented!()
    }
}
