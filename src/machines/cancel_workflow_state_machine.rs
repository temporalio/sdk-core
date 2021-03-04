use rustfsm::{fsm, TransitionResult};

fsm! {
    pub(super) name CancelWorkflowMachine; command CancelWorkflowCommand; error CancelWorkflowMachineError;

    CancelWorkflowCommandCreated --(CommandCancelWorkflowExecution) --> CancelWorkflowCommandCreated;
    CancelWorkflowCommandCreated --(WorkflowExecutionCanceled, on_workflow_execution_canceled) --> CancelWorkflowCommandRecorded;

    Created --(Schedule, on_schedule) --> CancelWorkflowCommandCreated;
}

#[derive(thiserror::Error, Debug)]
pub(super) enum CancelWorkflowMachineError {}

pub(super) enum CancelWorkflowCommand {}

#[derive(Default, Clone)]
pub(super) struct CancelWorkflowCommandCreated {}

impl CancelWorkflowCommandCreated {
    pub(super) fn on_workflow_execution_canceled(
        self,
    ) -> CancelWorkflowMachineTransition<CancelWorkflowCommandRecorded> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct CancelWorkflowCommandRecorded {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> CancelWorkflowMachineTransition<CancelWorkflowCommandCreated> {
        unimplemented!()
    }
}
