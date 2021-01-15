use rustfsm::{fsm, TransitionResult};

fsm! {
    name CancelExternalMachine; command CancelExternalCommand; error CancelExternalMachineError;

    Created --(Schedule, on_schedule) --> RequestCancelExternalCommandCreated;

    RequestCancelExternalCommandCreated --(CommandRequestCancelExternalWorkflowExecution) --> RequestCancelExternalCommandCreated;
    RequestCancelExternalCommandCreated --(RequestCancelExternalWorkflowExecutionInitiated, on_request_cancel_external_workflow_execution_initiated) --> RequestCancelExternalCommandRecorded;

    RequestCancelExternalCommandRecorded --(ExternalWorkflowExecutionCancelRequested, on_external_workflow_execution_cancel_requested) --> CancelRequested;
    RequestCancelExternalCommandRecorded --(RequestCancelExternalWorkflowExecutionFailed, on_request_cancel_external_workflow_execution_failed) --> RequestCancelFailed;
}

#[derive(thiserror::Error, Debug)]
pub enum CancelExternalMachineError {}

pub enum CancelExternalCommand {}

#[derive(Default, Clone)]
pub struct CancelRequested {}

#[derive(Default, Clone)]
pub struct Created {}

impl Created {
    pub fn on_schedule(self) -> CancelExternalMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct RequestCancelExternalCommandCreated {}

impl RequestCancelExternalCommandCreated {
    pub fn on_request_cancel_external_workflow_execution_initiated(
        self,
    ) -> CancelExternalMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct RequestCancelExternalCommandRecorded {}

impl RequestCancelExternalCommandRecorded {
    pub fn on_external_workflow_execution_cancel_requested(
        self,
    ) -> CancelExternalMachineTransition {
        unimplemented!()
    }
    pub fn on_request_cancel_external_workflow_execution_failed(
        self,
    ) -> CancelExternalMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct RequestCancelFailed {}
