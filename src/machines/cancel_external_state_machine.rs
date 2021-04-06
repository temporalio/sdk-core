use rustfsm::{fsm, TransitionResult};

fsm! {
    pub(super) name CancelExternalMachine; command CancelExternalCommand; error CancelExternalMachineError;

    Created --(Schedule, on_schedule) --> RequestCancelExternalCommandCreated;

    RequestCancelExternalCommandCreated --(CommandRequestCancelExternalWorkflowExecution) --> RequestCancelExternalCommandCreated;
    RequestCancelExternalCommandCreated --(RequestCancelExternalWorkflowExecutionInitiated, on_request_cancel_external_workflow_execution_initiated) --> RequestCancelExternalCommandRecorded;

    RequestCancelExternalCommandRecorded --(ExternalWorkflowExecutionCancelRequested, on_external_workflow_execution_cancel_requested) --> CancelRequested;
    RequestCancelExternalCommandRecorded --(RequestCancelExternalWorkflowExecutionFailed, on_request_cancel_external_workflow_execution_failed) --> RequestCancelFailed;
}

#[derive(thiserror::Error, Debug)]
pub(super) enum CancelExternalMachineError {}

pub(super) enum CancelExternalCommand {}

#[derive(Default, Clone)]
pub(super) struct CancelRequested {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> CancelExternalMachineTransition<RequestCancelExternalCommandCreated> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct RequestCancelExternalCommandCreated {}

impl RequestCancelExternalCommandCreated {
    pub(super) fn on_request_cancel_external_workflow_execution_initiated(
        self,
    ) -> CancelExternalMachineTransition<RequestCancelExternalCommandRecorded> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct RequestCancelExternalCommandRecorded {}

impl RequestCancelExternalCommandRecorded {
    pub(super) fn on_external_workflow_execution_cancel_requested(
        self,
    ) -> CancelExternalMachineTransition<CancelRequested> {
        unimplemented!()
    }
    pub(super) fn on_request_cancel_external_workflow_execution_failed(
        self,
    ) -> CancelExternalMachineTransition<RequestCancelFailed> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct RequestCancelFailed {}
