use rustfsm::{fsm, TransitionResult};

fsm! {
    pub(super) name SignalExternalMachine; command SignalExternalCommand; error SignalExternalMachineError;

    Created --(Schedule, on_schedule) --> SignalExternalCommandCreated;

    SignalExternalCommandCreated --(CommandSignalExternalWorkflowExecution) --> SignalExternalCommandCreated;
    SignalExternalCommandCreated --(Cancel, on_cancel) --> Canceled;
    SignalExternalCommandCreated --(SignalExternalWorkflowExecutionInitiated, on_signal_external_workflow_execution_initiated) --> SignalExternalCommandRecorded;

    SignalExternalCommandRecorded --(Cancel) --> SignalExternalCommandRecorded;
    SignalExternalCommandRecorded --(ExternalWorkflowExecutionSignaled, on_external_workflow_execution_signaled) --> Signaled;
    SignalExternalCommandRecorded --(SignalExternalWorkflowExecutionFailed, on_signal_external_workflow_execution_failed) --> Failed;
}

#[derive(thiserror::Error, Debug)]
pub(super) enum SignalExternalMachineError {}

pub(super) enum SignalExternalCommand {}

#[derive(Default, Clone)]
pub(super) struct Canceled {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> SignalExternalMachineTransition<SignalExternalCommandCreated> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct Failed {}

#[derive(Default, Clone)]
pub(super) struct SignalExternalCommandCreated {}

impl SignalExternalCommandCreated {
    pub(super) fn on_cancel(self) -> SignalExternalMachineTransition<Canceled> {
        unimplemented!()
    }
    pub(super) fn on_signal_external_workflow_execution_initiated(
        self,
    ) -> SignalExternalMachineTransition<SignalExternalCommandRecorded> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct SignalExternalCommandRecorded {}

impl SignalExternalCommandRecorded {
    pub(super) fn on_external_workflow_execution_signaled(
        self,
    ) -> SignalExternalMachineTransition<Signaled> {
        unimplemented!()
    }
    pub(super) fn on_signal_external_workflow_execution_failed(
        self,
    ) -> SignalExternalMachineTransition<Failed> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct Signaled {}
