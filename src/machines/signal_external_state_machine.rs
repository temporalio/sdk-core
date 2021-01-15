use rustfsm::{fsm, TransitionResult};

fsm! {
    name SignalExternalMachine; command SignalExternalCommand; error SignalExternalMachineError;

    Created --(Schedule, on_schedule) --> SignalExternalCommandCreated;

    SignalExternalCommandCreated --(CommandSignalExternalWorkflowExecution) --> SignalExternalCommandCreated;
    SignalExternalCommandCreated --(Cancel, on_cancel) --> Canceled;
    SignalExternalCommandCreated --(SignalExternalWorkflowExecutionInitiated, on_signal_external_workflow_execution_initiated) --> SignalExternalCommandRecorded;

    SignalExternalCommandRecorded --(Cancel) --> SignalExternalCommandRecorded;
    SignalExternalCommandRecorded --(ExternalWorkflowExecutionSignaled, on_external_workflow_execution_signaled) --> Signaled;
    SignalExternalCommandRecorded --(SignalExternalWorkflowExecutionFailed, on_signal_external_workflow_execution_failed) --> Failed;
}

#[derive(thiserror::Error, Debug)]
pub enum SignalExternalMachineError {}

pub enum SignalExternalCommand {}

#[derive(Default, Clone)]
pub struct Canceled {}

#[derive(Default, Clone)]
pub struct Created {}

impl Created {
    pub fn on_schedule(self) -> SignalExternalMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct Failed {}

#[derive(Default, Clone)]
pub struct SignalExternalCommandCreated {}

impl SignalExternalCommandCreated {
    pub fn on_cancel(self) -> SignalExternalMachineTransition {
        unimplemented!()
    }
    pub fn on_signal_external_workflow_execution_initiated(
        self,
    ) -> SignalExternalMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct SignalExternalCommandRecorded {}

impl SignalExternalCommandRecorded {
    pub fn on_external_workflow_execution_signaled(self) -> SignalExternalMachineTransition {
        unimplemented!()
    }
    pub fn on_signal_external_workflow_execution_failed(self) -> SignalExternalMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct Signaled {}
