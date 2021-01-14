use rustfsm::{fsm, TransitionResult};

fsm! {
	SignalExternalMachine, SignalExternalCommand, SignalExternalMachineError

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

#[derive(Default)]
pub struct Canceled {}

#[derive(Default)]
pub struct Created {}
impl Created {
	pub fn on_schedule(self) -> SignalExternalMachineTransition { unimplemented!() }
}

#[derive(Default)]
pub struct Failed {}

#[derive(Default)]
pub struct SignalExternalCommandCreated {}
impl SignalExternalCommandCreated {
	pub fn on_cancel(self) -> SignalExternalMachineTransition { unimplemented!() }
	pub fn on_signal_external_workflow_execution_initiated(self) -> SignalExternalMachineTransition { unimplemented!() }
}

#[derive(Default)]
pub struct SignalExternalCommandRecorded {}
impl SignalExternalCommandRecorded {
	pub fn on_external_workflow_execution_signaled(self) -> SignalExternalMachineTransition { unimplemented!() }
	pub fn on_signal_external_workflow_execution_failed(self) -> SignalExternalMachineTransition { unimplemented!() }
}

#[derive(Default)]
pub struct Signaled {}

