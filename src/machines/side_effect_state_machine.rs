use rustfsm::{fsm, TransitionResult};

fsm! {
	SideEffectMachine, SideEffectCommand, SideEffectMachineError

	Created --(Schedule, on_schedule) --> MarkerCommandCreated;
	Created --(Schedule, on_schedule) --> MarkerCommandCreatedReplaying;

	MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> ResultNotified;

	MarkerCommandCreatedReplaying --(CommandRecordMarker) --> ResultNotifiedReplaying;

	ResultNotified --(MarkerRecorded, on_marker_recorded) --> MarkerCommandRecorded;

	ResultNotifiedReplaying --(MarkerRecorded, on_marker_recorded) --> MarkerCommandRecorded;
}

#[derive(thiserror::Error, Debug)]
pub enum SideEffectMachineError {}
pub enum SideEffectCommand {}

#[derive(Default)]
pub struct Created {}
impl Created {
	pub fn on_schedule(self) -> SideEffectMachineTransition { unimplemented!() }
}

#[derive(Default)]
pub struct MarkerCommandCreated {}
impl MarkerCommandCreated {
	pub fn on_command_record_marker(self) -> SideEffectMachineTransition { unimplemented!() }
}

#[derive(Default)]
pub struct MarkerCommandCreatedReplaying {}

#[derive(Default)]
pub struct MarkerCommandRecorded {}

#[derive(Default)]
pub struct ResultNotified {}
impl ResultNotified {
	pub fn on_marker_recorded(self) -> SideEffectMachineTransition { unimplemented!() }
}

#[derive(Default)]
pub struct ResultNotifiedReplaying {}
impl ResultNotifiedReplaying {
	pub fn on_marker_recorded(self) -> SideEffectMachineTransition { unimplemented!() }
}

