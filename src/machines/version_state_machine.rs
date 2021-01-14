use rustfsm::{fsm, TransitionResult};

fsm! {
    VersionMachine, VersionCommand, VersionMachineError

    Created --(CheckExecutionState, on_check_execution_state) --> Replaying;
    Created --(CheckExecutionState, on_check_execution_state) --> Executing;

    Executing --(Schedule, on_schedule) --> MarkerCommandCreated;
    Executing --(Schedule, on_schedule) --> Skipped;

    MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> ResultNotified;

    MarkerCommandCreatedReplaying --(CommandRecordMarker) --> ResultNotifiedReplaying;

    Replaying --(Schedule, on_schedule) --> MarkerCommandCreatedReplaying;

    ResultNotified --(MarkerRecorded, on_marker_recorded) --> MarkerCommandRecorded;

    ResultNotifiedReplaying --(NonMatchingEvent, on_non_matching_event) --> SkippedNotified;
    ResultNotifiedReplaying --(MarkerRecorded, on_marker_recorded) --> MarkerCommandRecorded;
    ResultNotifiedReplaying --(MarkerRecorded, on_marker_recorded) --> SkippedNotified;

    Skipped --(CommandRecordMarker, on_command_record_marker) --> SkippedNotified;
}

#[derive(thiserror::Error, Debug)]
pub enum VersionMachineError {}

pub enum VersionCommand {}

#[derive(Default)]
pub struct Created {}

impl Created {
    pub fn on_check_execution_state(self) -> VersionMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct Executing {}

impl Executing {
    pub fn on_schedule(self) -> VersionMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct MarkerCommandCreated {}

impl MarkerCommandCreated {
    pub fn on_command_record_marker(self) -> VersionMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct MarkerCommandCreatedReplaying {}

#[derive(Default)]
pub struct MarkerCommandRecorded {}

#[derive(Default)]
pub struct Replaying {}

impl Replaying {
    pub fn on_schedule(self) -> VersionMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct ResultNotified {}

impl ResultNotified {
    pub fn on_marker_recorded(self) -> VersionMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct ResultNotifiedReplaying {}

impl ResultNotifiedReplaying {
    pub fn on_non_matching_event(self) -> VersionMachineTransition {
        unimplemented!()
    }
    pub fn on_marker_recorded(self) -> VersionMachineTransition {
        unimplemented!()
    }
}

impl From<MarkerCommandCreatedReplaying> for ResultNotifiedReplaying {
    fn from(_: MarkerCommandCreatedReplaying) -> Self {
        Self::default()
    }
}

#[derive(Default)]
pub struct Skipped {}

impl Skipped {
    pub fn on_command_record_marker(self) -> VersionMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct SkippedNotified {}
