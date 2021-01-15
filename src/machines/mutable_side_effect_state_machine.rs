use rustfsm::{fsm, TransitionResult};

fsm! {
    name MutableSideEffectMachine; command MutableSideEffectCommand; error MutableSideEffectMachineError;

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
pub enum MutableSideEffectMachineError {}

pub enum MutableSideEffectCommand {}

#[derive(Default, Clone)]
pub struct Created {}

impl Created {
    pub fn on_check_execution_state(self) -> MutableSideEffectMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct Executing {}

impl Executing {
    pub fn on_schedule(self) -> MutableSideEffectMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct MarkerCommandCreated {}

impl MarkerCommandCreated {
    pub fn on_command_record_marker(self) -> MutableSideEffectMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct MarkerCommandCreatedReplaying {}

#[derive(Default, Clone)]
pub struct MarkerCommandRecorded {}

#[derive(Default, Clone)]
pub struct Replaying {}

impl Replaying {
    pub fn on_schedule(self) -> MutableSideEffectMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct ResultNotified {}

impl ResultNotified {
    pub fn on_marker_recorded(self) -> MutableSideEffectMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct ResultNotifiedReplaying {}

impl ResultNotifiedReplaying {
    pub fn on_non_matching_event(self) -> MutableSideEffectMachineTransition {
        unimplemented!()
    }
    pub fn on_marker_recorded(self) -> MutableSideEffectMachineTransition {
        unimplemented!()
    }
}

impl From<MarkerCommandCreatedReplaying> for ResultNotifiedReplaying {
    fn from(_: MarkerCommandCreatedReplaying) -> Self {
        Self::default()
    }
}

#[derive(Default, Clone)]
pub struct Skipped {}

impl Skipped {
    pub fn on_command_record_marker(self) -> MutableSideEffectMachineTransition {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub struct SkippedNotified {}
