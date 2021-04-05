use rustfsm::{fsm, TransitionResult};

fsm! {
    pub(super) name SideEffectMachine; command SideEffectCommand; error SideEffectMachineError;

    Created --(Schedule, on_schedule) --> MarkerCommandCreated;
    Created --(Schedule, on_schedule) --> MarkerCommandCreatedReplaying;

    MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> ResultNotified;

    MarkerCommandCreatedReplaying --(CommandRecordMarker) --> ResultNotifiedReplaying;

    ResultNotified --(MarkerRecorded, on_marker_recorded) --> MarkerCommandRecorded;

    ResultNotifiedReplaying --(MarkerRecorded, on_marker_recorded) --> MarkerCommandRecorded;
}

#[derive(thiserror::Error, Debug)]
pub(super) enum SideEffectMachineError {}

pub(super) enum SideEffectCommand {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> SideEffectMachineTransition<MarkerCommandCreatedOrMarkerCommandCreatedReplaying> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandCreated {}

impl MarkerCommandCreated {
    pub(super) fn on_command_record_marker(self) -> SideEffectMachineTransition<ResultNotified> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandCreatedReplaying {}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandRecorded {}

#[derive(Default, Clone)]
pub(super) struct ResultNotified {}

impl ResultNotified {
    pub(super) fn on_marker_recorded(self) -> SideEffectMachineTransition<MarkerCommandRecorded> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct ResultNotifiedReplaying {}

impl ResultNotifiedReplaying {
    pub(super) fn on_marker_recorded(self) -> SideEffectMachineTransition<MarkerCommandRecorded> {
        unimplemented!()
    }
}

impl From<MarkerCommandCreatedReplaying> for ResultNotifiedReplaying {
    fn from(_: MarkerCommandCreatedReplaying) -> Self {
        Self::default()
    }
}
