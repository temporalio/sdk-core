use crate::machines::{NewMachineWithCommand, WFMachinesError};
use rustfsm::{fsm, TransitionResult};

fsm! {
    pub(super) name VersionMachine;
    command VersionCommand;
    error WFMachinesError;

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

#[derive(Debug, derive_more::Display)]
pub(super) enum VersionCommand {
    // TODO: probably need to include change ID
    /// Issued when the version machine resolves with what version the workflow should be told about
    #[display(fmt = "ReturnVersion({})", _0)]
    ReturnVersion(usize),
}

/// Version machines are created when the user invokes `get_version` (or whatever it may be named
/// in that lang).
///
/// `change_id`: identifier of a particular change. All calls to get_version that share a change id
/// are guaranteed to return the same version number.
/// `replaying_when_invoked`: If the workflow is replaying when this invocation occurs, this needs
/// to be set to true.
pub(super) fn get_version_invoked(
    change_id: String,
    replaying_when_invoked: bool,
) -> NewMachineWithCommand<VersionMachine> {
    let (activity, add_cmd) = VersionMachine::new_scheduled(attribs);
    NewMachineWithCommand {
        command: add_cmd,
        machine: activity,
    }
}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_check_execution_state(self) -> VersionMachineTransition<ReplayingOrExecuting> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct Executing {}

impl Executing {
    pub(super) fn on_schedule(self) -> VersionMachineTransition<MarkerCommandCreatedOrSkipped> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandCreated {}

impl MarkerCommandCreated {
    pub(super) fn on_command_record_marker(self) -> VersionMachineTransition<ResultNotified> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandCreatedReplaying {}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandRecorded {}

#[derive(Default, Clone)]
pub(super) struct Replaying {}

impl Replaying {
    pub(super) fn on_schedule(self) -> VersionMachineTransition<MarkerCommandCreatedReplaying> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct ResultNotified {}

impl ResultNotified {
    pub(super) fn on_marker_recorded(self) -> VersionMachineTransition<MarkerCommandRecorded> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct ResultNotifiedReplaying {}

impl ResultNotifiedReplaying {
    pub(super) fn on_non_matching_event(self) -> VersionMachineTransition<SkippedNotified> {
        unimplemented!()
    }
    pub(super) fn on_marker_recorded(
        self,
    ) -> VersionMachineTransition<MarkerCommandRecordedOrSkippedNotified> {
        unimplemented!()
    }
}

impl From<MarkerCommandCreatedReplaying> for ResultNotifiedReplaying {
    fn from(_: MarkerCommandCreatedReplaying) -> Self {
        Self::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Skipped {}

impl Skipped {
    pub(super) fn on_command_record_marker(self) -> VersionMachineTransition<SkippedNotified> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct SkippedNotified {}
