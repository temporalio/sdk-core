//! The version machine can be difficult to follow. Refer to this table for behavior:
//!
//! | History Has                  | Workflow Has          | Outcome                                                                            |
//! |------------------------------|-----------------------|------------------------------------------------------------------------------------|
//! | not replaying                | no has_change         | Nothing interesting. Versioning not involved.                                      |
//! | marker for change            | no has_change         | No matching command / workflow does not support this version                       |
//! | deprecated marker for change | no has_change         | Marker ignored, workflow continues as if it didn't exist                           |
//! | replaying, no marker         | no has_change         | Nothing interesting. Versioning not involved.                                      |
//! | not replaying                | has_change            | Marker command sent to server and recorded. Call returns true                      |
//! | marker for change            | has_change            | Call returns true upon replay                                                      |
//! | deprecated marker for change | has_change            | Call returns true upon replay                                                      |
//! | replaying, no marker         | has_change            | Call returns false upon replay                                                     |
//! | not replaying                | has_change deprecated | Marker command sent to server and recorded with deprecated flag. Call returns true |
//! | marker for change            | has_change deprecated | Call returns true upon replay                                                      |
//! | deprecated marker for change | has_change deprecated | Call returns true upon replay                                                      |
//! | replaying, no marker         | has_change deprecated | No matching event / history too old or too new                                     |

use crate::{
    machines::{
        workflow_machines::MachineResponse, Cancellable, OnEventWrapper, WFMachinesAdapter,
        WFMachinesError,
    },
    protos::temporal::api::{enums::v1::CommandType, history::v1::HistoryEvent},
};
use rustfsm::{fsm, TransitionResult};
use std::convert::TryFrom;

fsm! {
    pub(super) name VersionMachine;
    command VersionCommand;
    error WFMachinesError;
    shared_state SharedState;

    Executing --(Schedule, on_schedule) --> MarkerCommandCreated;
    Executing --(Schedule, on_schedule) --> Skipped;

    Replaying --(Schedule, on_schedule) --> MarkerCommandCreatedReplaying;

    MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> ResultNotified;

    MarkerCommandCreatedReplaying --(CommandRecordMarker) --> ResultNotifiedReplaying;

    ResultNotified --(MarkerRecorded, on_marker_recorded) --> MarkerCommandRecorded;

    ResultNotifiedReplaying --(NonMatchingEvent, on_non_matching_event) --> SkippedNotified;
    ResultNotifiedReplaying --(MarkerRecorded, on_marker_recorded) --> MarkerCommandRecorded;
    ResultNotifiedReplaying --(MarkerRecorded, on_marker_recorded) --> SkippedNotified;

    Skipped --(CommandRecordMarker, on_command_record_marker) --> SkippedNotified;
}

#[derive(Clone)]
pub(super) struct SharedState {
    change_id: String,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum VersionCommand {
    /// Issued when the version machine finds resolves the `has_change` call for the indicated
    /// change id, with the bool flag being true if the marker is present in history.
    #[display(fmt = "ChangeStatus({}, {})", _0, _1)]
    ChangeStatus(String, bool),
}

/// Version machines are created when the user invokes `has_change` (or whatever it may be named
/// in that lang).
///
/// `change_id`: identifier of a particular change. All calls to get_version that share a change id
/// are guaranteed to return the same value.
/// `replaying_when_invoked`: If the workflow is replaying when this invocation occurs, this needs
/// to be set to true.
pub(super) fn has_change(change_id: String, replaying_when_invoked: bool) -> VersionMachine {
    VersionMachine::new_scheduled(SharedState { change_id }, replaying_when_invoked)
}

impl VersionMachine {
    fn new_scheduled(state: SharedState, replaying_when_invoked: bool) -> Self {
        let initial_state = if replaying_when_invoked {
            Replaying {}.into()
        } else {
            Executing {}.into()
        };
        let mut machine = VersionMachine {
            state: initial_state,
            shared_state: state,
        };
        OnEventWrapper::on_event_mut(&mut machine, VersionMachineEvents::Schedule)
            .expect("Version machine scheduling doesn't fail");
        machine
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

impl WFMachinesAdapter for VersionMachine {
    fn adapt_response(
        &self,
        event: &HistoryEvent,
        has_next_event: bool,
        my_command: Self::Command,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        todo!()
    }
}

impl Cancellable for VersionMachine {}

impl TryFrom<CommandType> for VersionMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl TryFrom<HistoryEvent> for VersionMachineEvents {
    type Error = WFMachinesError;

    fn try_from(value: HistoryEvent) -> Result<Self, Self::Error> {
        todo!()
    }
}
