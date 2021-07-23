use crate::{
    machines::{
        workflow_machines::MachineResponse, Cancellable, OnEventWrapper, WFMachinesAdapter,
        WFMachinesError,
    },
    protos::temporal::api::{enums::v1::CommandType, history::v1::HistoryEvent},
};
use rustfsm::{fsm, TransitionResult};
use std::{convert::TryFrom, num::NonZeroU32};

fsm! {
    pub(super) name VersionMachine;
    command VersionCommand;
    error WFMachinesError;
    shared_state SharedState;

    // Bool inside this event is if we are replaying or not
    Created --(CheckExecutionState(bool), on_check_execution_state) --> Replaying;
    Created --(CheckExecutionState(bool), on_check_execution_state) --> Executing;

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

#[derive(Clone)]
pub enum MinVersion {
    Default,
    Numbered(u32),
}
impl From<u32> for MinVersion {
    fn from(v: u32) -> Self {
        if v == 0 {
            MinVersion::Default
        } else {
            MinVersion::Numbered(v)
        }
    }
}

#[derive(Clone)]
pub(super) struct SharedState {
    change_id: String,
    min_version: MinVersion,
    max_version: NonZeroU32,
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
pub(super) fn version_check(
    change_id: String,
    replaying_when_invoked: bool,
    min_version: MinVersion,
    max_version: NonZeroU32,
) -> VersionMachine {
    VersionMachine::new_scheduled(
        SharedState {
            change_id,
            min_version,
            max_version,
        },
        replaying_when_invoked,
    )
}

impl VersionMachine {
    fn new_scheduled(state: SharedState, replaying_when_invoked: bool) -> Self {
        let mut machine = VersionMachine {
            state: Created {}.into(),
            shared_state: state,
        };
        OnEventWrapper::on_event_mut(
            &mut machine,
            VersionMachineEvents::CheckExecutionState(replaying_when_invoked),
        )
        .expect("Version machine checking replay state doesn't fail");
        OnEventWrapper::on_event_mut(&mut machine, VersionMachineEvents::Schedule)
            .expect("Version machine scheduling doesn't fail");
        machine
    }
}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_check_execution_state(
        self,
        replaying_when_invoked: bool,
    ) -> VersionMachineTransition<ReplayingOrExecuting> {
        if replaying_when_invoked {
            TransitionResult::ok(vec![], ReplayingOrExecuting::Replaying(Replaying {}))
        } else {
            TransitionResult::ok(vec![], ReplayingOrExecuting::Executing(Executing {}))
        }
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
