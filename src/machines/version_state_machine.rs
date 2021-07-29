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
        workflow_machines::MachineResponse, Cancellable, EventInfo, NewMachineWithCommand,
        OnEventWrapper, WFMachinesAdapter, WFMachinesError,
    },
    protos::coresdk::{
        common::build_has_change_marker_details, workflow_activation::ResolveHasChange,
    },
    protos::temporal::api::{
        command::v1::{Command, RecordMarkerCommandAttributes},
        enums::v1::CommandType,
        history::v1::HistoryEvent,
    },
};
use rustfsm::{fsm, TransitionResult};
use std::convert::TryFrom;

pub const HAS_CHANGE_MARKER_NAME: &str = "core_has_change";

fsm! {
    pub(super) name VersionMachine;
    command VersionCommand;
    error WFMachinesError;
    shared_state SharedState;

    Executing --(Schedule, on_schedule) --> MarkerCommandCreated;
    Replaying --(Schedule, on_schedule) --> MarkerCommandCreatedReplaying;
    // TODO: Already seen marker before this command "known" path

    // Executing path =======================================================================
    MarkerCommandCreated --(CommandRecordMarker, shared on_command_record_marker) --> Notified;

    Notified --(MarkerRecorded, on_marker_recorded) --> MarkerCommandRecorded;

    // Replaying path =======================================================================
    MarkerCommandCreatedReplaying --(CommandRecordMarker) --> AwaitingEvent;

    AwaitingEvent --(MarkerRecorded, on_marker_recorded) --> MarkerCommandRecorded;
    AwaitingEvent --(NonMatchingEvent) --> NotifiedError;
}

#[derive(Clone)]
pub(super) struct SharedState {
    change_id: String,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum VersionCommand {
    /// Issued when the version machine finds resolves the `has_change` call for the indicated
    /// change id, with the bool flag being true if the change should be considered present
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
pub(super) fn has_change(
    change_id: String,
    replaying_when_invoked: bool,
    deprecated: bool,
) -> NewMachineWithCommand<VersionMachine> {
    let (machine, command) = VersionMachine::new_scheduled(
        SharedState { change_id },
        replaying_when_invoked,
        deprecated,
    );
    NewMachineWithCommand { command, machine }
}

impl VersionMachine {
    fn new_scheduled(
        state: SharedState,
        replaying_when_invoked: bool,
        deprecated: bool,
    ) -> (Self, Command) {
        let initial_state = if replaying_when_invoked {
            Replaying {}.into()
        } else {
            Executing {}.into()
        };
        let cmd = Command {
            command_type: CommandType::RecordMarker as i32,
            attributes: Some(
                RecordMarkerCommandAttributes {
                    marker_name: HAS_CHANGE_MARKER_NAME.to_string(),
                    details: build_has_change_marker_details(&state.change_id, deprecated),
                    header: None,
                    failure: None,
                }
                .into(),
            ),
        };
        let mut machine = VersionMachine {
            state: initial_state,
            shared_state: state,
        };
        OnEventWrapper::on_event_mut(&mut machine, VersionMachineEvents::Schedule)
            .expect("Version machine scheduling doesn't fail");

        (machine, cmd)
    }
}

#[derive(Default, Clone)]
pub(super) struct Executing {}

impl Executing {
    pub(super) fn on_schedule(self) -> VersionMachineTransition<MarkerCommandCreated> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandCreated {}

impl MarkerCommandCreated {
    pub(super) fn on_command_record_marker(
        self,
        dat: SharedState,
    ) -> VersionMachineTransition<Notified> {
        // We are *not* replaying, so immediately unblock the workflow with true for the result
        TransitionResult::commands(vec![VersionCommand::ChangeStatus(dat.change_id, true)])
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
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Notified {}

impl Notified {
    pub(super) fn on_marker_recorded(self) -> VersionMachineTransition<MarkerCommandRecorded> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct AwaitingEvent {}
impl From<MarkerCommandCreatedReplaying> for AwaitingEvent {
    fn from(_: MarkerCommandCreatedReplaying) -> Self {
        Self::default()
    }
}
impl AwaitingEvent {
    pub(super) fn on_marker_recorded(self) -> VersionMachineTransition<MarkerCommandRecorded> {
        todo!()
    }
}

#[derive(Default, Clone)]
pub(super) struct NotifiedError {}
impl From<AwaitingEvent> for NotifiedError {
    fn from(_: AwaitingEvent) -> Self {
        Self::default()
    }
}

impl WFMachinesAdapter for VersionMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            VersionCommand::ChangeStatus(change_id, is_present) => {
                vec![ResolveHasChange {
                    change_id,
                    is_present,
                }
                .into()]
            }
        })
    }
}

impl Cancellable for VersionMachine {}

impl TryFrom<CommandType> for VersionMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::RecordMarker => Self::CommandRecordMarker,
            _ => return Err(()),
        })
    }
}

impl TryFrom<HistoryEvent> for VersionMachineEvents {
    type Error = WFMachinesError;

    fn try_from(value: HistoryEvent) -> Result<Self, Self::Error> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        protos::coresdk::{
            workflow_activation::{wf_activation_job, ResolveHasChange, WfActivationJob},
            workflow_commands::StartTimer,
        },
        protos::temporal::api::enums::v1::CommandType,
        prototype_rust_sdk::{WfContext, WorkflowFunction},
        test_help::canned_histories,
        tracing_init,
        workflow::managed_wf::ManagedWFFunc,
    };
    use rstest::{fixture, rstest};
    use std::time::Duration;

    const MY_CHANGE_ID: &str = "change_name";

    #[fixture(replaying = false)]
    fn version_hist(replaying: bool) -> ManagedWFFunc {
        let wfn = WorkflowFunction::new(move |mut ctx: WfContext| async move {
            if ctx.has_version(MY_CHANGE_ID).await {
                ctx.timer(StartTimer {
                    timer_id: "had_change".to_string(),
                    start_to_fire_timeout: Some(Duration::from_secs(1).into()),
                })
                .await;
            } else {
                ctx.timer(StartTimer {
                    timer_id: "no_change".to_string(),
                    start_to_fire_timeout: Some(Duration::from_secs(1).into()),
                })
                .await;
            }
            Ok(().into())
        });

        let t = canned_histories::has_change_different_timers(Some(MY_CHANGE_ID), false);
        let histinfo = if replaying {
            t.get_full_history_info()
        } else {
            t.get_history_info(1)
        };
        ManagedWFFunc::new_from_update(histinfo.unwrap().into(), wfn, vec![])
    }

    #[rstest]
    #[case(version_hist(false))]
    #[tokio::test]
    async fn version_machine_with_call_not_replay(#[case] mut wfm: ManagedWFFunc) {
        tracing_init();

        // Start workflow activation
        let act = wfm.get_next_activation().await.unwrap();
        assert!(!act.is_replaying);
        // There should immediately be another activation unblocking the has version call
        let act = wfm.get_next_activation().await.unwrap();
        assert_matches!(
            act.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveHasChange(
                    ResolveHasChange {
                        change_id,
                        is_present
                    }
                ))
            }] => change_id == MY_CHANGE_ID && *is_present == true
        );
        let commands = wfm.get_server_commands().await.commands;
        // Should have record marker and start timer
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
        assert_eq!(commands[1].command_type, CommandType::StartTimer as i32);

        dbg!(wfm.get_next_activation().await.unwrap());

        wfm.shutdown().await.unwrap();
    }

    #[rstest]
    #[case(version_hist(true))]
    #[tokio::test]
    async fn version_machine_with_call_replay(#[case] mut wfm: ManagedWFFunc) {
        tracing_init();

        // Start workflow activation and resolve change should come right away
        let act = wfm.get_next_activation().await.unwrap();
        assert_matches!(
            act.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::StartWorkflow(_))
             },
             WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveHasChange(
                    ResolveHasChange {
                        change_id,
                        is_present
                    }
                ))
            }] => change_id == MY_CHANGE_ID && *is_present == true
        );
        let commands = wfm.get_server_commands().await.commands;
        // Should have record marker and start timer
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
        assert_eq!(commands[1].command_type, CommandType::StartTimer as i32);

        dbg!(wfm.get_next_activation().await.unwrap());

        wfm.shutdown().await.unwrap();
    }
}
