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
    protos::{
        coresdk::common::build_has_change_marker_details,
        temporal::api::{
            command::v1::{Command, RecordMarkerCommandAttributes},
            enums::v1::CommandType,
            history::v1::HistoryEvent,
        },
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

    MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> Notified;
    MarkerCommandCreatedReplaying --(CommandRecordMarker) --> Notified;

    Notified --(MarkerRecorded(String), shared on_marker_recorded) --> MarkerCommandRecorded;
}

#[derive(Clone)]
pub(super) struct SharedState {
    change_id: String,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum VersionCommand {}

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
    pub(super) fn on_command_record_marker(self) -> VersionMachineTransition<Notified> {
        TransitionResult::commands(vec![])
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
impl From<MarkerCommandCreatedReplaying> for Notified {
    fn from(_: MarkerCommandCreatedReplaying) -> Self {
        Self::default()
    }
}
impl Notified {
    pub(super) fn on_marker_recorded(
        self,
        dat: SharedState,
        id: String,
    ) -> VersionMachineTransition<MarkerCommandRecorded> {
        if id != dat.change_id {
            return TransitionResult::Err(WFMachinesError::Nondeterminism(format!(
                "Change id {} does not match expected id {}",
                id, dat.change_id
            )));
        }
        TransitionResult::default()
    }
}

impl WFMachinesAdapter for VersionMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        panic!("Version machine does not produce commands")
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        event.get_changed_marker_details().is_some()
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

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        match e.get_changed_marker_details() {
            Some((id, _)) => Ok(VersionMachineEvents::MarkerRecorded(id)),
            _ => Err(WFMachinesError::Nondeterminism(format!(
                "Change machine cannot handle this event: {}",
                e
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        machines::WFMachinesError,
        protos::coresdk::{
            workflow_activation::{wf_activation_job, ResolveHasChange, WfActivationJob},
            workflow_commands::StartTimer,
        },
        protos::temporal::api::enums::v1::{CommandType, EventType},
        protos::temporal::api::history::v1::{history_event, TimerFiredEventAttributes},
        prototype_rust_sdk::{WfContext, WorkflowFunction},
        test_help::canned_histories,
        tracing_init,
        workflow::managed_wf::ManagedWFFunc,
    };
    use rstest::{fixture, rstest};
    use std::time::Duration;

    const MY_CHANGE_ID: &str = "change_name";

    #[fixture(
        replaying = false,
        deprecated = false,
        call_deprecated = false,
        marker = false
    )]
    fn version_hist(
        replaying: bool,
        deprecated: bool,
        call_deprecated: bool,
        marker: bool,
    ) -> ManagedWFFunc {
        let wfn = WorkflowFunction::new(move |mut ctx: WfContext| async move {
            if ctx.has_version(MY_CHANGE_ID, call_deprecated) {
                ctx.timer(StartTimer {
                    timer_id: "had_change".to_string(),
                    start_to_fire_timeout: Some(Duration::from_secs(1).into()),
                })
                .await;
                if !marker {
                    panic!("False branch should be taken if there is no marker in history");
                }
            } else if marker {
                panic!("True branch should be taken unless there is no marker in history");
            } else {
                ctx.timer(StartTimer {
                    timer_id: "no_change".to_string(),
                    start_to_fire_timeout: Some(Duration::from_secs(1).into()),
                })
                .await;
            }
            Ok(().into())
        });

        let t = if marker {
            canned_histories::has_change_different_timers(Some(MY_CHANGE_ID), deprecated)
        } else {
            canned_histories::has_change_different_timers(None, deprecated)
        };
        let histinfo = if replaying {
            t.get_full_history_info()
        } else {
            t.get_history_info(1)
        };
        ManagedWFFunc::new_from_update(histinfo.unwrap().into(), wfn, vec![])
    }

    // Deprecated or not in history irrelevant for this test since we don't replay the event
    #[rstest]
    #[case::call_not_dep(version_hist(false, false, false, true))]
    #[case::call_has_dep(version_hist(false, false, true, true))]
    #[tokio::test]
    async fn version_machine_with_call_not_replay(#[case] mut wfm: ManagedWFFunc) {
        // Start workflow activation
        let act = wfm.get_next_activation().await.unwrap();
        assert!(!act.is_replaying);
        // There is no activation for the change command, as we know that we are not replaying and
        // the workflow unblocks itself
        let commands = wfm.get_server_commands().await.commands;
        // Should have record marker and start timer
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
        assert_eq!(commands[1].command_type, CommandType::StartTimer as i32);

        // Feed more history, to verify correct branch is chosen (by seeing the right timer fire)
        wfm.new_history(
            canned_histories::has_change_different_timers(Some(MY_CHANGE_ID), false)
                .get_full_history_info()
                .unwrap()
                .into(),
        )
        .await
        .unwrap();

        wfm.shutdown().await.unwrap();
    }

    #[derive(Eq, PartialEq)]
    enum CallOutcome {
        HasChange,
        NoChange,
        MarkerMissing,
    }
    use crate::test_help::TestHistoryBuilder;
    use CallOutcome::*;

    #[rstest]
    #[case::call_not_dep_marker_not(version_hist(true, false, false, true), HasChange)]
    #[case::call_not_dep_marker_is(version_hist(true, true, false, true), HasChange)]
    #[case::call_dep_marker_not(version_hist(true, false, true, true), HasChange)]
    #[case::call_dep_marker_is(version_hist(true, true, true, true), HasChange)]
    #[case::call_not_dep_no_marker(version_hist(true, false, false, false), NoChange)]
    #[case::call_dep_no_marker(version_hist(true, false, true, false), MarkerMissing)]
    #[tokio::test]
    async fn version_machine_with_call_replay(
        #[case] mut wfm: ManagedWFFunc,
        #[case] call_outcome: CallOutcome,
    ) {
        let act = wfm.get_next_activation().await;

        if call_outcome == MarkerMissing {
            assert_matches!(act.unwrap_err(), WFMachinesError::Nondeterminism(_));
        } else {
            let act = act.unwrap();

            assert_matches!(
                act.jobs[0],
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::StartWorkflow(_))
                }
            );
            // If we have the change marker in the history, we should always expect to see the
            // change get pre-emptively resolved in the first workflow task
            if call_outcome == HasChange {
                assert_matches!(
                    &act.jobs[1],
                     WfActivationJob {
                        variant: Some(wf_activation_job::Variant::ResolveHasChange(
                            ResolveHasChange {
                                change_id,
                                is_present
                            }
                        ))
                    } => change_id == MY_CHANGE_ID && *is_present
                );
            } else if call_outcome == NoChange {
                // If there's no change marker, and we do not set the deprecated flag, the call
                // should resolve as false, but it will not happen pre-emptively.
                assert_eq!(act.jobs.len(), 1);
            } else {
                panic!("Test should've exited already, failing the activation")
            };

            // Timer should get fired
            let act = wfm.get_next_activation().await.unwrap();
            assert_matches!(
                act.jobs.as_slice(),
                [WfActivationJob {
                    variant: Some(wf_activation_job::Variant::FireTimer(_))
                }]
            );
        };

        wfm.shutdown().await.unwrap();
    }

    #[fixture(deprecated = false)]
    fn version_no_call(deprecated: bool) -> ManagedWFFunc {
        let wfn = WorkflowFunction::new(move |mut ctx: WfContext| async move {
            ctx.timer(StartTimer {
                timer_id: "had_change".to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(1).into()),
            })
            .await;
            Ok(().into())
        });

        let t = canned_histories::has_change_different_timers(Some(MY_CHANGE_ID), deprecated);
        ManagedWFFunc::new_from_update(t.get_full_history_info().unwrap().into(), wfn, vec![])
    }

    // The no-marker-no-call case isn't interesting. Nothing in here is involved.
    #[rstest]
    #[case(version_no_call(false), false)]
    #[case::deprecated(version_no_call(true), true)]
    #[tokio::test]
    async fn version_machine_no_call_replay(
        #[case] mut wfm: ManagedWFFunc,
        #[case] deprecated: bool,
    ) {
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
            }] => change_id == MY_CHANGE_ID && *is_present
        );
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);

        let act = wfm.get_next_activation().await;
        if deprecated {
            // Should be fine, deprecated marker is ignored even if we didn't make change call
            act.unwrap();
        } else {
            // Should error out because we didn't send the record marker command
            let err = act.err().unwrap();
            assert_matches!(err, WFMachinesError::Nondeterminism(_));
        }

        wfm.shutdown().await.unwrap();
    }

    #[rstest]
    #[case::has_change_replay(true, true)]
    #[case::no_change_replay(false, true)]
    #[case::has_change_inc(true, false)]
    // The false-false case doesn't make sense, as the incremental cases act as if working against
    // a sticky queue, and it'd be impossible for a worker with the call to get an incremental
    // history that then suddenly doesn't have the marker.
    #[tokio::test]
    async fn same_change_multiple_spots(#[case] have_marker_in_hist: bool, #[case] replay: bool) {
        tracing_init();

        let wfn = WorkflowFunction::new(move |mut ctx: WfContext| async move {
            if ctx.has_version(MY_CHANGE_ID, false) {
                ctx.timer(StartTimer {
                    timer_id: "had_change_1".to_string(),
                    start_to_fire_timeout: Some(Duration::from_secs(1).into()),
                })
                .await;
            } else {
                ctx.timer(StartTimer {
                    timer_id: "no_change_1".to_string(),
                    start_to_fire_timeout: Some(Duration::from_secs(1).into()),
                })
                .await;
            }
            ctx.timer(StartTimer {
                timer_id: "always_timer".to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(1).into()),
            })
            .await;
            if ctx.has_version(MY_CHANGE_ID, false) {
                ctx.timer(StartTimer {
                    timer_id: "had_change_2".to_string(),
                    start_to_fire_timeout: Some(Duration::from_secs(1).into()),
                })
                .await;
            } else {
                ctx.timer(StartTimer {
                    timer_id: "no_change_2".to_string(),
                    start_to_fire_timeout: Some(Duration::from_secs(1).into()),
                })
                .await;
            }
            Ok(().into())
        });

        // Just build the history directly in here since it's very specific to this test
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        if have_marker_in_hist {
            t.add_has_change_marker(MY_CHANGE_ID, false);
        }
        let timer_id = if have_marker_in_hist {
            "had_change_1"
        } else {
            "no_change_1"
        };
        let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
        t.add(
            EventType::TimerFired,
            history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
                started_event_id: timer_started_event_id,
                timer_id: timer_id.to_string(),
            }),
        );
        t.add_full_wf_task();
        let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
        t.add(
            EventType::TimerFired,
            history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
                started_event_id: timer_started_event_id,
                timer_id: "always_timer".to_string(),
            }),
        );
        t.add_full_wf_task();
        let timer_id = if have_marker_in_hist {
            "had_change_2"
        } else {
            "no_change_2"
        };
        let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
        t.add(
            EventType::TimerFired,
            history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
                started_event_id: timer_started_event_id,
                timer_id: timer_id.to_string(),
            }),
        );
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let mut wfm = if replay {
            let mut wfm = ManagedWFFunc::new_from_update(
                t.get_full_history_info().unwrap().into(),
                wfn,
                vec![],
            );
            // Errors would appear as nondeterminism problems
            wfm.process_all_activations().await.unwrap();
            wfm
        } else {
            let mut wfm =
                ManagedWFFunc::new_from_update(t.get_history_info(1).unwrap().into(), wfn, vec![]);
            wfm.process_all_activations().await.unwrap();
            for i in 2..=4 {
                wfm.new_history(t.get_history_info(i).unwrap().into())
                    .await
                    .unwrap();
                wfm.process_all_activations().await.unwrap();
            }
            wfm
        };

        wfm.shutdown().await.unwrap();
    }
}
