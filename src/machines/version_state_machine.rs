//! The version machine can be difficult to follow. Refer to below table for behavior. The
//! deprecated calls simply say "allowed" because if they returned a value, it's always true. Old
//! code cannot exist in workflows which use the deprecated call.
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
//! | not replaying                | has_change deprecated | Marker command sent to server and recorded with deprecated flag. Call allowed      |
//! | marker for change            | has_change deprecated | Call allowed                                                                       |
//! | deprecated marker for change | has_change deprecated | Call allowed                                                                       |
//! | replaying, no marker         | has_change deprecated | Call allowed                                                                       |

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
        machines::{WFMachinesError, HAS_CHANGE_MARKER_NAME},
        protos::{
            coresdk::{
                common::decode_change_marker_details,
                workflow_activation::{wf_activation_job, ResolveHasChange, WfActivationJob},
                workflow_commands::StartTimer,
            },
            temporal::api::command::v1::{
                command::Attributes, RecordMarkerCommandAttributes, StartTimerCommandAttributes,
            },
            temporal::api::enums::v1::{CommandType, EventType},
            temporal::api::history::v1::{history_event, TimerFiredEventAttributes},
        },
        prototype_rust_sdk::{WfContext, WorkflowFunction},
        test_help::TestHistoryBuilder,
        tracing_init,
        workflow::managed_wf::ManagedWFFunc,
    };
    use rstest::rstest;
    use std::time::Duration;

    const MY_CHANGE_ID: &str = "test_change_name";
    #[derive(Eq, PartialEq, Copy, Clone)]
    enum MarkerType {
        Deprecated,
        NotDeprecated,
        NoMarker,
    }

    /// EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
    /// EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    /// EVENT_TYPE_WORKFLOW_TASK_STARTED
    /// EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    /// EVENT_TYPE_MARKER_RECORDED (depending on marker_type)
    /// EVENT_TYPE_TIMER_STARTED
    /// EVENT_TYPE_TIMER_FIRED
    /// EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    /// EVENT_TYPE_WORKFLOW_TASK_STARTED
    /// EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    /// EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    fn change_marker_single_timer(marker_type: MarkerType, timer_id: &str) -> TestHistoryBuilder {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        match marker_type {
            MarkerType::Deprecated => t.add_has_change_marker(MY_CHANGE_ID, true),
            MarkerType::NotDeprecated => t.add_has_change_marker(MY_CHANGE_ID, false),
            MarkerType::NoMarker => {}
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
        t
    }

    async fn v1(ctx: &mut WfContext) {
        ctx.timer(StartTimer {
            timer_id: "no_change".to_string(),
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
        })
        .await;
    }

    async fn v2(ctx: &mut WfContext) -> bool {
        if ctx.has_version(MY_CHANGE_ID, false) {
            ctx.timer(StartTimer {
                timer_id: "had_change".to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(1).into()),
            })
            .await;
            true
        } else {
            ctx.timer(StartTimer {
                timer_id: "no_change".to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(1).into()),
            })
            .await;
            false
        }
    }

    async fn v3(ctx: &mut WfContext) {
        ctx.has_version(MY_CHANGE_ID, true);
        ctx.timer(StartTimer {
            timer_id: "had_change".to_string(),
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
        })
        .await;
    }

    async fn v4(ctx: &mut WfContext) {
        ctx.timer(StartTimer {
            timer_id: "had_change".to_string(),
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
        })
        .await;
    }

    fn change_setup(
        replaying: bool,
        marker_type: MarkerType,
        workflow_version: usize,
    ) -> ManagedWFFunc {
        let wfn = WorkflowFunction::new(move |mut ctx: WfContext| async move {
            match workflow_version {
                1 => {
                    v1(&mut ctx).await;
                }
                2 => {
                    v2(&mut ctx).await;
                }
                3 => {
                    v3(&mut ctx).await;
                }
                4 => {
                    v4(&mut ctx).await;
                }
                _ => panic!("Invalid workflow version for test setup"),
            }
            Ok(().into())
        });

        let t = match marker_type {
            MarkerType::NotDeprecated => change_marker_single_timer(marker_type, "had_change"),
            MarkerType::Deprecated => {
                let timer_id = if workflow_version == 1 {
                    "no_change"
                } else {
                    "had_change"
                };
                change_marker_single_timer(marker_type, timer_id)
            }
            MarkerType::NoMarker => {
                let timer_id = if workflow_version <= 2 {
                    "no_change"
                } else {
                    "had_change"
                };
                change_marker_single_timer(marker_type, timer_id)
            }
        };
        let histinfo = if replaying {
            t.get_full_history_info()
        } else {
            t.get_history_info(1)
        };
        ManagedWFFunc::new_from_update(histinfo.unwrap().into(), wfn, vec![])
    }

    #[rstest]
    #[case::v1_breaks_on_normal_marker(false, MarkerType::NotDeprecated, 1)]
    #[case::v1_accepts_dep_marker(false, MarkerType::Deprecated, 1)]
    #[case::v1_replay_breaks_on_normal_marker(true, MarkerType::NotDeprecated, 1)]
    #[case::v1_replay_accepts_dep_marker(true, MarkerType::Deprecated, 1)]
    #[case::v4_breaks_on_normal_marker(false, MarkerType::NotDeprecated, 4)]
    #[case::v4_accepts_dep_marker(false, MarkerType::Deprecated, 4)]
    #[case::v4_replay_breaks_on_normal_marker(true, MarkerType::NotDeprecated, 4)]
    #[case::v4_replay_accepts_dep_marker(true, MarkerType::Deprecated, 4)]
    #[tokio::test]
    async fn v1_and_v4_changes(
        #[case] replaying: bool,
        #[case] marker_type: MarkerType,
        #[case] wf_version: usize,
    ) {
        let mut wfm = change_setup(replaying, marker_type, wf_version);
        // Start workflow activation
        wfm.get_next_activation().await.unwrap();
        // Just starts timer
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);

        let act = if !replaying {
            // Feed more history
            let expected_timer_id = if wf_version == 1 {
                "no_change"
            } else {
                "had_change"
            };
            wfm.new_history(
                change_marker_single_timer(marker_type, expected_timer_id)
                    .get_full_history_info()
                    .unwrap()
                    .into(),
            )
            .await
        } else {
            wfm.get_next_activation().await
        };

        if marker_type == MarkerType::Deprecated {
            let act = act.unwrap();
            // Timer is fired
            assert_matches!(
                act.jobs.as_slice(),
                [WfActivationJob {
                    variant: Some(wf_activation_job::Variant::FireTimer(_))
                }]
            );
        } else {
            // should explode b/c non-dep marker is present
            assert_matches!(act.unwrap_err(), WFMachinesError::Nondeterminism(_));
        }

        wfm.shutdown().await.unwrap();
    }

    #[rstest]
    #[case::v2_no_marker_old_path(false, MarkerType::NoMarker, 2)]
    #[case::v2_marker_new_path(false, MarkerType::NotDeprecated, 2)]
    #[case::v2_dep_marker_new_path(false, MarkerType::Deprecated, 2)]
    #[case::v2_replay_no_marker_old_path(true, MarkerType::NoMarker, 2)]
    #[case::v2_replay_marker_new_path(true, MarkerType::NotDeprecated, 2)]
    #[case::v2_replay_dep_marker_new_path(true, MarkerType::Deprecated, 2)]
    #[case::v3_no_marker_old_path(false, MarkerType::NoMarker, 3)]
    #[case::v3_marker_new_path(false, MarkerType::NotDeprecated, 3)]
    #[case::v3_dep_marker_new_path(false, MarkerType::Deprecated, 3)]
    #[case::v3_replay_no_marker_old_path(true, MarkerType::NoMarker, 3)]
    #[case::v3_replay_marker_new_path(true, MarkerType::NotDeprecated, 3)]
    #[case::v3_replay_dep_marker_new_path(true, MarkerType::Deprecated, 3)]
    #[tokio::test]
    async fn v2_and_v3_changes(
        #[case] replaying: bool,
        #[case] marker_type: MarkerType,
        #[case] wf_version: usize,
    ) {
        tracing_init();
        let mut wfm = change_setup(replaying, marker_type, wf_version);
        let act = wfm.get_next_activation().await.unwrap();
        // replaying cases should immediately get a resolve change activation when marker is present
        if replaying && marker_type != MarkerType::NoMarker {
            assert_matches!(
                &act.jobs[1],
                 WfActivationJob {
                    variant: Some(wf_activation_job::Variant::ResolveHasChange(
                        ResolveHasChange {
                            change_id,
                        }
                    ))
                } => change_id == MY_CHANGE_ID
            );
        } else {
            assert_eq!(act.jobs.len(), 1);
        }
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 2);
        let dep_flag_expected = if wf_version == 2 { false } else { true };
        assert_matches!(
            commands[0].attributes.as_ref().unwrap(),
            Attributes::RecordMarkerCommandAttributes(
                RecordMarkerCommandAttributes { marker_name, details,.. })

            if marker_name == HAS_CHANGE_MARKER_NAME
              && decode_change_marker_details(details).unwrap().1 == dep_flag_expected
        );
        // The only time the "old" timer should fire is in v2, replaying, without a marker.
        let expected_timer_id =
            if replaying && marker_type == MarkerType::NoMarker && wf_version == 2 {
                "no_change"
            } else {
                "had_change"
            };
        assert_matches!(
            commands[1].attributes.as_ref().unwrap(),
            Attributes::StartTimerCommandAttributes(StartTimerCommandAttributes { timer_id, .. })
            if timer_id == expected_timer_id
        );

        let act = if !replaying {
            // Feed more history. Since we are *not* replaying, we *always* "have" the change
            // and the history should have the has-change timer. v3 of course always has the change
            // regardless.
            wfm.new_history(
                change_marker_single_timer(marker_type, "had_change")
                    .get_full_history_info()
                    .unwrap()
                    .into(),
            )
            .await
        } else {
            wfm.get_next_activation().await
        };

        let act = act.unwrap();
        // Timer is fired
        assert_matches!(
            act.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(_))
            }]
        );

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
