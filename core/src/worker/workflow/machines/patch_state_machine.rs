//! The patch machine can be difficult to follow. Refer to below table for behavior. The
//! deprecated calls simply say "allowed" because if they returned a value, it's always true. Old
//! code cannot exist in workflows which use the deprecated call.
//!
//! | History Has                  | Workflow Has    | Outcome                                                                            |
//! |------------------------------|-----------------|------------------------------------------------------------------------------------|
//! | not replaying                | no patched      | Nothing interesting. Versioning not involved.                                      |
//! | marker for change            | no patched      | No matching command / workflow does not support this version                       |
//! | deprecated marker for change | no patched      | Marker ignored, workflow continues as if it didn't exist                           |
//! | replaying, no marker         | no patched      | Nothing interesting. Versioning not involved.                                      |
//! | not replaying                | patched         | Marker command sent to server and recorded. Call returns true                      |
//! | marker for change            | patched         | Call returns true upon replay                                                      |
//! | deprecated marker for change | patched         | Call returns true upon replay                                                      |
//! | replaying, no marker         | patched         | Call returns false upon replay                                                     |
//! | not replaying                | deprecate_patch | Marker command sent to server and recorded with deprecated flag. Call allowed      |
//! | marker for change            | deprecate_patch | Call allowed                                                                       |
//! | deprecated marker for change | deprecate_patch | Call allowed                                                                       |
//! | replaying, no marker         | deprecate_patch | Call allowed                                                                       |

use super::{
    workflow_machines::MachineResponse, Cancellable, EventInfo, NewMachineWithCommand,
    OnEventWrapper, WFMachinesAdapter, WFMachinesError,
};
use crate::protosext::HistoryEventExt;
use rustfsm::{fsm, TransitionResult};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    constants::PATCH_MARKER_NAME,
    coresdk::common::build_has_change_marker_details,
    temporal::api::{
        command::v1::{Command, RecordMarkerCommandAttributes},
        enums::v1::CommandType,
        history::v1::HistoryEvent,
    },
};

fsm! {
    pub(super) name PatchMachine;
    command PatchCommand;
    error WFMachinesError;
    shared_state SharedState;

    // Machine is created in either executing or replaying, and then immediately scheduled and
    // transitions to the command created state (creating the command in the process)
    Executing --(Schedule, on_schedule) --> MarkerCommandCreated;
    Replaying --(Schedule, on_schedule) --> MarkerCommandCreatedReplaying;

    // Pretty much nothing happens here - once we issue the command it is the responsibility of
    // machinery above us to notify lang SDK about the change. This is in order to allow the
    // change call to be sync and not have to wait for the command to resolve.
    MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> Notified;
    MarkerCommandCreatedReplaying --(CommandRecordMarker) --> Notified;

    // Once we've played back the marker recorded event, all we need to do is double-check that
    // it matched what we expected
    Notified --(MarkerRecorded(String), shared on_marker_recorded) --> MarkerCommandRecorded;
}

#[derive(Clone)]
pub(super) struct SharedState {
    patch_id: String,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum PatchCommand {}

/// Patch machines are created when the user invokes `has_change` (or whatever it may be named
/// in that lang).
///
/// `patch_id`: identifier of a particular change. All calls to get_version that share a change id
/// are guaranteed to return the same value.
/// `replaying_when_invoked`: If the workflow is replaying when this invocation occurs, this needs
/// to be set to true.
pub(super) fn has_change(
    patch_id: String,
    replaying_when_invoked: bool,
    deprecated: bool,
) -> NewMachineWithCommand {
    let (machine, command) =
        PatchMachine::new_scheduled(SharedState { patch_id }, replaying_when_invoked, deprecated);
    NewMachineWithCommand {
        command,
        machine: machine.into(),
    }
}

impl PatchMachine {
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
                    marker_name: PATCH_MARKER_NAME.to_string(),
                    details: build_has_change_marker_details(&state.patch_id, deprecated),
                    header: None,
                    failure: None,
                }
                .into(),
            ),
        };
        let mut machine = Self {
            state: initial_state,
            shared_state: state,
        };
        OnEventWrapper::on_event_mut(&mut machine, PatchMachineEvents::Schedule)
            .expect("Patch machine scheduling doesn't fail");

        (machine, cmd)
    }
}

#[derive(Default, Clone)]
pub(super) struct Executing {}

impl Executing {
    pub(super) fn on_schedule(self) -> PatchMachineTransition<MarkerCommandCreated> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandCreated {}

impl MarkerCommandCreated {
    pub(super) fn on_command_record_marker(self) -> PatchMachineTransition<Notified> {
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
    pub(super) fn on_schedule(self) -> PatchMachineTransition<MarkerCommandCreatedReplaying> {
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
    ) -> PatchMachineTransition<MarkerCommandRecorded> {
        if id != dat.patch_id {
            return TransitionResult::Err(WFMachinesError::Nondeterminism(format!(
                "Change id {} does not match expected id {}",
                id, dat.patch_id
            )));
        }
        TransitionResult::default()
    }
}

impl WFMachinesAdapter for PatchMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        panic!("Patch machine does not produce commands")
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        event.get_patch_marker_details().is_some()
    }
}

impl Cancellable for PatchMachine {}

impl TryFrom<CommandType> for PatchMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::RecordMarker => Self::CommandRecordMarker,
            _ => return Err(()),
        })
    }
}

impl TryFrom<HistoryEvent> for PatchMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        match e.get_patch_marker_details() {
            Some((id, _)) => Ok(Self::MarkerRecorded(id)),
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
        replay::TestHistoryBuilder,
        worker::workflow::{machines::WFMachinesError, ManagedWFFunc},
    };
    use rstest::rstest;
    use std::time::Duration;
    use temporal_sdk::{ActivityOptions, WfContext, WorkflowFunction};
    use temporal_sdk_core_protos::{
        constants::PATCH_MARKER_NAME,
        coresdk::{
            common::decode_change_marker_details,
            workflow_activation::{workflow_activation_job, NotifyHasPatch, WorkflowActivationJob},
        },
        temporal::api::{
            command::v1::{
                command::Attributes, RecordMarkerCommandAttributes,
                ScheduleActivityTaskCommandAttributes,
            },
            common::v1::ActivityType,
            enums::v1::{CommandType, EventType},
            history::v1::{
                history_event, ActivityTaskCompletedEventAttributes,
                ActivityTaskScheduledEventAttributes, ActivityTaskStartedEventAttributes,
                TimerFiredEventAttributes,
            },
        },
    };

    const MY_PATCH_ID: &str = "test_patch_id";
    #[derive(Eq, PartialEq, Copy, Clone)]
    enum MarkerType {
        Deprecated,
        NotDeprecated,
        NoMarker,
    }

    const ONE_SECOND: Duration = Duration::from_secs(1);

    /// EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
    /// EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    /// EVENT_TYPE_WORKFLOW_TASK_STARTED
    /// EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    /// EVENT_TYPE_MARKER_RECORDED (depending on marker_type)
    /// EVENT_TYPE_ACTIVITY_TASK_SCHEDULED
    /// EVENT_TYPE_ACTIVITY_TASK_STARTED
    /// EVENT_TYPE_ACTIVITY_TASK_COMPLETED
    /// EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
    /// EVENT_TYPE_WORKFLOW_TASK_STARTED
    /// EVENT_TYPE_WORKFLOW_TASK_COMPLETED
    /// EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    fn patch_marker_single_activity(marker_type: MarkerType) -> TestHistoryBuilder {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        match marker_type {
            MarkerType::Deprecated => t.add_has_change_marker(MY_PATCH_ID, true),
            MarkerType::NotDeprecated => t.add_has_change_marker(MY_PATCH_ID, false),
            MarkerType::NoMarker => {}
        };

        let scheduled_event_id = t.add_get_event_id(
            EventType::ActivityTaskScheduled,
            Some(
                history_event::Attributes::ActivityTaskScheduledEventAttributes(
                    ActivityTaskScheduledEventAttributes {
                        activity_id: "0".to_string(),
                        activity_type: Some(ActivityType {
                            name: "".to_string(),
                        }),
                        ..Default::default()
                    },
                ),
            ),
        );
        let started_event_id = t.add_get_event_id(
            EventType::ActivityTaskStarted,
            Some(
                history_event::Attributes::ActivityTaskStartedEventAttributes(
                    ActivityTaskStartedEventAttributes {
                        scheduled_event_id,
                        ..Default::default()
                    },
                ),
            ),
        );
        t.add(
            EventType::ActivityTaskCompleted,
            history_event::Attributes::ActivityTaskCompletedEventAttributes(
                ActivityTaskCompletedEventAttributes {
                    scheduled_event_id,
                    started_event_id,
                    ..Default::default()
                },
            ),
        );
        t.add_full_wf_task();
        t.add_workflow_execution_completed();
        t
    }

    async fn v1(ctx: &mut WfContext) {
        ctx.activity(ActivityOptions {
            activity_id: Some("no_change".to_owned()),
            ..Default::default()
        })
        .await;
    }

    async fn v2(ctx: &mut WfContext) -> bool {
        if ctx.patched(MY_PATCH_ID) {
            ctx.activity(ActivityOptions {
                activity_id: Some("had_change".to_owned()),
                ..Default::default()
            })
            .await;
            true
        } else {
            ctx.activity(ActivityOptions {
                activity_id: Some("no_change".to_owned()),
                ..Default::default()
            })
            .await;
            false
        }
    }

    async fn v3(ctx: &mut WfContext) {
        ctx.deprecate_patch(MY_PATCH_ID);
        ctx.activity(ActivityOptions {
            activity_id: Some("had_change".to_owned()),
            ..Default::default()
        })
        .await;
    }

    async fn v4(ctx: &mut WfContext) {
        ctx.activity(ActivityOptions {
            activity_id: Some("had_change".to_owned()),
            ..Default::default()
        })
        .await;
    }

    fn patch_setup(
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

        let t = patch_marker_single_activity(marker_type);
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
        let mut wfm = patch_setup(replaying, marker_type, wf_version);
        // Start workflow activation
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::ScheduleActivityTask as i32
        );
        let act = if replaying {
            wfm.get_next_activation().await
        } else {
            // Feed more history
            wfm.new_history(
                patch_marker_single_activity(marker_type)
                    .get_full_history_info()
                    .unwrap()
                    .into(),
            )
            .await
        };

        if marker_type == MarkerType::Deprecated {
            let act = act.unwrap();
            // Activity is resolved
            assert_matches!(
                act.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(_))
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
        let mut wfm = patch_setup(replaying, marker_type, wf_version);
        let act = wfm.get_next_activation().await.unwrap();
        // replaying cases should immediately get a resolve change activation when marker is present
        if replaying && marker_type != MarkerType::NoMarker {
            assert_matches!(
                &act.jobs[1],
                 WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::NotifyHasPatch(
                        NotifyHasPatch {
                            patch_id,
                        }
                    ))
                } => patch_id == MY_PATCH_ID
            );
        } else {
            assert_eq!(act.jobs.len(), 1);
        }
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 2);
        let dep_flag_expected = wf_version != 2;
        assert_matches!(
            commands[0].attributes.as_ref().unwrap(),
            Attributes::RecordMarkerCommandAttributes(
                RecordMarkerCommandAttributes { marker_name, details,.. })

            if marker_name == PATCH_MARKER_NAME
              && decode_change_marker_details(details).unwrap().1 == dep_flag_expected
        );
        // The only time the "old" timer should fire is in v2, replaying, without a marker.
        let expected_activity_id =
            if replaying && marker_type == MarkerType::NoMarker && wf_version == 2 {
                "no_change"
            } else {
                "had_change"
            };
        assert_matches!(
            commands[1].attributes.as_ref().unwrap(),
            Attributes::ScheduleActivityTaskCommandAttributes(
                ScheduleActivityTaskCommandAttributes { activity_id, .. }
            )
            if activity_id == expected_activity_id
        );

        let act = if replaying {
            wfm.get_next_activation().await
        } else {
            // Feed more history. Since we are *not* replaying, we *always* "have" the change
            // and the history should have the has-change timer. v3 of course always has the change
            // regardless.
            wfm.new_history(
                patch_marker_single_activity(marker_type)
                    .get_full_history_info()
                    .unwrap()
                    .into(),
            )
            .await
        };

        let act = act.unwrap();
        // Activity is resolved
        assert_matches!(
            act.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_))
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
        let wfn = WorkflowFunction::new(move |ctx: WfContext| async move {
            if ctx.patched(MY_PATCH_ID) {
                ctx.activity(ActivityOptions::default()).await;
            } else {
                ctx.timer(ONE_SECOND).await;
            }
            ctx.timer(ONE_SECOND).await;
            if ctx.patched(MY_PATCH_ID) {
                ctx.activity(ActivityOptions::default()).await;
            } else {
                ctx.timer(ONE_SECOND).await;
            }
            Ok(().into())
        });

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        if have_marker_in_hist {
            t.add_has_change_marker(MY_PATCH_ID, false);
            let scheduled_event_id = t.add_get_event_id(
                EventType::ActivityTaskScheduled,
                Some(
                    history_event::Attributes::ActivityTaskScheduledEventAttributes(
                        ActivityTaskScheduledEventAttributes {
                            activity_id: "1".to_owned(),
                            activity_type: Some(ActivityType {
                                name: "".to_string(),
                            }),
                            ..Default::default()
                        },
                    ),
                ),
            );
            let started_event_id = t.add_get_event_id(
                EventType::ActivityTaskStarted,
                Some(
                    history_event::Attributes::ActivityTaskStartedEventAttributes(
                        ActivityTaskStartedEventAttributes {
                            scheduled_event_id,
                            ..Default::default()
                        },
                    ),
                ),
            );
            t.add(
                EventType::ActivityTaskCompleted,
                history_event::Attributes::ActivityTaskCompletedEventAttributes(
                    ActivityTaskCompletedEventAttributes {
                        scheduled_event_id,
                        started_event_id,
                        // TODO result: Some(Payloads { payloads: vec![Payload{ metadata: Default::default(), data: vec![] }] }),
                        ..Default::default()
                    },
                ),
            );
            t.add_full_wf_task();
            let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
            t.add(
                EventType::TimerFired,
                history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
                    started_event_id: timer_started_event_id,
                    timer_id: "1".to_owned(),
                }),
            );
        } else {
            let started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
            t.add(
                EventType::TimerFired,
                history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
                    started_event_id,
                    timer_id: "1".to_owned(),
                }),
            );
            t.add_full_wf_task();
            let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
            t.add(
                EventType::TimerFired,
                history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
                    started_event_id: timer_started_event_id,
                    timer_id: "2".to_owned(),
                }),
            );
        }
        t.add_full_wf_task();

        if have_marker_in_hist {
            let scheduled_event_id = t.add_get_event_id(
                EventType::ActivityTaskScheduled,
                Some(
                    history_event::Attributes::ActivityTaskScheduledEventAttributes(
                        ActivityTaskScheduledEventAttributes {
                            activity_id: "2".to_string(),
                            activity_type: Some(ActivityType {
                                name: "".to_string(),
                            }),
                            ..Default::default()
                        },
                    ),
                ),
            );
            let started_event_id = t.add_get_event_id(
                EventType::ActivityTaskStarted,
                Some(
                    history_event::Attributes::ActivityTaskStartedEventAttributes(
                        ActivityTaskStartedEventAttributes {
                            scheduled_event_id,
                            ..Default::default()
                        },
                    ),
                ),
            );
            t.add(
                EventType::ActivityTaskCompleted,
                history_event::Attributes::ActivityTaskCompletedEventAttributes(
                    ActivityTaskCompletedEventAttributes {
                        scheduled_event_id,
                        started_event_id,
                        // TODO result: Some(Payloads { payloads: vec![Payload{ metadata: Default::default(), data: vec![] }] }),
                        ..Default::default()
                    },
                ),
            );
        } else {
            let started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
            t.add(
                EventType::TimerFired,
                history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
                    started_event_id,
                    timer_id: "3".to_owned(),
                }),
            );
        }
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
