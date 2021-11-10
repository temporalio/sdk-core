use crate::{
    machines::{
        workflow_machines::MachineResponse, Cancellable, EventInfo, MachineKind, OnEventWrapper,
        WFMachinesAdapter, WFMachinesError,
    },
    protosext::{CompleteLocalActivityData, HistoryEventExt},
};
use rustfsm::{fsm, MachineError, StateMachine, TransitionResult};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{activity_result, ActivityResult, Failure as ActFail, Success},
        common::build_local_activity_marker_details,
        external_data::LocalActivityMarkerData,
        workflow_activation::ResolveActivity,
        workflow_commands::ScheduleActivity,
    },
    temporal::api::{
        command::v1::{Command, RecordMarkerCommandAttributes},
        enums::v1::CommandType,
        history::v1::HistoryEvent,
    },
};

pub const LOCAL_ACTIVITY_MARKER_NAME: &str = "core_local_activity";

fsm! {
    pub(super) name LocalActivityMachine;
    command LocalActivityCommand;
    error WFMachinesError;
    shared_state SharedState;

    // Machine is created in either executing or replaying, and then immediately scheduled and
    // transitions to the command created state (creating the command in the process)
    Executing --(Schedule, shared on_schedule) --> RequestPrepared;
    Replaying --(Schedule, on_schedule) --> WaitingMarkerEvent;

    // Execution path =============================================================================
    // TODO: I don't think we really need this. Timeouts etc should be handled up higher. Verify
    //   and remove if so
    RequestPrepared --(MarkAsSent) --> RequestSent;

    // TODO: Unclear if this is needed for core. Address while implementing WFT heartbeats
    // RequestSent --(NonReplayWorkflowTaskStarted) --> RequestSent;
    RequestSent --(HandleResult(ResolveDat), on_handle_result) --> MarkerCommandCreated;

    MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> ResultNotified;

    ResultNotified --(MarkerRecorded(CompleteLocalActivityData), on_marker_recorded) --> MarkerCommandRecorded;

    // Replay path ================================================================================
    // WaitingMarkerEvent --(NonReplayWorkflowTaskStarted, on_non_replay_workflow_task_started)
    //   --> RequestPrepared;

    WaitingMarkerEvent --(HandleResult(ResolveDat), on_handle_result)
      --> WaitingMarkerEvent;
    WaitingMarkerEvent --(MarkerRecorded(CompleteLocalActivityData), on_marker_recorded)
      --> MarkerCommandRecorded;
}

#[derive(Debug)]
pub(super) struct ResolveDat {
    pub(super) seq: u32,
    pub(super) result: ActivityResult,
}

impl From<CompleteLocalActivityData> for ResolveDat {
    fn from(d: CompleteLocalActivityData) -> Self {
        let status = match d.result {
            Ok(res) => activity_result::Status::Completed(Success {
                result: Some(res.into()),
            }),
            Err(fail) => activity_result::Status::Failed(ActFail {
                failure: Some(fail),
            }),
        };
        ResolveDat {
            seq: d.marker_dat.seq,
            result: ActivityResult {
                status: Some(status),
            },
        }
    }
}

/// Creates a new local activity state machine & immediately schedules the local activity for
/// execution. No command is produced immediately to be sent to the server, as the local activity
/// must resolve before we send a record marker command. A [MachineResponse] may be produced,
/// to queue the LA for execution if it needs to be.
pub(super) fn new_local_activity(
    attrs: ScheduleActivity,
    replaying_when_invoked: bool,
) -> (LocalActivityMachine, Vec<MachineResponse>) {
    let initial_state = if replaying_when_invoked {
        Replaying {}.into()
    } else {
        Executing {}.into()
    };

    let mut machine = LocalActivityMachine {
        state: initial_state,
        shared_state: SharedState {
            attrs,
            replaying_when_invoked,
        },
    };

    let mut res = OnEventWrapper::on_event_mut(&mut machine, LocalActivityMachineEvents::Schedule)
        .expect("Scheduling local activities doesn't fail");
    let mr = if let Some(res) = res.pop() {
        machine
            .adapt_response(res, None)
            .expect("Adapting LA schedule response doesn't fail")
    } else {
        vec![]
    };
    (machine, mr)
}

impl LocalActivityMachine {
    pub(super) fn try_resolve(
        &mut self,
        result: ActivityResult,
        seq: u32,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        let mut res = OnEventWrapper::on_event_mut(
            self,
            LocalActivityMachineEvents::HandleResult(ResolveDat { seq, result }),
        )
        .map_err(|e| match e {
            MachineError::InvalidTransition => WFMachinesError::Fatal(format!(
                "Invalid transition while attempting to resolve local activity (seq {}) in {}",
                self.shared_state.attrs.seq,
                self.state(),
            )),
            MachineError::Underlying(e) => e,
        })?;
        let mr = if let Some(res) = res.pop() {
            self.adapt_response(res, None)
                .expect("Adapting LA resolve response doesn't fail")
        } else {
            vec![]
        };
        Ok(mr)
    }

    pub(super) fn mark_sent(&mut self) -> Result<(), WFMachinesError> {
        OnEventWrapper::on_event_mut(self, LocalActivityMachineEvents::MarkAsSent).map_err(
            |e| match e {
                MachineError::InvalidTransition => WFMachinesError::Fatal(format!(
                    "Invalid transition while attempting to resolve local activity in {}",
                    self.state(),
                )),
                MachineError::Underlying(e) => e,
            },
        )?;
        Ok(())
    }
}

#[derive(Clone)]
pub(super) struct SharedState {
    attrs: ScheduleActivity,
    replaying_when_invoked: bool,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum LocalActivityCommand {
    RequestActivityExecution(ScheduleActivity),
    #[display(fmt = "Resolved")]
    Resolved(ResolveDat),
    #[display(fmt = "FakeMarker")]
    FakeMarker,
}

#[derive(Default, Clone)]
pub(super) struct Executing {}

impl Executing {
    pub(super) fn on_schedule(
        self,
        dat: SharedState,
    ) -> LocalActivityMachineTransition<RequestPrepared> {
        TransitionResult::commands([LocalActivityCommand::RequestActivityExecution(dat.attrs)])
    }
}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandCreated {}

impl MarkerCommandCreated {
    pub(super) fn on_command_record_marker(self) -> LocalActivityMachineTransition<ResultNotified> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandRecorded {}

#[derive(Default, Clone)]
pub(super) struct Replaying {}
impl Replaying {
    pub(super) fn on_schedule(self) -> LocalActivityMachineTransition<WaitingMarkerEvent> {
        TransitionResult::commands([LocalActivityCommand::FakeMarker])
    }
}

#[derive(Default, Clone)]
pub(super) struct RequestPrepared {}

#[derive(Default, Clone)]
pub(super) struct RequestSent {}

impl RequestSent {
    pub(super) fn on_handle_result(
        self,
        dat: ResolveDat,
    ) -> LocalActivityMachineTransition<MarkerCommandCreated> {
        TransitionResult::commands([LocalActivityCommand::Resolved(dat)])
    }
}

impl From<RequestPrepared> for RequestSent {
    fn from(_: RequestPrepared) -> Self {
        Self::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct ResultNotified {}

impl ResultNotified {
    pub(super) fn on_marker_recorded(
        self,
        _: CompleteLocalActivityData,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        // TODO: Verify same marker
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct WaitingMarkerEvent {}

impl WaitingMarkerEvent {
    pub(super) fn on_handle_result(
        self,
        dat: ResolveDat,
    ) -> LocalActivityMachineTransition<WaitingMarkerEvent> {
        TransitionResult::commands([LocalActivityCommand::Resolved(dat)])
    }

    pub(super) fn on_marker_recorded(
        self,
        _: CompleteLocalActivityData,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        TransitionResult::default()
    }
}

impl From<Replaying> for WaitingMarkerEvent {
    fn from(_: Replaying) -> Self {
        Self::default()
    }
}

impl Cancellable for LocalActivityMachine {
    fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<Self::Error>> {
        todo!()
    }

    fn was_cancelled_before_sent_to_server(&self) -> bool {
        // TODO: Technically is always true if I impl cancels (and it's cancelled), but, will it
        //  matter to caller? semantics are different from other commands that do this. Double check
        false
    }
}

impl WFMachinesAdapter for LocalActivityMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        match my_command {
            LocalActivityCommand::RequestActivityExecution(act) => {
                Ok(vec![MachineResponse::QueueLocalActivity(act)])
            }
            LocalActivityCommand::Resolved(ResolveDat { seq, result }) => {
                let ok_res = result.status.clone().and_then(|s| {
                    if let activity_result::Status::Completed(suc) = s {
                        suc.result
                    } else {
                        None
                    }
                });
                let marker_data = RecordMarkerCommandAttributes {
                    marker_name: LOCAL_ACTIVITY_MARKER_NAME.to_string(),
                    details: build_local_activity_marker_details(
                        LocalActivityMarkerData {
                            seq,
                            activity_id: self.shared_state.attrs.activity_id.clone(),
                            activity_type: self.shared_state.attrs.activity_type.clone(),
                            // TODO: populate
                            time: None,
                        },
                        ok_res,
                    ),
                    header: None,
                    // TODO: Fill in
                    failure: None,
                };
                let mut responses = vec![MachineResponse::PushWFJob(
                    ResolveActivity {
                        seq: self.shared_state.attrs.seq,
                        result: Some(result),
                    }
                    .into(),
                )];
                // Only issue record marker commands if we weren't replaying
                if !self.shared_state.replaying_when_invoked {
                    responses.push(MachineResponse::IssueNewCommand(Command {
                        command_type: CommandType::RecordMarker as i32,
                        attributes: Some(marker_data.into()),
                    }));
                }
                Ok(responses)
            }
            LocalActivityCommand::FakeMarker => {
                // The fake marker is used to avoid special casing marker recorded event handling.
                // If we didn't have the fake marker, there would be no "outgoing command" to match
                // against the event. This way there is, but the command never will be issued to
                // server because it is understood to be meaningless.
                Ok(vec![MachineResponse::IssueFakeLocalActivityMarker(
                    self.shared_state.attrs.seq,
                )])
            }
        }
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        // TODO: Avoid clone, make just is method
        event.clone().into_local_activity_marker_details().is_some()
    }

    fn kind(&self) -> MachineKind {
        MachineKind::LocalActivity
    }
}

impl TryFrom<CommandType> for LocalActivityMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::RecordMarker => Self::CommandRecordMarker,
            _ => return Err(()),
        })
    }
}

impl TryFrom<HistoryEvent> for LocalActivityMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        // TODO: Use check method to return with well formed error first
        // _ => Err(WFMachinesError::Nondeterminism(format!(
        // "Local activity machine cannot handle this event: {}",
        // e
        // ))),

        match e.into_local_activity_marker_details() {
            Some(marker_dat) => Ok(LocalActivityMachineEvents::MarkerRecorded(marker_dat)),
            _ => Err(WFMachinesError::Nondeterminism(
                "Local activity machine encountered an unparsable marker".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        prototype_rust_sdk::{LocalActivityOptions, WfContext, WorkflowFunction, WorkflowResult},
        test_help::canned_histories,
        workflow::managed_wf::ManagedWFFunc,
    };
    use rstest::rstest;
    use std::time::Duration;
    use temporal_sdk_core_protos::coresdk::workflow_activation::{
        wf_activation_job, WfActivationJob,
    };

    async fn la_wf(mut ctx: WfContext) -> WorkflowResult<()> {
        ctx.local_activity(LocalActivityOptions::default()).await;
        Ok(().into())
    }

    #[rstest]
    #[case::incremental(false)]
    #[case::replay(true)]
    #[tokio::test]
    async fn one_la_success(#[case] replay: bool) {
        let func = WorkflowFunction::new(la_wf);
        let t = canned_histories::single_local_activity("activity-id-1");
        let histinfo = if replay {
            t.get_full_history_info().unwrap().into()
        } else {
            t.get_history_info(1).unwrap().into()
        };
        let mut wfm = ManagedWFFunc::new_from_update(histinfo, func, vec![]);

        // First activation will have no server commands. Activity will be put into the activity
        // queue locally
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 0);

        let ready_to_execute_las = wfm.drain_queued_local_activities();
        if !replay {
            assert_eq!(ready_to_execute_las.len(), 1);
        } else {
            assert_eq!(ready_to_execute_las.len(), 0);
        }

        if !replay {
            wfm.complete_local_activity(1, ActivityResult::ok(b"Resolved".into()))
                .unwrap();
        }

        // Now the next activation will unblock the local activity
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().await.commands;
        if replay {
            assert_eq!(commands.len(), 1);
            assert_eq!(
                commands[0].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        } else {
            assert_eq!(commands.len(), 2);
            assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
            assert_eq!(
                commands[1].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        }

        if !replay {
            wfm.new_history(t.get_full_history_info().unwrap().into())
                .await
                .unwrap();
        }
        assert_eq!(wfm.get_next_activation().await.unwrap().jobs.len(), 0);
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 0);

        wfm.shutdown().await.unwrap();
    }

    async fn two_la_wf(mut ctx: WfContext) -> WorkflowResult<()> {
        ctx.local_activity(LocalActivityOptions::default()).await;
        ctx.local_activity(LocalActivityOptions::default()).await;
        Ok(().into())
    }

    #[rstest]
    #[case::incremental(false)]
    #[case::replay(true)]
    #[tokio::test]
    async fn two_sequential_las(#[case] replay: bool) {
        let func = WorkflowFunction::new(two_la_wf);
        let t = canned_histories::two_local_activities_one_wft();
        let histinfo = if replay {
            t.get_full_history_info().unwrap().into()
        } else {
            t.get_history_info(1).unwrap().into()
        };
        let mut wfm = ManagedWFFunc::new_from_update(histinfo, func, vec![]);

        // First activation will have no server commands. Activity will be put into the activity
        // queue locally
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 0);
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        let num_queued = if !replay { 1 } else { 0 };
        assert_eq!(ready_to_execute_las.len(), num_queued);

        if !replay {
            wfm.complete_local_activity(1, ActivityResult::ok(b"Resolved".into()))
                .unwrap();
        }

        let act = wfm.get_next_activation().await.unwrap();
        assert_matches!(
            act.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(ra))
            }] => assert_eq!(ra.seq, 1)
        );
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        if !replay {
            assert_eq!(ready_to_execute_las.len(), 1);
        } else {
            assert_eq!(ready_to_execute_las.len(), 0);
        }

        if !replay {
            wfm.complete_local_activity(2, ActivityResult::ok(b"Resolved".into()))
                .unwrap();
        }

        let act = wfm.get_next_activation().await.unwrap();
        assert_matches!(
            act.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(ra))
            }] => assert_eq!(ra.seq, 2)
        );
        let commands = wfm.get_server_commands().await.commands;
        if replay {
            assert_eq!(commands.len(), 1);
            assert_eq!(
                commands[0].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        } else {
            assert_eq!(commands.len(), 3);
            assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
            assert_eq!(commands[1].command_type, CommandType::RecordMarker as i32);
            assert_eq!(
                commands[2].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        }

        if !replay {
            wfm.new_history(t.get_full_history_info().unwrap().into())
                .await
                .unwrap();
        }
        assert_eq!(wfm.get_next_activation().await.unwrap().jobs.len(), 0);
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 0);

        wfm.shutdown().await.unwrap();
    }

    async fn two_la_wf_parallel(mut ctx: WfContext) -> WorkflowResult<()> {
        tokio::join!(
            ctx.local_activity(LocalActivityOptions::default()),
            ctx.local_activity(LocalActivityOptions::default())
        );
        Ok(().into())
    }

    #[rstest]
    #[case::incremental(false)]
    #[case::replay(true)]
    #[tokio::test]
    async fn two_parallel_las(#[case] replay: bool) {
        let func = WorkflowFunction::new(two_la_wf_parallel);
        let t = canned_histories::two_local_activities_one_wft();
        let histinfo = if replay {
            t.get_full_history_info().unwrap().into()
        } else {
            t.get_history_info(1).unwrap().into()
        };
        let mut wfm = ManagedWFFunc::new_from_update(histinfo, func, vec![]);

        // First activation will have no server commands. Activity(ies) will be put into the queue
        // for execution
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 0);
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        let num_queued = if !replay { 2 } else { 0 };
        assert_eq!(ready_to_execute_las.len(), num_queued);

        if !replay {
            wfm.complete_local_activity(1, ActivityResult::ok(b"Resolved".into()))
                .unwrap();
            wfm.complete_local_activity(2, ActivityResult::ok(b"Resolved".into()))
                .unwrap();
        }

        let act = wfm.get_next_activation().await.unwrap();
        assert_matches!(
            act.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(ra))
            },
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(ra2))
            }] => {assert_eq!(ra.seq, 1); assert_eq!(ra2.seq, 2)}
        );
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        assert_eq!(ready_to_execute_las.len(), 0);

        let commands = wfm.get_server_commands().await.commands;
        if replay {
            assert_eq!(commands.len(), 1);
            assert_eq!(
                commands[0].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        } else {
            assert_eq!(commands.len(), 3);
            assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
            assert_eq!(commands[1].command_type, CommandType::RecordMarker as i32);
            assert_eq!(
                commands[2].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        }

        if !replay {
            wfm.new_history(t.get_full_history_info().unwrap().into())
                .await
                .unwrap();
        }
        assert_eq!(wfm.get_next_activation().await.unwrap().jobs.len(), 0);
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 0);

        wfm.shutdown().await.unwrap();
    }

    async fn la_timer_la(mut ctx: WfContext) -> WorkflowResult<()> {
        ctx.local_activity(LocalActivityOptions::default()).await;
        ctx.timer(Duration::from_secs(5)).await;
        ctx.local_activity(LocalActivityOptions::default()).await;
        Ok(().into())
    }

    #[rstest]
    #[case::incremental(false)]
    #[case::replay(true)]
    #[tokio::test]
    async fn las_separated_by_timer(#[case] replay: bool) {
        let func = WorkflowFunction::new(la_timer_la);
        let t = canned_histories::two_local_activities_separated_by_timer();
        let histinfo = if replay {
            t.get_full_history_info().unwrap().into()
        } else {
            t.get_history_info(1).unwrap().into()
        };
        let mut wfm = ManagedWFFunc::new_from_update(histinfo, func, vec![]);

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 0);
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        let num_queued = if !replay { 1 } else { 0 };
        assert_eq!(ready_to_execute_las.len(), num_queued);

        if !replay {
            wfm.complete_local_activity(1, ActivityResult::ok(b"Resolved".into()))
                .unwrap();
        }

        let act = wfm.get_next_activation().await.unwrap();
        assert_matches!(
            act.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(ra))
            }] => assert_eq!(ra.seq, 1)
        );
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        assert_eq!(ready_to_execute_las.len(), 0);

        let commands = wfm.get_server_commands().await.commands;
        if replay {
            assert_eq!(commands.len(), 1);
            assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
        } else {
            assert_eq!(commands.len(), 2);
            assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
            assert_eq!(commands[1].command_type, CommandType::StartTimer as i32);
        }

        let act = if !replay {
            wfm.new_history(t.get_history_info(2).unwrap().into())
                .await
                .unwrap()
        } else {
            wfm.get_next_activation().await.unwrap()
        };
        assert_matches!(
            act.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(_))
            }]
        );
        let ready_to_execute_las = wfm.drain_queued_local_activities();
        let num_queued = if !replay { 1 } else { 0 };
        assert_eq!(ready_to_execute_las.len(), num_queued);
        if !replay {
            wfm.complete_local_activity(2, ActivityResult::ok(b"Resolved".into()))
                .unwrap();
        }

        let act = wfm.get_next_activation().await.unwrap();
        assert_matches!(
            act.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(ra))
            }] => assert_eq!(ra.seq, 2)
        );

        let commands = wfm.get_server_commands().await.commands;
        if replay {
            assert_eq!(commands.len(), 1);
            assert_eq!(
                commands[0].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        } else {
            assert_eq!(commands.len(), 2);
            assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
            assert_eq!(
                commands[1].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        }

        wfm.shutdown().await.unwrap();
    }
}
