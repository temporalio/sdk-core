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
        enums::v1::{CommandType, EventType},
        history::v1::HistoryEvent,
    },
};

pub const LOCAL_ACTIVITY_MARKER_NAME: &str = "core_local_activity";

fsm! {
    pub(super) name LocalActivityMachine;
    command LocalActivityCommand;
    error WFMachinesError;
    shared_state SharedState;

    // Machine is created in either executing or replaying (referring to whether or not the workflow
    // is replaying), and then immediately scheduled and transitions to either requesting that lang
    // execute the activity, or waiting for the marker from history.
    Executing --(Schedule, shared on_schedule) --> RequestSent;
    Replaying --(Schedule, on_schedule) --> WaitingMarkerEvent;
    ReplayingPreResolved --(Schedule, on_schedule) --> WaitingMarkerEventPreResolved;

    // Execution path =============================================================================
    RequestSent --(HandleResult(ResolveDat), on_handle_result) --> MarkerCommandCreated;

    MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> ResultNotified;

    ResultNotified --(MarkerRecorded(CompleteLocalActivityData), shared on_marker_recorded) --> MarkerCommandRecorded;

    // Replay path ================================================================================
    // LAs on the replay path should never have handle result explicitly called on them, but do need
    // to eventually see the marker
    WaitingMarkerEvent --(MarkerRecorded(CompleteLocalActivityData), shared on_marker_recorded)
      --> MarkerCommandRecorded;
    // If the activity is pre resolved we still expect to see marker recorded event at some point,
    // even though we already resolved the activity.
    WaitingMarkerEventPreResolved --(MarkerRecorded(CompleteLocalActivityData),
                                     shared on_marker_recorded) --> MarkerCommandRecorded;
}

#[derive(Debug, Clone)]
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
    maybe_pre_resolved: Option<ResolveDat>,
) -> Result<(LocalActivityMachine, Vec<MachineResponse>), WFMachinesError> {
    let initial_state = if replaying_when_invoked {
        if let Some(dat) = maybe_pre_resolved {
            ReplayingPreResolved { dat }.into()
        } else {
            Replaying {}.into()
        }
    } else {
        if maybe_pre_resolved.is_some() {
            return Err(WFMachinesError::Nondeterminism(
                "Local activity cannot be created as pre-resolved while not replaying".to_string(),
            ));
        }
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
    Ok((machine, mr))
}

impl LocalActivityMachine {
    /// Is called to check if, while handling the LA marker event, we should avoid doing normal
    /// command-event processing - instead simply applying the event to this machine and then
    /// skipping over the rest. If this machine is in the `ResultNotified` state, that means
    /// command handling should proceed as normal (ie: The command needs to be matched and removed).
    /// The other valid states to make this check in are the `WaitingMarkerEvent[PreResolved]`
    /// states, which will return true.
    ///
    /// Attempting the check in any other state likely means a bug in the SDK.
    pub(super) fn marker_should_get_special_handling(&self) -> Result<bool, WFMachinesError> {
        match &self.state {
            LocalActivityMachineState::ResultNotified(_) => Ok(false),
            LocalActivityMachineState::WaitingMarkerEvent(_) => Ok(true),
            LocalActivityMachineState::WaitingMarkerEventPreResolved(_) => Ok(true),
            _ => Err(WFMachinesError::Fatal(format!(
                "Attempted to check for LA marker handling in invalid state {}",
                self.state
            ))),
        }
    }

    /// Attempt to resolve the local activity with a result from execution (not from history)
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
    /// The fake marker is used to avoid special casing marker recorded event handling.
    /// If we didn't have the fake marker, there would be no "outgoing command" to match
    /// against the event. This way there is, but the command never will be issued to
    /// server because it is understood to be meaningless.
    #[display(fmt = "FakeMarker")]
    FakeMarker,
}

#[derive(Default, Clone)]
pub(super) struct Executing {}

impl Executing {
    pub(super) fn on_schedule(
        self,
        dat: SharedState,
    ) -> LocalActivityMachineTransition<RequestSent> {
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
        TransitionResult::ok([], WaitingMarkerEvent {})
    }
}

#[derive(Clone)]
pub(super) struct ReplayingPreResolved {
    dat: ResolveDat,
}
impl ReplayingPreResolved {
    pub(super) fn on_schedule(
        self,
    ) -> LocalActivityMachineTransition<WaitingMarkerEventPreResolved> {
        TransitionResult::ok(
            [
                LocalActivityCommand::FakeMarker,
                LocalActivityCommand::Resolved(self.dat),
            ],
            WaitingMarkerEventPreResolved {},
        )
    }
}

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

macro_rules! verify_marker_dat {
    ($shared:expr, $dat:expr, $ok_expr:expr) => {
        if let Err(err) = verify_marker_data_matches($shared, $dat) {
            TransitionResult::Err(err)
        } else {
            $ok_expr
        }
    };
}

#[derive(Default, Clone)]
pub(super) struct ResultNotified {}

impl ResultNotified {
    pub(super) fn on_marker_recorded(
        self,
        shared: SharedState,
        dat: CompleteLocalActivityData,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        verify_marker_dat!(&shared, &dat, TransitionResult::default())
    }
}

#[derive(Default, Clone)]
pub(super) struct WaitingMarkerEvent {}

impl WaitingMarkerEvent {
    pub(super) fn on_marker_recorded(
        self,
        shared: SharedState,
        dat: CompleteLocalActivityData,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        verify_marker_dat!(
            &shared,
            &dat,
            TransitionResult::commands([LocalActivityCommand::Resolved(dat.into())])
        )
    }
}

#[derive(Default, Clone)]
pub(super) struct WaitingMarkerEventPreResolved {}
impl WaitingMarkerEventPreResolved {
    pub(super) fn on_marker_recorded(
        self,
        shared: SharedState,
        dat: CompleteLocalActivityData,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        verify_marker_dat!(&shared, &dat, TransitionResult::default())
    }
}

impl Cancellable for LocalActivityMachine {
    fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<Self::Error>> {
        todo!("Cancellation of local activities not yet implemented")
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
                let mut maybe_ok_result = None;
                let mut maybe_failure = None;
                if let Some(s) = result.status.clone() {
                    match s {
                        activity_result::Status::Completed(suc) => {
                            maybe_ok_result = suc.result;
                        }
                        activity_result::Status::Failed(fail) => {
                            maybe_failure = fail.failure;
                        }
                        activity_result::Status::Cancelled(_) => {
                            todo!("Local activity cancellation not implemented yet")
                        }
                        activity_result::Status::WillCompleteAsync(_) => {
                            return Err(WFMachinesError::Fatal(
                                "Local activities cannot be completed async. \
                                 This is a lang SDK bug."
                                    .to_string(),
                            ))
                        }
                    }
                };
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
                        maybe_ok_result,
                    ),
                    header: None,
                    failure: maybe_failure,
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
                // See docs for `FakeMarker` for more
                Ok(vec![MachineResponse::IssueFakeLocalActivityMarker(
                    self.shared_state.attrs.seq,
                )])
            }
        }
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        event.is_local_activity_marker()
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
        if e.event_type() != EventType::MarkerRecorded {
            return Err(WFMachinesError::Nondeterminism(format!(
                "Local activity machine cannot handle this event: {}",
                e
            )));
        }

        match e.into_local_activity_marker_details() {
            Some(marker_dat) => Ok(LocalActivityMachineEvents::MarkerRecorded(marker_dat)),
            _ => Err(WFMachinesError::Nondeterminism(
                "Local activity machine encountered an unparsable marker".to_string(),
            )),
        }
    }
}

fn verify_marker_data_matches(
    shared: &SharedState,
    dat: &CompleteLocalActivityData,
) -> Result<(), WFMachinesError> {
    if shared.attrs.seq != dat.marker_dat.seq {
        return Err(WFMachinesError::Nondeterminism(format!(
            "Local activity marker data has sequence number {} but matched against LA \
            command with sequence number {}",
            dat.marker_dat.seq, shared.attrs.seq
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        prototype_rust_sdk::{LocalActivityOptions, WfContext, WorkflowFunction, WorkflowResult},
        test_help::{canned_histories, TestHistoryBuilder},
        workflow::managed_wf::ManagedWFFunc,
    };
    use rstest::rstest;
    use std::time::Duration;
    use temporal_sdk_core_protos::{
        coresdk::workflow_activation::{wf_activation_job, WfActivationJob},
        temporal::api::{command::v1::command, failure::v1::Failure},
    };

    async fn la_wf(mut ctx: WfContext) -> WorkflowResult<()> {
        ctx.local_activity(LocalActivityOptions::default()).await;
        Ok(().into())
    }

    #[rstest]
    #[case::incremental(false, true)]
    #[case::replay(true, true)]
    #[case::incremental_fail(false, false)]
    #[case::replay_fail(true, false)]
    #[tokio::test]
    async fn one_la_success(#[case] replay: bool, #[case] completes_ok: bool) {
        let func = WorkflowFunction::new(la_wf);
        let activity_id = "1";
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        if completes_ok {
            t.add_local_activity_result_marker(1, activity_id, b"hi".into());
        } else {
            t.add_local_activity_fail_marker(
                1,
                activity_id,
                Failure::application_failure("I failed".to_string(), false),
            );
        }
        t.add_workflow_execution_completed();

        let histinfo = if replay {
            t.get_full_history_info().unwrap().into()
        } else {
            t.get_history_info(1).unwrap().into()
        };
        let mut wfm = ManagedWFFunc::new_from_update(histinfo, func, vec![]);

        // First activation will have no server commands. Activity will be put into the activity
        // queue locally
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 0);

        let ready_to_execute_las = wfm.drain_queued_local_activities();
        if !replay {
            assert_eq!(ready_to_execute_las.len(), 1);
        } else {
            assert_eq!(ready_to_execute_las.len(), 0);
        }

        if !replay {
            if completes_ok {
                wfm.complete_local_activity(1, ActivityResult::ok(b"hi".into()))
                    .unwrap();
            } else {
                wfm.complete_local_activity(
                    1,
                    ActivityResult::fail(Failure {
                        message: "I failed".to_string(),
                        ..Default::default()
                    }),
                )
                .unwrap();
            }
        }

        // Now the next activation will unblock the local activity
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        if replay {
            assert_eq!(commands.len(), 1);
            assert_eq!(
                commands[0].command_type,
                CommandType::CompleteWorkflowExecution as i32
            );
        } else {
            assert_eq!(commands.len(), 2);
            assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
            if completes_ok {
                assert_matches!(
                    commands[0].attributes.as_ref().unwrap(),
                    command::Attributes::RecordMarkerCommandAttributes(
                        RecordMarkerCommandAttributes { failure: None, .. }
                    )
                );
            } else {
                assert_matches!(
                    commands[0].attributes.as_ref().unwrap(),
                    command::Attributes::RecordMarkerCommandAttributes(
                        RecordMarkerCommandAttributes {
                            failure: Some(_),
                            ..
                        }
                    )
                );
            }
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
        assert_eq!(wfm.drain_queued_local_activities().len(), 0);
        assert_eq!(wfm.get_next_activation().await.unwrap().jobs.len(), 0);
        let commands = wfm.get_server_commands().commands;
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
        let commands = wfm.get_server_commands().commands;
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
        let commands = wfm.get_server_commands().commands;
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
        let commands = wfm.get_server_commands().commands;
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
        let commands = wfm.get_server_commands().commands;
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

        let commands = wfm.get_server_commands().commands;
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
        let commands = wfm.get_server_commands().commands;
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
        let commands = wfm.get_server_commands().commands;
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

        let commands = wfm.get_server_commands().commands;
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

        let commands = wfm.get_server_commands().commands;
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
