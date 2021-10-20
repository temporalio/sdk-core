use crate::{
    machines::{
        workflow_machines::MachineResponse, Cancellable, EventInfo, MachineKind, OnEventWrapper,
        WFMachinesAdapter, WFMachinesError,
    },
    protosext::HistoryEventExt,
};
use rustfsm::{fsm, MachineError, StateMachine, TransitionResult};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{activity_result, ActivityResult},
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

    RequestSent --(NonReplayWorkflowTaskStarted) --> RequestSent;
    RequestSent --(HandleResult(ActivityResult), on_handle_result) --> MarkerCommandCreated;

    MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> ResultNotified;

    ResultNotified --(MarkerRecorded(LocalActivityMarkerData), on_marker_recorded) --> MarkerCommandRecorded;

    // Replay path ================================================================================
    WaitingMarkerEvent --(MarkerRecorded(LocalActivityMarkerData), on_marker_recorded)
      --> MarkerCommandRecorded;
    WaitingMarkerEvent --(NonReplayWorkflowTaskStarted, on_non_replay_workflow_task_started)
      --> RequestPrepared;
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
        shared_state: SharedState { attrs },
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
        res: ActivityResult,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        let mut res =
            OnEventWrapper::on_event_mut(self, LocalActivityMachineEvents::HandleResult(res))
                .map_err(|e| match e {
                    MachineError::InvalidTransition => WFMachinesError::Fatal(format!(
                        "Invalid transition while attempting to resolve local activity in {}",
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
}

#[derive(Debug, derive_more::Display)]
pub(super) enum LocalActivityCommand {
    RequestActivityExecution(ScheduleActivity),
    #[display(fmt = "Resolved")]
    Resolved(ActivityResult),
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
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct RequestPrepared {}

#[derive(Default, Clone)]
pub(super) struct RequestSent {}

impl RequestSent {
    pub(super) fn on_handle_result(
        self,
        res: ActivityResult,
    ) -> LocalActivityMachineTransition<MarkerCommandCreated> {
        TransitionResult::commands([LocalActivityCommand::Resolved(res)])
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
        lamd: LocalActivityMarkerData,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        // TODO: Verify same marker
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct WaitingMarkerEvent {}

impl WaitingMarkerEvent {
    pub(super) fn on_marker_recorded(
        self,
        lamd: LocalActivityMarkerData,
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        unimplemented!()
    }
    pub(super) fn on_non_replay_workflow_task_started(
        self,
    ) -> LocalActivityMachineTransition<RequestPrepared> {
        unimplemented!()
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
        // TODO: Technically could return true if I impl cancels, but, will it matter to caller?
        //   semantics are different from other commands that do this. Double check
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
            LocalActivityCommand::Resolved(res) => {
                let result = res.status.clone().and_then(|s| {
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
                            activity_id: self.shared_state.attrs.activity_id.clone(),
                            activity_type: self.shared_state.attrs.activity_type.clone(),
                            // TODO: populate
                            time: None,
                        },
                        result,
                    ),
                    header: None,
                    // TODO: Fill in
                    failure: None,
                };
                Ok(vec![
                    MachineResponse::IssueNewCommand(Command {
                        command_type: CommandType::RecordMarker as i32,
                        attributes: Some(marker_data.into()),
                    }),
                    MachineResponse::PushWFJob(
                        ResolveActivity {
                            seq: self.shared_state.attrs.seq,
                            result: Some(res),
                        }
                        .into(),
                    ),
                ])
            }
        }
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        event.get_local_activity_marker_details().is_some()
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
        match e.get_local_activity_marker_details() {
            Some((marker_dat, _)) => Ok(LocalActivityMachineEvents::MarkerRecorded(marker_dat)),
            _ => Err(WFMachinesError::Nondeterminism(format!(
                "Local activity machine cannot handle this event: {}",
                e
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        prototype_rust_sdk::{LocalActivityOptions, WfContext, WorkflowFunction, WorkflowResult},
        telemetry::test_telem_console,
        test_help::canned_histories,
        workflow::managed_wf::ManagedWFFunc,
    };
    use rstest::rstest;

    async fn la_wf(mut command_sink: WfContext) -> WorkflowResult<()> {
        command_sink
            .local_activity(LocalActivityOptions::default())
            .await;
        Ok(().into())
    }

    #[rstest]
    #[case::incremental(false)]
    #[case::replay(true)]
    #[tokio::test]
    async fn one_la_success(#[case] replay: bool) {
        test_telem_console();
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

        // Complete the la itself TODO: Is it worth doing better than magic seq number for these?
        if !replay {
            wfm.complete_local_activity(1, ActivityResult::ok(b"Resolved".into()))
                .unwrap();
        }

        // Now the next activation will unblock the local activity and produce a record marker
        // command as well as complete the workflow
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 2);
        assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
        assert_eq!(
            commands[1].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );

        if !replay {
            wfm.new_history(t.get_full_history_info().unwrap().into())
                .await
                .unwrap();
            assert_eq!(wfm.get_next_activation().await.unwrap().jobs.len(), 0);
            let commands = wfm.get_server_commands().await.commands;
            assert_eq!(commands.len(), 0);
        }

        wfm.shutdown().await.unwrap();
    }
}
