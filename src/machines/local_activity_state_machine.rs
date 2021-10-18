use crate::{
    machines::{
        workflow_machines::MachineResponse, Cancellable, EventInfo, MachineKind, OnEventWrapper,
        WFMachinesAdapter, WFMachinesError,
    },
    protosext::HistoryEventExt,
};
use rustfsm::{fsm, MachineError, TransitionResult};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    coresdk::{activity_result::ActivityResult, workflow_commands::ScheduleActivity},
    temporal::api::{
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

    // Machine is created in either executing or replaying, and then immediately scheduled and
    // transitions to the command created state (creating the command in the process)
    Executing --(Schedule, on_schedule) --> RequestPrepared;
    Replaying --(Schedule, on_schedule) --> WaitingMarkerEvent;

    // Execution path =============================================================================
    RequestPrepared --(MarkAsSent) --> RequestSent;

    RequestSent --(NonReplayWorkflowTaskStarted) --> RequestSent;
    RequestSent --(HandleResult, on_handle_result) --> MarkerCommandCreated;

    MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> ResultNotified;

    ResultNotified --(MarkerRecorded, on_marker_recorded) --> MarkerCommandRecorded;

    // Replay path ================================================================================
    WaitingMarkerEvent --(MarkerRecorded, on_marker_recorded) --> MarkerCommandRecorded;
    WaitingMarkerEvent --(NonReplayWorkflowTaskStarted, on_non_replay_workflow_task_started)
      --> RequestPrepared;
}

/// Creates a new local activity state machine & immediately schedules the local activity for
/// execution. No command is produced immediately to be sent to the server, as the local activity
/// must resolve before we send a record marker command.
pub(super) fn new_local_activity(
    attrs: ScheduleActivity,
    replaying_when_invoked: bool,
) -> LocalActivityMachine {
    let initial_state = if replaying_when_invoked {
        Replaying {}.into()
    } else {
        Executing {}.into()
    };

    let mut machine = LocalActivityMachine {
        state: initial_state,
        shared_state: SharedState { attrs },
    };

    OnEventWrapper::on_event_mut(&mut machine, LocalActivityMachineEvents::Schedule)
        .expect("Scheduling local activities doesn't fail");
    machine
}

#[derive(Clone)]
pub(super) struct SharedState {
    attrs: ScheduleActivity,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum LocalActivityCommand {}

#[derive(Default, Clone)]
pub(super) struct Executing {}

impl Executing {
    pub(super) fn on_schedule(self) -> LocalActivityMachineTransition<RequestPrepared> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct MarkerCommandCreated {}

impl MarkerCommandCreated {
    pub(super) fn on_command_record_marker(self) -> LocalActivityMachineTransition<ResultNotified> {
        unimplemented!()
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
    pub(super) fn on_handle_result(self) -> LocalActivityMachineTransition<MarkerCommandCreated> {
        unimplemented!()
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
    ) -> LocalActivityMachineTransition<MarkerCommandRecorded> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct WaitingMarkerEvent {}

impl WaitingMarkerEvent {
    pub(super) fn on_marker_recorded(
        self,
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
        todo!()
    }
}

impl WFMachinesAdapter for LocalActivityMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        // TODO
        Ok(vec![])
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
            Some((marker_dat, _)) => todo!(),
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
        prototype_rust_sdk::{
            ActivityOptions, CancellableFuture, LocalActivityOptions, WfContext, WorkflowFunction,
            WorkflowResult,
        },
        test_help::{canned_histories, TestHistoryBuilder},
        workflow::managed_wf::ManagedWFFunc,
    };
    use rstest::{fixture, rstest};

    async fn la_wf(mut command_sink: WfContext) -> WorkflowResult<()> {
        command_sink
            .local_activity(LocalActivityOptions::default())
            .await;
        Ok(().into())
    }

    #[fixture]
    fn la_happy_hist() -> ManagedWFFunc {
        let func = WorkflowFunction::new(la_wf);
        let t = canned_histories::single_local_activity("activity-id-1");
        ManagedWFFunc::new(t, func, vec![])
    }

    #[rstest(wfm, case::success(la_happy_hist()))]
    #[tokio::test]
    async fn one_la_inc(mut wfm: ManagedWFFunc) {
        // First activation will have no server commands. Activity will be put into the activity
        // queue locally
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 0);

        // Complete the la itself TODO: Is it worth doing better than magic seq number for these?
        wfm.complete_local_activity(1, ActivityResult::ok(b"Resolved".into()));

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

        wfm.shutdown().await.unwrap();
    }
}
