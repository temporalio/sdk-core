use rustfsm::{fsm, TransitionResult};

fsm! {
    pub(super) name LocalActivityMachine;
    command LocalActivityCommand;
    error LocalActivityMachineError;

    Created --(CheckExecutionState, on_check_execution_state) --> Replaying;
    Created --(CheckExecutionState, on_check_execution_state) --> Executing;

    Executing --(Schedule, on_schedule) --> RequestPrepared;

    MarkerCommandCreated --(CommandRecordMarker, on_command_record_marker) --> ResultNotified;

    Replaying --(Schedule) --> WaitingMarkerEvent;

    RequestPrepared --(MarkAsSent) --> RequestSent;

    RequestSent --(NonReplayWorkflowTaskStarted) --> RequestSent;
    RequestSent --(HandleResult, on_handle_result) --> MarkerCommandCreated;

    ResultNotified --(MarkerRecorded, on_marker_recorded) --> MarkerCommandRecorded;

    WaitingMarkerEvent --(MarkerRecorded, on_marker_recorded) --> MarkerCommandRecorded;
    WaitingMarkerEvent --(NonReplayWorkflowTaskStarted, on_non_replay_workflow_task_started) --> RequestPrepared;
}

#[derive(thiserror::Error, Debug)]
pub(super) enum LocalActivityMachineError {}

pub(super) enum LocalActivityCommand {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_check_execution_state(
        self,
    ) -> LocalActivityMachineTransition<ReplayingOrExecuting> {
        unimplemented!()
    }
}

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
