use rustfsm::{fsm, TransitionResult};

fsm! {
    name LocalActivityMachine; command LocalActivityCommand; error LocalActivityMachineError;

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
pub enum LocalActivityMachineError {}

pub enum LocalActivityCommand {}

#[derive(Default)]
pub struct Created {}

impl Created {
    pub fn on_check_execution_state(self) -> LocalActivityMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct Executing {}

impl Executing {
    pub fn on_schedule(self) -> LocalActivityMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct MarkerCommandCreated {}

impl MarkerCommandCreated {
    pub fn on_command_record_marker(self) -> LocalActivityMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct MarkerCommandRecorded {}

#[derive(Default)]
pub struct Replaying {}

#[derive(Default)]
pub struct RequestPrepared {}

#[derive(Default)]
pub struct RequestSent {}

impl RequestSent {
    pub fn on_handle_result(self) -> LocalActivityMachineTransition {
        unimplemented!()
    }
}

impl From<RequestPrepared> for RequestSent {
    fn from(_: RequestPrepared) -> Self {
        Self::default()
    }
}

#[derive(Default)]
pub struct ResultNotified {}

impl ResultNotified {
    pub fn on_marker_recorded(self) -> LocalActivityMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct WaitingMarkerEvent {}

impl WaitingMarkerEvent {
    pub fn on_marker_recorded(self) -> LocalActivityMachineTransition {
        unimplemented!()
    }
    pub fn on_non_replay_workflow_task_started(self) -> LocalActivityMachineTransition {
        unimplemented!()
    }
}

impl From<Replaying> for WaitingMarkerEvent {
    fn from(_: Replaying) -> Self {
        Self::default()
    }
}
