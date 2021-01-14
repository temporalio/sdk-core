use rustfsm::{fsm, TransitionResult};

fsm! {
    TimerMachine, TimerCommand, TimerMachineError

    CancelTimerCommandCreated --(Cancel) --> CancelTimerCommandCreated;
    CancelTimerCommandCreated --(CommandCancelTimer, on_command_cancel_timer) --> CancelTimerCommandSent;

    CancelTimerCommandSent --(TimerCanceled, on_timer_canceled) --> Canceled;

    Created --(Schedule, on_schedule) --> StartCommandCreated;

    StartCommandCreated --(CommandStartTimer) --> StartCommandCreated;
    StartCommandCreated --(TimerStarted, on_timer_started) --> StartCommandRecorded;
    StartCommandCreated --(Cancel, on_cancel) --> Canceled;

    StartCommandRecorded --(TimerFired, on_timer_fired) --> Fired;
    StartCommandRecorded --(Cancel, on_cancel) --> CancelTimerCommandCreated;
}

#[derive(thiserror::Error, Debug)]
pub enum TimerMachineError {}
pub enum TimerCommand {}

#[derive(Default)]
pub struct CancelTimerCommandCreated {}
impl CancelTimerCommandCreated {
    pub fn on_command_cancel_timer(self) -> TimerMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct CancelTimerCommandSent {}
impl CancelTimerCommandSent {
    pub fn on_timer_canceled(self) -> TimerMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct Canceled {}

#[derive(Default)]
pub struct Created {}
impl Created {
    pub fn on_schedule(self) -> TimerMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct Fired {}

#[derive(Default)]
pub struct StartCommandCreated {}
impl StartCommandCreated {
    pub fn on_timer_started(self) -> TimerMachineTransition {
        unimplemented!()
    }
    pub fn on_cancel(self) -> TimerMachineTransition {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct StartCommandRecorded {}
impl StartCommandRecorded {
    pub fn on_timer_fired(self) -> TimerMachineTransition {
        unimplemented!()
    }
    pub fn on_cancel(self) -> TimerMachineTransition {
        unimplemented!()
    }
}
