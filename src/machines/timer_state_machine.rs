use rustfsm::{fsm, TransitionResult};

fsm! {
    TimerMachine, TimerCommand, TimerMachineError

    Created --(Schedule, on_schedule)--> StartCommandCreated;

    StartCommandCreated --(CommandStartTimer) --> StartCommandCreated;
    StartCommandCreated --(TimerStarted, on_timer_started) --> StartCommandRecorded;
    StartCommandCreated --(Cancel, on_cancel) --> Canceled;

    StartCommandRecorded --(TimerFired, on_fired) --> Fired;
    StartCommandRecorded --(Cancel, on_cancel) --> CancelTimerCommandCreated;

    CancelTimerCommandCreated --(Cancel) --> CancelTimerCommandCreated;
    CancelTimerCommandCreated --(CommandTypeCancelTimer, on_cancel) --> CancelTimerCommandSent;

    CancelTimerCommandSent --(TimerCanceled) --> Canceled;
}

#[derive(thiserror::Error, Debug)]
pub enum TimerMachineError {}
pub enum TimerCommand {}

#[derive(Default)]
pub struct Created {}
impl Created {
    pub fn on_schedule(self) -> TimerMachineTransition {
        // would add command here
        TimerMachineTransition::default::<StartCommandCreated>()
    }
}

#[derive(Default)]
pub struct StartCommandCreated {}
impl StartCommandCreated {
    pub fn on_timer_started(self) -> TimerMachineTransition {
        TimerMachineTransition::default::<StartCommandRecorded>()
    }
    pub fn on_cancel(self) -> TimerMachineTransition {
        TimerMachineTransition::default::<Canceled>()
    }
}

#[derive(Default)]
pub struct StartCommandRecorded {}
impl StartCommandRecorded {
    pub fn on_cancel(self) -> TimerMachineTransition {
        TimerMachineTransition::default::<CancelTimerCommandCreated>()
    }
    pub fn on_fired(self) -> TimerMachineTransition {
        TimerMachineTransition::default::<Fired>()
    }
}

#[derive(Default)]
pub struct CancelTimerCommandCreated {}
impl CancelTimerCommandCreated {
    pub fn on_cancel(self) -> TimerMachineTransition {
        TimerMachineTransition::default::<Canceled>()
    }
}

#[derive(Default)]
pub struct CancelTimerCommandSent {}

#[derive(Default)]
pub struct Fired {}

#[derive(Default)]
pub struct Canceled {}
impl From<CancelTimerCommandSent> for Canceled {
    fn from(_: CancelTimerCommandSent) -> Self {
        Canceled::default()
    }
}

#[cfg(test)]
mod activity_machine_tests {
    #[test]
    fn test() {}
}
