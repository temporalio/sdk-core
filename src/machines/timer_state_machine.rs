use rustfsm::{fsm, TransitionResult};

fsm! {
    name TimerMachine;
    command TimerCommand;
    error TimerMachineError;
    shared_state SharedState;

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

pub struct SharedState {}

#[derive(thiserror::Error, Debug)]
pub enum TimerMachineError {}

pub enum TimerCommand {
    StartTimer(/* TODO: Command attribs */),
    CancelTimer(/* TODO: Command attribs */),
}

#[derive(Default)]
pub struct CancelTimerCommandCreated {}
impl CancelTimerCommandCreated {
    pub fn on_command_cancel_timer(self) -> TimerMachineTransition {
        // TODO: Need to call notify_cancellation - but have no ref to machine
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
        TimerMachineTransition::ok(
            vec![TimerCommand::StartTimer()],
            StartCommandCreated::default(),
        )
    }
}

#[derive(Default)]
pub struct Fired {}

#[derive(Default)]
pub struct StartCommandCreated {}

impl StartCommandCreated {
    pub fn on_timer_started(self) -> TimerMachineTransition {
        // Record initial event ID
        unimplemented!()
    }
    pub fn on_cancel(self) -> TimerMachineTransition {
        // Cancel the initial command - which just sets a "canceled" flag in a wrapper of a
        // proto command.
        unimplemented!()
    }
}

#[derive(Default)]
pub struct StartCommandRecorded {}

impl StartCommandRecorded {
    pub fn on_timer_fired(self) -> TimerMachineTransition {
        // Complete callback with timer fired event
        unimplemented!()
    }
    pub fn on_cancel(self) -> TimerMachineTransition {
        TimerMachineTransition::ok(
            vec![TimerCommand::CancelTimer()],
            CancelTimerCommandCreated::default(),
        )
    }
}

impl TimerMachine {
    fn notify_cancellation(&self) {
        // TODO: Needs access to shared state
        // "notify cancellation"
        /*
        completionCallback.apply(
            HistoryEvent.newBuilder()
                .setEventType(EventType.EVENT_TYPE_TIMER_CANCELED)
                .setTimerCanceledEventAttributes(
                    TimerCanceledEventAttributes.newBuilder()
                        .setIdentity("workflow")
                        .setTimerId(startAttributes.getTimerId()))
                .build());
        */
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn wat() {}
}
