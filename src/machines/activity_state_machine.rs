use rustfsm::{fsm, TransitionResult};

// Schedule / cancel are "explicit events" (imperative rather than past events?)

fsm! {
    ActivityMachine, ActivityCommand, ActivityMachineError

    Created --(Schedule, on_schedule)--> ScheduleCommandCreated;

    ScheduleCommandCreated --(CommandScheduleActivityTask) --> ScheduleCommandCreated;
    ScheduleCommandCreated
      --(ActivityTaskScheduled, on_activity_task_scheduled) --> ScheduledEventRecorded;
    ScheduleCommandCreated --(Cancel, on_canceled) --> Canceled;

    ScheduledEventRecorded --(ActivityTaskStarted, on_task_started) --> Started;
    ScheduledEventRecorded --(ActivityTaskTimedOut, on_task_timed_out) --> TimedOut;
    ScheduledEventRecorded --(Cancel, on_canceled) --> ScheduledActivityCancelCommandCreated;

    Started --(ActivityTaskCompleted, on_activity_task_completed) --> Completed;
    Started --(ActivityTaskFailed, on_activity_task_failed) --> Failed;
    Started --(ActivityTaskTimedOut, on_activity_task_timed_out) --> TimedOut;
    Started --(Cancel, on_canceled) --> StartedActivityCancelCommandCreated;

    ScheduledActivityCancelCommandCreated
      --(CommandRequestCancelActivityTask,
         on_command_request_cancel_activity_task) --> ScheduledActivityCancelCommandCreated;
    ScheduledActivityCancelCommandCreated
      --(ActivityTaskCancelRequested) --> ScheduledActivityCancelEventRecorded;

    ScheduledActivityCancelEventRecorded
      --(ActivityTaskCanceled, on_activity_task_canceled) --> Canceled;
    ScheduledActivityCancelEventRecorded
      --(ActivityTaskStarted) --> StartedActivityCancelEventRecorded;
    ScheduledActivityCancelEventRecorded
      --(ActivityTaskTimedOut, on_activity_task_timed_out) --> TimedOut;

    StartedActivityCancelCommandCreated
      --(CommandRequestCancelActivityTask) --> StartedActivityCancelCommandCreated;
    StartedActivityCancelCommandCreated
      --(ActivityTaskCancelRequested,
         on_activity_task_cancel_requested) --> StartedActivityCancelEventRecorded;

    StartedActivityCancelEventRecorded --(ActivityTaskFailed, on_activity_task_failed) --> Failed;
    StartedActivityCancelEventRecorded
      --(ActivityTaskCompleted, on_activity_task_completed) --> Completed;
    StartedActivityCancelEventRecorded
      --(ActivityTaskTimedOut, on_activity_task_timed_out) --> TimedOut;
    StartedActivityCancelEventRecorded
      --(ActivityTaskCanceled, on_activity_task_canceled) --> Canceled;
}

#[derive(thiserror::Error, Debug)]
pub enum ActivityMachineError {}
pub enum ActivityCommand {}

#[derive(Default)]
pub struct Created {}
impl Created {
    pub fn on_schedule(self) -> ActivityMachineTransition {
        // would add command here
        ActivityMachineTransition::default::<ScheduleCommandCreated>()
    }
}

#[derive(Default)]
pub struct ScheduleCommandCreated {}
impl ScheduleCommandCreated {
    pub fn on_activity_task_scheduled(self) -> ActivityMachineTransition {
        // set initial command event id
        //  this.initialCommandEventId = currentEvent.getEventId();
        ActivityMachineTransition::default::<ScheduledEventRecorded>()
    }
    pub fn on_canceled(self) -> ActivityMachineTransition {
        // cancelCommandNotifyCanceled
        ActivityMachineTransition::default::<Canceled>()
    }
}

#[derive(Default)]
pub struct ScheduledEventRecorded {}
impl ScheduledEventRecorded {
    pub fn on_task_started(self) -> ActivityMachineTransition {
        // setStartedCommandEventId
        ActivityMachineTransition::default::<Started>()
    }
    pub fn on_task_timed_out(self) -> ActivityMachineTransition {
        // notify_timed_out
        ActivityMachineTransition::default::<TimedOut>()
    }
    pub fn on_canceled(self) -> ActivityMachineTransition {
        // createRequestCancelActivityTaskCommand
        ActivityMachineTransition::default::<ScheduledActivityCancelCommandCreated>()
    }
}

#[derive(Default)]
pub struct Started {}
impl Started {
    pub fn on_activity_task_completed(self) -> ActivityMachineTransition {
        // notify_completed
        ActivityMachineTransition::default::<Completed>()
    }
    pub fn on_activity_task_failed(self) -> ActivityMachineTransition {
        // notify_failed
        ActivityMachineTransition::default::<Failed>()
    }
    pub fn on_activity_task_timed_out(self) -> ActivityMachineTransition {
        // notify_timed_out
        ActivityMachineTransition::default::<TimedOut>()
    }
    pub fn on_canceled(self) -> ActivityMachineTransition {
        // createRequestCancelActivityTaskCommand
        ActivityMachineTransition::default::<Failed>()
    }
}

#[derive(Default)]
pub struct ScheduledActivityCancelCommandCreated {}
impl ScheduledActivityCancelCommandCreated {
    pub fn on_command_request_cancel_activity_task(self) -> ActivityMachineTransition {
        // notifyCanceledIfTryCancel
        ActivityMachineTransition::default::<ScheduledActivityCancelCommandCreated>()
    }
}

#[derive(Default)]
pub struct ScheduledActivityCancelEventRecorded {}
impl ScheduledActivityCancelEventRecorded {
    pub fn on_activity_task_canceled(self) -> ActivityMachineTransition {
        // notify_canceled
        ActivityMachineTransition::default::<Canceled>()
    }
    pub fn on_activity_task_timed_out(self) -> ActivityMachineTransition {
        // notify_timed_out
        ActivityMachineTransition::default::<Canceled>()
    }
}
impl From<ScheduledActivityCancelCommandCreated> for ScheduledActivityCancelEventRecorded {
    fn from(_: ScheduledActivityCancelCommandCreated) -> Self {
        Self::default()
    }
}

#[derive(Default)]
pub struct StartedActivityCancelCommandCreated {}
impl StartedActivityCancelCommandCreated {
    pub fn on_activity_task_cancel_requested(self) -> ActivityMachineTransition {
        // notifyCanceledIfTryCancel
        ActivityMachineTransition::default::<StartedActivityCancelEventRecorded>()
    }
}

#[derive(Default)]
pub struct StartedActivityCancelEventRecorded {}
impl StartedActivityCancelEventRecorded {
    pub fn on_activity_task_completed(self) -> ActivityMachineTransition {
        // notify_completed
        ActivityMachineTransition::default::<Completed>()
    }
    pub fn on_activity_task_failed(self) -> ActivityMachineTransition {
        // notify_failed
        ActivityMachineTransition::default::<Failed>()
    }
    pub fn on_activity_task_timed_out(self) -> ActivityMachineTransition {
        // notify_timed_out
        ActivityMachineTransition::default::<TimedOut>()
    }
    pub fn on_activity_task_canceled(self) -> ActivityMachineTransition {
        // notifyCancellationFromEvent
        ActivityMachineTransition::default::<Failed>()
    }
}
impl From<ScheduledActivityCancelEventRecorded> for StartedActivityCancelEventRecorded {
    fn from(_: ScheduledActivityCancelEventRecorded) -> Self {
        Self::default()
    }
}

#[derive(Default)]
pub struct Completed {}
#[derive(Default)]
pub struct Failed {}
#[derive(Default)]
pub struct TimedOut {}
#[derive(Default)]
pub struct Canceled {}

#[cfg(test)]
mod activity_machine_tests {
    #[test]
    fn test() {}
}
