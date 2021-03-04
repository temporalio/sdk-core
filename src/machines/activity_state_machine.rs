use rustfsm::{fsm, TransitionResult};

// Schedule / cancel are "explicit events" (imperative rather than past events?)

fsm! {
    pub(super) name ActivityMachine; command ActivityCommand; error ActivityMachineError;

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
pub(super) enum ActivityMachineError {}

pub(super) enum ActivityCommand {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(self) -> ActivityMachineTransition<ScheduleCommandCreated> {
        // would add command here
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduleCommandCreated {}

impl ScheduleCommandCreated {
    pub(super) fn on_activity_task_scheduled(
        self,
    ) -> ActivityMachineTransition<ScheduledEventRecorded> {
        // set initial command event id
        //  this.initialCommandEventId = currentEvent.getEventId();
        TransitionResult::default()
    }
    pub(super) fn on_canceled(self) -> ActivityMachineTransition<Canceled> {
        // cancelCommandNotifyCanceled
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledEventRecorded {}

impl ScheduledEventRecorded {
    pub(super) fn on_task_started(self) -> ActivityMachineTransition<Started> {
        // setStartedCommandEventId
        TransitionResult::default()
    }
    pub(super) fn on_task_timed_out(self) -> ActivityMachineTransition<TimedOut> {
        // notify_timed_out
        TransitionResult::default()
    }
    pub(super) fn on_canceled(
        self,
    ) -> ActivityMachineTransition<ScheduledActivityCancelCommandCreated> {
        // createRequestCancelActivityTaskCommand
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Started {}

impl Started {
    pub(super) fn on_activity_task_completed(self) -> ActivityMachineTransition<Completed> {
        // notify_completed
        TransitionResult::default()
    }
    pub(super) fn on_activity_task_failed(self) -> ActivityMachineTransition<Failed> {
        // notify_failed
        TransitionResult::default()
    }
    pub(super) fn on_activity_task_timed_out(self) -> ActivityMachineTransition<TimedOut> {
        // notify_timed_out
        TransitionResult::default()
    }
    pub(super) fn on_canceled(
        self,
    ) -> ActivityMachineTransition<StartedActivityCancelCommandCreated> {
        // createRequestCancelActivityTaskCommand
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledActivityCancelCommandCreated {}

impl ScheduledActivityCancelCommandCreated {
    pub(super) fn on_command_request_cancel_activity_task(
        self,
    ) -> ActivityMachineTransition<ScheduledActivityCancelCommandCreated> {
        // notifyCanceledIfTryCancel
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledActivityCancelEventRecorded {}

impl ScheduledActivityCancelEventRecorded {
    pub(super) fn on_activity_task_canceled(self) -> ActivityMachineTransition<Canceled> {
        // notify_canceled
        TransitionResult::default()
    }
    pub(super) fn on_activity_task_timed_out(self) -> ActivityMachineTransition<TimedOut> {
        // notify_timed_out
        TransitionResult::default()
    }
}

impl From<ScheduledActivityCancelCommandCreated> for ScheduledActivityCancelEventRecorded {
    fn from(_: ScheduledActivityCancelCommandCreated) -> Self {
        Self::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct StartedActivityCancelCommandCreated {}

impl StartedActivityCancelCommandCreated {
    pub(super) fn on_activity_task_cancel_requested(
        self,
    ) -> ActivityMachineTransition<StartedActivityCancelEventRecorded> {
        // notifyCanceledIfTryCancel
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct StartedActivityCancelEventRecorded {}

impl StartedActivityCancelEventRecorded {
    pub(super) fn on_activity_task_completed(self) -> ActivityMachineTransition<Completed> {
        // notify_completed
        TransitionResult::default()
    }
    pub(super) fn on_activity_task_failed(self) -> ActivityMachineTransition<Failed> {
        // notify_failed
        TransitionResult::default()
    }
    pub(super) fn on_activity_task_timed_out(self) -> ActivityMachineTransition<TimedOut> {
        // notify_timed_out
        TransitionResult::default()
    }
    pub(super) fn on_activity_task_canceled(self) -> ActivityMachineTransition<Canceled> {
        // notifyCancellationFromEvent
        TransitionResult::default()
    }
}

impl From<ScheduledActivityCancelEventRecorded> for StartedActivityCancelEventRecorded {
    fn from(_: ScheduledActivityCancelEventRecorded) -> Self {
        Self::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Completed {}

#[derive(Default, Clone)]
pub(super) struct Failed {}

#[derive(Default, Clone)]
pub(super) struct TimedOut {}

#[derive(Default, Clone)]
pub(super) struct Canceled {}

#[cfg(test)]
mod activity_machine_tests {
    #[test]
    fn test() {}
}
