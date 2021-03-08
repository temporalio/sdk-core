use crate::protos::temporal::api::command::v1::ScheduleActivityTaskCommandAttributes;
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

#[derive(Debug, Clone, derive_more::Display)]
pub(super) enum ActivityCancellationType {
    /**
     * Wait for activity cancellation completion. Note that activity must heartbeat to receive a
     * cancellation notification. This can block the cancellation for a long time if activity doesn't
     * heartbeat or chooses to ignore the cancellation request.
     */
    WaitCancellationCompleted,

    /** Initiate a cancellation request and immediately report cancellation to the workflow. */
    TryCancel,

    /**
     * Do not request cancellation of the activity and immediately report cancellation to the workflow
     */
    Abandon,
}

impl Default for ActivityCancellationType {
    fn default() -> Self {
        ActivityCancellationType::TryCancel
    }
}

#[derive(Default, Clone)]
pub(super) struct SharedState {
    attrs: ScheduleActivityTaskCommandAttributes,
    cancellation_type: ActivityCancellationType,
}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(self) -> ActivityMachineTransition {
        // would add command here
        ActivityMachineTransition::default::<ScheduleCommandCreated>()
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduleCommandCreated {}

impl ScheduleCommandCreated {
    pub(super) fn on_activity_task_scheduled(self) -> ActivityMachineTransition {
        // set initial command event id
        //  this.initialCommandEventId = currentEvent.getEventId();
        ActivityMachineTransition::default::<ScheduledEventRecorded>()
    }
    pub(super) fn on_canceled(self) -> ActivityMachineTransition {
        // cancelCommandNotifyCanceled
        ActivityMachineTransition::default::<Canceled>()
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledEventRecorded {}

impl ScheduledEventRecorded {
    pub(super) fn on_task_started(self) -> ActivityMachineTransition {
        // setStartedCommandEventId
        ActivityMachineTransition::default::<Started>()
    }
    pub(super) fn on_task_timed_out(self) -> ActivityMachineTransition {
        // notify_timed_out
        ActivityMachineTransition::default::<TimedOut>()
    }
    pub(super) fn on_canceled(self) -> ActivityMachineTransition {
        // createRequestCancelActivityTaskCommand
        ActivityMachineTransition::default::<ScheduledActivityCancelCommandCreated>()
    }
}

#[derive(Default, Clone)]
pub(super) struct Started {}

impl Started {
    pub(super) fn on_activity_task_completed(self) -> ActivityMachineTransition {
        // notify_completed
        ActivityMachineTransition::default::<Completed>()
    }
    pub(super) fn on_activity_task_failed(self) -> ActivityMachineTransition {
        // notify_failed
        ActivityMachineTransition::default::<Failed>()
    }
    pub(super) fn on_activity_task_timed_out(self) -> ActivityMachineTransition {
        // notify_timed_out
        ActivityMachineTransition::default::<TimedOut>()
    }
    pub(super) fn on_canceled(self) -> ActivityMachineTransition {
        // createRequestCancelActivityTaskCommand
        ActivityMachineTransition::default::<Failed>()
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledActivityCancelCommandCreated {}

impl ScheduledActivityCancelCommandCreated {
    pub(super) fn on_command_request_cancel_activity_task(self) -> ActivityMachineTransition {
        // notifyCanceledIfTryCancel
        ActivityMachineTransition::default::<ScheduledActivityCancelCommandCreated>()
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledActivityCancelEventRecorded {}

impl ScheduledActivityCancelEventRecorded {
    pub(super) fn on_activity_task_canceled(self) -> ActivityMachineTransition {
        // notify_canceled
        ActivityMachineTransition::default::<Canceled>()
    }
    pub(super) fn on_activity_task_timed_out(self) -> ActivityMachineTransition {
        // notify_timed_out
        ActivityMachineTransition::default::<Canceled>()
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
    pub(super) fn on_activity_task_cancel_requested(self) -> ActivityMachineTransition {
        // notifyCanceledIfTryCancel
        ActivityMachineTransition::default::<StartedActivityCancelEventRecorded>()
    }
}

#[derive(Default, Clone)]
pub(super) struct StartedActivityCancelEventRecorded {}

impl StartedActivityCancelEventRecorded {
    pub(super) fn on_activity_task_completed(self) -> ActivityMachineTransition {
        // notify_completed
        ActivityMachineTransition::default::<Completed>()
    }
    pub(super) fn on_activity_task_failed(self) -> ActivityMachineTransition {
        // notify_failed
        ActivityMachineTransition::default::<Failed>()
    }
    pub(super) fn on_activity_task_timed_out(self) -> ActivityMachineTransition {
        // notify_timed_out
        ActivityMachineTransition::default::<TimedOut>()
    }
    pub(super) fn on_activity_task_canceled(self) -> ActivityMachineTransition {
        // notifyCancellationFromEvent
        ActivityMachineTransition::default::<Failed>()
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
    use crate::machines::test_help::{CommandSender, TestHistoryBuilder, TestWorkflowDriver};
    use crate::machines::WorkflowMachines;
    use crate::protos::temporal::api::command::v1::CompleteWorkflowExecutionCommandAttributes;
    use crate::protos::temporal::api::command::v1::ScheduleActivityTaskCommandAttributes;
    use crate::protos::temporal::api::enums::v1::CommandType;
    use crate::test_help::canned_histories;
    use rstest::{fixture, rstest};
    use tracing::Level;

    #[fixture]
    fn activity_happy_hist() -> (TestHistoryBuilder, WorkflowMachines) {
        let twd = TestWorkflowDriver::new(|mut command_sink: CommandSender| async move {
            let activity = ScheduleActivityTaskCommandAttributes {
                ..Default::default()
            };
            command_sink.activity(activity);

            let complete = CompleteWorkflowExecutionCommandAttributes::default();
            command_sink.send(complete.into());
        });

        let t = canned_histories::single_activity("activity1");
        let state_machines = WorkflowMachines::new(
            "wfid".to_string(),
            "runid".to_string(),
            Box::new(twd).into(),
        );

        assert_eq!(2, t.as_history().get_workflow_task_count(None).unwrap());
        (t, state_machines)
    }

    #[rstest]
    fn test_activity_happy_path(activity_happy_hist: (TestHistoryBuilder, WorkflowMachines)) {
        let s = span!(Level::DEBUG, "Test start", t = "activity_happy_path");
        let _enter = s.enter();
        let (t, mut state_machines) = activity_happy_hist;
        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, Some(1))
            .unwrap();
        state_machines.get_wf_activation();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::ScheduleActivityTask as i32
        );
    }
}
