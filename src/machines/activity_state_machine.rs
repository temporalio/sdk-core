#![allow(clippy::large_enum_variant)]

use crate::protos::coresdk::PayloadsExt;
use crate::protos::temporal::api::failure::v1::Failure;
use crate::protos::temporal::api::history::v1::ActivityTaskCanceledEventAttributes;
use crate::{
    machines::{
        workflow_machines::MachineResponse, Cancellable, NewMachineWithCommand, WFMachinesAdapter,
        WFMachinesError,
    },
    protos::coresdk::workflow_commands::ScheduleActivity,
    protos::{
        coresdk::{
            activity_result::{self as ar, activity_result, ActivityResult},
            activity_task,
            workflow_activation::ResolveActivity,
        },
        temporal::api::{
            command::v1::Command,
            common::v1::Payloads,
            enums::v1::{CommandType, EventType},
            history::v1::{
                history_event, ActivityTaskCompletedEventAttributes,
                ActivityTaskFailedEventAttributes, HistoryEvent,
            },
        },
    },
};
use rustfsm::{fsm, MachineError, StateMachine, TransitionResult};
use std::convert::{TryFrom, TryInto};

// Schedule / cancel are "explicit events" (imperative rather than past events?)

fsm! {
    pub(super) name ActivityMachine;
    command ActivityMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(Schedule, on_schedule)--> ScheduleCommandCreated;

    ScheduleCommandCreated --(CommandScheduleActivityTask) --> ScheduleCommandCreated;
    ScheduleCommandCreated
      --(ActivityTaskScheduled, on_activity_task_scheduled) --> ScheduledEventRecorded;
    ScheduleCommandCreated --(Cancel, on_canceled) --> Canceled;

    ScheduledEventRecorded --(ActivityTaskStarted, on_task_started) --> Started;
    ScheduledEventRecorded --(ActivityTaskTimedOut, on_task_timed_out) --> TimedOut;
    ScheduledEventRecorded --(Cancel, on_canceled) --> ScheduledActivityCancelCommandCreated;

    Started --(ActivityTaskCompleted(ActivityTaskCompletedEventAttributes), on_activity_task_completed) --> Completed;
    Started --(ActivityTaskFailed(ActivityTaskFailedEventAttributes), on_activity_task_failed) --> Failed;
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

    StartedActivityCancelEventRecorded --(ActivityTaskFailed(ActivityTaskFailedEventAttributes), on_activity_task_failed) --> Failed;
    StartedActivityCancelEventRecorded
      --(ActivityTaskCompleted(ActivityTaskCompletedEventAttributes), on_activity_task_completed) --> Completed;
    StartedActivityCancelEventRecorded
      --(ActivityTaskTimedOut, on_activity_task_timed_out) --> TimedOut;
    StartedActivityCancelEventRecorded
      --(ActivityTaskCanceled, on_activity_task_canceled) --> Canceled;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum ActivityMachineCommand {
    #[display(fmt = "Complete")]
    Complete(Option<Payloads>),
    #[display(fmt = "Fail")]
    Fail(Option<Failure>),
    #[display(fmt = "Complete")]
    Cancel(Option<Payloads>),
}

#[derive(Debug, Clone, derive_more::Display)]
pub(super) enum ActivityCancellationType {
    /// Wait for activity cancellation completion. Note that activity must heartbeat to receive a
    /// cancellation notification. This can block the cancellation for a long time if activity doesn't
    /// heartbeat or chooses to ignore the cancellation request.
    WaitCancellationCompleted,

    /// Initiate a cancellation request and immediately report cancellation to the workflow.
    TryCancel,

    /// Do not request cancellation of the activity and immediately report cancellation to the workflow
    Abandon,
}

impl Default for ActivityCancellationType {
    fn default() -> Self {
        ActivityCancellationType::TryCancel
    }
}

/// Creates a new activity state machine and a command to schedule it on the server.
pub(super) fn new_activity(attribs: ScheduleActivity) -> NewMachineWithCommand<ActivityMachine> {
    let (activity, add_cmd) = ActivityMachine::new_scheduled(attribs);
    NewMachineWithCommand {
        command: add_cmd,
        machine: activity,
    }
}

impl ActivityMachine {
    /// Create a new activity and immediately schedule it.
    pub(crate) fn new_scheduled(attribs: ScheduleActivity) -> (Self, Command) {
        let mut s = Self {
            state: Created {}.into(),
            shared_state: SharedState {
                attrs: attribs,
                cancellation_type: ActivityCancellationType::TryCancel,
            },
        };
        s.on_event_mut(ActivityMachineEvents::Schedule)
            .expect("Scheduling activities doesn't fail");
        let cmd = Command {
            command_type: CommandType::ScheduleActivityTask as i32,
            attributes: Some(s.shared_state().attrs.clone().into()),
        };
        (s, cmd)
    }
}

impl TryFrom<HistoryEvent> for ActivityMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        Ok(match EventType::from_i32(e.event_type) {
            Some(EventType::ActivityTaskScheduled) => Self::ActivityTaskScheduled,
            Some(EventType::ActivityTaskStarted) => Self::ActivityTaskStarted,
            Some(EventType::ActivityTaskCompleted) => {
                if let Some(history_event::Attributes::ActivityTaskCompletedEventAttributes(
                    attrs,
                )) = e.attributes
                {
                    Self::ActivityTaskCompleted(attrs)
                } else {
                    return Err(WFMachinesError::MalformedEvent(
                        e,
                        "Activity completion attributes were unset".to_string(),
                    ));
                }
            }
            Some(EventType::ActivityTaskFailed) => {
                if let Some(history_event::Attributes::ActivityTaskFailedEventAttributes(attrs)) =
                    e.attributes
                {
                    Self::ActivityTaskFailed(attrs)
                } else {
                    return Err(WFMachinesError::MalformedEvent(
                        e,
                        "Activity completion attributes were unset".to_string(),
                    ));
                }
            }
            Some(EventType::ActivityTaskTimedOut) => Self::ActivityTaskTimedOut,
            Some(EventType::ActivityTaskCancelRequested) => Self::ActivityTaskCancelRequested,
            Some(EventType::ActivityTaskCanceled) => Self::ActivityTaskCanceled,
            _ => {
                return Err(WFMachinesError::UnexpectedEvent(
                    e,
                    "Activity machine does not handle this event",
                ))
            }
        })
    }
}

impl TryFrom<CommandType> for ActivityMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::ScheduleActivityTask => Self::CommandScheduleActivityTask,
            CommandType::RequestCancelActivityTask => Self::CommandRequestCancelActivityTask,
            _ => return Err(()),
        })
    }
}

impl WFMachinesAdapter for ActivityMachine {
    fn adapt_response(
        &self,
        event: &HistoryEvent,
        _has_next_event: bool,
        my_command: ActivityMachineCommand,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            ActivityMachineCommand::Complete(result) => {
                let result = result
                    .map(TryInto::try_into)
                    .transpose()
                    .map_err(|pe| WFMachinesError::NotExactlyOnePayload(pe, event.clone()))?;
                vec![ResolveActivity {
                    activity_id: self.shared_state.attrs.activity_id.clone(),
                    result: Some(ActivityResult {
                        status: Some(activity_result::Status::Completed(ar::Success { result })),
                    }),
                }
                .into()]
            }
            ActivityMachineCommand::Fail(failure) => vec![ResolveActivity {
                activity_id: self.shared_state.attrs.activity_id.clone(),
                result: Some(ActivityResult {
                    status: Some(activity_result::Status::Failed(ar::Failure {
                        failure: match failure {
                            Some(f) => Some(f.into()),
                            None => None,
                        },
                    })),
                }),
            }
            .into()],
            ActivityMachineCommand::Cancel(_) => unimplemented!(),
        })
    }
}

impl Cancellable for ActivityMachine {
    fn cancel(&mut self) -> Result<MachineResponse, MachineError<Self::Error>> {
        unimplemented!()
    }

    fn was_cancelled_before_sent_to_server(&self) -> bool {
        false // TODO return cancellation flag from the shared state
    }
}

#[derive(Default, Clone)]
pub(super) struct SharedState {
    attrs: ScheduleActivity,
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
    pub(super) fn on_activity_task_completed(
        self,
        attrs: ActivityTaskCompletedEventAttributes,
    ) -> ActivityMachineTransition {
        // notify_completed
        ActivityMachineTransition::ok(
            vec![ActivityMachineCommand::Complete(attrs.result)],
            Completed::default(),
        )
    }
    pub(super) fn on_activity_task_failed(
        self,
        attrs: ActivityTaskFailedEventAttributes,
    ) -> ActivityMachineTransition {
        // notify_failed
        ActivityMachineTransition::ok(
            vec![ActivityMachineCommand::Fail(attrs.failure)],
            Completed::default(),
        )
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
    pub(super) fn on_activity_task_completed(
        self,
        attrs: ActivityTaskCompletedEventAttributes,
    ) -> ActivityMachineTransition {
        // notify_completed
        ActivityMachineTransition::ok(
            vec![ActivityMachineCommand::Complete(attrs.result)],
            Completed::default(),
        )
    }
    pub(super) fn on_activity_task_failed(
        self,
        attrs: ActivityTaskFailedEventAttributes,
    ) -> ActivityMachineTransition {
        // notify_failed
        ActivityMachineTransition::ok(
            vec![ActivityMachineCommand::Fail(attrs.failure)],
            Completed::default(),
        )
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
mod test {
    use super::*;
    use crate::{
        machines::{
            test_help::{CommandSender, TestHistoryBuilder, TestWorkflowDriver},
            workflow_machines::WorkflowMachines,
        },
        protos::coresdk::workflow_commands::CompleteWorkflowExecution,
        test_help::canned_histories,
    };
    use rstest::{fixture, rstest};
    use std::time::Duration;

    #[fixture]
    fn activity_happy_hist() -> (TestHistoryBuilder, WorkflowMachines) {
        let twd = activity_workflow_driver();
        let t = canned_histories::single_activity("activity-id-1");
        let state_machines = WorkflowMachines::new(
            "wfid".to_string(),
            "runid".to_string(),
            Box::new(twd).into(),
        );

        assert_eq!(2, t.as_history().get_workflow_task_count(None).unwrap());
        (t, state_machines)
    }

    #[fixture]
    fn activity_failure_hist() -> (TestHistoryBuilder, WorkflowMachines) {
        let twd = activity_workflow_driver();
        let t = canned_histories::single_failed_activity("activity-id-1");
        let state_machines = WorkflowMachines::new(
            "wfid".to_string(),
            "runid".to_string(),
            Box::new(twd).into(),
        );

        assert_eq!(2, t.as_history().get_workflow_task_count(None).unwrap());
        (t, state_machines)
    }

    fn activity_workflow_driver() -> TestWorkflowDriver {
        TestWorkflowDriver::new(|mut command_sink: CommandSender| async move {
            let activity = ScheduleActivity {
                activity_id: "activity-id-1".to_string(),
                ..Default::default()
            };
            command_sink.activity(activity).await;

            let complete = CompleteWorkflowExecution::default();
            command_sink.send(complete.into());
        })
    }

    #[rstest(
        hist_batches,
        case::success(activity_happy_hist()),
        case::failure(activity_failure_hist())
    )]
    fn single_activity_inc(hist_batches: (TestHistoryBuilder, WorkflowMachines)) {
        let (t, mut state_machines) = hist_batches;

        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, Some(1))
            .unwrap();
        state_machines.get_wf_activation();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::ScheduleActivityTask as i32
        );
        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, Some(2))
            .unwrap();
        state_machines.get_wf_activation();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
    }

    #[rstest(
        hist_batches,
        case::success(activity_happy_hist()),
        case::failure(activity_failure_hist())
    )]
    fn single_activity_full(hist_batches: (TestHistoryBuilder, WorkflowMachines)) {
        let (t, mut state_machines) = hist_batches;
        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, None)
            .unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
    }
}
