#![allow(clippy::large_enum_variant)]

use crate::protos::coresdk::common::Payload;
use crate::protos::temporal::api::history::v1::ActivityTaskTimedOutEventAttributes;
use crate::{
    machines::{
        workflow_machines::MachineResponse, Cancellable, NewMachineWithCommand, WFMachinesAdapter,
        WFMachinesError,
    },
    protos::{
        coresdk::{
            activity_result::{self as ar, activity_result, ActivityResult},
            activity_task,
            workflow_activation::{
                wf_activation_job::{self, Variant},
                ResolveActivity, StartWorkflow, WfActivationJob,
            },
            workflow_commands::{
                ActivityCancellationType, RequestCancelActivity, ScheduleActivity,
            },
            PayloadsExt,
        },
        temporal::api::{
            command::v1::Command,
            common::v1::{ActivityType, Payloads},
            enums::v1::{CommandType, EventType, RetryState, RetryState::CancelRequested},
            failure::v1::{
                failure::{self, FailureInfo},
                ActivityFailureInfo, CanceledFailureInfo, Failure,
            },
            history::v1::{
                history_event, ActivityTaskCanceledEventAttributes,
                ActivityTaskCompletedEventAttributes, ActivityTaskFailedEventAttributes,
                HistoryEvent,
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
      --(ActivityTaskScheduled(i64), shared on_activity_task_scheduled) --> ScheduledEventRecorded;
    ScheduleCommandCreated --(Cancel, shared on_canceled) --> Canceled;

    ScheduledEventRecorded --(ActivityTaskStarted(i64), shared on_task_started) --> Started;
    ScheduledEventRecorded --(ActivityTaskTimedOut(ActivityTaskTimedOutEventAttributes), shared on_task_timed_out) --> TimedOut;
    ScheduledEventRecorded --(Cancel, shared on_canceled) --> ScheduledActivityCancelCommandCreated;
    ScheduledEventRecorded --(Abandon, shared on_abandoned) --> Canceled;

    Started --(ActivityTaskCompleted(ActivityTaskCompletedEventAttributes), on_activity_task_completed) --> Completed;
    Started --(ActivityTaskFailed(ActivityTaskFailedEventAttributes), on_activity_task_failed) --> Failed;
    Started --(ActivityTaskTimedOut(ActivityTaskTimedOutEventAttributes), shared on_activity_task_timed_out) --> TimedOut;
    Started --(Cancel, shared on_canceled) --> StartedActivityCancelCommandCreated;
    Started --(Abandon, shared on_abandoned) --> Canceled;

    ScheduledActivityCancelCommandCreated
      --(CommandRequestCancelActivityTask,
         shared on_command_request_cancel_activity_task) --> ScheduledActivityCancelCommandCreated;
    ScheduledActivityCancelCommandCreated
      --(ActivityTaskCancelRequested) --> ScheduledActivityCancelEventRecorded;

    ScheduledActivityCancelEventRecorded
      --(ActivityTaskCanceled(ActivityTaskCanceledEventAttributes), shared on_activity_task_canceled) --> Canceled;
    ScheduledActivityCancelEventRecorded
      --(ActivityTaskStarted(i64)) --> StartedActivityCancelEventRecorded;
    ScheduledActivityCancelEventRecorded
      --(ActivityTaskTimedOut(ActivityTaskTimedOutEventAttributes), shared on_activity_task_timed_out) --> TimedOut;

    StartedActivityCancelCommandCreated
      --(CommandRequestCancelActivityTask) --> StartedActivityCancelCommandCreated;
    StartedActivityCancelCommandCreated
      --(ActivityTaskCancelRequested,
         shared on_activity_task_cancel_requested) --> StartedActivityCancelEventRecorded;

    StartedActivityCancelEventRecorded --(ActivityTaskFailed(ActivityTaskFailedEventAttributes), on_activity_task_failed) --> Failed;
    StartedActivityCancelEventRecorded
      --(ActivityTaskCompleted(ActivityTaskCompletedEventAttributes), on_activity_task_completed) --> Completed;
    StartedActivityCancelEventRecorded
      --(ActivityTaskTimedOut(ActivityTaskTimedOutEventAttributes), shared on_activity_task_timed_out) --> TimedOut;
    StartedActivityCancelEventRecorded
      --(ActivityTaskCanceled(ActivityTaskCanceledEventAttributes), shared on_activity_task_canceled) --> Canceled;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum ActivityMachineCommand {
    #[display(fmt = "Complete")]
    Complete(Option<Payloads>),
    #[display(fmt = "Fail")]
    Fail(Option<Failure>),
    #[display(fmt = "Cancel")]
    Cancel(Option<Payloads>),
    #[display(fmt = "RequestCancellation")]
    RequestCancellation(Command),
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
                cancellation_type: ActivityCancellationType::from_i32(attribs.cancellation_type)
                    .unwrap(),
                attrs: attribs,
                ..Default::default()
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

    fn machine_responses_from_cancel_request(&self, cancel_cmd: Command) -> Vec<MachineResponse> {
        let mut r = vec![MachineResponse::IssueNewCommand(cancel_cmd)];
        if self.shared_state.cancellation_type
            != ActivityCancellationType::WaitCancellationCompleted
        {
            r.push(MachineResponse::PushWFJob(Variant::ResolveActivity(
                ResolveActivity {
                    activity_id: self.shared_state.attrs.activity_id.clone(),
                    result: Some(ActivityResult {
                        status: Some(activity_result::Status::Canceled(ar::Cancelation {
                            details: None,
                        })),
                    }),
                },
            )))
        }
        r
    }
}

impl TryFrom<HistoryEvent> for ActivityMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        Ok(match EventType::from_i32(e.event_type) {
            Some(EventType::ActivityTaskScheduled) => Self::ActivityTaskScheduled(e.event_id),
            Some(EventType::ActivityTaskStarted) => Self::ActivityTaskStarted(e.event_id),
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
                        "Activity failure attributes were unset".to_string(),
                    ));
                }
            }
            Some(EventType::ActivityTaskTimedOut) => {
                if let Some(history_event::Attributes::ActivityTaskTimedOutEventAttributes(attrs)) =
                    e.attributes
                {
                    Self::ActivityTaskTimedOut(attrs)
                } else {
                    return Err(WFMachinesError::MalformedEvent(
                        e,
                        "Activity timeout attributes were unset".to_string(),
                    ));
                }
            }
            Some(EventType::ActivityTaskCancelRequested) => Self::ActivityTaskCancelRequested,
            Some(EventType::ActivityTaskCanceled) => {
                if let Some(history_event::Attributes::ActivityTaskCanceledEventAttributes(attrs)) =
                    e.attributes
                {
                    Self::ActivityTaskCanceled(attrs)
                } else {
                    return Err(WFMachinesError::MalformedEvent(
                        e,
                        "Activity cancellation attributes were unset".to_string(),
                    ));
                }
            }
            _ => {
                return Err(WFMachinesError::UnexpectedEvent(
                    e,
                    "Activity machine does not handle this event",
                ))
            }
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
                vec![ResolveActivity {
                    activity_id: self.shared_state.attrs.activity_id.clone(),
                    result: Some(ActivityResult {
                        status: Some(activity_result::Status::Completed(ar::Success {
                            result: convert_payloads(event, result)?,
                        })),
                    }),
                }
                .into()]
            }
            ActivityMachineCommand::Fail(failure) => {
                vec![ResolveActivity {
                    activity_id: self.shared_state.attrs.activity_id.clone(),
                    result: Some(ActivityResult {
                        status: Some(activity_result::Status::Failed(ar::Failure {
                            failure: failure.map(Into::into),
                        })),
                    }),
                }
                .into()]
            }
            ActivityMachineCommand::RequestCancellation(c) => {
                self.machine_responses_from_cancel_request(c)
            }
            ActivityMachineCommand::Cancel(details) => {
                vec![ResolveActivity {
                    activity_id: self.shared_state.attrs.activity_id.clone(),
                    result: Some(ActivityResult {
                        status: Some(activity_result::Status::Canceled(ar::Cancelation {
                            details: convert_payloads(event, details)?,
                        })),
                    }),
                }
                .into()]
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

impl Cancellable for ActivityMachine {
    fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<Self::Error>> {
        let event = match self.shared_state.cancellation_type {
            ActivityCancellationType::Abandon => ActivityMachineEvents::Abandon,
            _ => ActivityMachineEvents::Cancel,
        };
        let vec = self.on_event_mut(event)?;
        let res = vec
            .into_iter()
            .flat_map(|amc| match amc {
                ActivityMachineCommand::RequestCancellation(cmd) => {
                    self.machine_responses_from_cancel_request(cmd)
                }
                ActivityMachineCommand::Cancel(details) => {
                    vec![MachineResponse::PushWFJob(Variant::ResolveActivity(
                        ResolveActivity {
                            activity_id: self.shared_state.attrs.activity_id.clone(),
                            result: Some(ActivityResult {
                                status: Some(activity_result::Status::Canceled(ar::Cancelation {
                                    details: None, // TODO convert paylods
                                })),
                            }),
                        },
                    ))]
                }
                x => panic!("Invalid cancel event response {:?}", x),
            })
            .collect();
        Ok(res)
    }

    fn was_cancelled_before_sent_to_server(&self) -> bool {
        self.shared_state().cancelled_before_sent
    }
}

#[derive(Default, Clone)]
pub(super) struct SharedState {
    scheduled_event_id: i64,
    started_event_id: i64,
    attrs: ScheduleActivity,
    cancellation_type: ActivityCancellationType,
    cancelled_before_sent: bool,
}

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
        dat: SharedState,
        scheduled_event_id: i64,
    ) -> ActivityMachineTransition<ScheduledEventRecorded> {
        ActivityMachineTransition::ok_shared(
            vec![],
            ScheduledEventRecorded::default(),
            SharedState {
                scheduled_event_id,
                ..dat
            },
        )
    }
    pub(super) fn on_canceled(self, dat: SharedState) -> ActivityMachineTransition<Canceled> {
        let canceled_state = SharedState {
            cancelled_before_sent: true,
            ..dat
        };
        match dat.cancellation_type {
            ActivityCancellationType::Abandon => {
                ActivityMachineTransition::ok_shared(vec![], Canceled::default(), canceled_state)
            }
            _ => notify_lang_activity_cancelled(canceled_state, None, Canceled::default()),
        }
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledEventRecorded {}

impl ScheduledEventRecorded {
    pub(super) fn on_task_started(
        self,
        dat: SharedState,
        started_event_id: i64,
    ) -> ActivityMachineTransition<Started> {
        ActivityMachineTransition::ok_shared(
            vec![],
            Started::default(),
            SharedState {
                started_event_id,
                ..dat
            },
        )
    }
    pub(super) fn on_task_timed_out(
        self,
        dat: SharedState,
        attrs: ActivityTaskTimedOutEventAttributes,
    ) -> ActivityMachineTransition<TimedOut> {
        notify_lang_activity_timed_out(dat, attrs)
    }
    pub(super) fn on_canceled(
        self,
        dat: SharedState,
    ) -> ActivityMachineTransition<ScheduledActivityCancelCommandCreated> {
        create_request_cancel_activity_task_command(
            dat,
            ScheduledActivityCancelCommandCreated::default(),
        )
    }
    pub(super) fn on_abandoned(self, dat: SharedState) -> ActivityMachineTransition<Canceled> {
        notify_lang_activity_cancelled(dat, None, Canceled::default())
    }
}

#[derive(Default, Clone)]
pub(super) struct Started {}

impl Started {
    pub(super) fn on_activity_task_completed(
        self,
        attrs: ActivityTaskCompletedEventAttributes,
    ) -> ActivityMachineTransition<Completed> {
        ActivityMachineTransition::ok(
            vec![ActivityMachineCommand::Complete(attrs.result)],
            Completed::default(),
        )
    }
    pub(super) fn on_activity_task_failed(
        self,
        attrs: ActivityTaskFailedEventAttributes,
    ) -> ActivityMachineTransition<Failed> {
        ActivityMachineTransition::ok(
            vec![ActivityMachineCommand::Fail(attrs.failure)],
            Failed::default(),
        )
    }
    pub(super) fn on_activity_task_timed_out(
        self,
        dat: SharedState,
        attrs: ActivityTaskTimedOutEventAttributes,
    ) -> ActivityMachineTransition<TimedOut> {
        notify_lang_activity_timed_out(dat, attrs)
    }
    pub(super) fn on_canceled(
        self,
        dat: SharedState,
    ) -> ActivityMachineTransition<StartedActivityCancelCommandCreated> {
        create_request_cancel_activity_task_command(
            dat,
            StartedActivityCancelCommandCreated::default(),
        )
    }
    pub(super) fn on_abandoned(self, dat: SharedState) -> ActivityMachineTransition<Canceled> {
        notify_lang_activity_cancelled(dat, None, Canceled::default())
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledActivityCancelCommandCreated {}

impl ScheduledActivityCancelCommandCreated {
    pub(super) fn on_command_request_cancel_activity_task(
        self,
        dat: SharedState,
    ) -> ActivityMachineTransition<ScheduledActivityCancelCommandCreated> {
        match dat.cancellation_type {
            ActivityCancellationType::Abandon => unreachable!(
                "Cancellations with type Abandon should go into terminal state immediately."
            ),
            /// We don't need to notify lang side here as we've already unblocked it by calling `machine_responses_from_cancel_request`
            /// when `RequestCancellation` has been created for `TryCancel`, while `WaitCancellationCompleted`
            /// shouldn't unblock lang side until `ActivityTaskCanceled` event has been received.
            _ => ActivityMachineTransition::default(),
        }
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledActivityCancelEventRecorded {}

impl ScheduledActivityCancelEventRecorded {
    pub(super) fn on_activity_task_canceled(
        self,
        dat: SharedState,
        attrs: ActivityTaskCanceledEventAttributes,
    ) -> ActivityMachineTransition<Canceled> {
        match dat.cancellation_type {
            /// At this point if we are in TryCancel mode, we've already sent a cancellation failure
            /// to lang unblocking it, so there is no need to send another activation.
            ActivityCancellationType::TryCancel => ActivityMachineTransition::default(),
            ActivityCancellationType::WaitCancellationCompleted => {
                notify_lang_activity_cancelled(dat, Some(attrs), Canceled::default())
            }
            /// Abandon results in going into Cancelled immediately, so we should never reach this state.
            ActivityCancellationType::Abandon => unreachable!(
                "Cancellations with type Abandon should go into terminal state immediately."
            ),
        }
    }

    pub(super) fn on_activity_task_timed_out(
        self,
        dat: SharedState,
        attrs: ActivityTaskTimedOutEventAttributes,
    ) -> ActivityMachineTransition<TimedOut> {
        match dat.cancellation_type {
            /// At this point if we are in TryCancel mode, we've already sent a cancellation failure
            /// to lang unblocking it, so there is no need to send another activation for the timeout.
            ActivityCancellationType::TryCancel => ActivityMachineTransition::default(),
            ActivityCancellationType::WaitCancellationCompleted => {
                notify_lang_activity_timed_out(dat, attrs)
            }
            /// Abandon results in going into Cancelled immediately, so we should never reach this state.
            ActivityCancellationType::Abandon => unreachable!(
                "Cancellations with type Abandon should go into terminal state immediately."
            ),
        }
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
        dat: SharedState,
    ) -> ActivityMachineTransition<StartedActivityCancelEventRecorded> {
        match dat.cancellation_type {
            ActivityCancellationType::TryCancel => notify_lang_activity_cancelled(
                dat,
                None,
                StartedActivityCancelEventRecorded::default(),
            ),
            ActivityCancellationType::WaitCancellationCompleted => {
                ActivityMachineTransition::default()
            }
            ActivityCancellationType::Abandon => unreachable!(
                "Cancellations with type Abandon should go into terminal state immediately."
            ),
        }
    }
}

#[derive(Default, Clone)]
pub(super) struct StartedActivityCancelEventRecorded {}

impl StartedActivityCancelEventRecorded {
    pub(super) fn on_activity_task_completed(
        self,
        attrs: ActivityTaskCompletedEventAttributes,
    ) -> ActivityMachineTransition<Completed> {
        ActivityMachineTransition::ok(
            vec![ActivityMachineCommand::Complete(attrs.result)],
            Completed::default(),
        )
    }
    pub(super) fn on_activity_task_failed(
        self,
        attrs: ActivityTaskFailedEventAttributes,
    ) -> ActivityMachineTransition<Failed> {
        ActivityMachineTransition::ok(
            vec![ActivityMachineCommand::Fail(attrs.failure)],
            Failed::default(),
        )
    }
    pub(super) fn on_activity_task_timed_out(
        self,
        dat: SharedState,
        attrs: ActivityTaskTimedOutEventAttributes,
    ) -> ActivityMachineTransition<TimedOut> {
        notify_lang_activity_timed_out(dat, attrs)
    }
    pub(super) fn on_activity_task_canceled(
        self,
        dat: SharedState,
        attrs: ActivityTaskCanceledEventAttributes,
    ) -> ActivityMachineTransition<Canceled> {
        match dat.cancellation_type {
            ActivityCancellationType::WaitCancellationCompleted => {
                notify_lang_activity_cancelled(dat, Some(attrs), Canceled::default())
            }
            ActivityCancellationType::TryCancel => {
                ActivityMachineTransition::ok(vec![], Canceled::default())
            }
            ActivityCancellationType::Abandon => unreachable!(
                "Cancellations with type Abandon should go into terminal state immediately."
            ),
        }
    }
}

impl From<ScheduledActivityCancelEventRecorded> for StartedActivityCancelEventRecorded {
    fn from(_: ScheduledActivityCancelEventRecorded) -> Self {
        Self::default()
    }
}

impl From<ScheduledEventRecorded> for Canceled {
    fn from(_: ScheduledEventRecorded) -> Self {
        Self::default()
    }
}

impl From<Started> for Canceled {
    fn from(_: Started) -> Self {
        Self::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Completed {}

#[derive(Default, Clone)]
pub(super) struct Failed {}

impl From<ScheduledEventRecorded> for TimedOut {
    fn from(_: ScheduledEventRecorded) -> Self {
        Self::default()
    }
}

impl From<Started> for TimedOut {
    fn from(_: Started) -> Self {
        Self::default()
    }
}

impl From<ScheduledActivityCancelEventRecorded> for TimedOut {
    fn from(_: ScheduledActivityCancelEventRecorded) -> Self {
        Self::default()
    }
}

impl From<StartedActivityCancelEventRecorded> for TimedOut {
    fn from(_: StartedActivityCancelEventRecorded) -> Self {
        Self::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct TimedOut {}

#[derive(Default, Clone)]
pub(super) struct Canceled {}

fn create_request_cancel_activity_task_command<S>(
    dat: SharedState,
    next_state: S,
) -> ActivityMachineTransition<S>
where
    S: Into<ActivityMachineState>,
{
    let cmd = Command {
        command_type: CommandType::RequestCancelActivityTask as i32,
        attributes: Some(
            RequestCancelActivity {
                scheduled_event_id: dat.scheduled_event_id,
                activity_id: dat.attrs.activity_id,
            }
            .into(),
        ),
    };
    ActivityMachineTransition::ok(
        vec![ActivityMachineCommand::RequestCancellation(cmd)],
        next_state,
    )
}

/// Notifies lang side that activity has timed out by sending a failure with timeout error as a cause.
/// State machine will transition into the TimedOut state.
fn notify_lang_activity_timed_out(
    dat: SharedState,
    attrs: ActivityTaskTimedOutEventAttributes,
) -> TransitionResult<ActivityMachine, TimedOut> {
    ActivityMachineTransition::ok_shared(
        vec![ActivityMachineCommand::Fail(Some(new_timeout_failure(
            &dat, attrs,
        )))],
        TimedOut::default(),
        dat,
    )
}

/// Notifies lang side that activity has been cancelled by sending a failure with cancelled failure
/// as a cause. Optional cancelled_event, if passed, is used to supply event IDs. State machine will
/// transition into the `next_state` provided as a parameter.
fn notify_lang_activity_cancelled<S>(
    dat: SharedState,
    canceled_event: Option<ActivityTaskCanceledEventAttributes>,
    next_state: S,
) -> ActivityMachineTransition<S>
where
    S: Into<ActivityMachineState>,
{
    ActivityMachineTransition::ok_shared(
        vec![ActivityMachineCommand::Cancel(
            canceled_event.map(|e| e.details).flatten(),
        )],
        next_state,
        dat,
    )
}

fn new_timeout_failure(dat: &SharedState, attrs: ActivityTaskTimedOutEventAttributes) -> Failure {
    let failure_info = ActivityFailureInfo {
        activity_id: dat.attrs.activity_id.to_string(),
        activity_type: Some(ActivityType {
            name: dat.attrs.activity_type.to_string(),
        }),
        scheduled_event_id: attrs.scheduled_event_id,
        started_event_id: attrs.started_event_id,
        identity: "workflow".to_string(),
        retry_state: attrs.retry_state,
    };
    Failure {
        message: "Activity task timed out".to_string(),
        cause: attrs.failure.map(Box::new),
        failure_info: Some(failure::FailureInfo::ActivityFailureInfo(failure_info)),
        ..Default::default()
    }
}

fn convert_payloads(
    event: &HistoryEvent,
    result: Option<Payloads>,
) -> Result<Option<Payload>, WFMachinesError> {
    result
        .map(TryInto::try_into)
        .transpose()
        .map_err(|pe| WFMachinesError::NotExactlyOnePayload(pe, event.clone()))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::protos::coresdk::workflow_activation::WfActivation;
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
        let twd = activity_workflow_driver("activity-id-1");
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
        let twd = activity_workflow_driver("activity-id-1");
        let t = canned_histories::single_failed_activity("activity-id-1");
        let state_machines = WorkflowMachines::new(
            "wfid".to_string(),
            "runid".to_string(),
            Box::new(twd).into(),
        );

        assert_eq!(2, t.as_history().get_workflow_task_count(None).unwrap());
        (t, state_machines)
    }

    fn activity_workflow_driver(activity_id: &'static str) -> TestWorkflowDriver {
        TestWorkflowDriver::new(|mut command_sink: CommandSender| async move {
            let activity = ScheduleActivity {
                activity_id: activity_id.to_string(),
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

    #[test]
    fn immediate_activity_cancelation() {
        let twd = TestWorkflowDriver::new(|mut cmd_sink: CommandSender| async move {
            let cancel_activity_future = cmd_sink.activity(ScheduleActivity {
                activity_id: "activity-id-1".to_string(),
                ..Default::default()
            });
            // Immediately cancel the activity
            cmd_sink.cancel_activity("activity-id-1");
            cancel_activity_future.await;

            let complete = CompleteWorkflowExecution::default();
            cmd_sink.send(complete.into());
        });

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let mut state_machines = WorkflowMachines::new(
            "wfid".to_string(),
            "runid".to_string(),
            Box::new(twd).into(),
        );

        let commands = t
            .handle_workflow_task_take_cmds(&mut state_machines, None)
            .unwrap();
        assert_eq!(commands.len(), 0);
        let activation = state_machines.get_wf_activation().unwrap();
        assert_matches!(
            activation.jobs.as_slice(),
            [
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::StartWorkflow(_)),
                },
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::ResolveActivity(
                        ResolveActivity {
                            activity_id,
                            result: Some(ActivityResult {
                                status: Some(activity_result::Status::Canceled(_))
                            })
                        }
                    )),
                },
            ]
        )
    }
}
