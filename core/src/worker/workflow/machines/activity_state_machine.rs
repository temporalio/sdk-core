#![allow(clippy::large_enum_variant)]

use super::{
    workflow_machines::MachineResponse, Cancellable, EventInfo, NewMachineWithCommand,
    OnEventWrapper, WFMachinesAdapter, WFMachinesError,
};
use crate::{
    internal_flags::CoreInternalFlags,
    worker::workflow::{machines::HistEventData, InternalFlagsRef},
};
use rustfsm::{fsm, MachineError, StateMachine, TransitionResult};
use std::convert::{TryFrom, TryInto};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{self as ar, activity_resolution, ActivityResolution, Cancellation},
        workflow_activation::ResolveActivity,
        workflow_commands::{ActivityCancellationType, ScheduleActivity},
    },
    temporal::api::{
        command::v1::{
            command, schedule_activity_cmd_to_api, Command,
            RequestCancelActivityTaskCommandAttributes,
        },
        common::v1::{ActivityType, Payload, Payloads},
        enums::v1::{CommandType, EventType, RetryState},
        failure::v1::{failure::FailureInfo, ActivityFailureInfo, CanceledFailureInfo, Failure},
        history::v1::{
            history_event, ActivityTaskCanceledEventAttributes,
            ActivityTaskCompletedEventAttributes, ActivityTaskFailedEventAttributes,
            ActivityTaskTimedOutEventAttributes, HistoryEvent,
        },
    },
};

fsm! {
    pub(super) name ActivityMachine;
    command ActivityMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(Schedule, on_schedule)--> ScheduleCommandCreated;

    ScheduleCommandCreated --(CommandScheduleActivityTask) --> ScheduleCommandCreated;
    ScheduleCommandCreated --(ActivityTaskScheduled(ActTaskScheduledData),
        shared on_activity_task_scheduled) --> ScheduledEventRecorded;
    ScheduleCommandCreated --(Cancel, shared on_canceled) --> Canceled;

    ScheduledEventRecorded --(ActivityTaskStarted(i64), shared on_task_started) --> Started;
    ScheduledEventRecorded --(ActivityTaskTimedOut(ActivityTaskTimedOutEventAttributes),
        shared on_task_timed_out) --> TimedOut;
    ScheduledEventRecorded --(Cancel, shared on_canceled) --> ScheduledActivityCancelCommandCreated;
    ScheduledEventRecorded --(Abandon, on_abandoned) --> Canceled;

    Started --(ActivityTaskCompleted(ActivityTaskCompletedEventAttributes),
        on_activity_task_completed) --> Completed;
    Started --(ActivityTaskFailed(ActivityTaskFailedEventAttributes),
        shared on_activity_task_failed) --> Failed;
    Started --(ActivityTaskTimedOut(ActivityTaskTimedOutEventAttributes),
        shared on_activity_task_timed_out) --> TimedOut;
    Started --(Cancel, shared on_canceled) --> StartedActivityCancelCommandCreated;
    Started --(Abandon, on_abandoned) --> Canceled;

    ScheduledActivityCancelCommandCreated --(CommandRequestCancelActivityTask) --> ScheduledActivityCancelCommandCreated;
    ScheduledActivityCancelCommandCreated --(ActivityTaskCancelRequested) --> ScheduledActivityCancelEventRecorded;

    ScheduledActivityCancelEventRecorded --(ActivityTaskCanceled(ActivityTaskCanceledEventAttributes),
        shared on_activity_task_canceled) --> Canceled;
    ScheduledActivityCancelEventRecorded --(ActivityTaskStarted(i64)) --> StartedActivityCancelEventRecorded;
    ScheduledActivityCancelEventRecorded --(ActivityTaskTimedOut(ActivityTaskTimedOutEventAttributes),
        shared on_activity_task_timed_out) --> TimedOut;

    StartedActivityCancelCommandCreated --(CommandRequestCancelActivityTask) --> StartedActivityCancelCommandCreated;
    StartedActivityCancelCommandCreated --(ActivityTaskCancelRequested) --> StartedActivityCancelEventRecorded;

    StartedActivityCancelEventRecorded --(ActivityTaskFailed(ActivityTaskFailedEventAttributes),
        shared on_activity_task_failed) --> Failed;
    StartedActivityCancelEventRecorded --(ActivityTaskCompleted(ActivityTaskCompletedEventAttributes),
        shared on_activity_task_completed) --> Completed;
    StartedActivityCancelEventRecorded --(ActivityTaskTimedOut(ActivityTaskTimedOutEventAttributes),
        shared on_activity_task_timed_out) --> TimedOut;
    StartedActivityCancelEventRecorded --(ActivityTaskCanceled(ActivityTaskCanceledEventAttributes),
        shared on_activity_task_canceled) --> Canceled;

    Canceled --(ActivityTaskStarted(i64), shared on_activity_task_started) --> Canceled;
    Canceled --(ActivityTaskCompleted(ActivityTaskCompletedEventAttributes),
        shared on_activity_task_completed) --> Canceled;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum ActivityMachineCommand {
    #[display(fmt = "Complete")]
    Complete(Option<Payloads>),
    #[display(fmt = "Fail")]
    Fail(Failure),
    #[display(fmt = "Cancel")]
    Cancel(Option<ActivityTaskCanceledEventAttributes>),
    #[display(fmt = "RequestCancellation")]
    RequestCancellation(Command),
}

pub(super) struct ActTaskScheduledData {
    event_id: i64,
    act_type: String,
    act_id: String,
    last_task_in_history: bool,
}

impl ActivityMachine {
    /// Create a new activity and immediately schedule it.
    pub(super) fn new_scheduled(
        attrs: ScheduleActivity,
        internal_flags: InternalFlagsRef,
        use_compatible_version: bool,
    ) -> NewMachineWithCommand {
        let mut s = Self::from_parts(
            Created {}.into(),
            SharedState {
                cancellation_type: ActivityCancellationType::from_i32(attrs.cancellation_type)
                    .unwrap(),
                attrs,
                internal_flags,
                scheduled_event_id: 0,
                started_event_id: 0,
                cancelled_before_sent: false,
            },
        );
        OnEventWrapper::on_event_mut(&mut s, ActivityMachineEvents::Schedule)
            .expect("Scheduling activities doesn't fail");
        let command = Command {
            command_type: CommandType::ScheduleActivityTask as i32,
            attributes: Some(schedule_activity_cmd_to_api(
                s.shared_state().attrs.clone(),
                use_compatible_version,
            )),
        };
        NewMachineWithCommand {
            command,
            machine: s.into(),
        }
    }

    fn machine_responses_from_cancel_request(&self, cancel_cmd: Command) -> Vec<MachineResponse> {
        let mut r = vec![MachineResponse::IssueNewCommand(cancel_cmd)];
        if self.shared_state.cancellation_type
            != ActivityCancellationType::WaitCancellationCompleted
        {
            r.push(MachineResponse::PushWFJob(
                self.create_cancelation_resolve(None).into(),
            ));
        }
        r
    }

    fn create_cancelation_resolve(
        &self,
        attrs: Option<ActivityTaskCanceledEventAttributes>,
    ) -> ResolveActivity {
        let attrs = attrs.unwrap_or_default();
        ResolveActivity {
            seq: self.shared_state.attrs.seq,
            result: Some(ActivityResolution {
                status: Some(activity_resolution::Status::Cancelled(Cancellation {
                    failure: Some(new_cancel_failure(&self.shared_state, attrs)),
                })),
            }),
        }
    }
}

impl TryFrom<HistEventData> for ActivityMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let last_task_in_history = e.current_task_is_last_in_history;
        let e = e.event;
        Ok(match e.event_type() {
            EventType::ActivityTaskScheduled => {
                if let Some(history_event::Attributes::ActivityTaskScheduledEventAttributes(
                    attrs,
                )) = e.attributes
                {
                    Self::ActivityTaskScheduled(ActTaskScheduledData {
                        event_id: e.event_id,
                        act_id: attrs.activity_id,
                        act_type: attrs.activity_type.unwrap_or_default().name,
                        last_task_in_history,
                    })
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "Activity scheduled attributes were unset: {e}"
                    )));
                }
            }
            EventType::ActivityTaskStarted => Self::ActivityTaskStarted(e.event_id),
            EventType::ActivityTaskCompleted => {
                if let Some(history_event::Attributes::ActivityTaskCompletedEventAttributes(
                    attrs,
                )) = e.attributes
                {
                    Self::ActivityTaskCompleted(attrs)
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "Activity completion attributes were unset: {e}"
                    )));
                }
            }
            EventType::ActivityTaskFailed => {
                if let Some(history_event::Attributes::ActivityTaskFailedEventAttributes(attrs)) =
                    e.attributes
                {
                    Self::ActivityTaskFailed(attrs)
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "Activity failure attributes were unset: {e}"
                    )));
                }
            }
            EventType::ActivityTaskTimedOut => {
                if let Some(history_event::Attributes::ActivityTaskTimedOutEventAttributes(attrs)) =
                    e.attributes
                {
                    Self::ActivityTaskTimedOut(attrs)
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "Activity timeout attributes were unset: {e}"
                    )));
                }
            }
            EventType::ActivityTaskCancelRequested => Self::ActivityTaskCancelRequested,
            EventType::ActivityTaskCanceled => {
                if let Some(history_event::Attributes::ActivityTaskCanceledEventAttributes(attrs)) =
                    e.attributes
                {
                    Self::ActivityTaskCanceled(attrs)
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "Activity cancellation attributes were unset: {e}"
                    )));
                }
            }
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Activity machine does not handle this event: {e}"
                )))
            }
        })
    }
}

impl WFMachinesAdapter for ActivityMachine {
    fn adapt_response(
        &self,
        my_command: ActivityMachineCommand,
        event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            ActivityMachineCommand::Complete(result) => {
                vec![ResolveActivity {
                    seq: self.shared_state.attrs.seq,
                    result: Some(ActivityResolution {
                        status: Some(activity_resolution::Status::Completed(ar::Success {
                            result: convert_payloads(event_info, result)?,
                        })),
                    }),
                }
                .into()]
            }
            ActivityMachineCommand::Fail(failure) => {
                vec![ResolveActivity {
                    seq: self.shared_state.attrs.seq,
                    result: Some(ActivityResolution {
                        status: Some(activity_resolution::Status::Failed(ar::Failure {
                            failure: Some(failure),
                        })),
                    }),
                }
                .into()]
            }
            ActivityMachineCommand::RequestCancellation(c) => {
                self.machine_responses_from_cancel_request(c)
            }
            ActivityMachineCommand::Cancel(attrs) => {
                vec![self.create_cancelation_resolve(attrs).into()]
            }
        })
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        matches!(
            event.event_type(),
            EventType::ActivityTaskScheduled
                | EventType::ActivityTaskStarted
                | EventType::ActivityTaskCompleted
                | EventType::ActivityTaskFailed
                | EventType::ActivityTaskTimedOut
                | EventType::ActivityTaskCancelRequested
                | EventType::ActivityTaskCanceled
        )
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
        if matches!(
            self.state(),
            ActivityMachineState::Completed(_)
                | ActivityMachineState::Canceled(_)
                | ActivityMachineState::Failed(_)
                | ActivityMachineState::TimedOut(_)
        ) {
            // Ignore attempted cancels in terminal states
            debug!(
                "Attempted to cancel already resolved activity (seq {})",
                self.shared_state.attrs.seq
            );
            return Ok(vec![]);
        }
        let event = match self.shared_state.cancellation_type {
            ActivityCancellationType::Abandon => ActivityMachineEvents::Abandon,
            _ => ActivityMachineEvents::Cancel,
        };
        let vec = OnEventWrapper::on_event_mut(self, event)?;
        let res = vec
            .into_iter()
            .flat_map(|amc| match amc {
                ActivityMachineCommand::RequestCancellation(cmd) => {
                    self.machine_responses_from_cancel_request(cmd)
                }
                ActivityMachineCommand::Cancel(details) => {
                    vec![self.create_cancelation_resolve(details).into()]
                }
                x => panic!("Invalid cancel event response {x:?}"),
            })
            .collect();
        Ok(res)
    }

    fn was_cancelled_before_sent_to_server(&self) -> bool {
        self.shared_state().cancelled_before_sent
    }
}

#[derive(Clone)]
pub(super) struct SharedState {
    scheduled_event_id: i64,
    started_event_id: i64,
    attrs: ScheduleActivity,
    cancellation_type: ActivityCancellationType,
    cancelled_before_sent: bool,
    internal_flags: InternalFlagsRef,
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
        dat: &mut SharedState,
        sched_dat: ActTaskScheduledData,
    ) -> ActivityMachineTransition<ScheduledEventRecorded> {
        if dat.internal_flags.borrow_mut().try_use(
            CoreInternalFlags::IdAndTypeDeterminismChecks,
            sched_dat.last_task_in_history,
        ) {
            if sched_dat.act_id != dat.attrs.activity_id {
                return TransitionResult::Err(WFMachinesError::Nondeterminism(format!(
                    "Activity id of scheduled event '{}' does not \
                 match activity id of activity command '{}'",
                    sched_dat.act_id, dat.attrs.activity_id
                )));
            }
            if sched_dat.act_type != dat.attrs.activity_type {
                return TransitionResult::Err(WFMachinesError::Nondeterminism(format!(
                    "Activity type of scheduled event '{}' does not \
                 match activity type of activity command '{}'",
                    sched_dat.act_type, dat.attrs.activity_type
                )));
            }
        }
        dat.scheduled_event_id = sched_dat.event_id;
        ActivityMachineTransition::default()
    }

    pub(super) fn on_canceled(self, dat: &mut SharedState) -> ActivityMachineTransition<Canceled> {
        dat.cancelled_before_sent = true;
        match dat.cancellation_type {
            ActivityCancellationType::Abandon => ActivityMachineTransition::default(),
            _ => notify_lang_activity_cancelled(None),
        }
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledEventRecorded {}

impl ScheduledEventRecorded {
    pub(super) fn on_task_started(
        self,
        dat: &mut SharedState,
        started_event_id: i64,
    ) -> ActivityMachineTransition<Started> {
        dat.started_event_id = started_event_id;
        ActivityMachineTransition::default()
    }
    pub(super) fn on_task_timed_out(
        self,
        dat: &mut SharedState,
        attrs: ActivityTaskTimedOutEventAttributes,
    ) -> ActivityMachineTransition<TimedOut> {
        notify_lang_activity_timed_out(dat, attrs)
    }

    pub(super) fn on_canceled(
        self,
        dat: &mut SharedState,
    ) -> ActivityMachineTransition<ScheduledActivityCancelCommandCreated> {
        create_request_cancel_activity_task_command(
            dat,
            ScheduledActivityCancelCommandCreated::default(),
        )
    }
    pub(super) fn on_abandoned(self) -> ActivityMachineTransition<Canceled> {
        notify_lang_activity_cancelled(None)
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
        dat: &mut SharedState,
        attrs: ActivityTaskFailedEventAttributes,
    ) -> ActivityMachineTransition<Failed> {
        ActivityMachineTransition::ok(
            vec![ActivityMachineCommand::Fail(new_failure(dat, attrs))],
            Failed::default(),
        )
    }

    pub(super) fn on_activity_task_timed_out(
        self,
        dat: &mut SharedState,
        attrs: ActivityTaskTimedOutEventAttributes,
    ) -> ActivityMachineTransition<TimedOut> {
        notify_lang_activity_timed_out(dat, attrs)
    }

    pub(super) fn on_canceled(
        self,
        dat: &mut SharedState,
    ) -> ActivityMachineTransition<StartedActivityCancelCommandCreated> {
        create_request_cancel_activity_task_command(
            dat,
            StartedActivityCancelCommandCreated::default(),
        )
    }
    pub(super) fn on_abandoned(self) -> ActivityMachineTransition<Canceled> {
        notify_lang_activity_cancelled(None)
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledActivityCancelCommandCreated {}

#[derive(Default, Clone)]
pub(super) struct ScheduledActivityCancelEventRecorded {}

impl ScheduledActivityCancelEventRecorded {
    pub(super) fn on_activity_task_canceled(
        self,
        dat: &mut SharedState,
        attrs: ActivityTaskCanceledEventAttributes,
    ) -> ActivityMachineTransition<Canceled> {
        notify_if_not_already_cancelled(dat, |_| notify_lang_activity_cancelled(Some(attrs)))
    }

    pub(super) fn on_activity_task_timed_out(
        self,
        dat: &mut SharedState,
        attrs: ActivityTaskTimedOutEventAttributes,
    ) -> ActivityMachineTransition<TimedOut> {
        notify_if_not_already_cancelled(dat, |dat| notify_lang_activity_timed_out(dat, attrs))
    }
}

impl From<ScheduledActivityCancelCommandCreated> for ScheduledActivityCancelEventRecorded {
    fn from(_: ScheduledActivityCancelCommandCreated) -> Self {
        Self::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct StartedActivityCancelCommandCreated {}

#[derive(Default, Clone)]
pub(super) struct StartedActivityCancelEventRecorded {}

impl StartedActivityCancelEventRecorded {
    pub(super) fn on_activity_task_completed(
        self,
        dat: &mut SharedState,
        attrs: ActivityTaskCompletedEventAttributes,
    ) -> ActivityMachineTransition<Completed> {
        notify_if_not_already_cancelled(dat, |_| {
            TransitionResult::commands(vec![ActivityMachineCommand::Complete(attrs.result)])
        })
    }
    pub(super) fn on_activity_task_failed(
        self,
        dat: &mut SharedState,
        attrs: ActivityTaskFailedEventAttributes,
    ) -> ActivityMachineTransition<Failed> {
        notify_if_not_already_cancelled(dat, |dat| {
            TransitionResult::commands(vec![ActivityMachineCommand::Fail(new_failure(dat, attrs))])
        })
    }
    pub(super) fn on_activity_task_timed_out(
        self,
        dat: &mut SharedState,
        attrs: ActivityTaskTimedOutEventAttributes,
    ) -> ActivityMachineTransition<TimedOut> {
        notify_if_not_already_cancelled(dat, |dat| notify_lang_activity_timed_out(dat, attrs))
    }
    pub(super) fn on_activity_task_canceled(
        self,
        dat: &mut SharedState,
        attrs: ActivityTaskCanceledEventAttributes,
    ) -> ActivityMachineTransition<Canceled> {
        notify_if_not_already_cancelled(dat, |_| notify_lang_activity_cancelled(Some(attrs)))
    }
}

fn notify_if_not_already_cancelled<S>(
    dat: &mut SharedState,
    notifier: impl FnOnce(&mut SharedState) -> ActivityMachineTransition<S>,
) -> ActivityMachineTransition<S>
where
    S: Into<ActivityMachineState> + Default,
{
    match &dat.cancellation_type {
        // At this point if we are in TryCancel mode, we've already sent a cancellation failure
        // to lang unblocking it, so there is no need to send another activation.
        ActivityCancellationType::TryCancel => ActivityMachineTransition::default(),
        ActivityCancellationType::WaitCancellationCompleted => notifier(dat),
        // Abandon results in going into Cancelled immediately, so we should never reach this state
        ActivityCancellationType::Abandon => unreachable!(
            "Cancellations with type Abandon should go into terminal state immediately."
        ),
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

impl From<StartedActivityCancelCommandCreated> for StartedActivityCancelEventRecorded {
    fn from(_: StartedActivityCancelCommandCreated) -> Self {
        Self::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct TimedOut {}

#[derive(Default, Clone)]
pub(super) struct Canceled {}
impl Canceled {
    pub(super) fn on_activity_task_started(
        self,
        dat: &mut SharedState,
        seq_num: i64,
    ) -> ActivityMachineTransition<Canceled> {
        // Abandoned activities might start anyway. Ignore the result.
        if dat.cancellation_type == ActivityCancellationType::Abandon {
            TransitionResult::default()
        } else {
            TransitionResult::Err(WFMachinesError::Nondeterminism(format!(
                "Non-Abandon cancel mode activities cannot be started after being cancelled. \
                 Seq: {seq_num:?}"
            )))
        }
    }
    pub(super) fn on_activity_task_completed(
        self,
        dat: &mut SharedState,
        attrs: ActivityTaskCompletedEventAttributes,
    ) -> ActivityMachineTransition<Canceled> {
        // Abandoned activities might complete anyway. Ignore the result.
        if dat.cancellation_type == ActivityCancellationType::Abandon {
            TransitionResult::default()
        } else {
            TransitionResult::Err(WFMachinesError::Nondeterminism(format!(
                "Non-Abandon cancel mode activities cannot be completed after being cancelled: {attrs:?}"
            )))
        }
    }
}

fn create_request_cancel_activity_task_command<S>(
    dat: &mut SharedState,
    next_state: S,
) -> ActivityMachineTransition<S>
where
    S: Into<ActivityMachineState>,
{
    let cmd = Command {
        command_type: CommandType::RequestCancelActivityTask as i32,
        attributes: Some(
            command::Attributes::RequestCancelActivityTaskCommandAttributes(
                RequestCancelActivityTaskCommandAttributes {
                    scheduled_event_id: dat.scheduled_event_id,
                },
            ),
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
    dat: &SharedState,
    attrs: ActivityTaskTimedOutEventAttributes,
) -> TransitionResult<ActivityMachine, TimedOut> {
    ActivityMachineTransition::commands(vec![ActivityMachineCommand::Fail(new_timeout_failure(
        dat, attrs,
    ))])
}

/// Returns the transition to indicate activity has been cancelled by sending a failure with
/// cancelled failure as a cause. Optional cancelled_event, if passed, is used to supply event IDs.
fn notify_lang_activity_cancelled(
    canceled_event: Option<ActivityTaskCanceledEventAttributes>,
) -> ActivityMachineTransition<Canceled> {
    ActivityMachineTransition::commands(vec![ActivityMachineCommand::Cancel(canceled_event)])
}

fn new_failure(dat: &SharedState, attrs: ActivityTaskFailedEventAttributes) -> Failure {
    let rs = attrs.retry_state();
    Failure {
        message: "Activity task failed".to_owned(),
        cause: attrs.failure.map(Box::new),
        failure_info: Some(activity_fail_info(
            dat.attrs.activity_type.clone(),
            dat.attrs.activity_id.clone(),
            Some(attrs.identity),
            rs,
            attrs.started_event_id,
            attrs.scheduled_event_id,
        )),
        ..Default::default()
    }
}

fn new_timeout_failure(dat: &SharedState, attrs: ActivityTaskTimedOutEventAttributes) -> Failure {
    let rs = attrs.retry_state();
    Failure {
        message: "Activity task timed out".to_string(),
        cause: attrs.failure.map(Box::new),
        failure_info: Some(activity_fail_info(
            dat.attrs.activity_type.clone(),
            dat.attrs.activity_id.clone(),
            None,
            rs,
            attrs.started_event_id,
            attrs.scheduled_event_id,
        )),
        ..Default::default()
    }
}

fn new_cancel_failure(dat: &SharedState, attrs: ActivityTaskCanceledEventAttributes) -> Failure {
    Failure {
        message: "Activity cancelled".to_string(),
        cause: Some(Box::from(Failure {
            failure_info: Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
                details: attrs.details.map(Into::into),
            })),
            ..Default::default()
        })),
        failure_info: Some(activity_fail_info(
            dat.attrs.activity_type.clone(),
            dat.attrs.activity_id.clone(),
            Some(attrs.identity),
            RetryState::CancelRequested,
            attrs.started_event_id,
            attrs.scheduled_event_id,
        )),
        ..Default::default()
    }
}

pub fn activity_fail_info(
    act_type: String,
    act_id: String,
    identity: Option<String>,
    retry_state: RetryState,
    started_event_id: i64,
    scheduled_event_id: i64,
) -> FailureInfo {
    FailureInfo::ActivityFailureInfo(ActivityFailureInfo {
        identity: identity.unwrap_or_default(),
        activity_type: Some(ActivityType { name: act_type }),
        activity_id: act_id,
        retry_state: retry_state as i32,
        started_event_id,
        scheduled_event_id,
    })
}

fn convert_payloads(
    event_info: Option<EventInfo>,
    result: Option<Payloads>,
) -> Result<Option<Payload>, WFMachinesError> {
    result.map(TryInto::try_into).transpose().map_err(|pe| {
        WFMachinesError::Fatal(format!(
            "Not exactly one payload in activity result ({pe}) for event: {event_info:?}"
        ))
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        internal_flags::InternalFlags, replay::TestHistoryBuilder, test_help::canned_histories,
        worker::workflow::ManagedWFFunc,
    };
    use rstest::{fixture, rstest};
    use std::{cell::RefCell, mem::discriminant, rc::Rc};
    use temporal_sdk::{
        ActivityOptions, CancellableFuture, WfContext, WorkflowFunction, WorkflowResult,
    };
    use temporal_sdk_core_protos::{
        coresdk::workflow_activation::{workflow_activation_job, WorkflowActivationJob},
        DEFAULT_ACTIVITY_TYPE,
    };

    #[fixture]
    fn activity_happy_hist() -> ManagedWFFunc {
        let func = WorkflowFunction::new(activity_wf);
        let t = canned_histories::single_activity("activity-id-1");
        assert_eq!(2, t.get_full_history_info().unwrap().wf_task_count());
        ManagedWFFunc::new(t, func, vec![])
    }

    #[fixture]
    fn activity_failure_hist() -> ManagedWFFunc {
        let func = WorkflowFunction::new(activity_wf);
        let t = canned_histories::single_failed_activity("activity-id-1");
        assert_eq!(2, t.get_full_history_info().unwrap().wf_task_count());
        ManagedWFFunc::new(t, func, vec![])
    }

    async fn activity_wf(command_sink: WfContext) -> WorkflowResult<()> {
        command_sink
            .activity(ActivityOptions {
                activity_id: Some("activity-id-1".to_string()),
                activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
                ..Default::default()
            })
            .await;
        Ok(().into())
    }

    #[rstest(
        wfm,
        case::success(activity_happy_hist()),
        case::failure(activity_failure_hist())
    )]
    #[tokio::test]
    async fn single_activity_inc(mut wfm: ManagedWFFunc) {
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::ScheduleActivityTask as i32
        );

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
        wfm.shutdown().await.unwrap();
    }

    #[rstest(
        wfm,
        case::success(activity_happy_hist()),
        case::failure(activity_failure_hist())
    )]
    #[tokio::test]
    async fn single_activity_full(mut wfm: ManagedWFFunc) {
        wfm.process_all_activations().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
        wfm.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn immediate_activity_cancelation() {
        let func = WorkflowFunction::new(|ctx: WfContext| async move {
            let cancel_activity_future = ctx.activity(ActivityOptions::default());
            // Immediately cancel the activity
            cancel_activity_future.cancel(&ctx);
            cancel_activity_future.await;
            Ok(().into())
        });

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();
        let mut wfm = ManagedWFFunc::new(t, func, vec![]);

        let activation = wfm.process_all_activations().await.unwrap();
        wfm.get_server_commands();
        assert_matches!(
            activation.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        result: Some(ActivityResolution {
                            status: Some(activity_resolution::Status::Cancelled(_))
                        }),
                        ..
                    }
                )),
            },]
        );
        wfm.shutdown().await.unwrap();
    }

    #[test]
    fn cancels_ignored_terminal() {
        for state in [
            ActivityMachineState::Canceled(Canceled {}),
            Failed {}.into(),
            TimedOut {}.into(),
            Completed {}.into(),
        ] {
            let mut s = ActivityMachine::from_parts(
                state.clone(),
                SharedState {
                    scheduled_event_id: 0,
                    started_event_id: 0,
                    attrs: Default::default(),
                    cancellation_type: Default::default(),
                    cancelled_before_sent: false,
                    internal_flags: Rc::new(RefCell::new(InternalFlags::new(&Default::default()))),
                },
            );
            let cmds = s.cancel().unwrap();
            assert_eq!(cmds.len(), 0);
            assert_eq!(discriminant(&state), discriminant(s.state()));
        }
    }
}
