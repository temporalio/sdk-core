#![allow(clippy::large_enum_variant)]

use crate::{
    machines::{
        workflow_machines::MachineResponse, Cancellable, NewMachineWithCommand, OnEventWrapper,
        WFMachinesAdapter, WFMachinesError,
    },
    protos::{
        coresdk::{
            activity_result::{self as ar, activity_result, ActivityResult},
            common::Payload,
            workflow_activation::ResolveActivity,
            workflow_commands::{
                ActivityCancellationType, RequestCancelActivity, ScheduleActivity,
            },
        },
        temporal::api::{
            command::v1::Command,
            common::v1::{ActivityType, Payloads},
            enums::v1::{CommandType, EventType},
            failure::v1::{
                failure::{self},
                ActivityFailureInfo, Failure,
            },
            history::v1::{
                history_event, ActivityTaskCanceledEventAttributes,
                ActivityTaskCompletedEventAttributes, ActivityTaskFailedEventAttributes,
                ActivityTaskTimedOutEventAttributes, HistoryEvent,
            },
        },
    },
};
use rustfsm::{fsm, MachineError, StateMachine, TransitionResult};
use std::convert::{TryFrom, TryInto};

fsm! {
    pub(super) name ActivityMachine;
    command ActivityMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(Schedule, on_schedule)--> ScheduleCommandCreated;

    ScheduleCommandCreated --(CommandScheduleActivityTask) --> ScheduleCommandCreated;
    ScheduleCommandCreated --(ActivityTaskScheduled(i64),
        shared on_activity_task_scheduled) --> ScheduledEventRecorded;
    ScheduleCommandCreated --(Cancel, shared on_canceled) --> Canceled;

    ScheduledEventRecorded --(ActivityTaskStarted(i64), shared on_task_started) --> Started;
    ScheduledEventRecorded --(ActivityTaskTimedOut(ActivityTaskTimedOutEventAttributes),
        shared on_task_timed_out) --> TimedOut;
    ScheduledEventRecorded --(Cancel, shared on_canceled) --> ScheduledActivityCancelCommandCreated;
    ScheduledEventRecorded --(Abandon, shared on_abandoned) --> Canceled;

    Started --(ActivityTaskCompleted(ActivityTaskCompletedEventAttributes),
        on_activity_task_completed) --> Completed;
    Started --(ActivityTaskFailed(ActivityTaskFailedEventAttributes),
        on_activity_task_failed) --> Failed;
    Started --(ActivityTaskTimedOut(ActivityTaskTimedOutEventAttributes),
        shared on_activity_task_timed_out) --> TimedOut;
    Started --(Cancel, shared on_canceled) --> StartedActivityCancelCommandCreated;
    Started --(Abandon, shared on_abandoned) --> Canceled;

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
        OnEventWrapper::on_event_mut(&mut s, ActivityMachineEvents::Schedule)
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
            r.push(self.create_cancelation_resolve(None).into())
        }
        r
    }

    fn create_cancelation_resolve(&self, details: Option<Payload>) -> ResolveActivity {
        ResolveActivity {
            activity_id: self.shared_state.attrs.activity_id.clone(),
            result: Some(ActivityResult {
                status: Some(activity_result::Status::Canceled(ar::Cancelation {
                    details,
                })),
            }),
        }
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
                            result: convert_payloads(event.clone(), result)?,
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
                vec![self
                    .create_cancelation_resolve(convert_payloads(event.clone(), details)?)
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
        let vec = OnEventWrapper::on_event_mut(self, event)?;
        let res = vec
            .into_iter()
            .flat_map(|amc| match amc {
                ActivityMachineCommand::RequestCancellation(cmd) => {
                    self.machine_responses_from_cancel_request(cmd)
                }
                ActivityMachineCommand::Cancel(_details) => {
                    // TODO: Convert payloads
                    vec![self.create_cancelation_resolve(None).into()]
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
            _ => notify_lang_activity_cancelled(canceled_state, None),
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
        notify_lang_activity_cancelled(dat, None)
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
        notify_lang_activity_cancelled(dat, None)
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledActivityCancelCommandCreated {}

#[derive(Default, Clone)]
pub(super) struct ScheduledActivityCancelEventRecorded {}

impl ScheduledActivityCancelEventRecorded {
    pub(super) fn on_activity_task_canceled(
        self,
        dat: SharedState,
        attrs: ActivityTaskCanceledEventAttributes,
    ) -> ActivityMachineTransition<Canceled> {
        notify_if_not_already_cancelled(dat, |dat| notify_lang_activity_cancelled(dat, Some(attrs)))
    }

    pub(super) fn on_activity_task_timed_out(
        self,
        dat: SharedState,
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
        dat: SharedState,
        attrs: ActivityTaskCompletedEventAttributes,
    ) -> ActivityMachineTransition<Completed> {
        notify_if_not_already_cancelled(dat, |_| {
            TransitionResult::commands(vec![ActivityMachineCommand::Complete(attrs.result)])
        })
    }
    pub(super) fn on_activity_task_failed(
        self,
        dat: SharedState,
        attrs: ActivityTaskFailedEventAttributes,
    ) -> ActivityMachineTransition<Failed> {
        notify_if_not_already_cancelled(dat, |_| {
            TransitionResult::commands(vec![ActivityMachineCommand::Fail(attrs.failure)])
        })
    }
    pub(super) fn on_activity_task_timed_out(
        self,
        dat: SharedState,
        attrs: ActivityTaskTimedOutEventAttributes,
    ) -> ActivityMachineTransition<TimedOut> {
        notify_if_not_already_cancelled(dat, |dat| notify_lang_activity_timed_out(dat, attrs))
    }
    pub(super) fn on_activity_task_canceled(
        self,
        dat: SharedState,
        attrs: ActivityTaskCanceledEventAttributes,
    ) -> ActivityMachineTransition<Canceled> {
        notify_if_not_already_cancelled(dat, |dat| notify_lang_activity_cancelled(dat, Some(attrs)))
    }
}

fn notify_if_not_already_cancelled<S>(
    dat: SharedState,
    notifier: impl FnOnce(SharedState) -> ActivityMachineTransition<S>,
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
fn notify_lang_activity_cancelled(
    dat: SharedState,
    canceled_event: Option<ActivityTaskCanceledEventAttributes>,
) -> ActivityMachineTransition<Canceled> {
    ActivityMachineTransition::ok_shared(
        vec![ActivityMachineCommand::Cancel(
            canceled_event.map(|e| e.details).flatten(),
        )],
        Canceled::default(),
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
    event: HistoryEvent,
    result: Option<Payloads>,
) -> Result<Option<Payload>, WFMachinesError> {
    result
        .map(TryInto::try_into)
        .transpose()
        .map_err(|pe| WFMachinesError::NotExactlyOnePayload(pe, event))
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        machines::workflow_machines::WorkflowMachines,
        protos::coresdk::workflow_activation::{wf_activation_job, WfActivationJob},
        test_help::{canned_histories, TestHistoryBuilder},
        test_workflow_driver::{TestWorkflowDriver, WfContext},
        workflow::WorkflowManager,
    };
    use rstest::{fixture, rstest};

    #[fixture]
    fn activity_happy_hist() -> (TestHistoryBuilder, WorkflowMachines) {
        let twd = activity_workflow_driver("activity-id-1");
        let t = canned_histories::single_activity("activity-id-1");
        let state_machines = WorkflowMachines::new(
            "wfid".to_string(),
            "runid".to_string(),
            t.as_history_update(),
            Box::new(twd).into(),
        );

        assert_eq!(2, t.get_full_history_info().unwrap().wf_task_count());
        (t, state_machines)
    }

    #[fixture]
    fn activity_failure_hist() -> (TestHistoryBuilder, WorkflowMachines) {
        let twd = activity_workflow_driver("activity-id-1");
        let t = canned_histories::single_failed_activity("activity-id-1");
        let state_machines = WorkflowMachines::new(
            "wfid".to_string(),
            "runid".to_string(),
            t.as_history_update(),
            Box::new(twd).into(),
        );

        assert_eq!(2, t.get_full_history_info().unwrap().wf_task_count());
        (t, state_machines)
    }

    fn activity_workflow_driver(activity_id: &'static str) -> TestWorkflowDriver {
        TestWorkflowDriver::new(vec![], move |mut command_sink: WfContext| async move {
            let activity = ScheduleActivity {
                activity_id: activity_id.to_string(),
                ..Default::default()
            };
            command_sink.activity(activity).await;
            command_sink.complete_workflow_execution();
        })
    }

    #[rstest(
        hist_batches,
        case::success(activity_happy_hist()),
        case::failure(activity_failure_hist())
    )]
    fn single_activity_inc(hist_batches: (TestHistoryBuilder, WorkflowMachines)) {
        let (_, state_machines) = hist_batches;
        let mut wfm = WorkflowManager::new_from_machines(state_machines);

        wfm.get_next_activation().unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::ScheduleActivityTask as i32
        );

        wfm.get_next_activation().unwrap();
        let commands = wfm.get_server_commands().commands;
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
        let (_, state_machines) = hist_batches;
        let mut wfm = WorkflowManager::new_from_machines(state_machines);

        wfm.process_all_activations().unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
    }

    #[test]
    fn immediate_activity_cancelation() {
        let twd = TestWorkflowDriver::new(vec![], |mut cmd_sink: WfContext| async move {
            let cancel_activity_future = cmd_sink.activity(ScheduleActivity {
                activity_id: "activity-id-1".to_string(),
                ..Default::default()
            });
            // Immediately cancel the activity
            cmd_sink.cancel_activity("activity-id-1");
            cancel_activity_future.await;

            cmd_sink.complete_workflow_execution();
        });

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let state_machines = WorkflowMachines::new(
            "wfid".to_string(),
            "runid".to_string(),
            t.as_history_update(),
            Box::new(twd).into(),
        );
        let mut wfm = WorkflowManager::new_from_machines(state_machines);

        let activation = wfm.process_all_activations().unwrap();
        wfm.get_server_commands().commands;
        assert_matches!(
            activation.jobs.as_slice(),
            [
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::StartWorkflow(_)),
                },
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::ResolveActivity(
                        ResolveActivity {
                            result: Some(ActivityResult {
                                status: Some(activity_result::Status::Canceled(_))
                            }),
                            ..
                        }
                    )),
                },
            ]
        )
    }
}
