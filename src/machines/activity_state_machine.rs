#![allow(clippy::large_enum_variant)]

use crate::protos::coresdk::workflow_activation::wf_activation_job::Variant;
use crate::protos::coresdk::workflow_commands::{ActivityCancellationType, RequestCancelActivity};
use crate::protos::coresdk::PayloadsExt;
use crate::protos::temporal::api::common::v1::ActivityType;
use crate::protos::temporal::api::enums::v1::RetryState;
use crate::protos::temporal::api::enums::v1::RetryState::CancelRequested;
use crate::protos::temporal::api::failure::v1::failure::FailureInfo;
use crate::protos::temporal::api::failure::v1::{
    failure, ActivityFailureInfo, CanceledFailureInfo, Failure,
};
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
            workflow_activation::{
                wf_activation_job, ResolveActivity, StartWorkflow, WfActivationJob,
            },
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
      --(ActivityTaskScheduled(i64), shared on_activity_task_scheduled) --> ScheduledEventRecorded;
    ScheduleCommandCreated --(Cancel, shared on_canceled) --> Canceled;

    ScheduledEventRecorded --(ActivityTaskStarted(i64), shared on_task_started) --> Started;
    ScheduledEventRecorded --(ActivityTaskTimedOut, on_task_timed_out) --> TimedOut;
    ScheduledEventRecorded --(Cancel, shared on_canceled) --> ScheduledActivityCancelCommandCreated;
    ScheduledEventRecorded --(Abandon, shared on_abandoned) --> Canceled;

    Started --(ActivityTaskCompleted(ActivityTaskCompletedEventAttributes), on_activity_task_completed) --> Completed;
    Started --(ActivityTaskFailed(ActivityTaskFailedEventAttributes), on_activity_task_failed) --> Failed;
    Started --(ActivityTaskTimedOut, on_activity_task_timed_out) --> TimedOut;
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
      --(ActivityTaskTimedOut, on_activity_task_timed_out) --> TimedOut;

    StartedActivityCancelCommandCreated
      --(CommandRequestCancelActivityTask) --> StartedActivityCancelCommandCreated;
    StartedActivityCancelCommandCreated
      --(ActivityTaskCancelRequested,
         shared on_activity_task_cancel_requested) --> StartedActivityCancelEventRecorded;

    StartedActivityCancelEventRecorded --(ActivityTaskFailed(ActivityTaskFailedEventAttributes), on_activity_task_failed) --> Failed;
    StartedActivityCancelEventRecorded
      --(ActivityTaskCompleted(ActivityTaskCompletedEventAttributes), on_activity_task_completed) --> Completed;
    StartedActivityCancelEventRecorded
      --(ActivityTaskTimedOut, on_activity_task_timed_out) --> TimedOut;
    StartedActivityCancelEventRecorded
      --(ActivityTaskCanceled(ActivityTaskCanceledEventAttributes), shared on_activity_task_canceled) --> Canceled;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum ActivityMachineCommand {
    #[display(fmt = "Complete")]
    Complete(Option<Payloads>),
    #[display(fmt = "Fail")]
    Fail(Option<Failure>),
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
            Some(EventType::ActivityTaskTimedOut) => Self::ActivityTaskTimedOut,
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
                ActivityMachineCommand::Fail(failure) => {
                    vec![MachineResponse::PushWFJob(Variant::ResolveActivity(
                        ResolveActivity {
                            activity_id: self.shared_state.attrs.activity_id.clone(),
                            result: Some(ActivityResult {
                                status: Some(activity_result::Status::Failed(ar::Failure {
                                    failure: failure.map(Into::into),
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
    pub(super) fn on_schedule(self) -> ActivityMachineTransition {
        // would add command here
        ActivityMachineTransition::default::<ScheduleCommandCreated>()
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduleCommandCreated {}

impl ScheduleCommandCreated {
    pub(super) fn on_activity_task_scheduled(
        self,
        dat: SharedState,
        scheduled_event_id: i64,
    ) -> ActivityMachineTransition {
        ActivityMachineTransition::ok_shared(
            vec![],
            ScheduledEventRecorded::default(),
            SharedState {
                scheduled_event_id,
                ..dat
            },
        )
    }
    pub(super) fn on_canceled(self, dat: SharedState) -> ActivityMachineTransition {
        let canceled_state = SharedState {
            cancelled_before_sent: true,
            ..dat
        };
        match dat.cancellation_type {
            ActivityCancellationType::Abandon => {
                ActivityMachineTransition::ok_shared(vec![], Canceled::default(), canceled_state)
            }
            _ => notify_lang_activity_cancelled(canceled_state, None, Canceled::default().into()),
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
    ) -> ActivityMachineTransition {
        ActivityMachineTransition::ok_shared(
            vec![],
            Started::default(),
            SharedState {
                started_event_id,
                ..dat
            },
        )
    }
    pub(super) fn on_task_timed_out(self) -> ActivityMachineTransition {
        ActivityMachineTransition::default::<TimedOut>()
    }
    pub(super) fn on_canceled(self, dat: SharedState) -> ActivityMachineTransition {
        create_request_cancel_activity_task_command(
            dat,
            ScheduledActivityCancelCommandCreated::default().into(),
        )
    }
    pub(super) fn on_abandoned(self, dat: SharedState) -> ActivityMachineTransition {
        notify_lang_activity_cancelled(dat, None, Canceled::default().into())
    }
}

#[derive(Default, Clone)]
pub(super) struct Started {}

impl Started {
    pub(super) fn on_activity_task_completed(
        self,
        attrs: ActivityTaskCompletedEventAttributes,
    ) -> ActivityMachineTransition {
        ActivityMachineTransition::ok(
            vec![ActivityMachineCommand::Complete(attrs.result)],
            Completed::default(),
        )
    }
    pub(super) fn on_activity_task_failed(
        self,
        attrs: ActivityTaskFailedEventAttributes,
    ) -> ActivityMachineTransition {
        ActivityMachineTransition::ok(
            vec![ActivityMachineCommand::Fail(attrs.failure)],
            Completed::default(),
        )
    }
    pub(super) fn on_activity_task_timed_out(self) -> ActivityMachineTransition {
        ActivityMachineTransition::default::<TimedOut>()
    }
    pub(super) fn on_canceled(self, dat: SharedState) -> ActivityMachineTransition {
        create_request_cancel_activity_task_command(
            dat,
            StartedActivityCancelCommandCreated::default().into(),
        )
    }
    pub(super) fn on_abandoned(self, dat: SharedState) -> ActivityMachineTransition {
        notify_lang_activity_cancelled(dat, None, Canceled::default().into())
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledActivityCancelCommandCreated {}

impl ScheduledActivityCancelCommandCreated {
    pub(super) fn on_command_request_cancel_activity_task(
        self,
        dat: SharedState,
    ) -> ActivityMachineTransition {
        match dat.cancellation_type {
            ActivityCancellationType::TryCancel => notify_lang_activity_cancelled(
                dat,
                None,
                ScheduledActivityCancelCommandCreated::default().into(),
            ),
            _ => ActivityMachineTransition::default::<ScheduledActivityCancelCommandCreated>(),
        }
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledActivityCancelEventRecorded {}

impl ScheduledActivityCancelEventRecorded {
    pub(super) fn on_activity_task_canceled(
        self,
        dat: SharedState,
        _attrs: ActivityTaskCanceledEventAttributes,
    ) -> ActivityMachineTransition {
        notify_lang_activity_cancelled(dat, None, Canceled::default().into())
    }

    pub(super) fn on_activity_task_timed_out(self) -> ActivityMachineTransition {
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
    pub(super) fn on_activity_task_cancel_requested(
        self,
        dat: SharedState,
    ) -> ActivityMachineTransition {
        match dat.cancellation_type {
            ActivityCancellationType::TryCancel => notify_lang_activity_cancelled(
                dat,
                None,
                StartedActivityCancelEventRecorded::default().into(),
            ),
            _ => ActivityMachineTransition::default::<StartedActivityCancelEventRecorded>(),
        }
    }
}

#[derive(Default, Clone)]
pub(super) struct StartedActivityCancelEventRecorded {}

impl StartedActivityCancelEventRecorded {
    pub(super) fn on_activity_task_completed(
        self,
        attrs: ActivityTaskCompletedEventAttributes,
    ) -> ActivityMachineTransition {
        ActivityMachineTransition::ok(
            vec![ActivityMachineCommand::Complete(attrs.result)],
            Completed::default(),
        )
    }
    pub(super) fn on_activity_task_failed(
        self,
        attrs: ActivityTaskFailedEventAttributes,
    ) -> ActivityMachineTransition {
        ActivityMachineTransition::ok(
            vec![ActivityMachineCommand::Fail(attrs.failure)],
            Completed::default(),
        )
    }
    pub(super) fn on_activity_task_timed_out(self) -> ActivityMachineTransition {
        ActivityMachineTransition::default::<TimedOut>()
    }
    pub(super) fn on_activity_task_canceled(
        self,
        dat: SharedState,
        attrs: ActivityTaskCanceledEventAttributes,
    ) -> ActivityMachineTransition {
        match dat.cancellation_type {
            ActivityCancellationType::WaitCancellationCompleted => {
                notify_lang_activity_cancelled(dat, Some(attrs), Canceled::default().into())
            }
            _ => ActivityMachineTransition::ok(vec![], Canceled::default()),
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

#[derive(Default, Clone)]
pub(super) struct TimedOut {}

#[derive(Default, Clone)]
pub(super) struct Canceled {}

fn create_request_cancel_activity_task_command(
    dat: SharedState,
    next_state: ActivityMachineState,
) -> TransitionResult<ActivityMachine> {
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

/// Notifies lang side that activity has been cancelled by sending a failure with cancelled failure as a cause.
/// Optional cancelled_event, if passed, is used to supply event IDs.
/// State machine will transition into the `next_state` provided as a parameter.
fn notify_lang_activity_cancelled(
    dat: SharedState,
    canceled_event: Option<ActivityTaskCanceledEventAttributes>,
    next_state: ActivityMachineState,
) -> TransitionResult<ActivityMachine> {
    let (scheduled_event_id, started_event_id) = match canceled_event {
        None => (dat.scheduled_event_id, dat.started_event_id),
        Some(e) => (e.scheduled_event_id, e.scheduled_event_id),
    };
    ActivityMachineTransition::ok_shared(
        vec![ActivityMachineCommand::Fail(Some(
            new_cancellation_failure(&dat, scheduled_event_id, started_event_id),
        ))],
        next_state,
        dat,
    )
}

fn new_cancellation_failure(
    dat: &SharedState,
    scheduled_event_id: i64,
    started_event_id: i64,
) -> Failure {
    let cancelled_failure = Failure {
        source: "CoreSDK".to_string(),
        failure_info: Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
            details: None,
        })),
        ..Default::default()
    };
    let activity_failure_info = ActivityFailureInfo {
        activity_id: dat.attrs.activity_id.to_string(),
        activity_type: Some(ActivityType {
            name: dat.attrs.activity_type.to_string(),
        }),
        scheduled_event_id,
        started_event_id,
        identity: "workflow".to_string(),
        retry_state: RetryState::Unspecified as i32,
    };
    Failure {
        message: "Activity canceled".to_string(),
        cause: Some(Box::new(cancelled_failure)),
        failure_info: Some(failure::FailureInfo::ActivityFailureInfo(
            activity_failure_info,
        )),
        ..Default::default()
    }
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
                                status: Some(activity_result::Status::Failed(_))
                            })
                        }
                    )),
                },
            ]
        )
    }
}
