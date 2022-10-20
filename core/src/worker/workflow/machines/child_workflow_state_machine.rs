use super::{
    workflow_machines::MachineResponse, Cancellable, EventInfo, NewMachineWithCommand,
    OnEventWrapper, WFMachinesAdapter, WFMachinesError,
};
use rustfsm::{fsm, MachineError, TransitionResult};
use std::convert::{TryFrom, TryInto};
use temporal_sdk_core_protos::{
    coresdk::{
        child_workflow::{
            self as wfr, child_workflow_result::Status as ChildWorkflowStatus,
            ChildWorkflowCancellationType, ChildWorkflowResult,
        },
        workflow_activation::{
            resolve_child_workflow_execution_start, ResolveChildWorkflowExecution,
            ResolveChildWorkflowExecutionStart, ResolveChildWorkflowExecutionStartCancelled,
            ResolveChildWorkflowExecutionStartFailure, ResolveChildWorkflowExecutionStartSuccess,
        },
        workflow_commands::StartChildWorkflowExecution,
    },
    temporal::api::{
        command::v1::{Command, RequestCancelExternalWorkflowExecutionCommandAttributes},
        common::v1::{Payload, Payloads, WorkflowExecution, WorkflowType},
        enums::v1::{
            CommandType, EventType, RetryState, StartChildWorkflowExecutionFailedCause, TimeoutType,
        },
        failure::v1::{self as failure, failure::FailureInfo, Failure},
        history::v1::{
            history_event, ChildWorkflowExecutionCompletedEventAttributes,
            ChildWorkflowExecutionFailedEventAttributes,
            ChildWorkflowExecutionStartedEventAttributes, HistoryEvent,
            StartChildWorkflowExecutionFailedEventAttributes,
        },
    },
};

fsm! {
    pub(super) name ChildWorkflowMachine;
    command ChildWorkflowCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(Schedule, on_schedule) --> StartCommandCreated;
    StartCommandCreated --(CommandStartChildWorkflowExecution) --> StartCommandCreated;
    StartCommandCreated --(StartChildWorkflowExecutionInitiated(i64),
        shared on_start_child_workflow_execution_initiated) --> StartEventRecorded;
    StartCommandCreated --(Cancel, shared on_cancelled) --> Cancelled;

    StartEventRecorded --(ChildWorkflowExecutionStarted(ChildWorkflowExecutionStartedEvent),
        shared on_child_workflow_execution_started) --> Started;
    StartEventRecorded --(StartChildWorkflowExecutionFailed(StartChildWorkflowExecutionFailedCause),
        on_start_child_workflow_execution_failed) --> StartFailed;

    Started --(ChildWorkflowExecutionCompleted(Option<Payloads>),
        on_child_workflow_execution_completed) --> Completed;
    Started --(ChildWorkflowExecutionFailed(ChildWorkflowExecutionFailedEventAttributes),
        shared on_child_workflow_execution_failed) --> Failed;
    Started --(ChildWorkflowExecutionTimedOut(RetryState),
        shared on_child_workflow_execution_timed_out) --> TimedOut;
    Started --(ChildWorkflowExecutionCancelled,
        on_child_workflow_execution_cancelled) --> Cancelled;
    Started --(ChildWorkflowExecutionTerminated,
        shared on_child_workflow_execution_terminated) --> Terminated;
    // If cancelled after started, we need to issue a cancel external workflow command, and then
    // the child workflow will resolve somehow, so we want to go back to started and wait for that
    // resolution.
    Started --(Cancel, shared on_cancelled) --> Started;
    // Abandon & try cancel modes may immediately move to cancelled
    Started --(Cancel, shared on_cancelled) --> Cancelled;
    Started --(CommandRequestCancelExternalWorkflowExecution) --> Started;

    // Ignore any spurious cancellations after resolution
    Cancelled --(Cancel) --> Cancelled;
    Failed --(Cancel) --> Failed;
    StartFailed --(Cancel) --> StartFailed;
    TimedOut --(Cancel) --> TimedOut;
    Completed --(Cancel) --> Completed;
}

pub struct ChildWorkflowExecutionStartedEvent {
    workflow_execution: WorkflowExecution,
    started_event_id: i64,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum ChildWorkflowCommand {
    #[display(fmt = "Start")]
    Start(WorkflowExecution),
    #[display(fmt = "Complete")]
    Complete(Option<Payloads>),
    #[display(fmt = "Fail")]
    Fail(Failure),
    #[display(fmt = "Cancel")]
    Cancel,
    #[display(fmt = "StartFail")]
    StartFail(StartChildWorkflowExecutionFailedCause),
    #[display(fmt = "StartCancel")]
    StartCancel(Failure),
    #[display(fmt = "CancelAfterStarted")]
    IssueCancelAfterStarted { reason: String },
}

#[derive(Default, Clone)]
pub(super) struct Cancelled {}

#[derive(Default, Clone)]
pub(super) struct Completed {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(self) -> ChildWorkflowMachineTransition<StartCommandCreated> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Failed {}

#[derive(Default, Clone)]
pub(super) struct StartCommandCreated {}

impl StartCommandCreated {
    pub(super) fn on_start_child_workflow_execution_initiated(
        self,
        state: SharedState,
        initiated_event_id: i64,
    ) -> ChildWorkflowMachineTransition<StartEventRecorded> {
        ChildWorkflowMachineTransition::ok_shared(
            vec![],
            StartEventRecorded::default(),
            SharedState {
                initiated_event_id,
                ..state
            },
        )
    }

    pub(super) fn on_cancelled(
        self,
        state: SharedState,
    ) -> ChildWorkflowMachineTransition<Cancelled> {
        let state = SharedState {
            cancelled_before_sent: true,
            ..state
        };
        ChildWorkflowMachineTransition::ok_shared(
            vec![ChildWorkflowCommand::StartCancel(Failure {
                message: "Child Workflow execution cancelled before scheduled".to_owned(),
                cause: Some(Box::new(Failure {
                    failure_info: Some(FailureInfo::CanceledFailureInfo(
                        failure::CanceledFailureInfo {
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                })),
                failure_info: failure_info_from_state(&state, RetryState::NonRetryableFailure),
                ..Default::default()
            })],
            Cancelled::default(),
            state,
        )
    }
}

#[derive(Default, Clone)]
pub(super) struct StartEventRecorded {}

impl StartEventRecorded {
    pub(super) fn on_child_workflow_execution_started(
        self,
        state: SharedState,
        event: ChildWorkflowExecutionStartedEvent,
    ) -> ChildWorkflowMachineTransition<Started> {
        ChildWorkflowMachineTransition::ok_shared(
            vec![ChildWorkflowCommand::Start(
                event.workflow_execution.clone(),
            )],
            Started::default(),
            SharedState {
                started_event_id: event.started_event_id,
                run_id: event.workflow_execution.run_id,
                ..state
            },
        )
    }
    pub(super) fn on_start_child_workflow_execution_failed(
        self,
        cause: StartChildWorkflowExecutionFailedCause,
    ) -> ChildWorkflowMachineTransition<StartFailed> {
        ChildWorkflowMachineTransition::ok(
            vec![ChildWorkflowCommand::StartFail(cause)],
            StartFailed::default(),
        )
    }
}

#[derive(Default, Clone)]
pub(super) struct StartFailed {}

#[derive(Default, Clone)]
pub(super) struct Started {}

impl Started {
    fn on_child_workflow_execution_completed(
        self,
        result: Option<Payloads>,
    ) -> ChildWorkflowMachineTransition<Completed> {
        ChildWorkflowMachineTransition::ok(
            vec![ChildWorkflowCommand::Complete(result)],
            Completed::default(),
        )
    }
    fn on_child_workflow_execution_failed(
        self,
        state: SharedState,
        attrs: ChildWorkflowExecutionFailedEventAttributes,
    ) -> ChildWorkflowMachineTransition<Failed> {
        ChildWorkflowMachineTransition::ok(
            vec![ChildWorkflowCommand::Fail(Failure {
                message: "Child Workflow execution failed".to_owned(),
                failure_info: failure_info_from_state(&state, attrs.retry_state()),
                cause: attrs.failure.map(Box::new),
                ..Default::default()
            })],
            Failed::default(),
        )
    }
    fn on_child_workflow_execution_timed_out(
        self,
        state: SharedState,
        retry_state: RetryState,
    ) -> ChildWorkflowMachineTransition<TimedOut> {
        ChildWorkflowMachineTransition::ok(
            vec![ChildWorkflowCommand::Fail(Failure {
                message: "Child Workflow execution timed out".to_owned(),
                cause: Some(Box::new(Failure {
                    message: "Timed out".to_owned(),
                    failure_info: Some(FailureInfo::TimeoutFailureInfo(
                        failure::TimeoutFailureInfo {
                            last_heartbeat_details: None,
                            timeout_type: TimeoutType::StartToClose as i32,
                        },
                    )),
                    ..Default::default()
                })),
                failure_info: failure_info_from_state(&state, retry_state),
                ..Default::default()
            })],
            TimedOut::default(),
        )
    }
    fn on_child_workflow_execution_cancelled(self) -> ChildWorkflowMachineTransition<Cancelled> {
        ChildWorkflowMachineTransition::ok(vec![ChildWorkflowCommand::Cancel], Cancelled::default())
    }
    fn on_child_workflow_execution_terminated(
        self,
        state: SharedState,
    ) -> ChildWorkflowMachineTransition<Terminated> {
        ChildWorkflowMachineTransition::ok(
            vec![ChildWorkflowCommand::Fail(Failure {
                message: "Child Workflow execution terminated".to_owned(),
                cause: Some(Box::new(Failure {
                    message: "Terminated".to_owned(),
                    failure_info: Some(FailureInfo::TerminatedFailureInfo(
                        failure::TerminatedFailureInfo {},
                    )),
                    ..Default::default()
                })),
                failure_info: failure_info_from_state(&state, RetryState::NonRetryableFailure),
                ..Default::default()
            })],
            Terminated::default(),
        )
    }
    fn on_cancelled(
        self,
        state: SharedState,
    ) -> ChildWorkflowMachineTransition<StartedOrCancelled> {
        let dest = match state.cancel_type {
            ChildWorkflowCancellationType::Abandon | ChildWorkflowCancellationType::TryCancel => {
                StartedOrCancelled::Cancelled(Default::default())
            }
            _ => StartedOrCancelled::Started(Default::default()),
        };
        TransitionResult::ok(
            [ChildWorkflowCommand::IssueCancelAfterStarted {
                reason: "Parent workflow requested cancel".to_string(),
            }],
            dest,
        )
    }
}

#[derive(Default, Clone)]
pub(super) struct Terminated {}

#[derive(Default, Clone)]
pub(super) struct TimedOut {}

#[derive(Default, Clone, Debug)]
pub(super) struct SharedState {
    initiated_event_id: i64,
    started_event_id: i64,
    lang_sequence_number: u32,
    namespace: String,
    workflow_id: String,
    run_id: String,
    workflow_type: String,
    cancelled_before_sent: bool,
    cancel_type: ChildWorkflowCancellationType,
}

/// Creates a new child workflow state machine and a command to start it on the server.
pub(super) fn new_child_workflow(attribs: StartChildWorkflowExecution) -> NewMachineWithCommand {
    let (wf, add_cmd) = ChildWorkflowMachine::new_scheduled(attribs);
    NewMachineWithCommand {
        command: add_cmd,
        machine: wf.into(),
    }
}

impl ChildWorkflowMachine {
    /// Create a new child workflow and immediately schedule it.
    pub(crate) fn new_scheduled(attribs: StartChildWorkflowExecution) -> (Self, Command) {
        let mut s = Self {
            state: Created {}.into(),
            shared_state: SharedState {
                lang_sequence_number: attribs.seq,
                workflow_id: attribs.workflow_id.clone(),
                workflow_type: attribs.workflow_type.clone(),
                namespace: attribs.namespace.clone(),
                cancel_type: attribs.cancellation_type(),
                ..Default::default()
            },
        };
        OnEventWrapper::on_event_mut(&mut s, ChildWorkflowMachineEvents::Schedule)
            .expect("Scheduling child workflows doesn't fail");
        let cmd = Command {
            command_type: CommandType::StartChildWorkflowExecution as i32,
            attributes: Some(attribs.into()),
        };
        (s, cmd)
    }

    fn resolve_cancelled_msg(&self) -> ResolveChildWorkflowExecution {
        let failure = Failure {
            message: "Child Workflow execution cancelled".to_owned(),
            cause: Some(Box::new(Failure {
                failure_info: Some(FailureInfo::CanceledFailureInfo(
                    failure::CanceledFailureInfo {
                        ..Default::default()
                    },
                )),
                ..Default::default()
            })),
            failure_info: failure_info_from_state(
                &self.shared_state,
                RetryState::NonRetryableFailure,
            ),
            ..Default::default()
        };
        ResolveChildWorkflowExecution {
            seq: self.shared_state.lang_sequence_number,
            result: Some(ChildWorkflowResult {
                status: Some(ChildWorkflowStatus::Cancelled(wfr::Cancellation {
                    failure: Some(failure),
                })),
            }),
        }
    }
}

impl TryFrom<HistoryEvent> for ChildWorkflowMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        Ok(match EventType::from_i32(e.event_type) {
            Some(EventType::StartChildWorkflowExecutionInitiated) => {
                Self::StartChildWorkflowExecutionInitiated(e.event_id)
            }
            Some(EventType::StartChildWorkflowExecutionFailed) => {
                if let Some(
                    history_event::Attributes::StartChildWorkflowExecutionFailedEventAttributes(
                        StartChildWorkflowExecutionFailedEventAttributes { cause, .. },
                    ),
                ) = e.attributes
                {
                    Self::StartChildWorkflowExecutionFailed(
                        StartChildWorkflowExecutionFailedCause::from_i32(cause).ok_or_else(
                            || {
                                WFMachinesError::Fatal(
                                    "Invalid StartChildWorkflowExecutionFailedCause".to_string(),
                                )
                            },
                        )?,
                    )
                } else {
                    return Err(WFMachinesError::Fatal(
                        "StartChildWorkflowExecutionFailed attributes were unset".to_string(),
                    ));
                }
            }
            Some(EventType::ChildWorkflowExecutionStarted) => {
                if let Some(
                    history_event::Attributes::ChildWorkflowExecutionStartedEventAttributes(
                        ChildWorkflowExecutionStartedEventAttributes {
                            workflow_execution: Some(we),
                            ..
                        },
                    ),
                ) = e.attributes
                {
                    Self::ChildWorkflowExecutionStarted(ChildWorkflowExecutionStartedEvent {
                        workflow_execution: we,
                        started_event_id: e.event_id,
                    })
                } else {
                    return Err(WFMachinesError::Fatal(
                        "ChildWorkflowExecutionStarted attributes were unset or malformed"
                            .to_string(),
                    ));
                }
            }
            Some(EventType::ChildWorkflowExecutionCompleted) => {
                if let Some(
                    history_event::Attributes::ChildWorkflowExecutionCompletedEventAttributes(
                        ChildWorkflowExecutionCompletedEventAttributes { result, .. },
                    ),
                ) = e.attributes
                {
                    Self::ChildWorkflowExecutionCompleted(result)
                } else {
                    return Err(WFMachinesError::Fatal(
                        "ChildWorkflowExecutionCompleted attributes were unset or malformed"
                            .to_string(),
                    ));
                }
            }
            Some(EventType::ChildWorkflowExecutionFailed) => {
                if let Some(
                    history_event::Attributes::ChildWorkflowExecutionFailedEventAttributes(attrs),
                ) = e.attributes
                {
                    Self::ChildWorkflowExecutionFailed(attrs)
                } else {
                    return Err(WFMachinesError::Fatal(
                        "ChildWorkflowExecutionFailed attributes were unset".to_string(),
                    ));
                }
            }
            Some(EventType::ChildWorkflowExecutionTimedOut) => {
                if let Some(
                    history_event::Attributes::ChildWorkflowExecutionTimedOutEventAttributes(atts),
                ) = e.attributes
                {
                    Self::ChildWorkflowExecutionTimedOut(atts.retry_state())
                } else {
                    return Err(WFMachinesError::Fatal(
                        "ChildWorkflowExecutionTimedOut attributes were unset or malformed"
                            .to_string(),
                    ));
                }
            }
            Some(EventType::ChildWorkflowExecutionTerminated) => {
                Self::ChildWorkflowExecutionTerminated
            }
            Some(EventType::ChildWorkflowExecutionCanceled) => {
                Self::ChildWorkflowExecutionCancelled
            }
            _ => {
                return Err(WFMachinesError::Fatal(format!(
                    "Child workflow machine does not handle this event: {:?}",
                    e
                )))
            }
        })
    }
}

impl WFMachinesAdapter for ChildWorkflowMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            ChildWorkflowCommand::Start(we) => {
                vec![ResolveChildWorkflowExecutionStart {
                    seq: self.shared_state.lang_sequence_number,
                    status: Some(resolve_child_workflow_execution_start::Status::Succeeded(
                        ResolveChildWorkflowExecutionStartSuccess { run_id: we.run_id },
                    )),
                }
                .into()]
            }
            ChildWorkflowCommand::StartFail(cause) => {
                vec![ResolveChildWorkflowExecutionStart {
                    seq: self.shared_state.lang_sequence_number,
                    status: Some(resolve_child_workflow_execution_start::Status::Failed(
                        ResolveChildWorkflowExecutionStartFailure {
                            workflow_id: self.shared_state.workflow_id.clone(),
                            workflow_type: self.shared_state.workflow_type.clone(),
                            cause: cause as i32,
                        },
                    )),
                }
                .into()]
            }
            ChildWorkflowCommand::StartCancel(failure) => {
                vec![ResolveChildWorkflowExecutionStart {
                    seq: self.shared_state.lang_sequence_number,
                    status: Some(resolve_child_workflow_execution_start::Status::Cancelled(
                        ResolveChildWorkflowExecutionStartCancelled {
                            failure: Some(failure),
                        },
                    )),
                }
                .into()]
            }
            ChildWorkflowCommand::Complete(result) => {
                vec![ResolveChildWorkflowExecution {
                    seq: self.shared_state.lang_sequence_number,
                    result: Some(ChildWorkflowResult {
                        status: Some(ChildWorkflowStatus::Completed(wfr::Success {
                            result: convert_payloads(event_info, result)?,
                        })),
                    }),
                }
                .into()]
            }
            ChildWorkflowCommand::Fail(failure) => {
                vec![ResolveChildWorkflowExecution {
                    seq: self.shared_state.lang_sequence_number,
                    result: Some(ChildWorkflowResult {
                        status: Some(ChildWorkflowStatus::Failed(wfr::Failure {
                            failure: Some(failure),
                        })),
                    }),
                }
                .into()]
            }
            ChildWorkflowCommand::Cancel => {
                vec![self.resolve_cancelled_msg().into()]
            }
            ChildWorkflowCommand::IssueCancelAfterStarted { reason } => {
                let mut resps = vec![];
                if self.shared_state.cancel_type != ChildWorkflowCancellationType::Abandon {
                    resps.push(MachineResponse::NewCoreOriginatedCommand(
                        RequestCancelExternalWorkflowExecutionCommandAttributes {
                            namespace: self.shared_state.namespace.clone(),
                            workflow_id: self.shared_state.workflow_id.clone(),
                            run_id: self.shared_state.run_id.clone(),
                            child_workflow_only: true,
                            reason,
                            control: "".to_string(),
                        }
                        .into(),
                    ))
                }
                // Immediately resolve abandon/trycancel modes
                if matches!(
                    self.shared_state.cancel_type,
                    ChildWorkflowCancellationType::Abandon
                        | ChildWorkflowCancellationType::TryCancel
                ) {
                    resps.push(self.resolve_cancelled_msg().into())
                }
                resps
            }
        })
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        matches!(
            event.event_type(),
            EventType::StartChildWorkflowExecutionInitiated
                | EventType::StartChildWorkflowExecutionFailed
                | EventType::ChildWorkflowExecutionStarted
                | EventType::ChildWorkflowExecutionCompleted
                | EventType::ChildWorkflowExecutionFailed
                | EventType::ChildWorkflowExecutionTimedOut
                | EventType::ChildWorkflowExecutionTerminated
                | EventType::ChildWorkflowExecutionCanceled
        )
    }
}

impl TryFrom<CommandType> for ChildWorkflowMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::StartChildWorkflowExecution => Self::CommandStartChildWorkflowExecution,
            CommandType::RequestCancelExternalWorkflowExecution => {
                Self::CommandRequestCancelExternalWorkflowExecution
            }
            _ => return Err(()),
        })
    }
}

impl Cancellable for ChildWorkflowMachine {
    fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<Self::Error>> {
        let event = ChildWorkflowMachineEvents::Cancel;
        let vec = OnEventWrapper::on_event_mut(self, event)?;
        let res = vec
            .into_iter()
            .map(|mc| match mc {
                c @ ChildWorkflowCommand::StartCancel(_)
                | c @ ChildWorkflowCommand::IssueCancelAfterStarted { .. } => {
                    self.adapt_response(c, None)
                }
                x => panic!("Invalid cancel event response {:?}", x),
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();
        Ok(res)
    }

    fn was_cancelled_before_sent_to_server(&self) -> bool {
        self.shared_state.cancelled_before_sent
    }
}

fn failure_info_from_state(state: &SharedState, retry_state: RetryState) -> Option<FailureInfo> {
    Some(FailureInfo::ChildWorkflowExecutionFailureInfo(
        failure::ChildWorkflowExecutionFailureInfo {
            namespace: state.namespace.clone(),
            workflow_type: Some(WorkflowType {
                name: state.workflow_type.clone(),
            }),
            initiated_event_id: state.initiated_event_id,
            started_event_id: state.started_event_id,
            retry_state: retry_state as i32,
            workflow_execution: Some(WorkflowExecution {
                workflow_id: state.workflow_id.clone(),
                run_id: state.run_id.clone(),
            }),
        },
    ))
}

fn convert_payloads(
    event_info: Option<EventInfo>,
    result: Option<Payloads>,
) -> Result<Option<Payload>, WFMachinesError> {
    result.map(TryInto::try_into).transpose().map_err(|pe| {
        WFMachinesError::Fatal(format!(
            "Not exactly one payload in child workflow result ({}) for event: {:?}",
            pe, event_info
        ))
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::{
        replay::TestHistoryBuilder, test_help::canned_histories, worker::workflow::ManagedWFFunc,
    };
    use anyhow::anyhow;
    use rstest::{fixture, rstest};
    use std::mem::discriminant;
    use temporal_sdk::{
        CancellableFuture, ChildWorkflowOptions, WfContext, WorkflowFunction, WorkflowResult,
    };
    use temporal_sdk_core_protos::coresdk::{
        child_workflow::child_workflow_result,
        workflow_activation::resolve_child_workflow_execution_start::Status as StartStatus,
    };

    #[derive(Clone, Copy)]
    enum Expectation {
        Success,
        Failure,
        StartFailure,
    }

    impl Expectation {
        const fn try_from_u8(x: u8) -> Option<Self> {
            Some(match x {
                0 => Self::Success,
                1 => Self::Failure,
                2 => Self::StartFailure,
                _ => return None,
            })
        }
    }

    #[fixture]
    fn child_workflow_happy_hist() -> ManagedWFFunc {
        let func = WorkflowFunction::new(parent_wf);
        let t = canned_histories::single_child_workflow("child-id-1");
        assert_eq!(3, t.get_full_history_info().unwrap().wf_task_count());
        ManagedWFFunc::new(t, func, vec![[Expectation::Success as u8].into()])
    }

    #[fixture]
    fn child_workflow_fail_hist() -> ManagedWFFunc {
        let func = WorkflowFunction::new(parent_wf);
        let t = canned_histories::single_child_workflow_fail("child-id-1");
        assert_eq!(3, t.get_full_history_info().unwrap().wf_task_count());
        ManagedWFFunc::new(t, func, vec![[Expectation::Failure as u8].into()])
    }

    #[fixture]
    fn child_workflow_start_fail_hist() -> ManagedWFFunc {
        let func = WorkflowFunction::new(parent_wf);
        let t = canned_histories::single_child_workflow_start_fail("child-id-1");
        assert_eq!(2, t.get_full_history_info().unwrap().wf_task_count());
        ManagedWFFunc::new(t, func, vec![[Expectation::StartFailure as u8].into()])
    }

    async fn parent_wf(ctx: WfContext) -> WorkflowResult<()> {
        let expectation = Expectation::try_from_u8(ctx.get_args()[0].data[0]).unwrap();
        let child = ctx.child_workflow(ChildWorkflowOptions {
            workflow_id: "child-id-1".to_string(),
            workflow_type: "child".to_string(),
            ..Default::default()
        });

        let start_res = child.start(&ctx).await;
        match (expectation, &start_res.status) {
            (Expectation::Success | Expectation::Failure, StartStatus::Succeeded(_)) => {}
            (Expectation::StartFailure, StartStatus::Failed(_)) => return Ok(().into()),
            _ => return Err(anyhow!("Unexpected start status")),
        };
        match (
            expectation,
            start_res.into_started().unwrap().result().await.status,
        ) {
            (Expectation::Success, Some(child_workflow_result::Status::Completed(_))) => {
                Ok(().into())
            }
            (Expectation::Failure, _) => Ok(().into()),
            _ => Err(anyhow!("Unexpected child WF status")),
        }
    }

    #[rstest(
        wfm,
        case::success(child_workflow_happy_hist()),
        case::failure(child_workflow_fail_hist())
    )]
    #[tokio::test]
    async fn single_child_workflow_until_completion(mut wfm: ManagedWFFunc) {
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::StartChildWorkflowExecution as i32
        );

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        // Workflow is activated because the child WF has started.
        // It does not generate any commands, just waits for completion.
        assert_eq!(commands.len(), 0);

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
        wfm.shutdown().await.unwrap();
    }

    #[rstest(wfm, case::start_failure(child_workflow_start_fail_hist()))]
    #[tokio::test]
    async fn single_child_workflow_start_fail(mut wfm: ManagedWFFunc) {
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::StartChildWorkflowExecution as i32
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

    async fn cancel_before_send_wf(ctx: WfContext) -> WorkflowResult<()> {
        let workflow_id = "child-id-1";
        let child = ctx.child_workflow(ChildWorkflowOptions {
            workflow_id: workflow_id.to_string(),
            workflow_type: "child".to_string(),
            ..Default::default()
        });
        let start = child.start(&ctx);
        start.cancel(&ctx);
        match start.await.status {
            StartStatus::Cancelled(_) => Ok(().into()),
            _ => Err(anyhow!("Unexpected start status")),
        }
    }

    #[fixture]
    fn child_workflow_cancel_before_sent() -> ManagedWFFunc {
        let func = WorkflowFunction::new(cancel_before_send_wf);
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_task_scheduled_and_started();
        assert_eq!(2, t.get_full_history_info().unwrap().wf_task_count());
        ManagedWFFunc::new(t, func, vec![])
    }

    #[rstest(wfm, case::default(child_workflow_cancel_before_sent()))]
    #[tokio::test]
    async fn single_child_workflow_cancel_before_sent(mut wfm: ManagedWFFunc) {
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        // Workflow starts and cancels the child workflow, no commands should be sent to server.
        assert_eq!(commands.len(), 0);

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
        wfm.shutdown().await.unwrap();
    }

    #[test]
    fn cancels_ignored_terminal() {
        for state in [
            ChildWorkflowMachineState::Cancelled(Cancelled {}),
            Failed {}.into(),
            StartFailed {}.into(),
            TimedOut {}.into(),
            Completed {}.into(),
        ] {
            let mut s = ChildWorkflowMachine {
                state: state.clone(),
                shared_state: Default::default(),
            };
            let cmds = s.cancel().unwrap();
            assert_eq!(cmds.len(), 0);
            assert_eq!(discriminant(&state), discriminant(&s.state));
        }
    }
}
