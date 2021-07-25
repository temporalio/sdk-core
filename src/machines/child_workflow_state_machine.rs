use crate::machines::activity_state_machine::ActivityMachineCommand::Fail;
use crate::{
    machines::{
        workflow_machines::MachineResponse, Cancellable, EventInfo, MachineKind,
        NewMachineWithCommand, OnEventWrapper, WFMachinesAdapter, WFMachinesError,
    },
    protos::{
        coresdk::{
            child_workflow::{
                self as wfr, child_workflow_result::Status as ChildWorkflowStatus,
                ChildWorkflowCancellationType, ChildWorkflowResult,
            },
            common::Payload,
            workflow_activation::{
                resolve_child_workflow_execution_start, ResolveChildWorkflowExecution,
                ResolveChildWorkflowExecutionStart, ResolveChildWorkflowExecutionStartCancelled,
                ResolveChildWorkflowExecutionStartFailure,
                ResolveChildWorkflowExecutionStartSuccess,
            },
            workflow_commands::StartChildWorkflowExecution,
        },
        temporal::api::{
            command::v1::{command, Command},
            common::v1::{Payloads, WorkflowExecution, WorkflowType},
            enums::v1::{
                CommandType, EventType, RetryState, StartChildWorkflowExecutionFailedCause,
                TimeoutType,
            },
            failure::v1::{self as failure, failure::FailureInfo, ActivityFailureInfo, Failure},
            history::v1::{
                history_event, ChildWorkflowExecutionCanceledEventAttributes,
                ChildWorkflowExecutionCompletedEventAttributes,
                ChildWorkflowExecutionFailedEventAttributes,
                ChildWorkflowExecutionStartedEventAttributes,
                ChildWorkflowExecutionTerminatedEventAttributes,
                ChildWorkflowExecutionTimedOutEventAttributes, HistoryEvent,
                StartChildWorkflowExecutionFailedEventAttributes,
            },
        },
    },
};
use rustfsm::{fsm, MachineError, TransitionResult};
use std::convert::{TryFrom, TryInto};
use tonic::Status;

fsm! {
    pub(super) name ChildWorkflowMachine;
    command ChildWorkflowCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(Schedule, on_schedule) --> StartCommandCreated;
    StartCommandCreated --(CommandStartChildWorkflowExecution) --> StartCommandCreated;
    StartCommandCreated --(StartChildWorkflowExecutionInitiated(i64), shared on_start_child_workflow_execution_initiated) --> StartEventRecorded;
    StartCommandCreated --(Cancel, shared on_cancelled) --> Cancelled;

    StartEventRecorded --(ChildWorkflowExecutionStarted(ChildWorkflowExecutionStartedEventAttributes), on_child_workflow_execution_started) --> Started;
    StartEventRecorded --(StartChildWorkflowExecutionFailed(StartChildWorkflowExecutionFailedEventAttributes), on_start_child_workflow_execution_failed) --> StartFailed;

    Started --(ChildWorkflowExecutionCompleted(ChildWorkflowExecutionCompletedEventAttributes), on_child_workflow_execution_completed) --> Completed;
    Started --(ChildWorkflowExecutionFailed(ChildWorkflowExecutionFailedEventAttributes), shared on_child_workflow_execution_failed) --> Failed;
    Started --(ChildWorkflowExecutionTimedOut(ChildWorkflowExecutionTimedOutEventAttributes), shared on_child_workflow_execution_timed_out) --> TimedOut;
    Started --(ChildWorkflowExecutionCancelled(ChildWorkflowExecutionCanceledEventAttributes), shared on_child_workflow_execution_cancelled) --> Cancelled;
    Started --(ChildWorkflowExecutionTerminated(ChildWorkflowExecutionTerminatedEventAttributes), shared on_child_workflow_execution_terminated) --> Terminated;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum ChildWorkflowCommand {
    #[display(fmt = "Start")]
    Start(WorkflowExecution),
    #[display(fmt = "Complete")]
    Complete(Option<Payloads>),
    #[display(fmt = "Fail")]
    Fail(Failure),
    #[display(fmt = "StartFail")]
    StartFail(StartChildWorkflowExecutionFailedCause),
    #[display(fmt = "StartCancel")]
    StartCancel(Failure),
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
        dat: SharedState,
        scheduled_event_id: i64,
    ) -> ChildWorkflowMachineTransition<StartEventRecorded> {
        ChildWorkflowMachineTransition::ok_shared(
            vec![],
            StartEventRecorded::default(),
            SharedState {
                scheduled_event_id,
                ..dat
            },
        )
    }

    pub(super) fn on_cancelled(
        self,
        dat: SharedState,
    ) -> ChildWorkflowMachineTransition<Cancelled> {
        let state = SharedState {
            cancelled_before_sent: true,
            ..dat
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
                failure_info: Some(FailureInfo::ChildWorkflowExecutionFailureInfo(
                    failure::ChildWorkflowExecutionFailureInfo {
                        namespace: state.attrs.namespace.clone(),
                        workflow_type: Some(WorkflowType {
                            name: state.attrs.workflow_type.clone(),
                        }),
                        initiated_event_id: 0,
                        started_event_id: 0,
                        retry_state: RetryState::NonRetryableFailure as i32,
                        workflow_execution: Some(WorkflowExecution {
                            workflow_id: state.attrs.workflow_id.clone(),
                            ..Default::default()
                        }),
                    },
                )),
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
        attrs: ChildWorkflowExecutionStartedEventAttributes,
    ) -> ChildWorkflowMachineTransition<Started> {
        ChildWorkflowMachineTransition::ok(vec![
            ChildWorkflowCommand::Start(attrs.workflow_execution.expect("ChildWorkflowExecutionStartedEventAttributes.workflow_execution should not be empty"))
        ], Started::default())
    }
    pub(super) fn on_start_child_workflow_execution_failed(
        self,
        attrs: StartChildWorkflowExecutionFailedEventAttributes,
    ) -> ChildWorkflowMachineTransition<StartFailed> {
        ChildWorkflowMachineTransition::ok(
            vec![ChildWorkflowCommand::StartFail(
                StartChildWorkflowExecutionFailedCause::from_i32(attrs.cause)
                    .expect("Invalid StartChildWorkflowExecutionFailedCause"),
            )],
            StartFailed::default(),
        )
    }
}

#[derive(Default, Clone)]
pub(super) struct StartFailed {}

#[derive(Default, Clone)]
pub(super) struct Started {}

impl Started {
    pub(super) fn on_child_workflow_execution_completed(
        self,
        attrs: ChildWorkflowExecutionCompletedEventAttributes,
    ) -> ChildWorkflowMachineTransition<Completed> {
        ChildWorkflowMachineTransition::ok(
            vec![ChildWorkflowCommand::Complete(attrs.result)],
            Completed::default(),
        )
    }
    pub(super) fn on_child_workflow_execution_failed(
        self,
        dat: SharedState,
        attrs: ChildWorkflowExecutionFailedEventAttributes,
    ) -> ChildWorkflowMachineTransition<Failed> {
        ChildWorkflowMachineTransition::ok(
            vec![ChildWorkflowCommand::Fail(Failure {
                message: "Child Workflow execution failed".to_owned(),
                cause: attrs.failure.map(Box::new),
                failure_info: Some(FailureInfo::ChildWorkflowExecutionFailureInfo(
                    failure::ChildWorkflowExecutionFailureInfo {
                        namespace: attrs.namespace,
                        workflow_type: Some(WorkflowType {
                            name: dat.attrs.workflow_type,
                        }),
                        initiated_event_id: attrs.initiated_event_id,
                        started_event_id: attrs.started_event_id,
                        retry_state: attrs.retry_state,
                        workflow_execution: attrs.workflow_execution,
                    },
                )),
                ..Default::default()
            })],
            Failed::default(),
        )
    }
    pub(super) fn on_child_workflow_execution_timed_out(
        self,
        dat: SharedState,
        attrs: ChildWorkflowExecutionTimedOutEventAttributes,
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
                failure_info: Some(FailureInfo::ChildWorkflowExecutionFailureInfo(
                    failure::ChildWorkflowExecutionFailureInfo {
                        namespace: attrs.namespace,
                        workflow_type: Some(WorkflowType {
                            name: dat.attrs.workflow_type,
                        }),
                        initiated_event_id: attrs.initiated_event_id,
                        started_event_id: attrs.started_event_id,
                        retry_state: attrs.retry_state,
                        workflow_execution: attrs.workflow_execution,
                    },
                )),
                ..Default::default()
            })],
            TimedOut::default(),
        )
    }
    pub(super) fn on_child_workflow_execution_cancelled(
        self,
        dat: SharedState,
        attrs: ChildWorkflowExecutionCanceledEventAttributes,
    ) -> ChildWorkflowMachineTransition<Cancelled> {
        match dat.cancellation_type {
            ChildWorkflowCancellationType::Abandon => {
                ChildWorkflowMachineTransition::ok(vec![], Cancelled::default())
            }
            _ => ChildWorkflowMachineTransition::ok(
                vec![ChildWorkflowCommand::Fail(Failure {
                    message: "Child Workflow execution cancelled before scheduled".to_owned(),
                    cause: Some(Box::new(Failure {
                        failure_info: Some(FailureInfo::CanceledFailureInfo(
                            failure::CanceledFailureInfo {
                                ..Default::default()
                            },
                        )),
                        ..Default::default()
                    })),
                    failure_info: Some(FailureInfo::ChildWorkflowExecutionFailureInfo(
                        failure::ChildWorkflowExecutionFailureInfo {
                            namespace: dat.attrs.namespace.clone(),
                            workflow_type: Some(WorkflowType {
                                name: dat.attrs.workflow_type.clone(),
                            }),
                            initiated_event_id: attrs.initiated_event_id,
                            started_event_id: attrs.started_event_id,
                            retry_state: RetryState::NonRetryableFailure as i32,
                            workflow_execution: Some(WorkflowExecution {
                                workflow_id: dat.attrs.workflow_id,
                                ..Default::default()
                            }),
                        },
                    )),
                    ..Default::default()
                })],
                Cancelled::default(),
            ),
        }
    }
    pub(super) fn on_child_workflow_execution_terminated(
        self,
        dat: SharedState,
        attrs: ChildWorkflowExecutionTerminatedEventAttributes,
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
                failure_info: Some(FailureInfo::ChildWorkflowExecutionFailureInfo(
                    failure::ChildWorkflowExecutionFailureInfo {
                        namespace: attrs.namespace,
                        workflow_type: Some(WorkflowType {
                            name: dat.attrs.workflow_type,
                        }),
                        initiated_event_id: attrs.initiated_event_id,
                        started_event_id: attrs.started_event_id,
                        retry_state: RetryState::NonRetryableFailure as i32,
                        workflow_execution: attrs.workflow_execution,
                    },
                )),
                ..Default::default()
            })],
            Terminated::default(),
        )
    }
}

#[derive(Default, Clone)]
pub(super) struct Terminated {}

#[derive(Default, Clone)]
pub(super) struct TimedOut {}

#[derive(Default, Clone)]
pub(super) struct SharedState {
    scheduled_event_id: i64,
    //     started_event_id: i64,
    attrs: StartChildWorkflowExecution,
    cancellation_type: ChildWorkflowCancellationType,
    cancelled_before_sent: bool,
}

/// Creates a new child workflow state machine and a command to start it on the server.
pub(super) fn new_child_workflow(
    attribs: StartChildWorkflowExecution,
) -> NewMachineWithCommand<ChildWorkflowMachine> {
    let (wf, add_cmd) = ChildWorkflowMachine::new_scheduled(attribs);
    NewMachineWithCommand {
        command: add_cmd,
        machine: wf,
    }
}

impl ChildWorkflowMachine {
    /// Create a new child workflow and immediately schedule it.
    pub(crate) fn new_scheduled(attribs: StartChildWorkflowExecution) -> (Self, Command) {
        let mut s = Self {
            state: Created {}.into(),
            shared_state: SharedState {
                cancellation_type: ChildWorkflowCancellationType::from_i32(
                    attribs.cancellation_type,
                )
                .unwrap(),
                attrs: attribs.clone(),
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
                        attrs,
                    ),
                ) = e.attributes
                {
                    Self::StartChildWorkflowExecutionFailed(attrs)
                } else {
                    return Err(WFMachinesError::Fatal(
                        "StartChildWorkflowExecutionFailed attributes were unset".to_string(),
                    ));
                }
            }
            Some(EventType::ChildWorkflowExecutionStarted) => {
                if let Some(
                    history_event::Attributes::ChildWorkflowExecutionStartedEventAttributes(attrs),
                ) = e.attributes
                {
                    Self::ChildWorkflowExecutionStarted(attrs)
                } else {
                    return Err(WFMachinesError::Fatal(
                        "ChildWorkflowExecutionStarted attributes were unset".to_string(),
                    ));
                }
            }
            Some(EventType::ChildWorkflowExecutionCompleted) => {
                if let Some(
                    history_event::Attributes::ChildWorkflowExecutionCompletedEventAttributes(
                        attrs,
                    ),
                ) = e.attributes
                {
                    Self::ChildWorkflowExecutionCompleted(attrs)
                } else {
                    return Err(WFMachinesError::Fatal(
                        "ChildWorkflowExecutionCompleted attributes were unset".to_string(),
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
            Some(EventType::ChildWorkflowExecutionTerminated) => {
                if let Some(
                    history_event::Attributes::ChildWorkflowExecutionTerminatedEventAttributes(
                        attrs,
                    ),
                ) = e.attributes
                {
                    Self::ChildWorkflowExecutionTerminated(attrs)
                } else {
                    return Err(WFMachinesError::Fatal(
                        "ChildWorkflowExecutionTerminated attributes were unset".to_string(),
                    ));
                }
            }
            Some(EventType::ChildWorkflowExecutionTimedOut) => {
                if let Some(
                    history_event::Attributes::ChildWorkflowExecutionTimedOutEventAttributes(attrs),
                ) = e.attributes
                {
                    Self::ChildWorkflowExecutionTimedOut(attrs)
                } else {
                    return Err(WFMachinesError::Fatal(
                        "ChildWorkflowExecutionTimedOut attributes were unset".to_string(),
                    ));
                }
            }
            Some(EventType::ChildWorkflowExecutionCanceled) => {
                if let Some(
                    history_event::Attributes::ChildWorkflowExecutionCanceledEventAttributes(attrs),
                ) = e.attributes
                {
                    Self::ChildWorkflowExecutionCancelled(attrs)
                } else {
                    return Err(WFMachinesError::Fatal(
                        "ChildWorkflowExecutionCanceled attributes were unset".to_string(),
                    ));
                }
            }
            _ => {
                return Err(WFMachinesError::Fatal(
                    "Child workflow machine does not handle this event".to_string(),
                ))
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
                    workflow_id: we.workflow_id.clone(),
                    status: Some(resolve_child_workflow_execution_start::Status::Succeeded(
                        ResolveChildWorkflowExecutionStartSuccess { run_id: we.run_id },
                    )),
                }
                .into()]
            }
            ChildWorkflowCommand::StartFail(cause) => {
                vec![ResolveChildWorkflowExecutionStart {
                    workflow_id: self.shared_state.attrs.workflow_id.clone(),
                    status: Some(resolve_child_workflow_execution_start::Status::Failed(
                        ResolveChildWorkflowExecutionStartFailure {
                            workflow_type: self.shared_state.attrs.workflow_type.clone(),
                            cause: cause as i32,
                        },
                    )),
                }
                .into()]
            }
            ChildWorkflowCommand::StartCancel(failure) => {
                vec![ResolveChildWorkflowExecutionStart {
                    workflow_id: self.shared_state.attrs.workflow_id.clone(),
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
                    workflow_id: self.shared_state.attrs.workflow_id.clone(),
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
                    workflow_id: self.shared_state.attrs.workflow_id.clone(),
                    result: Some(ChildWorkflowResult {
                        status: Some(ChildWorkflowStatus::Failed(wfr::Failure {
                            failure: Some(failure),
                        })),
                    }),
                }
                .into()]
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

    fn kind(&self) -> MachineKind {
        MachineKind::ChildWorkflow
    }
}

impl TryFrom<CommandType> for ChildWorkflowMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::StartChildWorkflowExecution => Self::CommandStartChildWorkflowExecution,
            _ => return Err(()),
        })
    }
}

/// Cancellation through this machine is valid only when start child workflow command is not sent
/// yet. Cancellation of an initiated child workflow is done through CancelExternal.
/// All of the types besides ABANDON are treated differently.
impl Cancellable for ChildWorkflowMachine {
    fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<Self::Error>> {
        let event = ChildWorkflowMachineEvents::Cancel;
        let vec = OnEventWrapper::on_event_mut(self, event)?;
        let res = vec
            .into_iter()
            .flat_map(|mc| match mc {
                ChildWorkflowCommand::StartCancel(failure) => {
                    vec![ResolveChildWorkflowExecutionStart {
                        workflow_id: self.shared_state.attrs.workflow_id.to_owned(),
                        status: Some(resolve_child_workflow_execution_start::Status::Cancelled(
                            ResolveChildWorkflowExecutionStartCancelled {
                                failure: Some(failure),
                            },
                        )),
                    }
                    .into()]
                }
                ChildWorkflowCommand::Fail(failure) => {
                    vec![ResolveChildWorkflowExecution {
                        workflow_id: self.shared_state.attrs.workflow_id.to_owned(),
                        result: Some(ChildWorkflowResult {
                            status: Some(ChildWorkflowStatus::Failed(wfr::Failure {
                                failure: Some(failure),
                            })),
                        }),
                    }
                    .into()]
                }
                x => panic!("Invalid cancel event response {:?}", x),
            })
            .collect();
        Ok(res)
    }

    fn was_cancelled_before_sent_to_server(&self) -> bool {
        self.shared_state.cancelled_before_sent
    }
}

fn convert_payloads(
    event_info: Option<EventInfo>,
    result: Option<Payloads>,
) -> Result<Option<Payload>, WFMachinesError> {
    result.map(TryInto::try_into).transpose().map_err(|pe| {
        WFMachinesError::Fatal(format!(
            "Not exactly one payload in child workflow result ({}) for event: {}",
            pe,
            event_info.map(|e| e.event.clone()).unwrap_or_default()
        ))
    })
}

#[cfg(test)]
mod test {
    extern crate derive_more;

    use super::*;
    use crate::machines::cancel_external_state_machine::RequestCancelExternalCommandCreated;
    use crate::{
        protos::coresdk::child_workflow::{child_workflow_result, Success},
        protos::coresdk::workflow_activation::resolve_child_workflow_execution_start::Status as StartStatus,
        protos::coresdk::workflow_activation::{
            wf_activation_job, ResolveChildWorkflowExecutionStartSuccess, WfActivationJob,
        },
        prototype_rust_sdk::{WfContext, WorkflowFunction, WorkflowResult},
        test_help::{canned_histories, TestHistoryBuilder},
        workflow::managed_wf::ManagedWFFunc,
    };
    use anyhow::anyhow;
    use rstest::{fixture, rstest};
    use std::time::Duration;

    #[derive(Clone)]
    enum Expectation {
        Success = 0,
        Failure = 1,
        StartFailure = 2,
    }

    impl Expectation {
        fn try_from_u8(x: u8) -> Option<Self> {
            Some(match x {
                0 => Expectation::Success,
                1 => Expectation::Failure,
                2 => Expectation::StartFailure,
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

    async fn parent_wf(mut ctx: WfContext) -> WorkflowResult<()> {
        let expectation = Expectation::try_from_u8(ctx.get_args()[0].data[0]).unwrap();
        let req = StartChildWorkflowExecution {
            workflow_id: "child-id-1".to_string(),
            workflow_type: "child".to_string(),
            input: vec![],
            ..Default::default()
        };
        match (expectation.clone(), ctx.start_child_workflow(req).await) {
            (Expectation::Success | Expectation::Failure, StartStatus::Succeeded(_)) => {}
            (Expectation::StartFailure, StartStatus::Failed(_)) => return Ok(().into()),
            _ => return Err(anyhow!("Unexpected start status")),
        };
        match (
            expectation,
            ctx.child_workflow_result("child-id-1".to_string())
                .await
                .status,
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
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::StartChildWorkflowExecution as i32
        );

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 0);

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().await.commands;
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
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::StartChildWorkflowExecution as i32
        );

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
        wfm.shutdown().await.unwrap();
    }

    async fn cancel_before_send_wf(mut ctx: WfContext) -> WorkflowResult<()> {
        let workflow_id = "child-id-1";
        let start = ctx.start_child_workflow(StartChildWorkflowExecution {
            workflow_id: workflow_id.to_string(),
            workflow_type: "child".to_string(),
            input: vec![],
            ..Default::default()
        });
        ctx.cancel_child_workflow(workflow_id);
        match start.await {
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
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 0);

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CompleteWorkflowExecution as i32
        );
        wfm.shutdown().await.unwrap();
    }
}
