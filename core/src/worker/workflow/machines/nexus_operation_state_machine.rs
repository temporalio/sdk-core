use crate::worker::workflow::{
    WFMachinesError,
    machines::{
        EventInfo, HistEventData, NewMachineWithCommand, OnEventWrapper, WFMachinesAdapter,
        workflow_machines::MachineResponse,
    },
};
use itertools::Itertools;
use rustfsm::{MachineError, StateMachine, TransitionResult, fsm};
use temporal_sdk_core_protos::{
    coresdk::{
        nexus::{NexusOperationResult, nexus_operation_result},
        workflow_activation::{
            ResolveNexusOperation, ResolveNexusOperationStart, resolve_nexus_operation_start,
        },
        workflow_commands::ScheduleNexusOperation,
    },
    temporal::api::{
        command::v1::{RequestCancelNexusOperationCommandAttributes, command},
        common::v1::Payload,
        enums::v1::{CommandType, EventType},
        failure::v1::{self as failure, Failure, failure::FailureInfo},
        history::v1::{
            NexusOperationCanceledEventAttributes, NexusOperationCompletedEventAttributes,
            NexusOperationFailedEventAttributes, NexusOperationStartedEventAttributes,
            NexusOperationTimedOutEventAttributes, history_event,
        },
    },
};

fsm! {
    pub(super) name NexusOperationMachine;
    command NexusOperationCommand;
    error WFMachinesError;
    shared_state SharedState;

    ScheduleCommandCreated --(CommandScheduleNexusOperation)--> ScheduleCommandCreated;
    ScheduleCommandCreated
      --(NexusOperationScheduled(NexusOpScheduledData), shared on_scheduled)--> ScheduledEventRecorded;
    ScheduleCommandCreated --(Cancel, shared on_cancelled)--> Cancelled;

    ScheduledEventRecorded --(Cancel, shared on_issue_cancel)--> ScheduledEventRecorded;
    ScheduledEventRecorded --(CommandRequestCancelNexusOperation)--> ScheduledEventRecorded;
    ScheduledEventRecorded --(NexusOperationCancelRequested)--> ScheduledEventRecorded;
    ScheduledEventRecorded
      --(NexusOperationCompleted(NexusOperationCompletedEventAttributes), on_completed)--> Completed;
    ScheduledEventRecorded
      --(NexusOperationFailed(NexusOperationFailedEventAttributes), on_failed)--> Failed;
    ScheduledEventRecorded
      --(NexusOperationCanceled(NexusOperationCanceledEventAttributes), on_canceled)--> Cancelled;
    ScheduledEventRecorded
      --(NexusOperationTimedOut(NexusOperationTimedOutEventAttributes), on_timed_out)--> TimedOut;
    ScheduledEventRecorded
      --(NexusOperationStarted(NexusOperationStartedEventAttributes), on_started)--> Started;

    Started --(Cancel, shared on_issue_cancel)--> Started;
    Started --(CommandRequestCancelNexusOperation)--> Started;
    Started --(NexusOperationCancelRequested)--> Started;
    Started
      --(NexusOperationCompleted(NexusOperationCompletedEventAttributes), on_completed)--> Completed;
    Started
      --(NexusOperationFailed(NexusOperationFailedEventAttributes), on_failed)--> Failed;
    Started
      --(NexusOperationCanceled(NexusOperationCanceledEventAttributes), on_canceled)--> Cancelled;
    Started
      --(NexusOperationTimedOut(NexusOperationTimedOutEventAttributes), on_timed_out)--> TimedOut;

    // Ignore cancels in all terminal states
    Completed --(Cancel)--> Completed;
    Failed --(Cancel)--> Failed;
    Cancelled --(Cancel)--> Cancelled;
    TimedOut --(Cancel)--> TimedOut;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum NexusOperationCommand {
    #[display("Start")]
    Start { operation_id: String },
    #[display("StartSync")]
    StartSync,
    #[display("CancelBeforeStart")]
    CancelBeforeStart,
    #[display("Complete")]
    Complete(Option<Payload>),
    #[display("Fail")]
    Fail(Failure),
    #[display("Cancel")]
    Cancel(Failure),
    #[display("TimedOut")]
    TimedOut(Failure),
    #[display("IssueCancel")]
    IssueCancel,
}

#[derive(Clone, Debug)]
pub(super) struct SharedState {
    lang_seq_num: u32,
    pub(super) scheduled_event_id: i64,
    endpoint: String,
    service: String,
    operation: String,

    cancelled_before_sent: bool,
    cancel_sent: bool,
}

impl NexusOperationMachine {
    pub(super) fn new_scheduled(attribs: ScheduleNexusOperation) -> NewMachineWithCommand {
        let s = Self::from_parts(
            ScheduleCommandCreated.into(),
            SharedState {
                lang_seq_num: attribs.seq,
                scheduled_event_id: 0,
                endpoint: attribs.endpoint.clone(),
                service: attribs.service.clone(),
                operation: attribs.operation.clone(),
                cancelled_before_sent: false,
                cancel_sent: false,
            },
        );
        NewMachineWithCommand {
            command: attribs.into(),
            machine: s.into(),
        }
    }

    pub(super) fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<WFMachinesError>> {
        let event = NexusOperationMachineEvents::Cancel;
        let cmds = OnEventWrapper::on_event_mut(self, event)?;
        let mach_resps = cmds
            .into_iter()
            .map(|mc| self.adapt_response(mc, None))
            .flatten_ok()
            .try_collect()?;
        Ok(mach_resps)
    }

    pub(super) fn was_cancelled_before_sent_to_server(&self) -> bool {
        self.shared_state.cancelled_before_sent
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduleCommandCreated;

pub(super) struct NexusOpScheduledData {
    event_id: i64,
}

impl ScheduleCommandCreated {
    pub(super) fn on_scheduled(
        self,
        state: &mut SharedState,
        event_dat: NexusOpScheduledData,
    ) -> NexusOperationMachineTransition<ScheduledEventRecorded> {
        state.scheduled_event_id = event_dat.event_id;
        NexusOperationMachineTransition::default()
    }

    pub(super) fn on_cancelled(
        self,
        state: &mut SharedState,
    ) -> NexusOperationMachineTransition<Cancelled> {
        state.cancelled_before_sent = true;
        NexusOperationMachineTransition::commands([NexusOperationCommand::CancelBeforeStart])
    }
}

#[derive(Default, Clone)]
pub(super) struct ScheduledEventRecorded;

impl ScheduledEventRecorded {
    pub(crate) fn on_issue_cancel(
        &self,
        ss: &mut SharedState,
    ) -> NexusOperationMachineTransition<ScheduledEventRecorded> {
        if !ss.cancel_sent {
            ss.cancel_sent = true;
            NexusOperationMachineTransition::commands([NexusOperationCommand::IssueCancel])
        } else {
            NexusOperationMachineTransition::default()
        }
    }

    pub(super) fn on_completed(
        self,
        ca: NexusOperationCompletedEventAttributes,
    ) -> NexusOperationMachineTransition<Completed> {
        NexusOperationMachineTransition::commands([
            NexusOperationCommand::StartSync,
            NexusOperationCommand::Complete(ca.result),
        ])
    }

    pub(super) fn on_failed(
        self,
        fa: NexusOperationFailedEventAttributes,
    ) -> NexusOperationMachineTransition<Failed> {
        NexusOperationMachineTransition::commands([
            NexusOperationCommand::StartSync,
            NexusOperationCommand::Fail(fa.failure.unwrap_or_else(|| Failure {
                message: "Nexus operation failed but failure field was not populated".to_owned(),
                ..Default::default()
            })),
        ])
    }

    pub(super) fn on_canceled(
        self,
        ca: NexusOperationCanceledEventAttributes,
    ) -> NexusOperationMachineTransition<Cancelled> {
        NexusOperationMachineTransition::commands([
            NexusOperationCommand::StartSync,
            NexusOperationCommand::Cancel(ca.failure.unwrap_or_else(|| Failure {
                message:
                    "Nexus operation was cancelled but failure field was not populated".to_owned(),
                ..Default::default()
            })),
        ])
    }

    pub(super) fn on_timed_out(
        self,
        toa: NexusOperationTimedOutEventAttributes,
    ) -> NexusOperationMachineTransition<TimedOut> {
        NexusOperationMachineTransition::commands([
            NexusOperationCommand::StartSync,
            NexusOperationCommand::TimedOut(toa.failure.unwrap_or_else(|| Failure {
                message: "Nexus operation timed out but failure field was not populated".to_owned(),
                ..Default::default()
            })),
        ])
    }

    pub(super) fn on_started(
        self,
        sa: NexusOperationStartedEventAttributes,
    ) -> NexusOperationMachineTransition<Started> {
        NexusOperationMachineTransition::commands([NexusOperationCommand::Start {
            operation_id: sa.operation_id,
        }])
    }
}

#[derive(Default, Clone)]
pub(super) struct Started;

impl Started {
    pub(crate) fn on_issue_cancel(
        &self,
        ss: &mut SharedState,
    ) -> NexusOperationMachineTransition<Started> {
        if !ss.cancel_sent {
            ss.cancel_sent = true;
            NexusOperationMachineTransition::commands([NexusOperationCommand::IssueCancel])
        } else {
            NexusOperationMachineTransition::default()
        }
    }

    pub(super) fn on_completed(
        self,
        ca: NexusOperationCompletedEventAttributes,
    ) -> NexusOperationMachineTransition<Completed> {
        NexusOperationMachineTransition::commands([NexusOperationCommand::Complete(ca.result)])
    }

    pub(super) fn on_failed(
        self,
        fa: NexusOperationFailedEventAttributes,
    ) -> NexusOperationMachineTransition<Failed> {
        NexusOperationMachineTransition::commands([NexusOperationCommand::Fail(
            fa.failure.unwrap_or_else(|| Failure {
                message: "Nexus operation failed but failure field was not populated".to_owned(),
                ..Default::default()
            }),
        )])
    }

    pub(super) fn on_canceled(
        self,
        ca: NexusOperationCanceledEventAttributes,
    ) -> NexusOperationMachineTransition<Cancelled> {
        NexusOperationMachineTransition::commands([NexusOperationCommand::Cancel(
            ca.failure.unwrap_or_else(|| Failure {
                message: "Nexus operation was cancelled but failure field was not populated"
                    .to_owned(),
                ..Default::default()
            }),
        )])
    }

    pub(super) fn on_timed_out(
        self,
        toa: NexusOperationTimedOutEventAttributes,
    ) -> NexusOperationMachineTransition<TimedOut> {
        NexusOperationMachineTransition::commands([NexusOperationCommand::TimedOut(
            toa.failure.unwrap_or_else(|| Failure {
                message: "Nexus operation timed out but failure field was not populated".to_owned(),
                ..Default::default()
            }),
        )])
    }
}

#[derive(Default, Clone)]
pub(super) struct Completed;

#[derive(Default, Clone)]
pub(super) struct Failed;

#[derive(Default, Clone)]
pub(super) struct TimedOut;

#[derive(Default, Clone)]
pub(super) struct Cancelled;

impl TryFrom<HistEventData> for NexusOperationMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match EventType::try_from(e.event_type) {
            Ok(EventType::NexusOperationScheduled) => {
                if let Some(history_event::Attributes::NexusOperationScheduledEventAttributes(_)) =
                    e.attributes
                {
                    Self::NexusOperationScheduled(NexusOpScheduledData {
                        event_id: e.event_id,
                    })
                } else {
                    return Err(WFMachinesError::Nondeterminism(
                        "NexusOperationScheduled attributes were unset or malformed".to_string(),
                    ));
                }
            }
            Ok(EventType::NexusOperationStarted) => {
                if let Some(history_event::Attributes::NexusOperationStartedEventAttributes(sa)) =
                    e.attributes
                {
                    Self::NexusOperationStarted(sa)
                } else {
                    return Err(WFMachinesError::Nondeterminism(
                        "NexusOperationStarted attributes were unset or malformed".to_string(),
                    ));
                }
            }
            Ok(EventType::NexusOperationCompleted) => {
                if let Some(history_event::Attributes::NexusOperationCompletedEventAttributes(ca)) =
                    e.attributes
                {
                    Self::NexusOperationCompleted(ca)
                } else {
                    return Err(WFMachinesError::Nondeterminism(
                        "NexusOperationCompleted attributes were unset or malformed".to_string(),
                    ));
                }
            }
            Ok(EventType::NexusOperationFailed) => {
                if let Some(history_event::Attributes::NexusOperationFailedEventAttributes(fa)) =
                    e.attributes
                {
                    Self::NexusOperationFailed(fa)
                } else {
                    return Err(WFMachinesError::Nondeterminism(
                        "NexusOperationFailed attributes were unset or malformed".to_string(),
                    ));
                }
            }
            Ok(EventType::NexusOperationCanceled) => {
                if let Some(history_event::Attributes::NexusOperationCanceledEventAttributes(ca)) =
                    e.attributes
                {
                    Self::NexusOperationCanceled(ca)
                } else {
                    return Err(WFMachinesError::Nondeterminism(
                        "NexusOperationCanceled attributes were unset or malformed".to_string(),
                    ));
                }
            }
            Ok(EventType::NexusOperationTimedOut) => {
                if let Some(history_event::Attributes::NexusOperationTimedOutEventAttributes(toa)) =
                    e.attributes
                {
                    Self::NexusOperationTimedOut(toa)
                } else {
                    return Err(WFMachinesError::Nondeterminism(
                        "NexusOperationTimedOut attributes were unset or malformed".to_string(),
                    ));
                }
            }
            Ok(EventType::NexusOperationCancelRequested) => Self::NexusOperationCancelRequested,
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Nexus operation machine does not handle this event: {e:?}"
                )));
            }
        })
    }
}

impl WFMachinesAdapter for NexusOperationMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        _: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            NexusOperationCommand::StartSync => {
                vec![
                    ResolveNexusOperationStart {
                        seq: self.shared_state.lang_seq_num,
                        status: Some(resolve_nexus_operation_start::Status::StartedSync(true)),
                    }
                    .into(),
                ]
            }
            NexusOperationCommand::Start { operation_id } => {
                vec![
                    ResolveNexusOperationStart {
                        seq: self.shared_state.lang_seq_num,
                        status: Some(resolve_nexus_operation_start::Status::OperationId(
                            operation_id,
                        )),
                    }
                    .into(),
                ]
            }
            NexusOperationCommand::CancelBeforeStart => {
                vec![
                    ResolveNexusOperationStart {
                        seq: self.shared_state.lang_seq_num,
                        status: Some(resolve_nexus_operation_start::Status::CancelledBeforeStart(
                            self.cancelled_failure(
                                "Nexus Operation cancelled before scheduled".to_owned(),
                            ),
                        )),
                    }
                    .into(),
                    ResolveNexusOperation {
                        seq: self.shared_state.lang_seq_num,
                        result: Some(NexusOperationResult {
                            status: Some(nexus_operation_result::Status::Cancelled(
                                self.cancelled_failure(
                                    "Nexus Operation cancelled before scheduled".to_owned(),
                                ),
                            )),
                        }),
                    }
                    .into(),
                ]
            }
            NexusOperationCommand::Complete(c) => {
                vec![
                    ResolveNexusOperation {
                        seq: self.shared_state.lang_seq_num,
                        result: Some(NexusOperationResult {
                            status: Some(nexus_operation_result::Status::Completed(
                                c.unwrap_or_default(),
                            )),
                        }),
                    }
                    .into(),
                ]
            }
            NexusOperationCommand::Fail(f) => {
                vec![
                    ResolveNexusOperation {
                        seq: self.shared_state.lang_seq_num,
                        result: Some(NexusOperationResult {
                            status: Some(nexus_operation_result::Status::Failed(f)),
                        }),
                    }
                    .into(),
                ]
            }
            NexusOperationCommand::Cancel(f) => {
                vec![
                    ResolveNexusOperation {
                        seq: self.shared_state.lang_seq_num,
                        result: Some(NexusOperationResult {
                            status: Some(nexus_operation_result::Status::Cancelled(f)),
                        }),
                    }
                    .into(),
                ]
            }
            NexusOperationCommand::TimedOut(f) => {
                vec![
                    ResolveNexusOperation {
                        seq: self.shared_state.lang_seq_num,
                        result: Some(NexusOperationResult {
                            status: Some(nexus_operation_result::Status::TimedOut(f)),
                        }),
                    }
                    .into(),
                ]
            }
            NexusOperationCommand::IssueCancel => {
                vec![MachineResponse::IssueNewCommand(
                    command::Attributes::RequestCancelNexusOperationCommandAttributes(
                        RequestCancelNexusOperationCommandAttributes {
                            scheduled_event_id: self.shared_state.scheduled_event_id,
                        },
                    )
                    .into(),
                )]
            }
        })
    }
}

impl TryFrom<CommandType> for NexusOperationMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::ScheduleNexusOperation => Self::CommandScheduleNexusOperation,
            CommandType::RequestCancelNexusOperation => Self::CommandRequestCancelNexusOperation,
            _ => return Err(()),
        })
    }
}

impl NexusOperationMachine {
    fn cancelled_failure(&self, message: String) -> Failure {
        Failure {
            message,
            cause: Some(Box::new(Failure {
                failure_info: Some(FailureInfo::CanceledFailureInfo(Default::default())),
                ..Default::default()
            })),
            failure_info: Some(FailureInfo::NexusOperationExecutionFailureInfo(
                failure::NexusOperationFailureInfo {
                    scheduled_event_id: self.shared_state.scheduled_event_id,
                    endpoint: self.shared_state.endpoint.clone(),
                    service: self.shared_state.service.clone(),
                    operation: self.shared_state.operation.clone(),
                    operation_id: "".to_string(),
                    operation_token: "".to_string(),
                },
            )),
            ..Default::default()
        }
    }
}
