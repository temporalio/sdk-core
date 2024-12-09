use crate::worker::workflow::{
    machines::{
        workflow_machines::MachineResponse, Cancellable, EventInfo, HistEventData,
        NewMachineWithCommand, OnEventWrapper, WFMachinesAdapter,
    },
    WFMachinesError,
};
use itertools::Itertools;
use rustfsm::{fsm, MachineError, StateMachine, TransitionResult};
use temporal_sdk_core_protos::{
    coresdk::{
        nexus::{nexus_operation_result, NexusOperationResult},
        workflow_activation::{
            resolve_nexus_operation_start, ResolveNexusOperation, ResolveNexusOperationStart,
        },
        workflow_commands::ScheduleNexusOperation,
    },
    temporal::api::{
        common::v1::Payload,
        enums::v1::{CommandType, EventType},
        failure::v1::{self as failure, failure::FailureInfo, Failure},
        history::v1::{
            history_event, NexusOperationCanceledEventAttributes,
            NexusOperationCompletedEventAttributes, NexusOperationFailedEventAttributes,
            NexusOperationStartedEventAttributes, NexusOperationTimedOutEventAttributes,
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

    Started
      --(NexusOperationCompleted(NexusOperationCompletedEventAttributes), on_completed)--> Completed;
    Started
      --(NexusOperationFailed(NexusOperationFailedEventAttributes), on_failed)--> Failed;
    Started
      --(NexusOperationCanceled(NexusOperationCanceledEventAttributes), on_canceled)--> Cancelled;
    Started
      --(NexusOperationTimedOut(NexusOperationTimedOutEventAttributes), on_timed_out)--> TimedOut;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum NexusOperationCommand {
    #[display("Start")]
    Start { operation_id: String },
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
}

#[derive(Clone, Debug)]
pub(super) struct SharedState {
    lang_seq_num: u32,
    scheduled_event_id: i64,
    cancelled_before_sent: bool,
}

impl NexusOperationMachine {
    pub(super) fn new_scheduled(attribs: ScheduleNexusOperation) -> NewMachineWithCommand {
        let s = Self::from_parts(
            ScheduleCommandCreated.into(),
            SharedState {
                lang_seq_num: attribs.seq,
                scheduled_event_id: 0,
                cancelled_before_sent: false,
            },
        );
        NewMachineWithCommand {
            command: attribs.into(),
            machine: s.into(),
        }
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
    pub(super) fn on_completed(
        self,
        ca: NexusOperationCompletedEventAttributes,
    ) -> NexusOperationMachineTransition<Completed> {
        NexusOperationMachineTransition::commands([
            NexusOperationCommand::Start {
                operation_id: String::default(),
            },
            NexusOperationCommand::Complete(ca.result),
        ])
    }

    pub(super) fn on_failed(
        self,
        fa: NexusOperationFailedEventAttributes,
    ) -> NexusOperationMachineTransition<Failed> {
        NexusOperationMachineTransition::commands([
            NexusOperationCommand::Start {
                operation_id: String::default(),
            },
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
            NexusOperationCommand::Start {
                operation_id: String::default(),
            },
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
            NexusOperationCommand::Start {
                operation_id: String::default(),
            },
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
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Nexus operation machine does not handle this event: {e:?}"
                )))
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
            NexusOperationCommand::Start { operation_id } => {
                vec![ResolveNexusOperationStart {
                    seq: self.shared_state.lang_seq_num,
                    status: Some(resolve_nexus_operation_start::Status::OperationId(
                        operation_id,
                    )),
                }
                .into()]
            }
            NexusOperationCommand::CancelBeforeStart => {
                vec![
                    ResolveNexusOperationStart {
                        seq: self.shared_state.lang_seq_num,
                        status: Some(resolve_nexus_operation_start::Status::CancelledBeforeStart(
                            Failure {
                                message: "Nexus Operation cancelled before scheduled".to_owned(),
                                cause: Some(Box::new(Failure {
                                    failure_info: Some(FailureInfo::CanceledFailureInfo(
                                        failure::CanceledFailureInfo {
                                            ..Default::default()
                                        },
                                    )),
                                    ..Default::default()
                                })),
                                failure_info: Some(
                                    FailureInfo::NexusOperationExecutionFailureInfo(
                                        failure::NexusOperationFailureInfo {
                                            // TODO: Get from shared state
                                            scheduled_event_id: 0,
                                            endpoint: "".to_string(),
                                            service: "".to_string(),
                                            operation: "".to_string(),
                                            operation_id: "".to_string(),
                                        },
                                    ),
                                ),
                                ..Default::default()
                            },
                        )),
                    }
                    .into(),
                    ResolveNexusOperation {
                        seq: self.shared_state.lang_seq_num,
                        result: Some(NexusOperationResult {
                            status: Some(nexus_operation_result::Status::Cancelled(Failure {
                                // TODO: Construct failure better
                                message: "Nexus operation was cancelled before it was started"
                                    .to_owned(),
                                ..Default::default()
                            })),
                        }),
                    }
                    .into(),
                ]
            }
            NexusOperationCommand::Complete(c) => {
                vec![ResolveNexusOperation {
                    seq: self.shared_state.lang_seq_num,
                    result: Some(NexusOperationResult {
                        status: Some(nexus_operation_result::Status::Completed(
                            c.unwrap_or_default(),
                        )),
                    }),
                }
                .into()]
            }
            NexusOperationCommand::Fail(f) => {
                vec![ResolveNexusOperation {
                    seq: self.shared_state.lang_seq_num,
                    result: Some(NexusOperationResult {
                        status: Some(nexus_operation_result::Status::Failed(f)),
                    }),
                }
                .into()]
            }
            NexusOperationCommand::Cancel(f) => {
                vec![ResolveNexusOperation {
                    seq: self.shared_state.lang_seq_num,
                    result: Some(NexusOperationResult {
                        status: Some(nexus_operation_result::Status::Cancelled(f)),
                    }),
                }
                .into()]
            }
            NexusOperationCommand::TimedOut(f) => {
                vec![ResolveNexusOperation {
                    seq: self.shared_state.lang_seq_num,
                    result: Some(NexusOperationResult {
                        status: Some(nexus_operation_result::Status::TimedOut(f)),
                    }),
                }
                .into()]
            }
        })
    }
}

impl TryFrom<CommandType> for NexusOperationMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::ScheduleNexusOperation => Self::CommandScheduleNexusOperation,
            // CommandType::RequestCancelNexusOperation handled by separate cancel nexus op fsm.
            _ => return Err(()),
        })
    }
}

impl Cancellable for NexusOperationMachine {
    fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<Self::Error>> {
        let event = NexusOperationMachineEvents::Cancel;
        let cmds = OnEventWrapper::on_event_mut(self, event)?;
        let mach_resps = cmds
            .into_iter()
            .map(|mc| self.adapt_response(mc, None))
            .flatten_ok()
            .try_collect()?;
        Ok(mach_resps)
    }

    fn was_cancelled_before_sent_to_server(&self) -> bool {
        self.shared_state.cancelled_before_sent
    }
}
