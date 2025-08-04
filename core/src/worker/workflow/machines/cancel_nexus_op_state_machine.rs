use super::{
    workflow_machines::MachineResponse, EventInfo, NewMachineWithCommand,
    WFMachinesAdapter, WFMachinesError,
};
use crate::worker::workflow::machines::HistEventData;
use rustfsm::{fsm, StateMachine, TransitionResult};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    coresdk::{
        nexus::{NexusOperationCancellationType, NexusOperationResult, nexus_operation_result},
        workflow_activation::ResolveNexusOperation,
    },
    temporal::api::{
        command::v1::{command, RequestCancelNexusOperationCommandAttributes},
        enums::v1::{CommandType, EventType},
        failure::v1::{self as failure, Failure, failure::FailureInfo},
        history::v1::history_event,
    },
};

fsm! {
    pub(super)
    name CancelNexusOpMachine;
    command CancelNexusOpCommand;
    error WFMachinesError;
    shared_state SharedState;

    RequestCancelNexusOpCommandCreated --(CommandRequestCancelNexusOpWorkflowExecution)
      --> RequestCancelNexusOpCommandCreated;

    RequestCancelNexusOpCommandCreated --(NexusOpCancelRequested, on_cancel_requested)
      --> CancelRequested;

    CancelRequested --(NexusOpCancelRequestCompleted, on_cancel_completed)
      --> CancelCompleted;

    CancelRequested --(NexusOpCancelRequestFailed(temporal_sdk_core_protos::temporal::api::failure::v1::Failure), on_cancel_failed)
      --> CancelFailed;
}

#[derive(Default, Clone)]
pub(super) struct SharedState {
    seq: u32,
    nexus_op_seq: u32,
    cancel_type: NexusOperationCancellationType,
    nexus_op_scheduled_event_id: i64,
    endpoint: String,
    service: String,
    operation: String,
    operation_token: Option<String>,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum CancelNexusOpCommand {
    Requested,
    Completed,
    Failed(temporal_sdk_core_protos::temporal::api::failure::v1::Failure),
}

pub(super) fn new_nexus_op_cancel(
    seq: u32,
    nexus_op_seq: u32,
    nexus_op_scheduled_event_id: i64,
    cancel_type: NexusOperationCancellationType,
    endpoint: String,
    service: String,
    operation: String,
    operation_token: Option<String>,
) -> NewMachineWithCommand {
    let s = CancelNexusOpMachine::from_parts(
        RequestCancelNexusOpCommandCreated {}.into(),
        SharedState { 
            seq, 
            nexus_op_seq, 
            cancel_type,
            nexus_op_scheduled_event_id,
            endpoint,
            service,
            operation,
            operation_token,
        },
    );
    let cmd_attrs = command::Attributes::RequestCancelNexusOperationCommandAttributes(
        RequestCancelNexusOperationCommandAttributes {
            scheduled_event_id: nexus_op_scheduled_event_id,
        },
    );
    NewMachineWithCommand {
        command: cmd_attrs,
        machine: s.into(),
    }
}

#[derive(Default, Clone)]
pub(super) struct CancelRequested {}

#[derive(Default, Clone)]
pub(super) struct CancelCompleted {}

#[derive(Default, Clone)]
pub(super) struct CancelFailed {}

#[derive(Default, Clone)]
pub(super) struct RequestCancelNexusOpCommandCreated {}

impl RequestCancelNexusOpCommandCreated {
    pub(super) fn on_cancel_requested(self) -> CancelNexusOpMachineTransition<CancelRequested> {
        TransitionResult::commands(vec![CancelNexusOpCommand::Requested])
    }
}

impl CancelRequested {
    pub(super) fn on_cancel_completed(self) -> CancelNexusOpMachineTransition<CancelCompleted> {
        TransitionResult::commands(vec![CancelNexusOpCommand::Completed])
    }

    pub(super) fn on_cancel_failed(
        self,
        failure: temporal_sdk_core_protos::temporal::api::failure::v1::Failure,
    ) -> CancelNexusOpMachineTransition<CancelFailed> {
        TransitionResult::commands(vec![CancelNexusOpCommand::Failed(failure)])
    }
}

impl TryFrom<HistEventData> for CancelNexusOpMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match e.event_type() {
            EventType::NexusOperationCancelRequested => Self::NexusOpCancelRequested,
            EventType::NexusOperationCancelRequestCompleted => Self::NexusOpCancelRequestCompleted,
            EventType::NexusOperationCancelRequestFailed => {
                if let Some(history_event::Attributes::NexusOperationCancelRequestFailedEventAttributes(attrs)) = e.attributes {
                    use temporal_sdk_core_protos::temporal::api::failure::v1::Failure;
                    let failure = attrs.failure.unwrap_or_else(|| Failure {
                        message: "Nexus operation cancel request failed but failure field was not populated".to_owned(),
                        ..Default::default()
                    });
                    Self::NexusOpCancelRequestFailed(failure)
                } else {
                    return Err(WFMachinesError::Nondeterminism(
                        "NexusOperationCancelRequestFailed attributes were unset or malformed".to_string(),
                    ));
                }
            }
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Cancel nexus op machine does not handle this event: {e}"
                )))
            }
        })
    }
}

impl TryFrom<CommandType> for CancelNexusOpMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::RequestCancelNexusOperation => {
                Self::CommandRequestCancelNexusOpWorkflowExecution
            }
            _ => return Err(()),
        })
    }
}

impl WFMachinesAdapter for CancelNexusOpMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            CancelNexusOpCommand::Requested => {
                vec![]
            }
            CancelNexusOpCommand::Completed => {
                if self.shared_state.cancel_type == NexusOperationCancellationType::WaitCancellationRequested {
                    vec![MachineResponse::PushWFJob(
                        ResolveNexusOperation {
                            seq: self.shared_state.nexus_op_seq,
                            result: Some(NexusOperationResult {
                                status: Some(nexus_operation_result::Status::Cancelled(
                                    Failure {
                                        message: "nexus operation completed unsuccessfully".to_string(),
                                        cause: Some(Box::new(Failure {
                                            message: "operation canceled".to_string(),
                                            failure_info: Some(FailureInfo::CanceledFailureInfo(
                                                failure::CanceledFailureInfo {
                                                    details: None,
                                                }
                                            )),
                                            ..Default::default()
                                        })),
                                        failure_info: Some(FailureInfo::NexusOperationExecutionFailureInfo(
                                            failure::NexusOperationFailureInfo {
                                                scheduled_event_id: self.shared_state.nexus_op_scheduled_event_id,
                                                endpoint: self.shared_state.endpoint.clone(),
                                                service: self.shared_state.service.clone(),
                                                operation: self.shared_state.operation.clone(),
                                                operation_token: self.shared_state.operation_token.clone().unwrap_or_default(),
                                                ..Default::default()
                                            },
                                        )),
                                        ..Default::default()
                                    }
                                )),
                            }),
                        }
                        .into(),
                    )]
                } else {
                    vec![]
                }
            }
            CancelNexusOpCommand::Failed(_failure) => {
                vec![]
            }
        })
    }
}


