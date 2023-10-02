use super::{workflow_machines::MachineResponse, EventInfo, WFMachinesAdapter, WFMachinesError};
use crate::{
    protosext::protocol_messages::UpdateRequest,
    worker::workflow::machines::{Cancellable, HistEventData, NewMachineWithResponse},
};
use itertools::Itertools;
use prost::EncodeError;
use rustfsm::{fsm, MachineError, StateMachine, TransitionResult};
use std::{convert::TryFrom, mem};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::DoUpdate,
        workflow_commands::{update_response, UpdateResponse},
    },
    temporal::api::{
        command::v1::{command, ProtocolMessageCommandAttributes},
        common::v1::Payload,
        enums::v1::{CommandType, EventType},
        failure::v1::Failure,
        history::v1::HistoryEvent,
        protocol::v1::Message as ProtocolMessage,
        update::v1::{outcome, Acceptance, Outcome, Rejection, Response},
    },
    utilities::pack_any,
};

fsm! {
    pub(super) name UpdateMachine;
    command UpdateMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    RequestInitiated --(Accept, on_accept)--> Accepted;
    RequestInitiated --(Reject(Failure), on_reject)--> Rejected;

    Accepted --(CommandProtocolMessage)--> AcceptCommandCreated;

    AcceptCommandCreated --(WorkflowExecutionUpdateAccepted)--> AcceptCommandRecorded;
    AcceptCommandRecorded --(Complete(Payload), on_complete)--> Completed;
    AcceptCommandRecorded --(Reject(Failure), on_fail)--> Completed;

    Completed --(CommandProtocolMessage)--> CompletedCommandCreated;
    CompletedCommandCreated --(WorkflowExecutionUpdateCompleted)--> CompletedCommandRecorded;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum UpdateMachineCommand {
    #[display(fmt = "Accept")]
    Accept,
    #[display(fmt = "Reject")]
    Reject(Failure),
    #[display(fmt = "Complete")]
    Complete(Payload),
    #[display(fmt = "Fail")]
    Fail(Failure),
}

#[derive(Clone)]
pub(super) struct SharedState {
    message_id: String,
    instance_id: String,
    event_seq_id: i64,
    request: UpdateRequest,
}

impl UpdateMachine {
    pub(crate) fn new(
        message_id: String,
        instance_id: String,
        event_seq_id: i64,
        mut request: UpdateRequest,
    ) -> NewMachineWithResponse {
        let me = Self::from_parts(
            RequestInitiated {}.into(),
            SharedState {
                message_id,
                instance_id: instance_id.clone(),
                event_seq_id,
                request: request.clone(),
            },
        );
        let do_update = DoUpdate {
            id: mem::replace(&mut request.meta.update_id, "".to_string()),
            protocol_instance_id: instance_id,
            name: request.name,
            input: request.input,
            headers: request.headers,
            meta: Some(request.meta),
        };
        NewMachineWithResponse {
            machine: me.into(),
            // TODO: Shouldn't be sent on replay
            response: MachineResponse::PushWFJob(do_update.into()),
        }
    }

    pub(crate) fn handle_response(
        &mut self,
        resp: UpdateResponse,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        let cmds = match resp.response {
            None => {
                return Err(WFMachinesError::Fatal(format!(
                    "Update response for update {} had an empty result, this is a lang layer bug.",
                    &self.shared_state.request.meta.update_id
                )))
            }
            Some(update_response::Response::Accepted(_)) => {
                self.on_event(UpdateMachineEvents::Accept)
            }
            Some(update_response::Response::Rejected(f)) => {
                self.on_event(UpdateMachineEvents::Reject(f))
            }
            Some(update_response::Response::Completed(p)) => {
                self.on_event(UpdateMachineEvents::Complete(p))
            }
        }
        .map_err(|e| match e {
            MachineError::InvalidTransition => WFMachinesError::Fatal(format!(
                "Invalid transition while handling update response (id {}) in state {}",
                &self.shared_state.request.meta.update_id,
                self.state(),
            )),
            MachineError::Underlying(e) => e,
        })?;
        Ok(cmds
            .into_iter()
            .map(|c| self.adapt_response(c, None))
            .flatten_ok()
            .try_collect()?)
    }

    fn build_command_msg(
        &self,
        outgoing_id: String,
        msg: UpdateMsg,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        let msg = self.build_msg(outgoing_id.clone(), msg)?;
        Ok(vec![
            MachineResponse::IssueNewMessage(msg),
            MachineResponse::IssueNewCommand(
                command::Attributes::ProtocolMessageCommandAttributes(
                    ProtocolMessageCommandAttributes {
                        message_id: outgoing_id,
                    },
                )
                .into(),
            ),
        ])
    }

    fn build_msg(
        &self,
        outgoing_id: String,
        msg: UpdateMsg,
    ) -> Result<ProtocolMessage, WFMachinesError> {
        let accept_body = msg.pack().map_err(|e| {
            WFMachinesError::Fatal(format!("Failed to serialize update response: {:?}", e))
        })?;
        Ok(ProtocolMessage {
            id: outgoing_id.clone(),
            protocol_instance_id: self.shared_state.instance_id.clone(),
            body: Some(accept_body),
            ..Default::default()
        })
    }
}

enum UpdateMsg {
    Accept(Acceptance),
    Reject(Rejection),
    Response(Response),
}
impl UpdateMsg {
    fn pack(self) -> Result<prost_types::Any, EncodeError> {
        match self {
            UpdateMsg::Accept(m) => pack_any(
                "type.googleapis.com/temporal.api.update.v1.Acceptance".to_string(),
                &m,
            ),
            UpdateMsg::Reject(m) => pack_any(
                "type.googleapis.com/temporal.api.update.v1.Rejection".to_string(),
                &m,
            ),
            UpdateMsg::Response(m) => pack_any(
                "type.googleapis.com/temporal.api.update.v1.Response".to_string(),
                &m,
            ),
        }
    }
}

impl TryFrom<HistEventData> for UpdateMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match e.event_type() {
            EventType::WorkflowExecutionUpdateAccepted => {
                UpdateMachineEvents::WorkflowExecutionUpdateAccepted
            }
            EventType::WorkflowExecutionUpdateCompleted => {
                UpdateMachineEvents::WorkflowExecutionUpdateCompleted
            }
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Update machine does not handle this event: {e}"
                )))
            }
        })
    }
}

impl WFMachinesAdapter for UpdateMachine {
    fn adapt_response(
        &self,
        my_command: UpdateMachineCommand,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            UpdateMachineCommand::Accept => self.build_command_msg(
                format!("{}/accept", self.shared_state.message_id),
                UpdateMsg::Accept(Acceptance {
                    accepted_request_message_id: self.shared_state.message_id.clone(),
                    accepted_request_sequencing_event_id: self.shared_state.event_seq_id,
                    ..Default::default()
                }),
            )?,
            UpdateMachineCommand::Reject(fail) => {
                vec![MachineResponse::IssueNewMessage(self.build_msg(
                    format!("{}/reject", self.shared_state.message_id),
                    UpdateMsg::Reject(Rejection {
                        rejected_request_message_id: self.shared_state.message_id.clone(),
                        rejected_request_sequencing_event_id: self.shared_state.event_seq_id,
                        failure: Some(fail),
                        ..Default::default()
                    }),
                )?)]
            }
            UpdateMachineCommand::Complete(p) => self.build_command_msg(
                format!("{}/complete", self.shared_state.message_id),
                UpdateMsg::Response(Response {
                    meta: Some(self.shared_state.request.meta.clone()),
                    outcome: Some(Outcome {
                        value: Some(outcome::Value::Success(p.into())),
                    }),
                }),
            )?,
            UpdateMachineCommand::Fail(f) => self.build_command_msg(
                format!("{}/complete", self.shared_state.message_id),
                UpdateMsg::Response(Response {
                    meta: Some(self.shared_state.request.meta.clone()),
                    outcome: Some(Outcome {
                        value: Some(outcome::Value::Failure(f)),
                    }),
                }),
            )?,
        })
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        matches!(
            event.event_type(),
            EventType::WorkflowExecutionUpdateAccepted
                | EventType::WorkflowExecutionUpdateRejected
                | EventType::WorkflowExecutionUpdateCompleted
        )
    }
}

impl TryFrom<CommandType> for UpdateMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::ProtocolMessage => UpdateMachineEvents::CommandProtocolMessage,
            _ => return Err(()),
        })
    }
}

#[derive(Default, Clone)]
pub(super) struct RequestInitiated {}
impl RequestInitiated {
    fn on_accept(self) -> UpdateMachineTransition<Accepted> {
        UpdateMachineTransition::commands([UpdateMachineCommand::Accept])
    }
    fn on_reject(self, fail: Failure) -> UpdateMachineTransition<Rejected> {
        UpdateMachineTransition::commands([UpdateMachineCommand::Reject(fail)])
    }
}

#[derive(Default, Clone)]
pub(super) struct Accepted {}
impl From<RequestInitiated> for Accepted {
    fn from(_: RequestInitiated) -> Self {
        Accepted {}
    }
}

#[derive(Default, Clone)]
pub(super) struct AcceptCommandCreated {}
impl From<Accepted> for AcceptCommandCreated {
    fn from(_: Accepted) -> Self {
        AcceptCommandCreated {}
    }
}

#[derive(Default, Clone)]
pub(super) struct AcceptCommandRecorded {}
impl AcceptCommandRecorded {
    fn on_complete(self, p: Payload) -> UpdateMachineTransition<Completed> {
        UpdateMachineTransition::commands([UpdateMachineCommand::Complete(p.into())])
    }
    fn on_fail(self, f: Failure) -> UpdateMachineTransition<Completed> {
        UpdateMachineTransition::commands([UpdateMachineCommand::Fail(f)])
    }
}
impl From<AcceptCommandCreated> for AcceptCommandRecorded {
    fn from(_: AcceptCommandCreated) -> Self {
        AcceptCommandRecorded {}
    }
}

#[derive(Default, Clone)]
pub(super) struct Completed {}
impl From<AcceptCommandRecorded> for Completed {
    fn from(_: AcceptCommandRecorded) -> Self {
        Completed {}
    }
}

#[derive(Default, Clone)]
pub(super) struct CompletedCommandCreated {}
impl From<Completed> for CompletedCommandCreated {
    fn from(_: Completed) -> Self {
        CompletedCommandCreated {}
    }
}

#[derive(Default, Clone)]
pub(super) struct CompletedCommandRecorded {}
impl From<CompletedCommandCreated> for CompletedCommandRecorded {
    fn from(_: CompletedCommandCreated) -> Self {
        CompletedCommandRecorded {}
    }
}

#[derive(Default, Clone)]
pub(super) struct Rejected {}
impl From<RequestInitiated> for Rejected {
    fn from(_: RequestInitiated) -> Self {
        Rejected {}
    }
}

impl Cancellable for UpdateMachine {}
