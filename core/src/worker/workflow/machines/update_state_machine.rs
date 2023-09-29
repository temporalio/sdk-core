use super::{workflow_machines::MachineResponse, EventInfo, WFMachinesAdapter, WFMachinesError};
use crate::{
    protosext::protocol_messages::UpdateRequest,
    worker::workflow::machines::{Cancellable, HistEventData, NewMachineWithResponse},
};
use itertools::Itertools;
use rustfsm::{fsm, MachineError, StateMachine, TransitionResult};
use std::{convert::TryFrom, mem};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::DoUpdate,
        workflow_commands::{update_response, UpdateResponse},
    },
    temporal::api::{
        command::v1::{command, ProtocolMessageCommandAttributes},
        common::v1::Payloads,
        enums::v1::{CommandType, EventType},
        failure::v1::Failure,
        history::v1::HistoryEvent,
        protocol::v1::Message as ProtocolMessage,
        update::v1::Acceptance,
    },
    utilities::pack_any,
};

fsm! {
    pub(super) name UpdateMachine;
    command UpdateMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    RequestInitiated --(Accept, on_accept)--> Accepted;

    Accepted --(CommandProtocolMessage)--> AcceptCommandCreated;

    AcceptCommandCreated --(WorkflowExecutionUpdateAccepted)--> AcceptCommandRecorded;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum UpdateMachineCommand {
    #[display(fmt = "Accept")]
    Accept,
    #[display(fmt = "Reject")]
    Reject,
    #[display(fmt = "Complete")]
    Complete(Option<Payloads>),
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
            Some(update_response::Response::Rejected(_)) => {
                todo!()
            }
            Some(update_response::Response::Completed(_)) => {
                todo!()
            }
        }
        .map_err(|e| match e {
            MachineError::InvalidTransition => WFMachinesError::Fatal(format!(
                "Invalid transition while handling update response (id {}): \
                 response: {:?} in state {}",
                &self.shared_state.request.meta.update_id,
                resp,
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
}

impl TryFrom<HistEventData> for UpdateMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let last_task_in_history = e.current_task_is_last_in_history;
        let e = e.event;
        Ok(match e.event_type() {
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
            UpdateMachineCommand::Accept => {
                let message_id = format!("{}/accept", self.shared_state.message_id);
                let accept_body = pack_any(
                    "type.googleapis.com/temporal.api.update.v1.Acceptance".to_string(),
                    &Acceptance {
                        accepted_request_message_id: self.shared_state.message_id.clone(),
                        accepted_request_sequencing_event_id: self.shared_state.event_seq_id,
                        ..Default::default()
                    },
                )
                .map_err(|e| {
                    WFMachinesError::Fatal(format!(
                        "Failed to serialize update acceptance: {:?}",
                        e
                    ))
                })?;
                vec![
                    MachineResponse::IssueNewMessage(ProtocolMessage {
                        id: message_id.clone(),
                        protocol_instance_id: self.shared_state.instance_id.clone(),
                        body: Some(accept_body),
                        ..Default::default()
                    }),
                    MachineResponse::IssueNewCommand(
                        command::Attributes::ProtocolMessageCommandAttributes(
                            ProtocolMessageCommandAttributes { message_id },
                        )
                        .into(),
                    ),
                ]
            }
            UpdateMachineCommand::Reject => {
                todo!()
            }
            UpdateMachineCommand::Complete(_) => {
                todo!()
            }
            UpdateMachineCommand::Fail(_) => {
                todo!()
            }
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
impl From<AcceptCommandCreated> for AcceptCommandRecorded {
    fn from(_: AcceptCommandCreated) -> Self {
        AcceptCommandRecorded {}
    }
}

impl Cancellable for UpdateMachine {}
