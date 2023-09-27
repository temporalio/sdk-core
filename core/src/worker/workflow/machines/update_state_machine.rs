use super::{workflow_machines::MachineResponse, EventInfo, WFMachinesAdapter, WFMachinesError};
use crate::worker::workflow::machines::HistEventData;
use rustfsm::{fsm, StateMachine, TransitionResult};
use std::convert::TryFrom;
use temporal_sdk_core_protos::temporal::api::{
    common::v1::Payloads,
    enums::v1::{CommandType, EventType},
    failure::v1::Failure,
    history::v1::HistoryEvent,
};

fsm! {
    pub(super) name UpdateMachine;
    command UpdateMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    RequestInitiated --(Accept)--> Accepted;
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

impl UpdateMachine {}

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
        event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        // Ok(match my_command {
        //     UpdateMachineCommand::Accept => {}
        //     UpdateMachineCommand::Reject => {}
        //     UpdateMachineCommand::Complete(_) => {}
        //     UpdateMachineCommand::Fail(_) => {}
        // })
        todo!()
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
            CommandType::ProtocolMessage => todo!(),
            _ => return Err(()),
        })
    }
}

#[derive(Clone)]
pub(super) struct SharedState {}

#[derive(Default, Clone)]
pub(super) struct RequestInitiated {}

#[derive(Default, Clone)]
pub(super) struct Accepted {}
impl From<RequestInitiated> for Accepted {
    fn from(_: RequestInitiated) -> Self {
        Accepted {}
    }
}
