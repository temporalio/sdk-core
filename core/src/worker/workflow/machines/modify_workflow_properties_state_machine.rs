use super::{
    workflow_machines::{MachineResponse, WFMachinesError},
    NewMachineWithCommand,
};
use crate::worker::workflow::machines::{Cancellable, EventInfo, WFMachinesAdapter};
use rustfsm::{fsm, TransitionResult};
use temporal_sdk_core_protos::{
    coresdk::workflow_commands::ModifyWorkflowProperties,
    temporal::api::{
        command::v1::Command,
        enums::v1::{CommandType, EventType},
        history::v1::HistoryEvent,
    },
};

fsm! {
    pub(super) name ModifyWorkflowPropertiesMachine;
    command ModifyWorkflowPropertiesMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(CommandScheduled) --> CommandIssued;
    CommandIssued --(CommandRecorded) --> Done;
}

/// Instantiates an ModifyWorkflowPropertiesMachine and packs it together with the command to
/// be sent to server.
pub(super) fn modify_workflow_properties(
    lang_cmd: ModifyWorkflowProperties,
) -> NewMachineWithCommand {
    let sm = ModifyWorkflowPropertiesMachine {
        state: Created {}.into(),
        shared_state: (),
    };
    let cmd = Command {
        command_type: CommandType::ModifyWorkflowProperties as i32,
        attributes: Some(lang_cmd.into()),
    };
    NewMachineWithCommand {
        command: cmd,
        machine: sm.into(),
    }
}

type SharedState = ();

#[derive(Debug, derive_more::Display)]
pub(super) enum ModifyWorkflowPropertiesMachineCommand {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Created {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct CommandIssued {}

#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Done {}

impl WFMachinesAdapter for ModifyWorkflowPropertiesMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, Self::Error> {
        Err(Self::Error::Nondeterminism(
            "ModifyWorkflowProperties does not use state machine commands".to_string(),
        ))
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        matches!(event.event_type(), EventType::WorkflowPropertiesModified)
    }
}

impl Cancellable for ModifyWorkflowPropertiesMachine {}

impl TryFrom<HistoryEvent> for ModifyWorkflowPropertiesMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        match e.event_type() {
            EventType::UpsertWorkflowSearchAttributes => {
                Ok(ModifyWorkflowPropertiesMachineEvents::CommandRecorded)
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "ModifyWorkflowPropertiesMachine does not handle {e}"
            ))),
        }
    }
}

impl TryFrom<CommandType> for ModifyWorkflowPropertiesMachineEvents {
    type Error = WFMachinesError;

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        match c {
            CommandType::UpsertWorkflowSearchAttributes => {
                Ok(ModifyWorkflowPropertiesMachineEvents::CommandScheduled)
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "ModifyWorkflowPropertiesMachine does not handle command type {c:?}"
            ))),
        }
    }
}

impl From<CommandIssued> for Done {
    fn from(_: CommandIssued) -> Self {
        Self {}
    }
}

impl From<Created> for CommandIssued {
    fn from(_: Created) -> Self {
        Self {}
    }
}
