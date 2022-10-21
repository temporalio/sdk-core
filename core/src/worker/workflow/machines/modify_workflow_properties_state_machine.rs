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
            EventType::WorkflowPropertiesModified => {
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
            CommandType::ModifyWorkflowProperties => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{replay::TestHistoryBuilder, worker::workflow::ManagedWFFunc};
    use temporal_sdk::{WfContext, WorkflowFunction};
    use temporal_sdk_core_protos::temporal::api::{
        command::v1::command::Attributes, common::v1::Payload,
    };

    #[tokio::test]
    async fn workflow_modify_props() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let (k1, k2) = ("foo", "bar");

        let wff = WorkflowFunction::new(move |ctx: WfContext| async move {
            ctx.upsert_memo([
                (
                    String::from(k1),
                    Payload {
                        data: vec![0x01],
                        ..Default::default()
                    },
                ),
                (
                    String::from(k2),
                    Payload {
                        data: vec![0x02],
                        ..Default::default()
                    },
                ),
            ]);
            Ok(().into())
        });
        let mut wfm = ManagedWFFunc::new(t, wff, vec![]);

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert!(!commands.is_empty());
        let cmd = commands[0].clone();
        assert_eq!(
            cmd.command_type,
            CommandType::ModifyWorkflowProperties as i32
        );
        assert_matches!(
        cmd.attributes.unwrap(),
        Attributes::ModifyWorkflowPropertiesCommandAttributes(msg) => {
            let fields = &msg.upserted_memo.unwrap().fields;
            let payload1 = fields.get(k1).unwrap();
            let payload2 = fields.get(k2).unwrap();
            assert_eq!(payload1.data[0], 0x01);
            assert_eq!(payload2.data[0], 0x02);
            assert_eq!(fields.len(), 2);
        });
        wfm.shutdown().await.unwrap();
    }
}
