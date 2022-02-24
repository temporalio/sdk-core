use std::convert::TryFrom;

use rustfsm::{fsm, StateMachine, TransitionResult};
use temporal_sdk_core_protos::{
    coresdk::workflow_commands::{
        workflow_command::Variant::UpsertWorkflowSearchAttributesCommandAttributes,
        UpsertWorkflowSearchAttributes,
    },
    temporal::api::{
        command::v1::command::Attributes,
        command::v1::Command,
        common::v1::SearchAttributes,
        enums::v1::{CommandType, EventType},
        history::v1::HistoryEvent,
    },
};

use crate::workflow::machines::{Cancellable, EventInfo, MachineKind, Machines, WFMachinesAdapter};

use super::{
    workflow_machines::{MachineResponse, WFMachinesError},
    NewMachineWithCommand,
};

fsm! {
    pub(super) name UpsertSearchAttributesMachine;
    command UpsertSearchAttributesMachineCommand;
    error WFMachinesError;
    shared_state SharedState;

    // Machine is instantiated into the Created state and then transitions into the CommandIssued
    // state when it receives the CommandScheduled event that is a result of looping back the
    // Command initially packaged with NewMachineWithCommand (see upsert_search_attrs)
    Created --(CommandScheduled) --> CommandIssued;

    // Having sent the command to the server, the machine transitions into a terminal state (Done)
    // upon observing a history event indicating that the command has been recorded. Note that this
    // does not imply that the command has been _executed_, only that it _will be_ executed at some
    // point in the future.
    CommandIssued --(CommandRecorded) --> Done;
}

/// Instantiates an UpsertSearchAttributesMachine and packs it together with an initial command
/// to apply the provided search attribute update.
pub(super) fn upsert_search_attrs(
    attribs: UpsertWorkflowSearchAttributes,
) -> NewMachineWithCommand {
    let mut sm = UpsertSearchAttributesMachine::new();
    let cmd = Command {
        command_type: CommandType::UpsertWorkflowSearchAttributes as i32,
        attributes: Some(attribs.into()),
    };
    NewMachineWithCommand {
        command: cmd,
        machine: sm.into(),
    }
}

/// Unused but must exist
type SharedState = ();

/// The state-machine-specific set of commands that are the results of state transition in the
/// UpsertSearchAttributesMachine.
#[derive(Debug, derive_more::Display)]
pub(super) enum UpsertSearchAttributesMachineCommand {
    #[display(fmt = "SendUpsertCommand")]
    SendUpsertCommand(Attributes),
}

/// The state of the UpsertSearchAttributesMachine at time zero (i.e. at instantiation)
#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Created {}

/// Once the UpsertSearchAttributesMachine has been known to have issued the upsert command to
/// higher-level machinery, it transitions into this state.
#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct CommandIssued {}

/// Once the server has recorded its receipt of the search attribute update, the
/// UpsertSearchAttributesMachine transitions into this terminal state.
#[derive(Debug, Default, Clone, derive_more::Display)]
pub(super) struct Done {}

impl UpsertSearchAttributesMachine {
    fn new() -> Self {
        Self {
            state: Created {}.into(),
            shared_state: (),
        }
    }
}

impl WFMachinesAdapter for UpsertSearchAttributesMachine {
    /// Transforms an UpsertSearchAttributesMachine-specific command (i.e. an instance of the type
    /// UpsertSearchAttributesMachineCommand) to a more generic form supported by the abstract
    /// StateMachine type.
    fn adapt_response(
        &self,
        my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, Self::Error> {
        let UpsertSearchAttributesMachineCommand::SendUpsertCommand(attrs) = my_command;
        Ok(vec![MachineResponse::IssueNewCommand(Command {
            command_type: CommandType::UpsertWorkflowSearchAttributes as i32,
            attributes: Some(attrs),
        })])
    }

    /// Filters for EventType::UpsertWorkflowSearchAttributes
    fn matches_event(&self, event: &HistoryEvent) -> bool {
        matches!(
            event.event_type(),
            EventType::UpsertWorkflowSearchAttributes
        )
    }

    fn kind(&self) -> MachineKind {
        MachineKind::UpsertSearchAttributes
    }
}

impl Cancellable for UpsertSearchAttributesMachine {}

/// Converts the generic history event with type EventType::UpsertWorkflowSearchAttributes into the
/// UpsertSearchAttributesMachine-specific event type
/// UpsertSearchAttributesMachineEvents::CommandRecorded.
impl TryFrom<HistoryEvent> for UpsertSearchAttributesMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        match e.event_type() {
            EventType::UpsertWorkflowSearchAttributes => {
                Ok(UpsertSearchAttributesMachineEvents::CommandRecorded)
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "UpsertWorkflowSearchAttributesMachine does not handle {e}"
            ))),
        }
    }
}

/// Converts generic state machine command type CommandType::UpsertWorkflowSearchAttributes into
/// the UpsertSearchAttributesMachine-specific event
impl TryFrom<CommandType> for UpsertSearchAttributesMachineEvents {
    type Error = WFMachinesError;

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        match c {
            CommandType::UpsertWorkflowSearchAttributes => {
                Ok(UpsertSearchAttributesMachineEvents::CommandScheduled)
            }
            _ => Err(Self::Error::Nondeterminism(format!(
                "UpsertWorkflowSearchAttributesMachine does not handle command type {c:?}"
            ))),
        }
    }
}

/// There is no Command/Reponse associated with this transition
impl From<CommandIssued> for Done {
    fn from(_: CommandIssued) -> Self {
        Self {}
    }
}

/// There is no Command/Reponse associated with this transition
impl From<Created> for CommandIssued {
    fn from(_: Created) -> Self {
        Self {}
    }
}

#[cfg(test)]
mod tests {
    use temporal_sdk::{WfContext, WorkflowFunction};
    use temporal_sdk_core_protos::coresdk::common::Payload;

    use crate::{replay::TestHistoryBuilder, workflow::managed_wf::ManagedWFFunc};

    use super::super::OnEventWrapper;
    use super::*;

    #[tokio::test]
    async fn upsert_search_attrs_from_workflow() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let wff = WorkflowFunction::new(|ctx: WfContext| async move {
            ctx.upsert_search_attributes([(
                String::from("foo"),
                Payload {
                    metadata: Default::default(),
                    data: vec![],
                },
            )]);
            Ok(().into())
        });
        let mut wfm = ManagedWFFunc::new(t, wff, vec![]);

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert!(!commands.is_empty());
        assert_eq!(
            commands[0].command_type,
            CommandType::UpsertWorkflowSearchAttributes as i32
        );
        wfm.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn upsert_search_attrs_sm() {
        let mut sm = UpsertSearchAttributesMachine::new();
        assert_eq!(Created {}.to_string(), sm.state());

        let cmd_scheduled_sm_event = CommandType::UpsertWorkflowSearchAttributes
            .try_into()
            .unwrap();
        let recorded_history_event = HistoryEvent {
            event_type: EventType::UpsertWorkflowSearchAttributes as i32,
            ..Default::default()
        };
        assert!(sm.matches_event(&recorded_history_event));
        let cmd_recorded_sm_event = recorded_history_event.try_into().unwrap();

        OnEventWrapper::on_event_mut(&mut sm, cmd_scheduled_sm_event)
            .expect("CommandScheduled should transition Created -> CommandIssued");
        assert_eq!(CommandIssued {}.to_string(), sm.state());

        OnEventWrapper::on_event_mut(&mut sm, cmd_recorded_sm_event)
            .expect("CommandRecorded should transition CommandIssued -> Done");
        assert_eq!(Done {}.to_string(), sm.state());
    }
}
