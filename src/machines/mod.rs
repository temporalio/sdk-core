mod workflow_machines;

#[allow(unused)]
mod activity_state_machine;
#[allow(unused)]
mod cancel_external_state_machine;
#[allow(unused)]
mod cancel_workflow_state_machine;
#[allow(unused)]
mod child_workflow_state_machine;
#[allow(unused)]
mod complete_workflow_state_machine;
#[allow(unused)]
mod continue_as_new_workflow_state_machine;
#[allow(unused)]
mod fail_workflow_state_machine;
#[allow(unused)]
mod local_activity_state_machine;
#[allow(unused)]
mod mutable_side_effect_state_machine;
#[allow(unused)]
mod side_effect_state_machine;
#[allow(unused)]
mod signal_external_state_machine;
#[allow(unused)]
mod timer_state_machine;
#[allow(unused)]
mod upsert_search_attributes_state_machine;
#[allow(unused)]
mod version_state_machine;
#[allow(unused)]
mod workflow_task_state_machine;

#[cfg(test)]
mod test_help;

use crate::{
    machines::workflow_machines::WFMachinesError,
    protos::temporal::api::{
        command::v1::Command, enums::v1::CommandType, history::v1::HistoryEvent,
    },
};
use rustfsm::{MachineError, StateMachine};
use std::{
    convert::{TryFrom, TryInto},
    fmt::Debug,
    time::SystemTime,
};

//  TODO: May need to be our SDKWFCommand type
pub(crate) type MachineCommand = Command;

#[derive(Debug)]
pub(crate) enum TSMCommand {
    /// Issed by the [WorkflowTaskMachine] to trigger the event loop
    WFTaskStartedTrigger {
        event_id: i64,
        time: SystemTime,
        only_if_last_event: bool,
    },
}

/// Extends [rustfsm::StateMachine] with some functionality specific to the temporal SDK.
///
/// Formerly known as `EntityStateMachine` in Java.
trait TemporalStateMachine: CheckStateMachineInFinal + IsWfTaskMachine {
    fn name(&self) -> &str;
    fn handle_command(&mut self, command_type: CommandType) -> Result<(), WFMachinesError>;
    fn handle_event(
        &mut self,
        event: &HistoryEvent,
        has_next_event: bool,
    ) -> Result<Vec<TSMCommand>, WFMachinesError>;

    // TODO: This is a weird one that only applies to version state machine. Introduce only if
    //  needed. Ideally handle differently.
    //  fn handle_workflow_task_started();
}

impl<SM> TemporalStateMachine for SM
where
    SM: StateMachine + CheckStateMachineInFinal + IsWfTaskMachine + Clone,
    <SM as StateMachine>::Event: TryFrom<HistoryEvent>,
    <SM as StateMachine>::Event: TryFrom<CommandType>,
    <SM as StateMachine>::Command: Debug,
    // TODO: Do we really need this bound? Check back and see how many fsms really issue them this way
    <SM as StateMachine>::Command: Into<TSMCommand>,
    <SM as StateMachine>::Error: Into<WFMachinesError> + 'static + Send + Sync,
{
    fn name(&self) -> &str {
        <Self as StateMachine>::name(self)
    }

    fn handle_command(&mut self, command_type: CommandType) -> Result<(), WFMachinesError> {
        dbg!(self.name(), "handling command", command_type);
        if let Ok(converted_command) = command_type.try_into() {
            match self.on_event_mut(converted_command) {
                Ok(c) => {
                    dbg!(c);
                    Ok(())
                }
                Err(MachineError::InvalidTransition) => {
                    Err(WFMachinesError::UnexpectedCommand(command_type))
                }
                Err(MachineError::Underlying(e)) => Err(WFMachinesError::Underlying(Box::new(e))),
            }
        } else {
            Err(WFMachinesError::UnexpectedCommand(command_type))
        }
    }

    fn handle_event(
        &mut self,
        event: &HistoryEvent,
        _has_next_event: bool,
    ) -> Result<Vec<TSMCommand>, WFMachinesError> {
        // TODO: Real tracing
        dbg!(self.name(), "handling event", &event);
        if let Ok(converted_event) = event.clone().try_into() {
            match self.on_event_mut(converted_event) {
                Ok(c) => {
                    dbg!(&c);
                    Ok(c.into_iter().map(Into::into).collect())
                }
                Err(MachineError::InvalidTransition) => {
                    Err(WFMachinesError::UnexpectedEvent(event.clone()))
                }
                Err(MachineError::Underlying(e)) => Err(WFMachinesError::Underlying(Box::new(e))),
            }
        } else {
            Err(WFMachinesError::UnexpectedEvent(event.clone()))
        }
    }
}

/// Exists purely to allow generic implementation of `is_final_state` for all [StateMachine]
/// implementors
trait CheckStateMachineInFinal {
    /// Returns true if the state machine is in a final state
    fn is_final_state(&self) -> bool;
}

impl<SM> CheckStateMachineInFinal for SM
where
    SM: StateMachine,
{
    fn is_final_state(&self) -> bool {
        self.on_final_state()
    }
}

//TODO: Remove now?
/// Only should be implemented (with `true`) for [WorkflowTaskMachine]
// This is a poor but effective substitute for specialization. Remove when it's finally
// stabilized: https://github.com/rust-lang/rust/issues/31844
trait IsWfTaskMachine {
    fn is_wf_task_machine(&self) -> bool {
        false
    }
}

/// A command which can be cancelled
#[derive(Debug, Clone)]
pub struct CancellableCommand {
    /// The inner protobuf command, if None, command has been cancelled
    command: Option<MachineCommand>,
}

impl CancellableCommand {
    pub(super) fn cancel(&mut self) {
        self.command = None;
    }
}

impl From<Command> for CancellableCommand {
    fn from(command: Command) -> Self {
        Self {
            command: Some(command),
        }
    }
}
