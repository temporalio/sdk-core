#[allow(unused)]
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
        command::v1::Command,
        enums::v1::CommandType,
        history::v1::{
            HistoryEvent, WorkflowExecutionCanceledEventAttributes,
            WorkflowExecutionSignaledEventAttributes, WorkflowExecutionStartedEventAttributes,
        },
    },
};
use prost::alloc::fmt::Formatter;
use rustfsm::{MachineError, StateMachine};
use std::{
    convert::{TryFrom, TryInto},
    fmt::Debug,
    rc::Rc,
    time::SystemTime,
};

//  TODO: May need to be our SDKWFCommand type
type MachineCommand = Command;

/// Implementors of this trait represent something that can (eventually) call into a workflow to
/// drive it, start it, signal it, cancel it, etc.
trait DrivenWorkflow {
    /// Start the workflow
    fn start(
        &self,
        attribs: WorkflowExecutionStartedEventAttributes,
    ) -> Result<Vec<WFCommand>, anyhow::Error>;

    /// Iterate the workflow. The workflow driver should execute workflow code until there is
    /// nothing left to do. EX: Awaiting an activity/timer, workflow completion.
    fn iterate_wf(&self) -> Result<Vec<WFCommand>, anyhow::Error>;

    /// Signal the workflow
    fn signal(
        &self,
        attribs: WorkflowExecutionSignaledEventAttributes,
    ) -> Result<(), anyhow::Error>;

    /// Cancel the workflow
    fn cancel(
        &self,
        attribs: WorkflowExecutionCanceledEventAttributes,
    ) -> Result<(), anyhow::Error>;
}

/// These commands are issued by state machines to inform their driver ([WorkflowMachines]) that
/// it may need to take some action.
///
/// In java this functionality was largely handled via callbacks passed into the state machines
/// which was difficult to follow
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum TSMCommand {
    /// Issed by the [WorkflowTaskMachine] to trigger the event loop
    WFTaskStartedTrigger {
        event_id: i64,
        time: SystemTime,
        only_if_last_event: bool,
    },
    /// Issued by state machines to create commands
    AddCommand(AddCommand),
    // TODO: This is not quite right. Should be more like "notify completion". Need to investigate
    //   more examples
    ProduceHistoryEvent(HistoryEvent),
}

/// The struct for [WFCommand::AddCommand]
#[derive(Debug, derive_more::From)]
pub(crate) struct AddCommand {
    /// The protobuf command
    pub(crate) command: Command,
}

/// [DrivenWorkflow]s respond with these when called, to indicate what they want to do next.
/// EX: Create a new timer, complete the workflow, etc.
///
/// TODO: Maybe this and TSMCommand are really the same thing?
#[derive(Debug, derive_more::From)]
enum WFCommand {
    /// Add a new entity/command
    Add(CancellableCommand),
}

/// Extends [rustfsm::StateMachine] with some functionality specific to the temporal SDK.
///
/// Formerly known as `EntityStateMachine` in Java.
trait TemporalStateMachine: CheckStateMachineInFinal {
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
    SM: StateMachine + CheckStateMachineInFinal + Clone,
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
                Err(MachineError::Underlying(e)) => Err(e.into()),
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
                Err(MachineError::Underlying(e)) => Err(e.into()),
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

/// A command which can be cancelled, associated with some state machine that produced it
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
enum CancellableCommand {
    // TODO: You'll be used soon, friend.
    #[allow(dead_code)]
    Cancelled,
    Active {
        /// The inner protobuf command, if None, command has been cancelled
        command: MachineCommand,
        machine: Rc<dyn TemporalStateMachine>,
    },
}

impl CancellableCommand {
    #[allow(dead_code)]
    pub(super) fn cancel(&mut self) {
        *self = CancellableCommand::Cancelled;
    }

    #[cfg(test)]
    fn unwrap_machine(&self) -> Rc<dyn TemporalStateMachine> {
        if let CancellableCommand::Active { machine, .. } = self {
            machine.clone()
        } else {
            panic!("No machine in command, already canceled")
        }
    }
}

impl Debug for dyn TemporalStateMachine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}
