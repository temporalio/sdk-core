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
pub(crate) mod test_help;

pub(crate) use workflow_machines::{WFMachinesError, WorkflowMachines};

use crate::protos::coresdk;
use crate::protos::coresdk::command::Variant;
use crate::protos::temporal::api::command::v1::command::Attributes;
use crate::protos::temporal::api::{
    command::v1::{
        Command, CompleteWorkflowExecutionCommandAttributes, StartTimerCommandAttributes,
    },
    enums::v1::CommandType,
    history::v1::{
        HistoryEvent, WorkflowExecutionCanceledEventAttributes,
        WorkflowExecutionSignaledEventAttributes, WorkflowExecutionStartedEventAttributes,
    },
};
use prost::alloc::fmt::Formatter;
use rustfsm::{MachineError, StateMachine};
use std::{
    cell::RefCell,
    convert::{TryFrom, TryInto},
    fmt::Debug,
    rc::Rc,
    sync::{atomic::AtomicBool, Arc},
};
use tracing::Level;

//  TODO: May need to be our SDKWFCommand type
type MachineCommand = Command;

/// Implementors of this trait represent something that can (eventually) call into a workflow to
/// drive it, start it, signal it, cancel it, etc.
pub(crate) trait DrivenWorkflow: Send {
    /// Start the workflow
    fn start(
        &mut self,
        attribs: WorkflowExecutionStartedEventAttributes,
    ) -> Result<Vec<WFCommand>, anyhow::Error>;

    /// Iterate the workflow. The workflow driver should execute workflow code until there is
    /// nothing left to do. EX: Awaiting an activity/timer, workflow completion.
    fn iterate_wf(&mut self) -> Result<Vec<WFCommand>, anyhow::Error>;

    /// Signal the workflow
    fn signal(
        &mut self,
        attribs: WorkflowExecutionSignaledEventAttributes,
    ) -> Result<(), anyhow::Error>;

    /// Cancel the workflow
    fn cancel(
        &mut self,
        attribs: WorkflowExecutionCanceledEventAttributes,
    ) -> Result<(), anyhow::Error>;
}

/// The struct for [WFCommand::AddCommand]
#[derive(Debug, derive_more::From)]
pub(crate) struct AddCommand {
    /// The protobuf command
    pub(crate) command: Command,
}

/// [DrivenWorkflow]s respond with these when called, to indicate what they want to do next.
/// EX: Create a new timer, complete the workflow, etc.
#[derive(Debug, derive_more::From)]
pub enum WFCommand {
    AddTimer(StartTimerCommandAttributes, Arc<AtomicBool>),
    CompleteWorkflow(CompleteWorkflowExecutionCommandAttributes),
}

#[derive(thiserror::Error, Debug, derive_more::From)]
#[error("Couldn't convert <lang> command")]
pub struct InconvertibleCommandError(pub coresdk::Command);

impl TryFrom<coresdk::Command> for WFCommand {
    type Error = InconvertibleCommandError;

    fn try_from(c: coresdk::Command) -> Result<Self, Self::Error> {
        // TODO: Return error without cloning
        match c.variant.clone() {
            Some(a) => match a {
                Variant::Api(Command {
                    attributes: Some(attrs),
                    ..
                }) => match attrs {
                    Attributes::StartTimerCommandAttributes(s) => {
                        Ok(WFCommand::AddTimer(s, Arc::new(AtomicBool::new(false))))
                    }
                    Attributes::CompleteWorkflowExecutionCommandAttributes(c) => {
                        Ok(WFCommand::CompleteWorkflow(c))
                    }
                    _ => unimplemented!(),
                },
                _ => Err(c.into()),
            },
            None => Err(c.into()),
        }
    }
}

/// Extends [rustfsm::StateMachine] with some functionality specific to the temporal SDK.
///
/// Formerly known as `EntityStateMachine` in Java.
trait TemporalStateMachine: CheckStateMachineInFinal {
    fn name(&self) -> &str;
    fn handle_command(&mut self, command_type: CommandType) -> Result<(), WFMachinesError>;

    /// Tell the state machine to handle some event. The [WorkflowMachines] instance calling this
    /// function passes a reference to itself in so that the [WFMachinesAdapter] trait can use
    /// the machines' specific type information while also manipulating [WorkflowMachines]
    fn handle_event(
        &mut self,
        event: &HistoryEvent,
        has_next_event: bool,
        wf_machines: &mut WorkflowMachines,
    ) -> Result<(), WFMachinesError>;
}

impl<SM> TemporalStateMachine for SM
where
    SM: StateMachine + CheckStateMachineInFinal + WFMachinesAdapter + Clone,
    <SM as StateMachine>::Event: TryFrom<HistoryEvent>,
    <SM as StateMachine>::Event: TryFrom<CommandType>,
    <SM as StateMachine>::Command: Debug,
    <SM as StateMachine>::Error: Into<WFMachinesError> + 'static + Send + Sync,
{
    fn name(&self) -> &str {
        <Self as StateMachine>::name(self)
    }

    fn handle_command(&mut self, command_type: CommandType) -> Result<(), WFMachinesError> {
        event!(
            Level::DEBUG,
            msg = "handling command",
            ?command_type,
            machine_name = %self.name()
        );
        if let Ok(converted_command) = command_type.try_into() {
            match self.on_event_mut(converted_command) {
                Ok(_c) => Ok(()),
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
        has_next_event: bool,
        wf_machines: &mut WorkflowMachines,
    ) -> Result<(), WFMachinesError> {
        event!(
            Level::DEBUG,
            msg = "handling event",
            %event,
            machine_name = %self.name()
        );
        if let Ok(converted_event) = event.clone().try_into() {
            match self.on_event_mut(converted_event) {
                Ok(c) => {
                    event!(Level::DEBUG, msg = "Machine produced commands", ?c);
                    for cmd in c {
                        self.adapt_response(wf_machines, event, has_next_event, cmd)?;
                    }
                    Ok(())
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

/// This trait exists to bridge [StateMachine]s and the [WorkflowMachines] instance. It has access
/// to the machine's concrete types while hiding those details from [WorkflowMachines]
pub(super) trait WFMachinesAdapter: StateMachine {
    /// Given a reference to [WorkflowMachines], the event being processed, and a command that this
    /// [StateMachine] instance just produced, perform any handling that needs access to the
    /// [WorkflowMachines] instance in response to that command.
    fn adapt_response(
        &self,
        _wf_machines: &mut WorkflowMachines,
        _event: &HistoryEvent,
        _has_next_event: bool,
        _my_command: Self::Command,
    ) -> Result<(), WFMachinesError>;
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
        machine: Rc<RefCell<dyn TemporalStateMachine>>,
    },
}

impl CancellableCommand {
    #[allow(dead_code)] // TODO: Use
    pub(super) fn cancel(&mut self) {
        *self = CancellableCommand::Cancelled;
    }
}

impl Debug for dyn TemporalStateMachine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}
