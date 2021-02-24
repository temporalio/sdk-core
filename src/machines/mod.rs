mod workflow_machines;

// TODO: Move all these inside a submachines module
#[allow(unused)]
mod activity_state_machine;
#[allow(unused)]
mod cancel_external_state_machine;
#[allow(unused)]
mod cancel_workflow_state_machine;
#[allow(unused)]
mod child_workflow_state_machine;
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
mod timer_state_machine;
#[allow(unused)]
mod upsert_search_attributes_state_machine;
#[allow(unused)]
mod version_state_machine;
mod workflow_task_state_machine;

#[cfg(test)]
pub(crate) mod test_help;

pub(crate) use workflow_machines::{WFMachinesError, WorkflowMachines};

use crate::{
    machines::workflow_machines::MachineResponse,
    protos::{
        coresdk::{self, command::Variant, wf_activation_job},
        temporal::api::{
            command::v1::{
                command::Attributes, CancelTimerCommandAttributes, Command,
                CompleteWorkflowExecutionCommandAttributes, StartTimerCommandAttributes,
            },
            enums::v1::CommandType,
            history::v1::{
                HistoryEvent, WorkflowExecutionCanceledEventAttributes,
                WorkflowExecutionSignaledEventAttributes, WorkflowExecutionStartedEventAttributes,
            },
        },
    },
};
use prost::alloc::fmt::Formatter;
use rustfsm::{MachineError, StateMachine};
use std::{
    convert::{TryFrom, TryInto},
    fmt::{Debug, Display},
};
use tracing::Level;

pub(crate) type ProtoCommand = Command;

/// Implementors of this trait represent something that can (eventually) call into a workflow to
/// drive it, start it, signal it, cancel it, etc.
pub(crate) trait DrivenWorkflow: ActivationListener + Send {
    /// Start the workflow
    fn start(&mut self, attribs: WorkflowExecutionStartedEventAttributes);

    /// Obtain any output from the workflow's recent execution(s). Because the lang sdk is
    /// responsible for calling workflow code as a result of receiving tasks from
    /// [crate::Core::poll_task], we cannot directly iterate it here. Thus implementations of this
    /// trait are expected to either buffer output or otherwise produce it on demand when this
    /// function is called.
    ///
    /// In the case of the real [WorkflowBridge] implementation, commands are simply pulled from
    /// a buffer that the language side sinks into when it calls [crate::Core::complete_task]
    fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand>;

    /// Signal the workflow
    fn signal(&mut self, attribs: WorkflowExecutionSignaledEventAttributes);

    /// Cancel the workflow
    fn cancel(&mut self, attribs: WorkflowExecutionCanceledEventAttributes);
}

/// Allows observers to listen to newly generated outgoing activation jobs. Used for testing, where
/// some activations must be handled before outgoing commands are issued to avoid deadlocking.
pub(crate) trait ActivationListener {
    fn on_activation_job(&mut self, _activation: &wf_activation_job::Attributes) {}
}

/// [DrivenWorkflow]s respond with these when called, to indicate what they want to do next.
/// EX: Create a new timer, complete the workflow, etc.
#[derive(Debug, derive_more::From)]
pub enum WFCommand {
    /// Returned when we need to wait for the lang sdk to send us something
    NoCommandsFromLang,
    AddTimer(StartTimerCommandAttributes),
    CancelTimer(CancelTimerCommandAttributes),
    CompleteWorkflow(CompleteWorkflowExecutionCommandAttributes),
}

#[derive(thiserror::Error, Debug, derive_more::From)]
#[error("Couldn't convert <lang> command")]
pub struct InconvertibleCommandError(pub coresdk::Command);

impl TryFrom<coresdk::Command> for WFCommand {
    type Error = InconvertibleCommandError;

    fn try_from(c: coresdk::Command) -> Result<Self, Self::Error> {
        match c.variant {
            Some(Variant::Api(Command {
                attributes: Some(attrs),
                ..
            })) => match attrs {
                Attributes::StartTimerCommandAttributes(s) => Ok(WFCommand::AddTimer(s)),
                Attributes::CancelTimerCommandAttributes(s) => Ok(WFCommand::CancelTimer(s)),
                Attributes::CompleteWorkflowExecutionCommandAttributes(c) => {
                    Ok(WFCommand::CompleteWorkflow(c))
                }
                _ => unimplemented!(),
            },
            _ => Err(c.into()),
        }
    }
}

/// Extends [rustfsm::StateMachine] with some functionality specific to the temporal SDK.
///
/// Formerly known as `EntityStateMachine` in Java.
trait TemporalStateMachine: CheckStateMachineInFinal + Send {
    fn name(&self) -> &str;
    fn handle_command(&mut self, command_type: CommandType) -> Result<(), WFMachinesError>;

    /// Tell the state machine to handle some event. Returns a list of responses that can be used
    /// to update the overall state of the workflow. EX: To issue outgoing WF activations.
    fn handle_event(
        &mut self,
        event: &HistoryEvent,
        has_next_event: bool,
    ) -> Result<Vec<MachineResponse>, WFMachinesError>;

    /// Attempt to cancel the command associated with this state machine, if it is cancellable
    fn cancel(&mut self) -> Result<MachineResponse, WFMachinesError>;

    /// Should return true if the command was cancelled before we sent it to the server. Always
    /// returns false for non-cancellable machines
    fn was_cancelled_before_sent_to_server(&self) -> bool;
}

impl<SM> TemporalStateMachine for SM
where
    SM: StateMachine + CheckStateMachineInFinal + WFMachinesAdapter + Cancellable + Clone + Send,
    <SM as StateMachine>::Event: TryFrom<HistoryEvent>,
    <SM as StateMachine>::Event: TryFrom<CommandType>,
    WFMachinesError: From<<<SM as StateMachine>::Event as TryFrom<HistoryEvent>>::Error>,
    <SM as StateMachine>::Command: Debug,
    <SM as StateMachine>::State: Display,
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
            machine_name = %self.name(),
            state = %self.state()
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
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        event!(
            Level::DEBUG,
            msg = "handling event",
            %event,
            machine_name = %self.name(),
            state = %self.state()
        );
        let converted_event = event.clone().try_into()?;
        match self.on_event_mut(converted_event) {
            Ok(c) => {
                if !c.is_empty() {
                    event!(Level::DEBUG, msg = "Machine produced commands", ?c, state = %self.state());
                }
                let mut machine_responses = vec![];
                for cmd in c {
                    machine_responses.extend(self.adapt_response(event, has_next_event, cmd)?);
                }
                Ok(machine_responses)
            }
            Err(MachineError::InvalidTransition) => {
                Err(WFMachinesError::InvalidTransitionDuringEvent(
                    event.clone(),
                    format!(
                        "{} in state {} says the transition is invalid",
                        self.name(),
                        self.state()
                    ),
                ))
            }
            Err(MachineError::Underlying(e)) => Err(e.into()),
        }
    }

    fn cancel(&mut self) -> Result<MachineResponse, WFMachinesError> {
        let res = self.cancel();
        res.map_err(|e| match e {
            MachineError::InvalidTransition => {
                WFMachinesError::InvalidTransition("while attempting to cancel")
            }
            MachineError::Underlying(e) => e.into(),
        })
    }

    fn was_cancelled_before_sent_to_server(&self) -> bool {
        self.was_cancelled_before_sent_to_server()
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
trait WFMachinesAdapter: StateMachine {
    /// Given a the event being processed, and a command that this [StateMachine] instance just
    /// produced, perform any handling that needs inform the [WorkflowMachines] instance of some
    /// action to be taken in response to that command.
    fn adapt_response(
        &self,
        event: &HistoryEvent,
        has_next_event: bool,
        my_command: Self::Command,
    ) -> Result<Vec<MachineResponse>, WFMachinesError>;
}

trait Cancellable: StateMachine {
    /// Cancel the machine / the command represented by the machine.
    ///
    /// # Panics
    /// * If the machine is not cancellable. It's a logic error on our part to call it on such
    ///   machines.
    fn cancel(&mut self) -> Result<MachineResponse, MachineError<Self::Error>> {
        // It's a logic error on our part if this is ever called on a machine that can't actually
        // be cancelled
        panic!(format!("Machine {} cannot be cancelled", self.name()))
    }

    /// Should return true if the command was cancelled before we sent it to the server
    fn was_cancelled_before_sent_to_server(&self) -> bool {
        false
    }
}

#[derive(Debug)]
struct NewMachineWithCommand<T: TemporalStateMachine> {
    command: ProtoCommand,
    machine: T,
}

impl Debug for dyn TemporalStateMachine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}
