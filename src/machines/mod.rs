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
#[macro_use]
pub(crate) mod test_help;

pub(crate) use workflow_machines::{WFMachinesError, WorkflowMachines};

use crate::{
    core_tracing::VecDisplayer,
    machines::workflow_machines::MachineResponse,
    protos::{
        coresdk::workflow_commands::{
            workflow_command, CancelTimer, CompleteWorkflowExecution, FailWorkflowExecution,
            RequestCancelActivity, ScheduleActivity, StartTimer, WorkflowCommand,
        },
        temporal::api::{command::v1::Command, enums::v1::CommandType, history::v1::HistoryEvent},
    },
};
use prost::alloc::fmt::Formatter;
use rustfsm::{MachineError, StateMachine};
use std::{
    convert::{TryFrom, TryInto},
    fmt::{Debug, Display},
};

#[cfg(test)]
use crate::machines::test_help::add_coverage;

pub(crate) type ProtoCommand = Command;

/// [DrivenWorkflow]s respond with these when called, to indicate what they want to do next.
/// EX: Create a new timer, complete the workflow, etc.
#[derive(Debug, derive_more::From)]
#[allow(clippy::large_enum_variant)]
pub enum WFCommand {
    /// Returned when we need to wait for the lang sdk to send us something
    NoCommandsFromLang,
    AddActivity(ScheduleActivity),
    RequestCancelActivity(RequestCancelActivity),
    AddTimer(StartTimer),
    CancelTimer(CancelTimer),
    CompleteWorkflow(CompleteWorkflowExecution),
    FailWorkflow(FailWorkflowExecution),
}

#[derive(thiserror::Error, Debug, derive_more::From)]
#[error("Lang provided workflow command with empty variant")]
pub struct EmptyWorkflowCommandErr;

impl TryFrom<WorkflowCommand> for WFCommand {
    type Error = EmptyWorkflowCommandErr;

    fn try_from(c: WorkflowCommand) -> Result<Self, Self::Error> {
        match c.variant.ok_or(EmptyWorkflowCommandErr)? {
            workflow_command::Variant::StartTimer(s) => Ok(WFCommand::AddTimer(s)),
            workflow_command::Variant::CancelTimer(s) => Ok(WFCommand::CancelTimer(s)),
            workflow_command::Variant::ScheduleActivity(s) => Ok(WFCommand::AddActivity(s)),
            workflow_command::Variant::RequestCancelActivity(s) => {
                Ok(WFCommand::RequestCancelActivity(s))
            }
            workflow_command::Variant::CompleteWorkflowExecution(c) => {
                Ok(WFCommand::CompleteWorkflow(c))
            }
            workflow_command::Variant::FailWorkflowExecution(s) => Ok(WFCommand::FailWorkflow(s)),
            _ => unimplemented!(),
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
    fn cancel(&mut self) -> Result<Vec<MachineResponse>, WFMachinesError>;

    /// Should return true if the command was cancelled before we sent it to the server. Always
    /// returns false for non-cancellable machines
    fn was_cancelled_before_sent_to_server(&self) -> bool;
}

impl<SM> TemporalStateMachine for SM
where
    SM: StateMachine
        + CheckStateMachineInFinal
        + WFMachinesAdapter
        + Cancellable
        + OnEventWrapper
        + Clone
        + Send,
    <SM as StateMachine>::Event: TryFrom<HistoryEvent>,
    <SM as StateMachine>::Event: TryFrom<CommandType>,
    <SM as StateMachine>::Event: Display,
    WFMachinesError: From<<<SM as StateMachine>::Event as TryFrom<HistoryEvent>>::Error>,
    <SM as StateMachine>::Command: Debug + Display,
    <SM as StateMachine>::State: Display,
    <SM as StateMachine>::Error: Into<WFMachinesError> + 'static + Send + Sync,
{
    fn name(&self) -> &str {
        <Self as StateMachine>::name(self)
    }

    fn handle_command(&mut self, command_type: CommandType) -> Result<(), WFMachinesError> {
        debug!(
            command_type = ?command_type,
            machine_name = %self.name(),
            state = %self.state(),
            "handling command"
        );
        if let Ok(converted_command) = command_type.try_into() {
            match OnEventWrapper::on_event_mut(self, converted_command) {
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
        debug!(
            event = %event,
            machine_name = %self.name(),
            state = %self.state(),
            "handling event"
        );
        let converted_event: <Self as StateMachine>::Event = event.clone().try_into()?;

        match OnEventWrapper::on_event_mut(self, converted_event) {
            Ok(c) => {
                if !c.is_empty() {
                    debug!(commands=%c.display(), state=%self.state(), machine_name=%self.name(),
                           "Machine produced commands");
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

    fn cancel(&mut self) -> Result<Vec<MachineResponse>, WFMachinesError> {
        let res = self.cancel();
        res.map_err(|e| match e {
            MachineError::InvalidTransition => WFMachinesError::InvalidTransition(format!(
                "while attempting to cancel in {}",
                self.state(),
            )),
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
    fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<Self::Error>> {
        // It's a logic error on our part if this is ever called on a machine that can't actually
        // be cancelled
        panic!("Machine {} cannot be cancelled", self.name())
    }

    /// Should return true if the command was cancelled before we sent it to the server
    fn was_cancelled_before_sent_to_server(&self) -> bool {
        false
    }
}

/// We need to wrap calls to [StateMachine::on_event_mut] to track coverage, or anything else
/// we'd like to do on every call.
pub(crate) trait OnEventWrapper: StateMachine
where
    <Self as StateMachine>::State: Display,
    <Self as StateMachine>::Event: Display,
    Self: Clone,
{
    fn on_event_mut(
        &mut self,
        event: Self::Event,
    ) -> Result<Vec<Self::Command>, MachineError<Self::Error>> {
        #[cfg(test)]
        let from_state = self.state().to_string();
        #[cfg(test)]
        let converted_event_str = event.to_string();

        let res = StateMachine::on_event_mut(self, event);
        if res.is_ok() {
            #[cfg(test)]
            add_coverage(
                self.name().to_owned(),
                from_state,
                self.state().to_string(),
                converted_event_str,
            );
        }
        res
    }
}

impl<SM> OnEventWrapper for SM
where
    SM: StateMachine,
    <Self as StateMachine>::State: Display,
    <Self as StateMachine>::Event: Display,
    Self: Clone,
{
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
