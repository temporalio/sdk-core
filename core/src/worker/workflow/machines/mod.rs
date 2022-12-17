mod workflow_machines;

mod activity_state_machine;
mod cancel_external_state_machine;
mod cancel_workflow_state_machine;
mod child_workflow_state_machine;
mod complete_workflow_state_machine;
mod continue_as_new_workflow_state_machine;
mod fail_workflow_state_machine;
mod local_activity_state_machine;
mod modify_workflow_properties_state_machine;
mod patch_state_machine;
mod signal_external_state_machine;
mod timer_state_machine;
mod upsert_search_attributes_state_machine;
mod workflow_task_state_machine;

#[cfg(test)]
mod transition_coverage;

pub(crate) use workflow_machines::{WFMachinesError, WorkflowMachines};

use crate::telemetry::VecDisplayer;
use activity_state_machine::ActivityMachine;
use cancel_external_state_machine::CancelExternalMachine;
use cancel_workflow_state_machine::CancelWorkflowMachine;
use child_workflow_state_machine::ChildWorkflowMachine;
use complete_workflow_state_machine::CompleteWorkflowMachine;
use continue_as_new_workflow_state_machine::ContinueAsNewWorkflowMachine;
use fail_workflow_state_machine::FailWorkflowMachine;
use local_activity_state_machine::LocalActivityMachine;
use modify_workflow_properties_state_machine::ModifyWorkflowPropertiesMachine;
use patch_state_machine::PatchMachine;
use rustfsm::{MachineError, StateMachine};
use signal_external_state_machine::SignalExternalMachine;
use std::{
    convert::{TryFrom, TryInto},
    fmt::{Debug, Display},
};
use temporal_sdk_core_protos::temporal::api::{
    command::v1::Command as ProtoCommand,
    enums::v1::{CommandType, EventType},
    history::v1::HistoryEvent,
};
use timer_state_machine::TimerMachine;
use upsert_search_attributes_state_machine::UpsertSearchAttributesMachine;
use workflow_machines::MachineResponse;
use workflow_task_state_machine::WorkflowTaskMachine;

#[cfg(test)]
use transition_coverage::add_coverage;

#[enum_dispatch::enum_dispatch]
#[allow(clippy::enum_variant_names, clippy::large_enum_variant)]
enum Machines {
    ActivityMachine,
    CancelExternalMachine,
    CancelWorkflowMachine,
    ChildWorkflowMachine,
    CompleteWorkflowMachine,
    ContinueAsNewWorkflowMachine,
    FailWorkflowMachine,
    LocalActivityMachine,
    PatchMachine,
    SignalExternalMachine,
    TimerMachine,
    WorkflowTaskMachine,
    UpsertSearchAttributesMachine,
    ModifyWorkflowPropertiesMachine,
}

/// Extends [rustfsm::StateMachine] with some functionality specific to the temporal SDK.
///
/// Formerly known as `EntityStateMachine` in Java.
#[enum_dispatch::enum_dispatch(Machines)]
trait TemporalStateMachine: Send {
    fn handle_command(
        &mut self,
        command_type: CommandType,
    ) -> Result<Vec<MachineResponse>, WFMachinesError>;

    /// Returns true if this machine is compatible with the provided event. Provides a way to know
    /// ahead of time if it's worth trying to call [TemporalStateMachine::handle_event] without
    /// requiring mutable access.
    fn matches_event(&self, event: &HistoryEvent) -> bool;

    /// Tell the state machine to handle some event. Returns a list of responses that can be used
    /// to update the overall state of the workflow. EX: To issue outgoing WF activations.
    fn handle_event(
        &mut self,
        event: HistoryEvent,
        has_next_event: bool,
    ) -> Result<Vec<MachineResponse>, WFMachinesError>;

    /// Attempt to cancel the command associated with this state machine, if it is cancellable
    fn cancel(&mut self) -> Result<Vec<MachineResponse>, WFMachinesError>;

    /// Should return true if the command was cancelled before we sent it to the server. Always
    /// returns false for non-cancellable machines
    fn was_cancelled_before_sent_to_server(&self) -> bool;

    /// Returns true if the state machine is in a final state
    fn is_final_state(&self) -> bool;

    /// Returns a friendly name for the type of this machine
    fn name(&self) -> &str;
}

impl<SM> TemporalStateMachine for SM
where
    SM: StateMachine + WFMachinesAdapter + Cancellable + OnEventWrapper + Clone + Send + 'static,
    <SM as StateMachine>::Event: TryFrom<HistoryEvent> + TryFrom<CommandType> + Display,
    WFMachinesError: From<<<SM as StateMachine>::Event as TryFrom<HistoryEvent>>::Error>,
    <SM as StateMachine>::Command: Debug + Display,
    <SM as StateMachine>::State: Display,
    <SM as StateMachine>::Error: Into<WFMachinesError> + 'static + Send + Sync,
{
    fn handle_command(
        &mut self,
        command_type: CommandType,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        debug!(
            command_type = ?command_type,
            machine_name = %self.name(),
            state = %self.state(),
            "handling command"
        );
        if let Ok(converted_command) = command_type.try_into() {
            match OnEventWrapper::on_event_mut(self, converted_command) {
                Ok(c) => process_machine_commands(self, c, None),
                Err(MachineError::InvalidTransition) => {
                    Err(WFMachinesError::Nondeterminism(format!(
                        "Unexpected command producing an invalid transition {:?} in state {}",
                        command_type,
                        self.state()
                    )))
                }
                Err(MachineError::Underlying(e)) => Err(e.into()),
            }
        } else {
            Err(WFMachinesError::Nondeterminism(format!(
                "Unexpected command {:?} generated by a {:?} machine",
                command_type,
                self.name()
            )))
        }
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        self.matches_event(event)
    }

    fn handle_event(
        &mut self,
        event: HistoryEvent,
        has_next_event: bool,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        trace!(
            event = %event,
            machine_name = %self.name(),
            state = %self.state(),
            "handling event"
        );
        let event_info = EventInfo {
            event_id: event.event_id,
            event_type: event.event_type(),
            has_next_event,
        };
        let converted_event: <Self as StateMachine>::Event = event.try_into()?;

        match OnEventWrapper::on_event_mut(self, converted_event) {
            Ok(c) => process_machine_commands(self, c, Some(event_info)),
            Err(MachineError::InvalidTransition) => Err(WFMachinesError::Fatal(format!(
                "{} in state {} says the transition is invalid during event {:?}",
                self.name(),
                self.state(),
                event_info
            ))),
            Err(MachineError::Underlying(e)) => Err(e.into()),
        }
    }

    fn cancel(&mut self) -> Result<Vec<MachineResponse>, WFMachinesError> {
        let res = self.cancel();
        res.map_err(|e| match e {
            MachineError::InvalidTransition => WFMachinesError::Fatal(format!(
                "Invalid transition while attempting to cancel {} in {}",
                self.name(),
                self.state(),
            )),
            MachineError::Underlying(e) => e.into(),
        })
    }

    fn was_cancelled_before_sent_to_server(&self) -> bool {
        self.was_cancelled_before_sent_to_server()
    }

    fn is_final_state(&self) -> bool {
        self.has_reached_final_state()
    }

    fn name(&self) -> &str {
        self.name()
    }
}

fn process_machine_commands<SM>(
    machine: &mut SM,
    commands: Vec<SM::Command>,
    event_info: Option<EventInfo>,
) -> Result<Vec<MachineResponse>, WFMachinesError>
where
    SM: TemporalStateMachine + StateMachine + WFMachinesAdapter,
    <SM as StateMachine>::Event: Display,
    <SM as StateMachine>::Command: Debug + Display,
    <SM as StateMachine>::State: Display,
{
    if !commands.is_empty() {
        trace!(commands=%commands.display(), state=%machine.state(),
               machine_name=%TemporalStateMachine::name(machine), "Machine produced commands");
    }
    let mut machine_responses = vec![];
    for cmd in commands {
        machine_responses.extend(machine.adapt_response(cmd, event_info)?);
    }
    Ok(machine_responses)
}

/// This trait exists to bridge [StateMachine]s and the [WorkflowMachines] instance. It has access
/// to the machine's concrete types while hiding those details from [WorkflowMachines]
trait WFMachinesAdapter: StateMachine {
    /// A command that this [StateMachine] instance just produced, and maybe the event being
    /// processed, perform any handling that needs inform the [WorkflowMachines] instance of some
    /// action to be taken in response to that command.
    fn adapt_response(
        &self,
        my_command: Self::Command,
        event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError>;

    /// Returns true if this machine is compatible with the provided event. Provides a way to know
    /// ahead of time if it's worth trying to call [TemporalStateMachine::handle_event] without
    /// requiring mutable access.
    fn matches_event(&self, event: &HistoryEvent) -> bool;
}

#[derive(Debug, Copy, Clone)]
struct EventInfo {
    event_id: i64,
    event_type: EventType,
    has_next_event: bool,
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

struct NewMachineWithCommand {
    command: ProtoCommand,
    machine: Machines,
}
