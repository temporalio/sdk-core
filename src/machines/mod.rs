mod workflow_machines;

// TODO: Move all these inside a submachines module
mod activity_state_machine;
mod cancel_external_state_machine;
mod cancel_workflow_state_machine;
mod child_workflow_state_machine;
mod complete_workflow_state_machine;
mod continue_as_new_workflow_state_machine;
mod fail_workflow_state_machine;
#[allow(unused)]
mod local_activity_state_machine;
#[allow(unused)]
mod mutable_side_effect_state_machine;
mod patch_state_machine;
#[allow(unused)]
mod side_effect_state_machine;
mod signal_external_state_machine;
mod timer_state_machine;
#[allow(unused)]
mod upsert_search_attributes_state_machine;
mod workflow_task_state_machine;

#[cfg(test)]
mod transition_coverage;

pub use patch_state_machine::HAS_CHANGE_MARKER_NAME;
pub(crate) use workflow_machines::{WFMachinesError, WorkflowMachines};

use crate::{machines::workflow_machines::MachineResponse, telemetry::VecDisplayer};
use prost::alloc::fmt::Formatter;
use rustfsm::{MachineError, StateMachine};
use std::{
    convert::{TryFrom, TryInto},
    fmt::{Debug, Display},
};
use temporal_sdk_core_protos::{
    coresdk::workflow_commands::*,
    temporal::api::{command::v1::Command, enums::v1::CommandType, history::v1::HistoryEvent},
};

#[cfg(test)]
use transition_coverage::add_coverage;

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
    QueryResponse(QueryResult),
    ContinueAsNew(ContinueAsNewWorkflowExecution),
    CancelWorkflow(CancelWorkflowExecution),
    SetPatchMarker(SetPatchMarker),
    AddChildWorkflow(StartChildWorkflowExecution),
    CancelUnstartedChild(CancelUnstartedChildWorkflowExecution),
    RequestCancelExternalWorkflow(RequestCancelExternalWorkflowExecution),
    SignalExternalWorkflow(SignalExternalWorkflowExecution),
    CancelSignalWorkflow(CancelSignalWorkflow),
}

#[derive(thiserror::Error, Debug, derive_more::From)]
#[error("Lang provided workflow command with empty variant")]
pub struct EmptyWorkflowCommandErr;

impl TryFrom<WorkflowCommand> for WFCommand {
    type Error = EmptyWorkflowCommandErr;

    fn try_from(c: WorkflowCommand) -> Result<Self, Self::Error> {
        match c.variant.ok_or(EmptyWorkflowCommandErr)? {
            workflow_command::Variant::StartTimer(s) => Ok(Self::AddTimer(s)),
            workflow_command::Variant::CancelTimer(s) => Ok(Self::CancelTimer(s)),
            workflow_command::Variant::ScheduleActivity(s) => Ok(Self::AddActivity(s)),
            workflow_command::Variant::RequestCancelActivity(s) => {
                Ok(Self::RequestCancelActivity(s))
            }
            workflow_command::Variant::CompleteWorkflowExecution(c) => {
                Ok(Self::CompleteWorkflow(c))
            }
            workflow_command::Variant::FailWorkflowExecution(s) => Ok(Self::FailWorkflow(s)),
            workflow_command::Variant::RespondToQuery(s) => Ok(Self::QueryResponse(s)),
            workflow_command::Variant::ContinueAsNewWorkflowExecution(s) => {
                Ok(Self::ContinueAsNew(s))
            }
            workflow_command::Variant::CancelWorkflowExecution(s) => Ok(Self::CancelWorkflow(s)),
            workflow_command::Variant::SetPatchMarker(s) => Ok(Self::SetPatchMarker(s)),
            workflow_command::Variant::StartChildWorkflowExecution(s) => {
                Ok(Self::AddChildWorkflow(s))
            }
            workflow_command::Variant::RequestCancelExternalWorkflowExecution(s) => {
                Ok(Self::RequestCancelExternalWorkflow(s))
            }
            workflow_command::Variant::SignalExternalWorkflowExecution(s) => {
                Ok(Self::SignalExternalWorkflow(s))
            }
            workflow_command::Variant::CancelSignalWorkflow(s) => Ok(Self::CancelSignalWorkflow(s)),
            workflow_command::Variant::CancelUnstartedChildWorkflowExecution(s) => {
                Ok(Self::CancelUnstartedChild(s))
            }
        }
    }
}

#[derive(Copy, Clone, Debug, derive_more::Display, Eq, PartialEq)]
enum MachineKind {
    Activity,
    CancelWorkflow,
    ChildWorkflow,
    CompleteWorkflow,
    ContinueAsNewWorkflow,
    FailWorkflow,
    Timer,
    Version,
    WorkflowTask,
    SignalExternalWorkflow,
    CancelExternalWorkflow,
}

/// Extends [rustfsm::StateMachine] with some functionality specific to the temporal SDK.
///
/// Formerly known as `EntityStateMachine` in Java.
trait TemporalStateMachine: CheckStateMachineInFinal + Send {
    fn kind(&self) -> MachineKind;
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
        + Send
        + 'static,
    <SM as StateMachine>::Event: TryFrom<HistoryEvent> + TryFrom<CommandType> + Display,
    WFMachinesError: From<<<SM as StateMachine>::Event as TryFrom<HistoryEvent>>::Error>,
    <SM as StateMachine>::Command: Debug + Display,
    <SM as StateMachine>::State: Display,
    <SM as StateMachine>::Error: Into<WFMachinesError> + 'static + Send + Sync,
{
    fn kind(&self) -> MachineKind {
        self.kind()
    }

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
                Err(MachineError::InvalidTransition) => Err(WFMachinesError::Nondeterminism(
                    format!("Unexpected command {:?}", command_type),
                )),
                Err(MachineError::Underlying(e)) => Err(e.into()),
            }
        } else {
            Err(WFMachinesError::Nondeterminism(format!(
                "Unexpected command {:?}",
                command_type
            )))
        }
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        self.matches_event(event)
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
            Ok(c) => process_machine_commands(
                self,
                c,
                Some(EventInfo {
                    event,
                    has_next_event,
                }),
            ),
            Err(MachineError::InvalidTransition) => Err(WFMachinesError::Fatal(format!(
                "{} in state {} says the transition is invalid during event {}",
                self.name(),
                self.state(),
                event
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
        debug!(commands=%commands.display(), state=%machine.state(),
               machine_name=%StateMachine::name(machine), "Machine produced commands");
    }
    let mut machine_responses = vec![];
    for cmd in commands {
        machine_responses.extend(machine.adapt_response(cmd, event_info)?);
    }
    Ok(machine_responses)
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

    /// Allows robust identification of the type of machine
    fn kind(&self) -> MachineKind;
}

#[derive(Debug, Copy, Clone)]
struct EventInfo<'a> {
    event: &'a HistoryEvent,
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

#[derive(Debug)]
struct NewMachineWithCommand<T: TemporalStateMachine> {
    command: ProtoCommand,
    machine: T,
}

impl Debug for dyn TemporalStateMachine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.kind(), f)
    }
}
