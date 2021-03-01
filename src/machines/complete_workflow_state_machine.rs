use crate::{
    machines::{
        workflow_machines::MachineResponse, Cancellable, NewMachineWithCommand, WFMachinesAdapter,
        WFMachinesError,
    },
    protos::temporal::api::{
        command::v1::{Command, CompleteWorkflowExecutionCommandAttributes},
        enums::v1::{CommandType, EventType},
        history::v1::HistoryEvent,
    },
};
use rustfsm::{fsm, StateMachine, TransitionResult};
use std::convert::TryFrom;

fsm! {
    pub(super)
    name CompleteWorkflowMachine;
    command CompleteWFCommand;
    error WFMachinesError;
    shared_state CompleteWorkflowExecutionCommandAttributes;

    Created --(Schedule, shared on_schedule) --> CompleteWorkflowCommandCreated;

    CompleteWorkflowCommandCreated --(CommandCompleteWorkflowExecution)
        --> CompleteWorkflowCommandCreated;
    CompleteWorkflowCommandCreated --(WorkflowExecutionCompleted)
        --> CompleteWorkflowCommandRecorded;
}

#[derive(Debug)]
pub(super) enum CompleteWFCommand {
    AddCommand(Command),
}

/// Complete a workflow
pub(super) fn complete_workflow(
    attribs: CompleteWorkflowExecutionCommandAttributes,
) -> NewMachineWithCommand<CompleteWorkflowMachine> {
    let (machine, add_cmd) = CompleteWorkflowMachine::new_scheduled(attribs);
    NewMachineWithCommand {
        command: add_cmd,
        machine,
    }
}

impl CompleteWorkflowMachine {
    /// Create a new WF machine and schedule it
    pub(crate) fn new_scheduled(
        attribs: CompleteWorkflowExecutionCommandAttributes,
    ) -> (Self, Command) {
        let mut s = Self {
            state: Created {}.into(),
            shared_state: attribs,
        };
        let cmd = match s
            .on_event_mut(CompleteWorkflowMachineEvents::Schedule)
            .expect("Scheduling timers doesn't fail")
            .pop()
        {
            Some(CompleteWFCommand::AddCommand(c)) => c,
            _ => panic!("Timer on_schedule must produce command"),
        };
        (s, cmd)
    }
}

impl TryFrom<HistoryEvent> for CompleteWorkflowMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        Ok(match EventType::from_i32(e.event_type) {
            Some(EventType::WorkflowExecutionCompleted) => Self::WorkflowExecutionCompleted,
            _ => {
                return Err(WFMachinesError::UnexpectedEvent(
                    e,
                    "Complete workflow machine does not handle this event",
                ))
            }
        })
    }
}

impl TryFrom<CommandType> for CompleteWorkflowMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::CompleteWorkflowExecution => Self::CommandCompleteWorkflowExecution,
            _ => return Err(()),
        })
    }
}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
        dat: CompleteWorkflowExecutionCommandAttributes,
    ) -> CompleteWorkflowMachineTransition {
        let cmd = Command {
            command_type: CommandType::CompleteWorkflowExecution as i32,
            attributes: Some(dat.into()),
        };
        TransitionResult::commands::<_, CompleteWorkflowCommandCreated>(vec![
            CompleteWFCommand::AddCommand(cmd),
        ])
    }
}

#[derive(thiserror::Error, Debug)]
pub(super) enum CompleteWorkflowMachineError {}

#[derive(Default, Clone)]
pub(super) struct CompleteWorkflowCommandCreated {}

#[derive(Default, Clone)]
pub(super) struct CompleteWorkflowCommandRecorded {}

impl From<CompleteWorkflowCommandCreated> for CompleteWorkflowCommandRecorded {
    fn from(_: CompleteWorkflowCommandCreated) -> Self {
        Default::default()
    }
}

impl WFMachinesAdapter for CompleteWorkflowMachine {
    fn adapt_response(
        &self,
        _event: &HistoryEvent,
        _has_next_event: bool,
        _my_command: CompleteWFCommand,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(vec![])
    }
}

impl Cancellable for CompleteWorkflowMachine {}
