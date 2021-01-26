use crate::{
    machines::{
        workflow_machines::WorkflowMachines, AddCommand, CancellableCommand, WFCommand,
        WFMachinesAdapter, WFMachinesError,
    },
    protos::temporal::api::{
        command::v1::{Command, CompleteWorkflowExecutionCommandAttributes},
        enums::v1::{CommandType, EventType},
        history::v1::HistoryEvent,
    },
};
use rustfsm::{fsm, StateMachine, TransitionResult};
use std::cell::RefCell;
use std::{convert::TryFrom, rc::Rc};

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
    AddCommand(AddCommand),
}

/// Complete a workflow
pub(super) fn complete_workflow(
    attribs: CompleteWorkflowExecutionCommandAttributes,
) -> CancellableCommand {
    let (machine, add_cmd) = CompleteWorkflowMachine::new_scheduled(attribs);
    CancellableCommand::Active {
        command: add_cmd.command,
        machine: Rc::new(RefCell::new(machine)),
    }
}

impl CompleteWorkflowMachine {
    /// Create a new WF machine and schedule it
    pub(crate) fn new_scheduled(
        attribs: CompleteWorkflowExecutionCommandAttributes,
    ) -> (Self, AddCommand) {
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
    type Error = ();

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        Ok(match EventType::from_i32(e.event_type) {
            Some(EventType::WorkflowExecutionCompleted) => Self::WorkflowExecutionCompleted,
            _ => return Err(()),
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
            CompleteWFCommand::AddCommand(cmd.into()),
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
        _wf_machines: &mut WorkflowMachines,
        _event: &HistoryEvent,
        _has_next_event: bool,
        _my_command: CompleteWFCommand,
    ) -> Result<(), WFMachinesError> {
        Ok(())
    }
}
