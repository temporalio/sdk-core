use super::{
    workflow_machines::MachineResponse, Cancellable, EventInfo, NewMachineWithCommand,
    OnEventWrapper, WFMachinesAdapter, WFMachinesError,
};
use crate::worker::workflow::machines::HistEventData;
use rustfsm::{fsm, StateMachine, TransitionResult};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    coresdk::workflow_commands::FailWorkflowExecution,
    temporal::api::{
        command::v1::Command as ProtoCommand,
        enums::v1::{CommandType, EventType},
        history::v1::HistoryEvent,
    },
};

fsm! {
    pub(super) name FailWorkflowMachine;
    command FailWFCommand;
    error WFMachinesError;
    shared_state ();

    Created --(Schedule, on_schedule) --> FailWorkflowCommandCreated;

    FailWorkflowCommandCreated --(CommandFailWorkflowExecution) --> FailWorkflowCommandCreated;
    FailWorkflowCommandCreated --(WorkflowExecutionFailed) --> FailWorkflowCommandRecorded;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum FailWFCommand {
    AddCommand(ProtoCommand),
}

/// Fail a workflow
pub(super) fn fail_workflow(attribs: FailWorkflowExecution) -> NewMachineWithCommand {
    let (machine, add_cmd) = FailWorkflowMachine::new_scheduled(attribs);
    NewMachineWithCommand {
        command: add_cmd,
        machine: machine.into(),
    }
}

impl FailWorkflowMachine {
    /// Create a new WF machine and schedule it
    pub(crate) fn new_scheduled(attribs: FailWorkflowExecution) -> (Self, ProtoCommand) {
        let mut s = Self::from_parts(Created { attribs }.into(), ());
        let cmd = match OnEventWrapper::on_event_mut(&mut s, FailWorkflowMachineEvents::Schedule)
            .expect("Scheduling fail wf machines doesn't fail")
            .pop()
        {
            Some(FailWFCommand::AddCommand(c)) => c,
            _ => panic!("Fail wf machine on_schedule must produce command"),
        };
        (s, cmd)
    }
}

#[derive(Default, Clone)]
pub(super) struct Created {
    attribs: FailWorkflowExecution,
}

impl Created {
    pub(super) fn on_schedule(self) -> FailWorkflowMachineTransition<FailWorkflowCommandCreated> {
        let cmd = ProtoCommand {
            command_type: CommandType::FailWorkflowExecution as i32,
            attributes: Some(self.attribs.into()),
        };
        TransitionResult::commands(vec![FailWFCommand::AddCommand(cmd)])
    }
}

#[derive(Default, Clone)]
pub(super) struct FailWorkflowCommandCreated {}

#[derive(Default, Clone)]
pub(super) struct FailWorkflowCommandRecorded {}

impl From<FailWorkflowCommandCreated> for FailWorkflowCommandRecorded {
    fn from(_: FailWorkflowCommandCreated) -> Self {
        Self::default()
    }
}

impl TryFrom<HistEventData> for FailWorkflowMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
        Ok(match e.event_type() {
            EventType::WorkflowExecutionFailed => Self::WorkflowExecutionFailed,
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Fail workflow machine does not handle this event: {e}"
                )))
            }
        })
    }
}

impl TryFrom<CommandType> for FailWorkflowMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::FailWorkflowExecution => Self::CommandFailWorkflowExecution,
            _ => return Err(()),
        })
    }
}

impl WFMachinesAdapter for FailWorkflowMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(vec![])
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        event.event_type() == EventType::WorkflowExecutionFailed
    }
}

impl Cancellable for FailWorkflowMachine {}
