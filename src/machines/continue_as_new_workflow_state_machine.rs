use crate::machines::{EventInfo, MachineKind};
use crate::{
    machines::{
        Cancellable, HistoryEvent, MachineResponse, NewMachineWithCommand, OnEventWrapper,
        WFMachinesAdapter, WFMachinesError,
    },
    protos::coresdk::workflow_commands::ContinueAsNewWorkflowExecution,
    protos::temporal::api::command::v1::Command,
    protos::temporal::api::enums::v1::{CommandType, EventType},
};
use rustfsm::{fsm, TransitionResult};
use std::convert::TryFrom;

fsm! {
    pub(super)
    name ContinueAsNewWorkflowMachine;
    command ContinueAsNewWorkflowCommand;
    error WFMachinesError;

    Created --(Schedule, on_schedule) --> ContinueAsNewWorkflowCommandCreated;

    ContinueAsNewWorkflowCommandCreated --(CommandContinueAsNewWorkflowExecution)
        --> ContinueAsNewWorkflowCommandCreated;
    ContinueAsNewWorkflowCommandCreated --(WorkflowExecutionContinuedAsNew)
        --> ContinueAsNewWorkflowCommandRecorded;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum ContinueAsNewWorkflowCommand {}

pub(super) fn continue_as_new(
    attribs: ContinueAsNewWorkflowExecution,
) -> NewMachineWithCommand<ContinueAsNewWorkflowMachine> {
    let mut machine = ContinueAsNewWorkflowMachine {
        state: Created {}.into(),
        shared_state: (),
    };
    OnEventWrapper::on_event_mut(&mut machine, ContinueAsNewWorkflowMachineEvents::Schedule)
        .expect("Scheduling continue as new machine doesn't fail");
    let command = Command {
        command_type: CommandType::ContinueAsNewWorkflowExecution as i32,
        attributes: Some(attribs.into()),
    };
    NewMachineWithCommand { command, machine }
}

#[derive(Default, Clone)]
pub(super) struct ContinueAsNewWorkflowCommandCreated {}

#[derive(Default, Clone)]
pub(super) struct ContinueAsNewWorkflowCommandRecorded {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> ContinueAsNewWorkflowMachineTransition<ContinueAsNewWorkflowCommandCreated> {
        TransitionResult::default()
    }
}

impl From<ContinueAsNewWorkflowCommandCreated> for ContinueAsNewWorkflowCommandRecorded {
    fn from(_: ContinueAsNewWorkflowCommandCreated) -> Self {
        Default::default()
    }
}

impl TryFrom<HistoryEvent> for ContinueAsNewWorkflowMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        Ok(match e.event_type() {
            EventType::WorkflowExecutionContinuedAsNew => Self::WorkflowExecutionContinuedAsNew,
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Continue as new workflow machine does not handle this event: {}",
                    e
                )))
            }
        })
    }
}

impl TryFrom<CommandType> for ContinueAsNewWorkflowMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::ContinueAsNewWorkflowExecution => {
                Self::CommandContinueAsNewWorkflowExecution
            }
            _ => return Err(()),
        })
    }
}

impl WFMachinesAdapter for ContinueAsNewWorkflowMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(vec![])
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        event.event_type() == EventType::WorkflowExecutionContinuedAsNew
    }

    fn kind(&self) -> MachineKind {
        MachineKind::ContinueAsNewWorkflow
    }
}

impl Cancellable for ContinueAsNewWorkflowMachine {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prototype_rust_sdk::{WfExitValue, WorkflowResult};
    use crate::{
        prototype_rust_sdk::{WfContext, WorkflowFunction},
        test_help::canned_histories,
        workflow::managed_wf::ManagedWFFunc,
    };
    use std::time::Duration;

    async fn wf_with_timer(mut ctx: WfContext) -> WorkflowResult<()> {
        ctx.timer(Duration::from_millis(500)).await;
        Ok(WfExitValue::ContinueAsNew(ContinueAsNewWorkflowExecution {
            arguments: vec![[1].into()],
            ..Default::default()
        }))
    }

    #[tokio::test]
    async fn wf_completing_with_continue_as_new() {
        let func = WorkflowFunction::new(wf_with_timer);
        let t = canned_histories::timer_then_continue_as_new("1");
        let mut wfm = ManagedWFFunc::new(t, func, vec![]);
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::ContinueAsNewWorkflowExecution as i32
        );

        assert!(wfm.get_next_activation().await.unwrap().jobs.is_empty());
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 0);
        wfm.shutdown().await.unwrap();
    }
}
