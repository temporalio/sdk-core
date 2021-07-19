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
        Ok(match EventType::from_i32(e.event_type) {
            Some(EventType::WorkflowExecutionContinuedAsNew) => {
                Self::WorkflowExecutionContinuedAsNew
            }
            _ => {
                return Err(WFMachinesError::UnexpectedEvent(
                    e,
                    "Continue as new workflow machine does not handle this event",
                ))
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
        _event: &HistoryEvent,
        _has_next_event: bool,
        _my_command: Self::Command,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(vec![])
    }
}

impl Cancellable for ContinueAsNewWorkflowMachine {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        machines::{workflow_machines::WorkflowMachines, StartTimer},
        test_help::canned_histories,
        test_workflow_driver::{TestWorkflowDriver, WfContext},
        workflow::WorkflowManager,
    };
    use std::time::Duration;

    async fn wf_with_timer(mut ctx: WfContext) {
        ctx.timer(StartTimer {
            timer_id: "timer1".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(500).into()),
        })
        .await;
        ctx.continue_as_new(ContinueAsNewWorkflowExecution {
            arguments: vec![[1].into()],
            ..Default::default()
        });
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn wf_completing_with_continue_as_new() {
        let twd = TestWorkflowDriver::new(vec![], wf_with_timer);
        let t = canned_histories::timer_then_continue_as_new("timer1");
        let state_machines = WorkflowMachines::new(
            "wfid".to_string(),
            "runid".to_string(),
            t.as_history_update(),
            Box::new(twd).into(),
        );
        let mut wfm = WorkflowManager::new_from_machines(state_machines);
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);

        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::ContinueAsNewWorkflowExecution as i32
        );

        assert!(wfm.get_next_activation().await.unwrap().jobs.is_empty());
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 0);
    }
}
