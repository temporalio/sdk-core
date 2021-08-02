use crate::{
    machines::EventInfo,
    machines::{
        Cancellable, HistoryEvent, MachineResponse, NewMachineWithCommand, OnEventWrapper,
        WFMachinesAdapter, WFMachinesError,
    },
    protos::coresdk::workflow_commands::CancelWorkflowExecution,
    protos::temporal::api::command::v1::Command,
    protos::temporal::api::enums::v1::{CommandType, EventType},
};
use rustfsm::{fsm, TransitionResult};
use std::convert::TryFrom;

fsm! {
    pub(super)
    name CancelWorkflowMachine;
    command CancelWorkflowCommand;
    error WFMachinesError;

    Created --(Schedule, on_schedule) --> CancelWorkflowCommandCreated;

    CancelWorkflowCommandCreated --(CommandCancelWorkflowExecution)
        --> CancelWorkflowCommandCreated;
    CancelWorkflowCommandCreated --(WorkflowExecutionCanceled)
        --> CancelWorkflowCommandRecorded;
}

#[derive(thiserror::Error, Debug)]
pub(super) enum CancelWorkflowMachineError {}

#[derive(Debug, derive_more::Display)]
pub(super) enum CancelWorkflowCommand {}

pub(super) fn cancel_workflow(
    attribs: CancelWorkflowExecution,
) -> NewMachineWithCommand<CancelWorkflowMachine> {
    let mut machine = CancelWorkflowMachine {
        state: Created {}.into(),
        shared_state: (),
    };
    OnEventWrapper::on_event_mut(&mut machine, CancelWorkflowMachineEvents::Schedule)
        .expect("Scheduling continue as new machine doesn't fail");
    let command = Command {
        command_type: CommandType::CancelWorkflowExecution as i32,
        attributes: Some(attribs.into()),
    };
    NewMachineWithCommand { command, machine }
}

#[derive(Default, Clone)]
pub(super) struct CancelWorkflowCommandCreated {}

#[derive(Default, Clone)]
pub(super) struct CancelWorkflowCommandRecorded {}

impl From<CancelWorkflowCommandCreated> for CancelWorkflowCommandRecorded {
    fn from(_: CancelWorkflowCommandCreated) -> Self {
        Self::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> CancelWorkflowMachineTransition<CancelWorkflowCommandCreated> {
        TransitionResult::default()
    }
}

impl TryFrom<HistoryEvent> for CancelWorkflowMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        Ok(match EventType::from_i32(e.event_type) {
            Some(EventType::WorkflowExecutionCanceled) => Self::WorkflowExecutionCanceled,
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Cancel workflow machine does not handle this event: {}",
                    e
                )))
            }
        })
    }
}

impl TryFrom<CommandType> for CancelWorkflowMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::CancelWorkflowExecution => Self::CommandCancelWorkflowExecution,
            _ => return Err(()),
        })
    }
}

impl WFMachinesAdapter for CancelWorkflowMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(vec![])
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        event.event_type() == EventType::WorkflowExecutionCanceled
    }
}

impl Cancellable for CancelWorkflowMachine {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prototype_rust_sdk::WfExitValue;
    use crate::{
        machines::StartTimer,
        protos::coresdk::workflow_activation::{wf_activation_job, WfActivationJob},
        prototype_rust_sdk::{WfContext, WorkflowFunction, WorkflowResult},
        test_help::canned_histories,
        workflow::managed_wf::ManagedWFFunc,
    };
    use std::time::Duration;

    async fn wf_with_timer(mut ctx: WfContext) -> WorkflowResult<()> {
        ctx.timer(StartTimer {
            timer_id: "timer1".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(500).into()),
        })
        .await;
        Ok(WfExitValue::Cancelled)
    }

    #[tokio::test]
    async fn wf_completing_with_cancelled() {
        let func = WorkflowFunction::new(wf_with_timer);
        let t = canned_histories::timer_wf_cancel_req_cancelled("timer1");
        let mut wfm = ManagedWFFunc::new(t, func, vec![]);
        wfm.get_next_activation().await.unwrap();
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);

        let act = wfm.get_next_activation().await.unwrap();
        assert_matches!(
            act.jobs.as_slice(),
            [
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::FireTimer(_)),
                },
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::CancelWorkflow(_)),
                }
            ]
        );
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(
            commands[0].command_type,
            CommandType::CancelWorkflowExecution as i32
        );

        assert!(wfm.get_next_activation().await.unwrap().jobs.is_empty());
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 0);
        wfm.shutdown().await.unwrap();
    }
}
