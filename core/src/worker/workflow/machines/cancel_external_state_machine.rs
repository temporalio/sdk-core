use super::{
    workflow_machines::MachineResponse, Cancellable, EventInfo, NewMachineWithCommand,
    OnEventWrapper, WFMachinesAdapter, WFMachinesError,
};
use rustfsm::{fsm, TransitionResult};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    coresdk::{
        common::NamespacedWorkflowExecution,
        workflow_activation::ResolveRequestCancelExternalWorkflow,
    },
    temporal::api::{
        command::v1::{command, Command, RequestCancelExternalWorkflowExecutionCommandAttributes},
        enums::v1::{CancelExternalWorkflowExecutionFailedCause, CommandType, EventType},
        failure::v1::{failure::FailureInfo, ApplicationFailureInfo, Failure},
        history::v1::{history_event, HistoryEvent},
    },
};

fsm! {
    pub(super)
    name CancelExternalMachine;
    command CancelExternalCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(Schedule, on_schedule) --> RequestCancelExternalCommandCreated;

    RequestCancelExternalCommandCreated --(CommandRequestCancelExternalWorkflowExecution)
      --> RequestCancelExternalCommandCreated;
    RequestCancelExternalCommandCreated
      --(RequestCancelExternalWorkflowExecutionInitiated,
         on_request_cancel_external_workflow_execution_initiated)
      --> RequestCancelExternalCommandRecorded;

    RequestCancelExternalCommandRecorded
      --(ExternalWorkflowExecutionCancelRequested, on_external_workflow_execution_cancel_requested)
      --> CancelRequested;
    RequestCancelExternalCommandRecorded
      --(RequestCancelExternalWorkflowExecutionFailed(CancelExternalWorkflowExecutionFailedCause),
         on_request_cancel_external_workflow_execution_failed) --> RequestCancelFailed;
}

#[derive(Default, Clone)]
pub(super) struct SharedState {
    seq: u32,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum CancelExternalCommand {
    /// The target workflow has been notified of the cancel
    Requested,
    #[display(fmt = "Failed")]
    Failed(CancelExternalWorkflowExecutionFailedCause),
}

pub(super) fn new_external_cancel(
    seq: u32,
    workflow_execution: NamespacedWorkflowExecution,
    only_child: bool,
    reason: String,
) -> NewMachineWithCommand {
    let mut s = CancelExternalMachine {
        state: Created {}.into(),
        shared_state: SharedState { seq },
    };
    OnEventWrapper::on_event_mut(&mut s, CancelExternalMachineEvents::Schedule)
        .expect("Scheduling cancel external wf command doesn't fail");
    let cmd_attrs = command::Attributes::RequestCancelExternalWorkflowExecutionCommandAttributes(
        RequestCancelExternalWorkflowExecutionCommandAttributes {
            namespace: workflow_execution.namespace,
            workflow_id: workflow_execution.workflow_id,
            run_id: workflow_execution.run_id,
            // Apparently this is effectively deprecated at this point
            control: "".to_string(),
            child_workflow_only: only_child,
            reason,
        },
    );
    let cmd = Command {
        command_type: CommandType::RequestCancelExternalWorkflowExecution as i32,
        attributes: Some(cmd_attrs),
    };
    NewMachineWithCommand {
        command: cmd,
        machine: s.into(),
    }
}

#[derive(Default, Clone)]
pub(super) struct CancelRequested {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> CancelExternalMachineTransition<RequestCancelExternalCommandCreated> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct RequestCancelExternalCommandCreated {}

impl RequestCancelExternalCommandCreated {
    pub(super) fn on_request_cancel_external_workflow_execution_initiated(
        self,
    ) -> CancelExternalMachineTransition<RequestCancelExternalCommandRecorded> {
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct RequestCancelExternalCommandRecorded {}

impl RequestCancelExternalCommandRecorded {
    pub(super) fn on_external_workflow_execution_cancel_requested(
        self,
    ) -> CancelExternalMachineTransition<CancelRequested> {
        TransitionResult::commands(vec![CancelExternalCommand::Requested])
    }
    pub(super) fn on_request_cancel_external_workflow_execution_failed(
        self,
        cause: CancelExternalWorkflowExecutionFailedCause,
    ) -> CancelExternalMachineTransition<RequestCancelFailed> {
        TransitionResult::commands(vec![CancelExternalCommand::Failed(cause)])
    }
}

#[derive(Default, Clone)]
pub(super) struct RequestCancelFailed {}

impl TryFrom<CommandType> for CancelExternalMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        match c {
            CommandType::RequestCancelExternalWorkflowExecution => {
                Ok(Self::CommandRequestCancelExternalWorkflowExecution)
            }
            _ => Err(()),
        }
    }
}

impl TryFrom<HistoryEvent> for CancelExternalMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        Ok(match e.event_type() {
            EventType::ExternalWorkflowExecutionCancelRequested => {
                Self::ExternalWorkflowExecutionCancelRequested
            }
            EventType::RequestCancelExternalWorkflowExecutionInitiated => {
                Self::RequestCancelExternalWorkflowExecutionInitiated
            }
            EventType::RequestCancelExternalWorkflowExecutionFailed => {
                if let Some(history_event::Attributes::RequestCancelExternalWorkflowExecutionFailedEventAttributes(attrs)) = e.attributes {
                    Self::RequestCancelExternalWorkflowExecutionFailed(attrs.cause())
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "Cancelworkflow failed attributes were unset: {}",
                        e
                    )));
                }
            }
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Cancel external WF machine does not handle this event: {}",
                    e
                )))
            }
        })
    }
}

impl WFMachinesAdapter for CancelExternalMachine {
    fn adapt_response(
        &self,
        my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            CancelExternalCommand::Requested => {
                vec![ResolveRequestCancelExternalWorkflow {
                    seq: self.shared_state.seq,
                    failure: None,
                }
                .into()]
            }
            CancelExternalCommand::Failed(f) => {
                let reason = match f {
                    CancelExternalWorkflowExecutionFailedCause::Unspecified => "unknown",
                    CancelExternalWorkflowExecutionFailedCause::ExternalWorkflowExecutionNotFound
                    | CancelExternalWorkflowExecutionFailedCause::NamespaceNotFound  => "not found"
                };
                vec![ResolveRequestCancelExternalWorkflow {
                    seq: self.shared_state.seq,
                    failure: Some(Failure {
                        message: format!("Unable to cancel external workflow because {}", reason),
                        failure_info: Some(FailureInfo::ApplicationFailureInfo(
                            ApplicationFailureInfo {
                                r#type: f.to_string(),
                                ..Default::default()
                            },
                        )),
                        ..Default::default()
                    }),
                }
                .into()]
            }
        })
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        matches!(
            event.event_type(),
            EventType::ExternalWorkflowExecutionCancelRequested
                | EventType::RequestCancelExternalWorkflowExecutionFailed
                | EventType::RequestCancelExternalWorkflowExecutionInitiated
        )
    }
}

impl Cancellable for CancelExternalMachine {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{replay::TestHistoryBuilder, worker::workflow::ManagedWFFunc};
    use temporal_sdk::{WfContext, WorkflowFunction, WorkflowResult};

    async fn cancel_sender(ctx: WfContext) -> WorkflowResult<()> {
        let res = ctx
            .cancel_external(NamespacedWorkflowExecution {
                namespace: "some_namespace".to_string(),
                workflow_id: "fake_wid".to_string(),
                run_id: "fake_rid".to_string(),
            })
            .await;
        if res.is_err() {
            Err(anyhow::anyhow!("Cancel fail!"))
        } else {
            Ok(().into())
        }
    }

    #[rstest::rstest]
    #[case::succeeds(false)]
    #[case::fails(true)]
    #[tokio::test]
    async fn sends_cancel(#[case] fails: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        let id = t.add_cancel_external_wf(NamespacedWorkflowExecution {
            namespace: "some_namespace".to_string(),
            workflow_id: "fake_wid".to_string(),
            run_id: "fake_rid".to_string(),
        });
        if fails {
            t.add_cancel_external_wf_failed(id);
        } else {
            t.add_cancel_external_wf_completed(id);
        }
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let wff = WorkflowFunction::new(cancel_sender);
        let mut wfm = ManagedWFFunc::new(t, wff, vec![]);
        wfm.get_next_activation().await.unwrap();
        let cmds = wfm.get_server_commands().commands;
        assert_eq!(cmds.len(), 1);
        assert_eq!(
            cmds[0].command_type(),
            CommandType::RequestCancelExternalWorkflowExecution
        );
        wfm.get_next_activation().await.unwrap();
        let cmds = wfm.get_server_commands().commands;
        assert_eq!(cmds.len(), 1);
        if fails {
            assert_eq!(cmds[0].command_type(), CommandType::FailWorkflowExecution);
        } else {
            assert_eq!(
                cmds[0].command_type(),
                CommandType::CompleteWorkflowExecution
            );
        }

        wfm.shutdown().await.unwrap();
    }
}
