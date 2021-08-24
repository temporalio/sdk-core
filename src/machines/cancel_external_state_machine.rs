use crate::{
    machines::{
        workflow_machines::MachineResponse, Cancellable, EventInfo, MachineKind,
        NewMachineWithCommand, OnEventWrapper, WFMachinesAdapter, WFMachinesError,
    },
    protos::{
        coresdk::common::NamespacedWorkflowExecution,
        coresdk::workflow_activation::ResolveRequestCancelExternalWorkflow,
        temporal::api::command::v1::{
            command, Command, RequestCancelExternalWorkflowExecutionCommandAttributes,
        },
        temporal::api::enums::v1::{
            CancelExternalWorkflowExecutionFailedCause, CommandType, EventType,
        },
        temporal::api::failure::v1::{failure::FailureInfo, ApplicationFailureInfo, Failure},
        temporal::api::history::v1::{history_event, HistoryEvent},
    },
};
use rustfsm::{fsm, TransitionResult};
use std::convert::TryFrom;

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
) -> NewMachineWithCommand<CancelExternalMachine> {
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
        },
    );
    let cmd = Command {
        command_type: CommandType::RequestCancelExternalWorkflowExecution as i32,
        attributes: Some(cmd_attrs),
    };
    NewMachineWithCommand {
        command: cmd,
        machine: s,
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
                    CancelExternalWorkflowExecutionFailedCause::ExternalWorkflowExecutionNotFound => "it was not found"
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

    fn kind(&self) -> MachineKind {
        MachineKind::CancelExternalWorkflow
    }
}

impl Cancellable for CancelExternalMachine {}
