use crate::machines::{EventInfo, MachineKind, NewMachineWithCommand, WFMachinesAdapter};
use crate::{
    machines::{workflow_machines::MachineResponse, Cancellable, OnEventWrapper, WFMachinesError},
    protos::{
        coresdk::{
            common::{Payload, WorkflowExecution},
            IntoPayloadsExt,
        },
        temporal::api::{
            command::v1::{command, Command, SignalExternalWorkflowExecutionCommandAttributes},
            common::v1::WorkflowExecution as UpstreamWE,
            enums::v1::{CommandType, EventType},
            history::v1::HistoryEvent,
        },
    },
};
use rustfsm::{fsm, MachineError, TransitionResult};
use std::convert::TryFrom;

fsm! {
    pub(super) name SignalExternalMachine;
    command SignalExternalCommand;
    error WFMachinesError;

    Created --(Schedule, on_schedule) --> SignalExternalCommandCreated;

    SignalExternalCommandCreated --(CommandSignalExternalWorkflowExecution)
        --> SignalExternalCommandCreated;
    SignalExternalCommandCreated --(Cancel, on_cancel) --> Canceled;
    SignalExternalCommandCreated
      --(SignalExternalWorkflowExecutionInitiated, on_signal_external_workflow_execution_initiated)
        --> SignalExternalCommandRecorded;

    SignalExternalCommandRecorded --(Cancel) --> SignalExternalCommandRecorded;
    SignalExternalCommandRecorded
      --(ExternalWorkflowExecutionSignaled, on_external_workflow_execution_signaled) --> Signaled;
    SignalExternalCommandRecorded
      --(SignalExternalWorkflowExecutionFailed, on_signal_external_workflow_execution_failed) --> Failed;
}

#[derive(Debug, derive_more::Display)]
pub(super) enum SignalExternalCommand {}

pub(super) fn new_external_signal(
    workflow_execution: WorkflowExecution,
    signal_name: String,
    payloads: impl IntoIterator<Item = Payload>,
    only_child: bool,
) -> NewMachineWithCommand<SignalExternalMachine> {
    let mut s = SignalExternalMachine {
        state: Created {}.into(),
        shared_state: (),
    };
    OnEventWrapper::on_event_mut(&mut s, SignalExternalMachineEvents::Schedule)
        .expect("Scheduling activities doesn't fail");
    let cmd_attrs = command::Attributes::SignalExternalWorkflowExecutionCommandAttributes(
        SignalExternalWorkflowExecutionCommandAttributes {
            namespace: workflow_execution.namespace,
            execution: Some(UpstreamWE {
                workflow_id: workflow_execution.workflow_id,
                run_id: workflow_execution.run_id,
            }),
            signal_name,
            input: payloads.into_payloads(),
            // Apparently this is effectively deprecated at this point
            control: "".to_string(),
            child_workflow_only: only_child,
        },
    );
    let cmd = Command {
        command_type: CommandType::ScheduleActivityTask as i32,
        attributes: Some(cmd_attrs),
    };
    NewMachineWithCommand {
        command: cmd,
        machine: s,
    }
}

#[derive(Default, Clone)]
pub(super) struct Canceled {}

#[derive(Default, Clone)]
pub(super) struct Created {}

impl Created {
    pub(super) fn on_schedule(
        self,
    ) -> SignalExternalMachineTransition<SignalExternalCommandCreated> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct Failed {}

#[derive(Default, Clone)]
pub(super) struct SignalExternalCommandCreated {}

impl SignalExternalCommandCreated {
    pub(super) fn on_cancel(self) -> SignalExternalMachineTransition<Canceled> {
        TransitionResult::default()
    }
    pub(super) fn on_signal_external_workflow_execution_initiated(
        self,
    ) -> SignalExternalMachineTransition<SignalExternalCommandRecorded> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct SignalExternalCommandRecorded {}

impl SignalExternalCommandRecorded {
    pub(super) fn on_external_workflow_execution_signaled(
        self,
    ) -> SignalExternalMachineTransition<Signaled> {
        unimplemented!()
    }
    pub(super) fn on_signal_external_workflow_execution_failed(
        self,
    ) -> SignalExternalMachineTransition<Failed> {
        unimplemented!()
    }
}

#[derive(Default, Clone)]
pub(super) struct Signaled {}

impl TryFrom<CommandType> for SignalExternalMachineEvents {
    type Error = ();

    fn try_from(c: CommandType) -> Result<Self, Self::Error> {
        Ok(match c {
            CommandType::SignalExternalWorkflowExecution => {
                Self::CommandSignalExternalWorkflowExecution
            }
            _ => return Err(()),
        })
    }
}
impl TryFrom<HistoryEvent> for SignalExternalMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistoryEvent) -> Result<Self, Self::Error> {
        Ok(match e.event_type() {
            EventType::ExternalWorkflowExecutionSignaled => Self::ExternalWorkflowExecutionSignaled,
            EventType::SignalExternalWorkflowExecutionInitiated => {
                Self::SignalExternalWorkflowExecutionInitiated
            }
            EventType::SignalExternalWorkflowExecutionFailed => {
                Self::SignalExternalWorkflowExecutionFailed
            }
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Signal external WF machine does not handle this event: {}",
                    e
                )))
            }
        })
    }
}

impl WFMachinesAdapter for SignalExternalMachine {
    fn adapt_response(
        &self,
        _my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        panic!("Signal external machine does not produce commands")
    }

    fn matches_event(&self, event: &HistoryEvent) -> bool {
        matches!(
            event.event_type(),
            EventType::ExternalWorkflowExecutionSignaled
                | EventType::SignalExternalWorkflowExecutionInitiated
                | EventType::SignalExternalWorkflowExecutionFailed
        )
    }

    fn kind(&self) -> MachineKind {
        MachineKind::SignalExternalWorkflow
    }
}

impl Cancellable for SignalExternalMachine {
    fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<Self::Error>> {
        let res = OnEventWrapper::on_event_mut(self, SignalExternalMachineEvents::Cancel)?;
        if !res.is_empty() {
            panic!("Signal external machine cancel event should not produce commands");
        }
        Ok(vec![])
    }

    fn was_cancelled_before_sent_to_server(&self) -> bool {
        // We are only ever in the canceled state if canceled before sent to server, there is no
        // after sent cancellation here.
        matches!(self.state, SignalExternalMachineState::Canceled(_))
    }
}
