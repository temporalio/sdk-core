use crate::{
    machines::{OnEventWrapper, WFMachinesError},
    protos::coresdk::IntoPayloadsExt,
    protos::{
        coresdk::{
            common::{Payload, WorkflowExecution},
            workflow_commands::{
                signal_external_workflow_execution::Target, SignalExternalWorkflowExecution,
            },
            FromPayloadsExt,
        },
        temporal::api::{
            command::v1::{command, Command, SignalExternalWorkflowExecutionCommandAttributes},
            common::v1::WorkflowExecution as UpstreamWE,
            enums::v1::CommandType,
        },
    },
};
use rustfsm::{fsm, TransitionResult};

fsm! {
    pub(super) name SignalExternalMachine;
    command SignalExternalCommand;
    error WFMachinesError;

    Created --(Schedule, on_schedule) --> SignalExternalCommandCreated;

    SignalExternalCommandCreated --(CommandSignalExternalWorkflowExecution) --> SignalExternalCommandCreated;
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

pub(super) enum SignalExternalCommand {}

pub(super) fn new_external_signal(
    workflow_execution: WorkflowExecution,
    signal_name: String,
    payloads: impl IntoIterator<Item = Payload>,
    only_child: bool,
) -> (SignalExternalMachine, Command) {
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
    (s, cmd)
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
        unimplemented!()
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
