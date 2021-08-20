use crate::{
    machines::{
        workflow_machines::MachineResponse, Cancellable, EventInfo, MachineKind,
        NewMachineWithCommand, OnEventWrapper, WFMachinesAdapter, WFMachinesError,
    },
    protos::{
        coresdk::{
            common::{Payload, WorkflowExecution},
            workflow_activation::ResolveSignalExternalWorkflow,
            IntoPayloadsExt,
        },
        temporal::api::{
            command::v1::{command, Command, SignalExternalWorkflowExecutionCommandAttributes},
            common::v1::WorkflowExecution as UpstreamWE,
            enums::v1::{CommandType, EventType, SignalExternalWorkflowExecutionFailedCause},
            failure::v1::{failure::FailureInfo, ApplicationFailureInfo, Failure},
            history::v1::{history_event, HistoryEvent},
        },
    },
};
use rustfsm::{fsm, MachineError, TransitionResult};
use std::convert::TryFrom;

fsm! {
    pub(super) name SignalExternalMachine;
    command SignalExternalCommand;
    error WFMachinesError;
    shared_state SharedState;

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
      --(SignalExternalWorkflowExecutionFailed(SignalExternalWorkflowExecutionFailedCause),
         on_signal_external_workflow_execution_failed) --> Failed;
}

#[derive(Default, Clone)]
pub(super) struct SharedState {
    seq: u32,
}

#[derive(Debug, derive_more::Display)]
pub(super) enum SignalExternalCommand {
    Signaled,
    #[display(fmt = "Failed")]
    Failed(SignalExternalWorkflowExecutionFailedCause),
}

pub(super) fn new_external_signal(
    seq: u32,
    workflow_execution: WorkflowExecution,
    signal_name: String,
    payloads: impl IntoIterator<Item = Payload>,
    only_child: bool,
) -> NewMachineWithCommand<SignalExternalMachine> {
    let mut s = SignalExternalMachine {
        state: Created {}.into(),
        shared_state: SharedState { seq },
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
        command_type: CommandType::SignalExternalWorkflowExecution as i32,
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
        TransitionResult::default()
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
        TransitionResult::default()
    }
}

#[derive(Default, Clone)]
pub(super) struct SignalExternalCommandRecorded {}

impl SignalExternalCommandRecorded {
    pub(super) fn on_external_workflow_execution_signaled(
        self,
    ) -> SignalExternalMachineTransition<Signaled> {
        TransitionResult::commands(vec![SignalExternalCommand::Signaled])
    }
    pub(super) fn on_signal_external_workflow_execution_failed(
        self,
        cause: SignalExternalWorkflowExecutionFailedCause,
    ) -> SignalExternalMachineTransition<Failed> {
        TransitionResult::commands(vec![SignalExternalCommand::Failed(cause)])
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
                if let Some(
                    history_event::Attributes::SignalExternalWorkflowExecutionFailedEventAttributes(
                        attrs,
                    ),
                ) = e.attributes
                {
                    Self::SignalExternalWorkflowExecutionFailed(attrs.cause())
                } else {
                    return Err(WFMachinesError::Fatal(format!(
                        "Signal workflow failed attributes were unset: {}",
                        e
                    )));
                }
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
        my_command: Self::Command,
        _event_info: Option<EventInfo>,
    ) -> Result<Vec<MachineResponse>, WFMachinesError> {
        Ok(match my_command {
            SignalExternalCommand::Signaled => {
                vec![ResolveSignalExternalWorkflow {
                    seq: self.shared_state.seq,
                    failure: None,
                }
                .into()]
            }
            SignalExternalCommand::Failed(f) => {
                vec![ResolveSignalExternalWorkflow {
                    seq: self.shared_state.seq,
                    // TODO: Create new failure type upstream for this
                    failure: Some(Failure {
                        message: "Unable to signal external workflow because it was not found"
                            .to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        prototype_rust_sdk::{CancellableFuture, WfContext, WorkflowFunction, WorkflowResult},
        test_help::TestHistoryBuilder,
        workflow::managed_wf::ManagedWFFunc,
    };

    const SIGNAME: &str = "signame";

    async fn signal_sender(mut ctx: WfContext) -> WorkflowResult<()> {
        let res = ctx
            .signal_workflow("fake_wid", "fake_rid", SIGNAME, b"hi!")
            .await;
        if res.is_err() {
            Err(anyhow::anyhow!("Signal fail!"))
        } else {
            Ok(().into())
        }
    }

    #[rstest::rstest]
    #[case::succeeds(false)]
    #[case::fails(true)]
    #[tokio::test]
    async fn sends_signal(#[case] fails: bool) {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        let id = t.add_signal_wf(SIGNAME, "fake_wid", "fake_rid");
        if fails {
            t.add_external_signal_failed(id);
        } else {
            t.add_external_signal_completed(id);
        }
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let wff = WorkflowFunction::new(signal_sender);
        let mut wfm = ManagedWFFunc::new(t, wff, vec![]);
        wfm.get_next_activation().await.unwrap();
        let cmds = wfm.get_server_commands().await.commands;
        assert_eq!(cmds.len(), 1);
        assert_eq!(
            cmds[0].command_type(),
            CommandType::SignalExternalWorkflowExecution
        );
        wfm.get_next_activation().await.unwrap();
        let cmds = wfm.get_server_commands().await.commands;
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

    #[tokio::test]
    async fn cancels_before_sending() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let wff = WorkflowFunction::new(|mut ctx: WfContext| async move {
            ctx.signal_workflow("fake_wid", "fake_rid", SIGNAME, b"hi!")
                .cancel(&mut ctx);
            Ok(().into())
        });
        let mut wfm = ManagedWFFunc::new(t, wff, vec![]);

        wfm.get_next_activation().await.unwrap();
        let cmds = wfm.get_server_commands().await.commands;
        assert_eq!(cmds.len(), 1);
        assert_eq!(
            cmds[0].command_type(),
            CommandType::CompleteWorkflowExecution
        );
        wfm.shutdown().await.unwrap();
    }
}
