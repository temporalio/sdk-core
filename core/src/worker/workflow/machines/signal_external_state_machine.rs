use super::{
    workflow_machines::MachineResponse, Cancellable, EventInfo, NewMachineWithCommand,
    OnEventWrapper, WFMachinesAdapter, WFMachinesError,
};
use crate::worker::workflow::machines::HistEventData;
use rustfsm::{fsm, MachineError, StateMachine, TransitionResult};
use std::convert::TryFrom;
use temporal_sdk_core_protos::{
    coresdk::{
        common::NamespacedWorkflowExecution,
        workflow_activation::ResolveSignalExternalWorkflow,
        workflow_commands::{
            signal_external_workflow_execution as sig_we, SignalExternalWorkflowExecution,
        },
        IntoPayloadsExt,
    },
    temporal::api::{
        command::v1::{command, Command, SignalExternalWorkflowExecutionCommandAttributes},
        common::v1::WorkflowExecution as UpstreamWE,
        enums::v1::{CommandType, EventType, SignalExternalWorkflowExecutionFailedCause},
        failure::v1::{failure::FailureInfo, ApplicationFailureInfo, CanceledFailureInfo, Failure},
        history::v1::{history_event, HistoryEvent},
    },
};

const SIG_CANCEL_MSG: &str = "Signal was cancelled before being sent";

fsm! {
    pub(super) name SignalExternalMachine;
    command SignalExternalCommand;
    error WFMachinesError;
    shared_state SharedState;

    Created --(Schedule, on_schedule) --> SignalExternalCommandCreated;

    SignalExternalCommandCreated --(CommandSignalExternalWorkflowExecution)
        --> SignalExternalCommandCreated;
    SignalExternalCommandCreated --(Cancel, on_cancel) --> Cancelled;
    SignalExternalCommandCreated
      --(SignalExternalWorkflowExecutionInitiated, on_signal_external_workflow_execution_initiated)
        --> SignalExternalCommandRecorded;

    SignalExternalCommandRecorded --(Cancel) --> SignalExternalCommandRecorded;
    SignalExternalCommandRecorded
      --(ExternalWorkflowExecutionSignaled, on_external_workflow_execution_signaled) --> Signaled;
    SignalExternalCommandRecorded
      --(SignalExternalWorkflowExecutionFailed(SignalExternalWorkflowExecutionFailedCause),
         on_signal_external_workflow_execution_failed) --> Failed;

    // Ignore any spurious cancellations after resolution
    Cancelled --(Cancel) --> Cancelled;
    Signaled --(Cancel) --> Signaled;
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
    Cancelled,
}

pub(super) fn new_external_signal(
    attrs: SignalExternalWorkflowExecution,
    this_namespace: &str,
) -> Result<NewMachineWithCommand, WFMachinesError> {
    let (workflow_execution, only_child) = match attrs.target {
        None => {
            return Err(WFMachinesError::Fatal(
                "Signal external workflow command had empty target field".to_string(),
            ))
        }
        Some(sig_we::Target::ChildWorkflowId(wfid)) => (
            NamespacedWorkflowExecution {
                namespace: this_namespace.to_string(),
                workflow_id: wfid,
                run_id: "".to_string(),
            },
            true,
        ),
        Some(sig_we::Target::WorkflowExecution(we)) => (we, false),
    };

    let mut s =
        SignalExternalMachine::from_parts(Created {}.into(), SharedState { seq: attrs.seq });
    OnEventWrapper::on_event_mut(&mut s, SignalExternalMachineEvents::Schedule)
        .expect("Scheduling signal external wf command doesn't fail");
    let cmd_attrs = command::Attributes::SignalExternalWorkflowExecutionCommandAttributes(
        SignalExternalWorkflowExecutionCommandAttributes {
            namespace: workflow_execution.namespace,
            execution: Some(UpstreamWE {
                workflow_id: workflow_execution.workflow_id,
                run_id: workflow_execution.run_id,
            }),
            header: if attrs.headers.is_empty() {
                None
            } else {
                Some(attrs.headers.into())
            },
            signal_name: attrs.signal_name,
            input: attrs.args.into_payloads(),
            // Is deprecated
            control: "".to_string(),
            child_workflow_only: only_child,
        },
    );
    let cmd = Command {
        command_type: CommandType::SignalExternalWorkflowExecution as i32,
        attributes: Some(cmd_attrs),
    };
    Ok(NewMachineWithCommand {
        command: cmd,
        machine: s.into(),
    })
}

#[derive(Default, Clone)]
pub(super) struct Cancelled {}

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
    pub(super) fn on_cancel(self) -> SignalExternalMachineTransition<Cancelled> {
        TransitionResult::commands(vec![SignalExternalCommand::Cancelled])
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
impl TryFrom<HistEventData> for SignalExternalMachineEvents {
    type Error = WFMachinesError;

    fn try_from(e: HistEventData) -> Result<Self, Self::Error> {
        let e = e.event;
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
                        "Signal workflow failed attributes were unset: {e}"
                    )));
                }
            }
            _ => {
                return Err(WFMachinesError::Nondeterminism(format!(
                    "Signal external WF machine does not handle this event: {e}"
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
                let reason = match f {
                    SignalExternalWorkflowExecutionFailedCause::Unspecified => "unknown",
                    SignalExternalWorkflowExecutionFailedCause::ExternalWorkflowExecutionNotFound
                    | SignalExternalWorkflowExecutionFailedCause::NamespaceNotFound =>
                        "it was not found",
                    SignalExternalWorkflowExecutionFailedCause::SignalCountLimitExceeded => {
                        "The per-workflow signal limit was exceeded"
                    }
                };
                vec![ResolveSignalExternalWorkflow {
                    seq: self.shared_state.seq,
                    // TODO: Create new failure type upstream for this
                    failure: Some(Failure {
                        message: format!("Unable to signal external workflow because {reason}"),
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
            SignalExternalCommand::Cancelled => {
                panic!("Cancelled command not expected as part of non-cancel transition")
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
}

impl Cancellable for SignalExternalMachine {
    fn cancel(&mut self) -> Result<Vec<MachineResponse>, MachineError<Self::Error>> {
        let res = OnEventWrapper::on_event_mut(self, SignalExternalMachineEvents::Cancel)?;
        let mut ret = vec![];
        match res.get(0) {
            Some(SignalExternalCommand::Cancelled) => {
                ret = vec![ResolveSignalExternalWorkflow {
                    seq: self.shared_state.seq,
                    failure: Some(Failure {
                        message: SIG_CANCEL_MSG.to_string(),
                        failure_info: Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
                            details: None,
                        })),
                        ..Default::default()
                    }),
                }
                .into()];
            }
            Some(_) => panic!("Signal external machine cancel produced unexpected result"),
            None => (),
        };
        Ok(ret)
    }

    fn was_cancelled_before_sent_to_server(&self) -> bool {
        // We are only ever in the cancelled state if cancelled before sent to server, there is no
        // after sent cancellation here.
        matches!(self.state(), SignalExternalMachineState::Cancelled(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{replay::TestHistoryBuilder, worker::workflow::ManagedWFFunc};
    use std::mem::discriminant;
    use temporal_sdk::{
        CancellableFuture, SignalWorkflowOptions, WfContext, WorkflowFunction, WorkflowResult,
    };
    use temporal_sdk_core_protos::coresdk::workflow_activation::{
        workflow_activation_job, WorkflowActivationJob,
    };

    const SIGNAME: &str = "signame";

    async fn signal_sender(ctx: WfContext) -> WorkflowResult<()> {
        let mut dat = SignalWorkflowOptions::new("fake_wid", "fake_rid", SIGNAME, [b"hi!"]);
        dat.with_header("tupac", b"shakur");
        let res = ctx.signal_workflow(dat).await;
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
        let mut cmds = wfm.get_server_commands().commands;
        assert_eq!(cmds.len(), 1);
        assert_eq!(
            cmds[0].command_type(),
            CommandType::SignalExternalWorkflowExecution
        );
        assert_matches!(
            cmds.remove(0).attributes.unwrap(),
            command::Attributes::SignalExternalWorkflowExecutionCommandAttributes(attrs) => {
                assert_eq!(attrs.signal_name, SIGNAME);
                assert_eq!(attrs.input.unwrap().payloads[0],
                           b"hi!".into());
                assert_eq!(*attrs.header.unwrap().fields.get("tupac").unwrap(), b"shakur".into());
            }
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

    #[tokio::test]
    async fn cancels_before_sending() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let wff = WorkflowFunction::new(|ctx: WfContext| async move {
            let sig = ctx.signal_workflow(SignalWorkflowOptions::new(
                "fake_wid",
                "fake_rid",
                SIGNAME,
                [b"hi!"],
            ));
            sig.cancel(&ctx);
            let _res = sig.await;
            Ok(().into())
        });
        let mut wfm = ManagedWFFunc::new(t, wff, vec![]);

        wfm.get_next_activation().await.unwrap();
        // No commands b/c we're waiting on the signal which is immediately going to be cancelled
        let cmds = wfm.get_server_commands().commands;
        assert_eq!(cmds.len(), 0);
        let act = wfm.get_next_activation().await.unwrap();
        assert_matches!(
            &act.jobs[0],
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveSignalExternalWorkflow(
                    ResolveSignalExternalWorkflow {
                        failure: Some(c),
                        ..
                    }
                ))
            } => c.message == SIG_CANCEL_MSG
        );
        let cmds = wfm.get_server_commands().commands;
        assert_eq!(cmds.len(), 1);
        assert_eq!(
            cmds[0].command_type(),
            CommandType::CompleteWorkflowExecution
        );
        wfm.shutdown().await.unwrap();
    }

    #[test]
    fn cancels_ignored_terminal() {
        for state in [
            SignalExternalMachineState::Cancelled(Cancelled {}),
            Signaled {}.into(),
        ] {
            let mut s = SignalExternalMachine::from_parts(state.clone(), Default::default());
            let cmds = s.cancel().unwrap();
            assert_eq!(cmds.len(), 0);
            assert_eq!(discriminant(&state), discriminant(s.state()));
        }
    }
}
