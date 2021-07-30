use crate::{
    protos::coresdk::{
        common::{Payload, UserCodeFailure},
        workflow_activation::{
            wf_activation_job::Variant, FireTimer, ResolveActivity, ResolveHasChange, WfActivation,
            WfActivationJob,
        },
        workflow_commands::{
            workflow_command, CancelTimer, CancelWorkflowExecution, CompleteWorkflowExecution,
            FailWorkflowExecution, RequestCancelActivity, ScheduleActivity, StartTimer,
        },
        workflow_completion::WfActivationCompletion,
    },
    prototype_rust_sdk::{
        RustWfCmd, UnblockEvent, WfContext, WfExitValue, WorkflowFunction, WorkflowResult,
    },
    workflow::CommandID,
};
use anyhow::anyhow;
use crossbeam::channel::Receiver;
use futures::{future::BoxFuture, FutureExt};
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot, watch,
};

impl WorkflowFunction {
    pub(crate) fn start_workflow(
        &self,
        args: Vec<Payload>,
        outgoing_completions: UnboundedSender<WfActivationCompletion>,
    ) -> (
        impl Future<Output = WorkflowResult<()>>,
        UnboundedSender<WfActivation>,
    ) {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (wf_context, cmd_receiver) = WfContext::new(args, cancel_rx);
        let (tx, incoming_activations) = unbounded_channel();
        (
            WorkflowFuture {
                changes: wf_context.get_changes_map(),
                inner: (self.wf_func)(wf_context).boxed(),
                incoming_commands: cmd_receiver,
                outgoing_completions,
                incoming_activations,
                command_status: Default::default(),
                cancel_sender: cancel_tx,
            },
            tx,
        )
    }
}

struct WFCommandFutInfo {
    unblocker: oneshot::Sender<UnblockEvent>,
}

pub struct WorkflowFuture {
    /// Future produced by calling the workflow function
    inner: BoxFuture<'static, WorkflowResult<()>>,
    /// Commands produced inside user's wf code
    incoming_commands: Receiver<RustWfCmd>,
    /// Once blocked or the workflow has finished or errored out, the result is sent here
    outgoing_completions: UnboundedSender<WfActivationCompletion>,
    /// Activations from core
    incoming_activations: UnboundedReceiver<WfActivation>,
    /// Commands by ID -> blocked status
    command_status: HashMap<CommandID, WFCommandFutInfo>,
    /// Use to notify workflow code of cancellation
    cancel_sender: watch::Sender<bool>,
    /// Maps change ids -> resolved status
    changes: Arc<RwLock<HashMap<String, bool>>>,
}

impl WorkflowFuture {
    fn unblock(&mut self, event: UnblockEvent) {
        let cmd_id = match &event {
            UnblockEvent::Timer(t) => CommandID::Timer(t.clone()),
            UnblockEvent::Activity { id, .. } => CommandID::Activity(id.clone()),
        };
        let unblocker = self.command_status.remove(&cmd_id);
        unblocker
            .expect("Command not found")
            .unblocker
            .send(event)
            .unwrap();
    }
}

impl Future for WorkflowFuture {
    type Output = WorkflowResult<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            // WF must always receive an activation first before responding with commands
            let activation = match self.incoming_activations.poll_recv(cx) {
                Poll::Ready(a) => a.expect("activation channel not dropped"),
                Poll::Pending => return Poll::Pending,
            };
            dbg!(&activation);

            let is_only_eviction = activation.is_only_eviction();
            let run_id = activation.run_id;
            let mut die_of_eviction_when_done = false;

            for WfActivationJob { variant } in activation.jobs {
                if let Some(v) = variant {
                    match v {
                        Variant::StartWorkflow(_) => {
                            // TODO: Can assign randomness seed whenever needed
                        }
                        Variant::FireTimer(FireTimer { timer_id }) => {
                            self.unblock(UnblockEvent::Timer(timer_id))
                        }
                        Variant::ResolveActivity(ResolveActivity {
                            activity_id,
                            result,
                        }) => self.unblock(UnblockEvent::Activity {
                            id: activity_id,
                            result: result.expect("Activity must have result"),
                        }),
                        Variant::UpdateRandomSeed(_) => {}
                        Variant::QueryWorkflow(_) => {
                            todo!()
                        }
                        Variant::CancelWorkflow(_) => {
                            // TODO: Cancel pending futures, etc
                            self.cancel_sender
                                .send(true)
                                .expect("Cancel rx not dropped");
                        }
                        Variant::SignalWorkflow(_) => {
                            todo!()
                        }
                        Variant::ResolveHasChange(ResolveHasChange {
                            change_id,
                            is_present,
                        }) => {
                            self.changes.write().insert(change_id, is_present);
                        }
                        Variant::RemoveFromCache(_) => {
                            die_of_eviction_when_done = true;
                        }
                    }
                } else {
                    return Err(anyhow!("Empty activation job variant")).into();
                }
            }

            if is_only_eviction {
                // No need to do anything with the workflow code in this case
                self.outgoing_completions
                    .send(WfActivationCompletion::from_cmds(vec![], run_id))
                    .expect("Completion channel intact");
                return Ok(WfExitValue::Evicted).into();
            }

            // TODO: Trap panics in wf code here?
            let mut res = self.inner.poll_unpin(cx);

            let mut activation_cmds = vec![];
            while let Ok(cmd) = self.incoming_commands.try_recv() {
                match cmd {
                    RustWfCmd::CancelTimer(tid) => {
                        activation_cmds.push(workflow_command::Variant::CancelTimer(CancelTimer {
                            timer_id: tid.clone(),
                        }));
                        self.unblock(UnblockEvent::Timer(tid));
                        // Re-poll wf future since a timer is now unblocked
                        res = self.inner.poll_unpin(cx);
                    }
                    RustWfCmd::CancelActivity(aid) => {
                        activation_cmds.push(workflow_command::Variant::RequestCancelActivity(
                            RequestCancelActivity {
                                activity_id: aid.clone(),
                            },
                        ));
                    }
                    RustWfCmd::NewCmd(cmd) => {
                        activation_cmds.push(cmd.cmd.clone());

                        let command_id = match cmd.cmd {
                            workflow_command::Variant::StartTimer(StartTimer {
                                timer_id, ..
                            }) => CommandID::Timer(timer_id),
                            workflow_command::Variant::ScheduleActivity(ScheduleActivity {
                                activity_id,
                                ..
                            }) => CommandID::Activity(activity_id),
                            workflow_command::Variant::HasChange(_) => {
                                panic!("Has change should be a nonblocking command")
                            }
                            _ => unimplemented!("Command type not implemented"),
                        };
                        self.command_status.insert(
                            command_id,
                            WFCommandFutInfo {
                                unblocker: cmd.unblocker,
                            },
                        );
                    }
                    RustWfCmd::NewNonblockingCmd(cmd) => {
                        activation_cmds.push(cmd);
                    }
                    RustWfCmd::ForceTimeout(dur) => {
                        // This is nasty
                        std::thread::sleep(dur);
                    }
                }
            }

            if let Poll::Ready(res) = res {
                // TODO: Auto reply with cancel when cancelled (instead of normal exit value)
                match res {
                    Ok(exit_val) => match exit_val {
                        // TODO: Generic values
                        WfExitValue::Normal(_) => {
                            activation_cmds.push(
                                workflow_command::Variant::CompleteWorkflowExecution(
                                    CompleteWorkflowExecution { result: None },
                                ),
                            );
                        }
                        WfExitValue::ContinueAsNew(cmd) => activation_cmds.push(cmd.into()),
                        WfExitValue::Cancelled => {
                            activation_cmds.push(
                                workflow_command::Variant::CancelWorkflowExecution(
                                    CancelWorkflowExecution {},
                                ),
                            );
                        }
                        WfExitValue::Evicted => {
                            panic!("Don't explicitly return this")
                        }
                    },
                    Err(e) => {
                        activation_cmds.push(workflow_command::Variant::FailWorkflowExecution(
                            FailWorkflowExecution {
                                failure: Some(UserCodeFailure {
                                    message: e.to_string(),
                                    ..Default::default()
                                }),
                            },
                        ));
                    }
                }
            }

            if activation_cmds.is_empty() {
                panic!(
                    "Workflow did not produce any commands or exit, but awaited. This \
                 means it will deadlock. You probably awaited on a non-WfContext future."
                );
            }
            self.outgoing_completions
                .send(WfActivationCompletion::from_cmds(activation_cmds, run_id))
                .expect("Completion channel intact");

            if die_of_eviction_when_done {
                return Ok(WfExitValue::Evicted).into();
            }

            // We don't actually return here, since we could be queried after finishing executing,
            // and it allows us to rely on evictions for death and cache management
        }
    }
}
