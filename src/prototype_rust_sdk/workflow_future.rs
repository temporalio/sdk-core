use crate::{
    protos::coresdk::{
        common::Payload,
        workflow_activation::{
            wf_activation_job::Variant, FireTimer, NotifyHasPatch, ResolveActivity,
            ResolveChildWorkflowExecution, ResolveChildWorkflowExecutionStart,
            ResolveSignalExternalWorkflow, WfActivation, WfActivationJob,
        },
        workflow_commands::{
            request_cancel_external_workflow_execution as cancel_we, workflow_command,
            CancelSignalWorkflow, CancelTimer, CancelWorkflowExecution, CompleteWorkflowExecution,
            FailWorkflowExecution, RequestCancelActivity, RequestCancelExternalWorkflowExecution,
            ScheduleActivity, StartChildWorkflowExecution, StartTimer,
        },
        workflow_completion::WfActivationCompletion,
    },
    protos::temporal::api::failure::v1::Failure,
    prototype_rust_sdk::{
        workflow_context::WfContextSharedData, CancellableID, RustWfCmd, UnblockEvent, WfContext,
        WfExitValue, WorkflowFunction, WorkflowResult,
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
        namespace: String,
        task_queue: String,
        args: Vec<Payload>,
        outgoing_completions: UnboundedSender<WfActivationCompletion>,
    ) -> (
        impl Future<Output = WorkflowResult<()>>,
        UnboundedSender<WfActivation>,
    ) {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (wf_context, cmd_receiver) = WfContext::new(namespace, args, cancel_rx);
        let (tx, incoming_activations) = unbounded_channel();
        (
            WorkflowFuture {
                task_queue,
                ctx_shared: wf_context.get_shared_data(),
                inner: (self.wf_func)(wf_context).boxed(),
                incoming_commands: cmd_receiver,
                outgoing_completions,
                incoming_activations,
                command_status: Default::default(),
                cancel_sender: cancel_tx,
                child_workflow_starts: Default::default(),
            },
            tx,
        )
    }
}

struct WFCommandFutInfo {
    unblocker: oneshot::Sender<UnblockEvent>,
}

pub struct WorkflowFuture {
    /// What task queue this workflow belongs to
    task_queue: String,
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
    /// Data shared with the context
    ctx_shared: Arc<RwLock<WfContextSharedData>>,
    /// Mapping of sequence number to a StartChildWorkflowExecution request
    child_workflow_starts: HashMap<u32, StartChildWorkflowExecution>,
}

impl WorkflowFuture {
    fn unblock(&mut self, event: UnblockEvent) {
        let cmd_id = match event {
            UnblockEvent::Timer(seq) => CommandID::Timer(seq),
            UnblockEvent::Activity(seq, _) => CommandID::Activity(seq),
            UnblockEvent::WorkflowStart(seq, _) => CommandID::ChildWorkflowStart(seq),
            UnblockEvent::WorkflowComplete(seq, _) => CommandID::ChildWorkflowComplete(seq),
            UnblockEvent::SignalExternal(seq, _) => CommandID::SignalExternal(seq),
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
        'activations: loop {
            // WF must always receive an activation first before responding with commands
            let activation = match self.incoming_activations.poll_recv(cx) {
                Poll::Ready(a) => a.expect("activation channel not dropped"),
                Poll::Pending => return Poll::Pending,
            };

            let is_only_eviction = activation.is_only_eviction();
            let run_id = activation.run_id;
            let mut die_of_eviction_when_done = false;
            self.ctx_shared.write().is_replaying = activation.is_replaying;

            for WfActivationJob { variant } in activation.jobs {
                if let Some(v) = variant {
                    match v {
                        Variant::StartWorkflow(_) => {
                            // TODO: Can assign randomness seed whenever needed
                        }
                        Variant::FireTimer(FireTimer { seq }) => {
                            self.unblock(UnblockEvent::Timer(seq))
                        }
                        Variant::ResolveActivity(ResolveActivity { seq, result }) => {
                            self.unblock(UnblockEvent::Activity(
                                seq,
                                Box::new(result.expect("Activity must have result")),
                            ))
                        }
                        Variant::ResolveChildWorkflowExecutionStart(
                            ResolveChildWorkflowExecutionStart { seq, status },
                        ) => self.unblock(UnblockEvent::WorkflowStart(
                            seq,
                            Box::new(status.expect("Workflow start must have status")),
                        )),
                        Variant::ResolveChildWorkflowExecution(ResolveChildWorkflowExecution {
                            seq,
                            result,
                        }) => self.unblock(UnblockEvent::WorkflowComplete(
                            seq,
                            Box::new(result.expect("Child Workflow execution must have a result")),
                        )),
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
                        Variant::NotifyHasPatch(NotifyHasPatch { patch_id }) => {
                            self.ctx_shared.write().changes.insert(patch_id, true);
                        }
                        Variant::ResolveSignalExternalWorkflow(ResolveSignalExternalWorkflow {
                            seq,
                            failure,
                        }) => self.unblock(UnblockEvent::SignalExternal(seq, failure)),
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
                    .send(WfActivationCompletion::from_cmds(
                        &self.task_queue,
                        run_id,
                        vec![],
                    ))
                    .expect("Completion channel intact");
                return Ok(WfExitValue::Evicted).into();
            }

            // TODO: Trap panics in wf code here?
            let mut res = self.inner.poll_unpin(cx);

            let mut activation_cmds = vec![];
            while let Ok(cmd) = self.incoming_commands.try_recv() {
                match cmd {
                    RustWfCmd::Cancel(cancellable_id) => {
                        match cancellable_id {
                            CancellableID::Timer(seq) => {
                                activation_cmds.push(workflow_command::Variant::CancelTimer(
                                    CancelTimer { seq },
                                ));
                                // TODO: cancelled timer should not simply be unblocked, in the
                                //   other SDKs this returns an error
                                self.unblock(UnblockEvent::Timer(seq));
                                // Re-poll wf future since a timer is now unblocked
                                res = self.inner.poll_unpin(cx);
                            }
                            CancellableID::Activity(seq) => {
                                activation_cmds.push(
                                    workflow_command::Variant::RequestCancelActivity(
                                        RequestCancelActivity { seq },
                                    ),
                                );
                            }
                            CancellableID::ChildWorkflow { cmd_seq, child_seq } => {
                                activation_cmds.push(
                                    workflow_command::Variant::RequestCancelExternalWorkflowExecution(
                                        RequestCancelExternalWorkflowExecution {
                                            seq: cmd_seq,
                                            target: Some(cancel_we::Target::ChildWorkflowSeq(child_seq)),
                                        },
                                    ),
                                );
                            }
                            CancellableID::SignalExternalWorkflow(seq) => {
                                activation_cmds.push(
                                    workflow_command::Variant::CancelSignalWorkflow(
                                        CancelSignalWorkflow { seq },
                                    ),
                                );
                            }
                        }
                    }
                    RustWfCmd::NewCmd(cmd) => {
                        activation_cmds.push(cmd.cmd.clone());

                        let command_id = match cmd.cmd {
                            workflow_command::Variant::StartTimer(StartTimer { seq, .. }) => {
                                CommandID::Timer(seq)
                            }
                            workflow_command::Variant::ScheduleActivity(ScheduleActivity {
                                seq,
                                ..
                            }) => CommandID::Activity(seq),
                            workflow_command::Variant::SetPatchMarker(_) => {
                                panic!("Set patch marker should be a nonblocking command")
                            }
                            workflow_command::Variant::StartChildWorkflowExecution(req) => {
                                let seq = req.seq;
                                // Save the start request to support cancellation later
                                self.child_workflow_starts.insert(seq, req);
                                CommandID::ChildWorkflowStart(seq)
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
                    RustWfCmd::SubscribeChildWorkflowCompletion(sub) => {
                        self.command_status.insert(
                            CommandID::ChildWorkflowComplete(sub.seq),
                            WFCommandFutInfo {
                                unblocker: sub.unblocker,
                            },
                        );
                    }
                    RustWfCmd::ForceWFTFailure(err) => {
                        self.outgoing_completions
                            .send(WfActivationCompletion::fail(
                                &self.task_queue,
                                run_id,
                                err.into(),
                            ))
                            .expect("Completion channel intact");
                        continue 'activations;
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
                                failure: Some(Failure {
                                    message: e.to_string(),
                                    ..Default::default()
                                }),
                            },
                        ));
                    }
                }
            }

            // TODO: deadlock detector
            // Check if there's nothing to unblock and workflow has not completed.
            // This is different from the assertion that was here before that checked that WF did
            // not produce any commands which is completely viable in the case WF is waiting on
            // multiple completions.

            self.outgoing_completions
                .send(WfActivationCompletion::from_cmds(
                    &self.task_queue,
                    run_id,
                    activation_cmds,
                ))
                .expect("Completion channel intact");

            if die_of_eviction_when_done {
                return Ok(WfExitValue::Evicted).into();
            }

            // We don't actually return here, since we could be queried after finishing executing,
            // and it allows us to rely on evictions for death and cache management
        }
    }
}
