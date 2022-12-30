use crate::{
    panic_formatter, workflow_context::WfContextSharedData, CancellableID, RustWfCmd, SignalData,
    TimerResult, UnblockEvent, WfContext, WfExitValue, WorkflowFunction, WorkflowResult,
};
use anyhow::{anyhow, bail, Context as AnyhowContext, Error};
use crossbeam::channel::Receiver;
use futures::{future::BoxFuture, FutureExt};
use parking_lot::RwLock;
use std::{
    collections::{hash_map::Entry, HashMap},
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{
            workflow_activation_job::Variant, FireTimer, NotifyHasPatch, ResolveActivity,
            ResolveChildWorkflowExecution, ResolveChildWorkflowExecutionStart, WorkflowActivation,
            WorkflowActivationJob,
        },
        workflow_commands::{
            request_cancel_external_workflow_execution as cancel_we, workflow_command,
            CancelChildWorkflowExecution, CancelSignalWorkflow, CancelTimer,
            CancelWorkflowExecution, CompleteWorkflowExecution, FailWorkflowExecution,
            RequestCancelActivity, RequestCancelExternalWorkflowExecution,
            RequestCancelLocalActivity, ScheduleActivity, ScheduleLocalActivity,
            StartChildWorkflowExecution, StartTimer,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{common::v1::Payload, failure::v1::Failure},
    utilities::TryIntoOrNone,
};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot, watch,
};

impl WorkflowFunction {
    /// Start a workflow function, returning a future that will resolve when the workflow does,
    /// and a channel that can be used to send it activations.
    #[doc(hidden)]
    pub fn start_workflow(
        &self,
        namespace: String,
        task_queue: String,
        args: Vec<Payload>,
        outgoing_completions: UnboundedSender<WorkflowActivationCompletion>,
    ) -> (
        impl Future<Output = WorkflowResult<()>>,
        UnboundedSender<WorkflowActivation>,
    ) {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (wf_context, cmd_receiver) = WfContext::new(namespace, task_queue, args, cancel_rx);
        let (tx, incoming_activations) = unbounded_channel();
        (
            WorkflowFuture {
                ctx_shared: wf_context.get_shared_data(),
                // We need to mark the workflow future as unconstrained, otherwise Tokio will impose
                // an artificial limit on how many commands we can unblock in one poll round.
                // TODO: Now we *need* deadlock detection or we could hose the whole system
                inner: tokio::task::unconstrained((self.wf_func)(wf_context)).boxed(),
                incoming_commands: cmd_receiver,
                outgoing_completions,
                incoming_activations,
                command_status: Default::default(),
                cancel_sender: cancel_tx,
                child_workflow_starts: Default::default(),
                sig_chans: Default::default(),
            },
            tx,
        )
    }
}

struct WFCommandFutInfo {
    unblocker: oneshot::Sender<UnblockEvent>,
}

// Allows the workflow to receive signals even though the signal handler may not yet be registered.
// TODO: Either make this go away by requiring all signals to be registered up-front in a more
//   production-ready SDK design, or if desired to allow dynamic signal registration, prevent this
//   from growing unbounded if being sent lots of unhandled signals.
enum SigChanOrBuffer {
    Chan(UnboundedSender<SignalData>),
    Buffer(Vec<SignalData>),
}

pub struct WorkflowFuture {
    /// Future produced by calling the workflow function
    inner: BoxFuture<'static, WorkflowResult<()>>,
    /// Commands produced inside user's wf code
    incoming_commands: Receiver<RustWfCmd>,
    /// Once blocked or the workflow has finished or errored out, the result is sent here
    outgoing_completions: UnboundedSender<WorkflowActivationCompletion>,
    /// Activations from core
    incoming_activations: UnboundedReceiver<WorkflowActivation>,
    /// Commands by ID -> blocked status
    command_status: HashMap<CommandID, WFCommandFutInfo>,
    /// Use to notify workflow code of cancellation
    cancel_sender: watch::Sender<bool>,
    /// Data shared with the context
    ctx_shared: Arc<RwLock<WfContextSharedData>>,
    /// Mapping of sequence number to a StartChildWorkflowExecution request
    child_workflow_starts: HashMap<u32, StartChildWorkflowExecution>,
    /// Maps signal IDs to channels to send down when they are signaled
    sig_chans: HashMap<String, SigChanOrBuffer>,
}

impl WorkflowFuture {
    fn unblock(&mut self, event: UnblockEvent) -> Result<(), Error> {
        let cmd_id = match event {
            UnblockEvent::Timer(seq, _) => CommandID::Timer(seq),
            UnblockEvent::Activity(seq, _) => CommandID::Activity(seq),
            UnblockEvent::WorkflowStart(seq, _) => CommandID::ChildWorkflowStart(seq),
            UnblockEvent::WorkflowComplete(seq, _) => CommandID::ChildWorkflowComplete(seq),
            UnblockEvent::SignalExternal(seq, _) => CommandID::SignalExternal(seq),
            UnblockEvent::CancelExternal(seq, _) => CommandID::CancelExternal(seq),
        };
        let unblocker = self.command_status.remove(&cmd_id);
        let _ = unblocker
            .ok_or_else(|| anyhow!("Command {:?} not found to unblock!", cmd_id))?
            .unblocker
            .send(event);
        Ok(())
    }

    fn fail_wft(&self, run_id: String, fail: Error) {
        warn!("Workflow task failed for {}: {}", run_id, fail);
        self.outgoing_completions
            .send(WorkflowActivationCompletion::fail(run_id, fail.into()))
            .expect("Completion channel intact");
    }

    fn send_completion(&self, run_id: String, activation_cmds: Vec<workflow_command::Variant>) {
        self.outgoing_completions
            .send(WorkflowActivationCompletion::from_cmds(
                run_id,
                activation_cmds,
            ))
            .expect("Completion channel intact");
    }

    /// Handle a particular workflow activation job.
    ///
    /// Returns Ok(true) if the workflow should be evicted. Returns an error in the event that
    /// the workflow task should be failed.
    ///
    /// Panics if internal assumptions are violated
    fn handle_job(&mut self, variant: Option<Variant>) -> Result<bool, Error> {
        if let Some(v) = variant {
            match v {
                Variant::StartWorkflow(_) => {
                    // TODO: Can assign randomness seed whenever needed
                }
                Variant::FireTimer(FireTimer { seq }) => {
                    self.unblock(UnblockEvent::Timer(seq, TimerResult::Fired))?
                }
                Variant::ResolveActivity(ResolveActivity { seq, result }) => {
                    self.unblock(UnblockEvent::Activity(
                        seq,
                        Box::new(result.context("Activity must have result")?),
                    ))?;
                }
                Variant::ResolveChildWorkflowExecutionStart(
                    ResolveChildWorkflowExecutionStart { seq, status },
                ) => self.unblock(UnblockEvent::WorkflowStart(
                    seq,
                    Box::new(status.context("Workflow start must have status")?),
                ))?,
                Variant::ResolveChildWorkflowExecution(ResolveChildWorkflowExecution {
                    seq,
                    result,
                }) => self.unblock(UnblockEvent::WorkflowComplete(
                    seq,
                    Box::new(result.context("Child Workflow execution must have a result")?),
                ))?,
                Variant::UpdateRandomSeed(_) => (),
                Variant::QueryWorkflow(_) => {
                    todo!()
                }
                Variant::CancelWorkflow(_) => {
                    // TODO: Cancel pending futures, etc
                    self.cancel_sender
                        .send(true)
                        .expect("Cancel rx not dropped");
                }
                Variant::SignalWorkflow(sig) => {
                    let mut dat = SignalData::new(sig.input);
                    dat.headers = sig.headers;
                    match self.sig_chans.entry(sig.signal_name) {
                        Entry::Occupied(mut o) => match o.get_mut() {
                            SigChanOrBuffer::Chan(chan) => {
                                let _ = chan.send(dat);
                            }
                            SigChanOrBuffer::Buffer(ref mut buf) => buf.push(dat),
                        },
                        Entry::Vacant(v) => {
                            v.insert(SigChanOrBuffer::Buffer(vec![dat]));
                        }
                    }
                }
                Variant::NotifyHasPatch(NotifyHasPatch { patch_id }) => {
                    self.ctx_shared.write().changes.insert(patch_id, true);
                }
                Variant::ResolveSignalExternalWorkflow(attrs) => {
                    self.unblock(UnblockEvent::SignalExternal(attrs.seq, attrs.failure))?;
                }
                Variant::ResolveRequestCancelExternalWorkflow(attrs) => {
                    self.unblock(UnblockEvent::CancelExternal(attrs.seq, attrs.failure))?;
                }

                Variant::RemoveFromCache(_) => {
                    return Ok(true);
                }
            }
        } else {
            bail!("Empty activation job variant");
        }

        Ok(false)
    }
}

impl Future for WorkflowFuture {
    type Output = WorkflowResult<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        'activations: loop {
            // WF must always receive an activation first before responding with commands
            let activation = match self.incoming_activations.poll_recv(cx) {
                Poll::Ready(a) => match a {
                    Some(act) => act,
                    None => {
                        return Poll::Ready(Err(anyhow!(
                            "Workflow future's activation channel was lost!"
                        )))
                    }
                },
                Poll::Pending => return Poll::Pending,
            };

            let is_only_eviction = activation.is_only_eviction();
            let run_id = activation.run_id;
            {
                let mut wlock = self.ctx_shared.write();
                wlock.is_replaying = activation.is_replaying;
                wlock.wf_time = activation.timestamp.try_into_or_none();
            }

            let mut die_of_eviction_when_done = false;
            for WorkflowActivationJob { variant } in activation.jobs {
                match self.handle_job(variant) {
                    Ok(true) => {
                        die_of_eviction_when_done = true;
                    }
                    Err(e) => {
                        self.fail_wft(run_id, e);
                        continue 'activations;
                    }
                    _ => (),
                }
            }

            if is_only_eviction {
                // No need to do anything with the workflow code in this case
                self.outgoing_completions
                    .send(WorkflowActivationCompletion::from_cmds(run_id, vec![]))
                    .expect("Completion channel intact");
                return Ok(WfExitValue::Evicted).into();
            }

            // TODO: Make sure this is *actually* safe before un-prototyping rust sdk
            let mut res = match AssertUnwindSafe(&mut self.inner)
                .catch_unwind()
                .poll_unpin(cx)
            {
                Poll::Ready(Err(e)) => {
                    let errmsg = format!("Workflow function panicked: {}", panic_formatter(e));
                    warn!("{}", errmsg);
                    self.outgoing_completions
                        .send(WorkflowActivationCompletion::fail(
                            run_id,
                            Failure {
                                message: errmsg,
                                ..Default::default()
                            },
                        ))
                        .expect("Completion channel intact");
                    // Loop back up because we're about to get evicted
                    continue;
                }
                Poll::Ready(Ok(r)) => Poll::Ready(r),
                Poll::Pending => Poll::Pending,
            };

            let mut activation_cmds = vec![];
            while let Ok(cmd) = self.incoming_commands.try_recv() {
                match cmd {
                    RustWfCmd::Cancel(cancellable_id) => {
                        match cancellable_id {
                            CancellableID::Timer(seq) => {
                                activation_cmds.push(workflow_command::Variant::CancelTimer(
                                    CancelTimer { seq },
                                ));
                                self.unblock(UnblockEvent::Timer(seq, TimerResult::Cancelled))?;
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
                            CancellableID::LocalActivity(seq) => {
                                activation_cmds.push(
                                    workflow_command::Variant::RequestCancelLocalActivity(
                                        RequestCancelLocalActivity { seq },
                                    ),
                                );
                            }
                            CancellableID::ChildWorkflow(seq) => {
                                activation_cmds.push(
                                    workflow_command::Variant::CancelChildWorkflowExecution(
                                        CancelChildWorkflowExecution {
                                            child_workflow_seq: seq,
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
                            CancellableID::ExternalWorkflow {
                                seqnum,
                                execution,
                                only_child,
                            } => {
                                activation_cmds.push(
                                    workflow_command::Variant::RequestCancelExternalWorkflowExecution(
                                        RequestCancelExternalWorkflowExecution {
                                            seq: seqnum,
                                            target: Some(if only_child {
                                                cancel_we::Target::ChildWorkflowId(execution.workflow_id)
                                            } else {
                                                cancel_we::Target::WorkflowExecution(execution)
                                            }),
                                        },
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
                            })
                            | workflow_command::Variant::ScheduleLocalActivity(
                                ScheduleLocalActivity { seq, .. },
                            ) => CommandID::Activity(seq),
                            workflow_command::Variant::SetPatchMarker(_) => {
                                panic!("Set patch marker should be a nonblocking command")
                            }
                            workflow_command::Variant::StartChildWorkflowExecution(req) => {
                                let seq = req.seq;
                                // Save the start request to support cancellation later
                                self.child_workflow_starts.insert(seq, req);
                                CommandID::ChildWorkflowStart(seq)
                            }
                            workflow_command::Variant::SignalExternalWorkflowExecution(req) => {
                                CommandID::SignalExternal(req.seq)
                            }
                            workflow_command::Variant::RequestCancelExternalWorkflowExecution(
                                req,
                            ) => CommandID::CancelExternal(req.seq),
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
                    RustWfCmd::SubscribeSignal(signame, chan) => {
                        // Deal with any buffered signal inputs for signals that were not yet
                        // registered
                        if let Some(SigChanOrBuffer::Buffer(buf)) = self.sig_chans.remove(&signame)
                        {
                            for input in buf {
                                let _ = chan.send(input);
                            }
                            // Re-poll wf future since signals may be unblocked
                            res = self.inner.poll_unpin(cx);
                        }
                        self.sig_chans.insert(signame, SigChanOrBuffer::Chan(chan));
                    }
                    RustWfCmd::ForceWFTFailure(err) => {
                        self.fail_wft(run_id, err);
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
                        WfExitValue::ContinueAsNew(cmd) => activation_cmds.push((*cmd).into()),
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

            self.send_completion(run_id, activation_cmds);

            if die_of_eviction_when_done {
                return Ok(WfExitValue::Evicted).into();
            }

            // We don't actually return here, since we could be queried after finishing executing,
            // and it allows us to rely on evictions for death and cache management
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
enum CommandID {
    Timer(u32),
    Activity(u32),
    ChildWorkflowStart(u32),
    ChildWorkflowComplete(u32),
    SignalExternal(u32),
    CancelExternal(u32),
}
