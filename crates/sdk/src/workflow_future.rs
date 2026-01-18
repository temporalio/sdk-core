use crate::{
    BaseWorkflowContext, CancellableID, RustWfCmd, SignalData, TimerResult, UnblockEvent,
    UpdateContext, UpdateFunctions, UpdateInfo, WfExitValue, WorkflowResult, panic_formatter,
    workflows::{DynWorkflowExecution, WorkflowExecutionFactory},
};
use anyhow::{Context as AnyhowContext, Error, anyhow, bail};
use futures_util::{
    FutureExt,
    future::{BoxFuture, LocalBoxFuture},
};
use std::{
    collections::{HashMap, hash_map::Entry},
    future::Future,
    panic,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::mpsc::Receiver,
    task::{Context, Poll},
};
use temporalio_common::{
    data_converters::PayloadConverter,
    protos::{
        coresdk::{
            workflow_activation::{
                DoUpdate, FireTimer, InitializeWorkflow, NotifyHasPatch, ResolveActivity,
                ResolveChildWorkflowExecution, ResolveChildWorkflowExecutionStart,
                WorkflowActivation, WorkflowActivationJob, workflow_activation_job::Variant,
            },
            workflow_commands::{
                CancelChildWorkflowExecution, CancelSignalWorkflow, CancelTimer,
                CancelWorkflowExecution, CompleteWorkflowExecution, FailWorkflowExecution,
                QueryResult, QuerySuccess, RequestCancelActivity,
                RequestCancelExternalWorkflowExecution, RequestCancelLocalActivity,
                RequestCancelNexusOperation, ScheduleActivity, ScheduleLocalActivity, StartTimer,
                UpdateResponse, WorkflowCommand, query_result, update_response, workflow_command,
            },
            workflow_completion,
            workflow_completion::{WorkflowActivationCompletion, workflow_activation_completion},
        },
        temporal::api::{
            common::v1::{Payload, Payloads},
            enums::v1::VersioningBehavior,
            failure::v1::Failure,
        },
        utilities::TryIntoOrNone,
    },
};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    oneshot, watch,
};

pub(crate) struct WorkflowFunction {
    factory: WorkflowExecutionFactory,
}

impl WorkflowFunction {
    pub(crate) fn from_invocation(factory: WorkflowExecutionFactory) -> Self {
        WorkflowFunction { factory }
    }

    /// Start a workflow function, returning a future that will resolve when the workflow does,
    /// and a channel that can be used to send it activations.
    ///
    /// IMPORTANT: The returned future should be wrapped in `tokio::task::unconstrained` by the
    /// caller. This prevents Tokio from imposing an artificial limit on how many commands can
    /// be unblocked in one poll round. Without this, workflows with many concurrent operations
    /// may not make progress.
    ///
    /// TODO: Now we *need* deadlock detection or we could hose the whole system
    pub(crate) fn start_workflow(
        &self,
        namespace: String,
        task_queue: String,
        init_workflow_job: InitializeWorkflow,
        outgoing_completions: UnboundedSender<WorkflowActivationCompletion>,
        payload_converter: PayloadConverter,
    ) -> Result<
        (
            impl Future<Output = WorkflowResult<Payload>> + use<>,
            UnboundedSender<WorkflowActivation>,
        ),
        anyhow::Error,
    > {
        let (cancel_tx, cancel_rx) = watch::channel(None);
        let span = info_span!(
            "RunWorkflow",
            "otel.name" = format!("RunWorkflow:{}", &init_workflow_job.workflow_type),
            "otel.kind" = "server"
        );

        let input = init_workflow_job.arguments.clone();
        let (base_ctx, cmd_receiver) = BaseWorkflowContext::new(
            namespace,
            task_queue,
            init_workflow_job,
            cancel_rx,
            payload_converter.clone(),
        );

        // Create the workflow execution using the factory
        let execution = (self.factory)(input, payload_converter.clone(), base_ctx.clone())
            .context("Failed to create workflow execution")?;

        let (tx, incoming_activations) = unbounded_channel();
        Ok((
            WorkflowFuture {
                base_ctx,
                execution,
                span,
                incoming_commands: cmd_receiver,
                outgoing_completions,
                incoming_activations,
                command_status: Default::default(),
                cancel_sender: cancel_tx,
                payload_converter,
                sig_chans: Default::default(),
                updates: Default::default(),
                update_futures: Default::default(),
                trait_update_futures: Default::default(),
                signal_futures: Default::default(),
            },
            tx,
        ))
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

pub(crate) struct WorkflowFuture {
    /// The workflow execution instance
    execution: Box<dyn DynWorkflowExecution>,
    /// The tracing span for this workflow
    span: tracing::Span,
    /// Commands produced inside user's wf code
    incoming_commands: Receiver<RustWfCmd>,
    /// Once blocked or the workflow has finished or errored out, the result is sent here
    outgoing_completions: UnboundedSender<WorkflowActivationCompletion>,
    /// Activations from core
    incoming_activations: UnboundedReceiver<WorkflowActivation>,
    /// Commands by ID -> blocked status
    command_status: HashMap<CommandID, WFCommandFutInfo>,
    /// Use to notify workflow code of cancellation
    cancel_sender: watch::Sender<Option<String>>,
    /// Base workflow context for sending commands
    base_ctx: BaseWorkflowContext,
    /// Payload converter for serialization/deserialization
    payload_converter: PayloadConverter,
    /// Maps signal IDs to channels to send down when they are signaled
    sig_chans: HashMap<String, SigChanOrBuffer>,
    /// Maps update handlers by name to implementations (legacy closure-based)
    updates: HashMap<String, UpdateFunctions>,
    /// Stores in-progress update futures (legacy closure-based)
    update_futures: Vec<(String, BoxFuture<'static, Result<Payload, Error>>)>,
    /// Stores in-progress update futures (trait-based dispatch)
    trait_update_futures: Vec<(
        String,
        LocalBoxFuture<'static, Result<Payload, crate::workflows::WorkflowError>>,
    )>,
    /// Stores in-progress signal futures (trait-based dispatch, for async signals)
    signal_futures: Vec<LocalBoxFuture<'static, Result<(), crate::workflows::WorkflowError>>>,
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
            UnblockEvent::NexusOperationStart(seq, _) => CommandID::NexusOpStart(seq),
            UnblockEvent::NexusOperationComplete(seq, _) => CommandID::NexusOpComplete(seq),
        };
        let unblocker = self.command_status.remove(&cmd_id);
        let _ = unblocker
            .ok_or_else(|| anyhow!("Command {cmd_id:?} not found to unblock!"))?
            .unblocker
            .send(event);
        Ok(())
    }

    fn fail_wft(&self, run_id: String, fail: Error) {
        warn!("Workflow task failed for {}: {}", run_id, fail);
        self.outgoing_completions
            .send(WorkflowActivationCompletion::fail(
                run_id,
                fail.into(),
                None,
            ))
            .expect("Completion channel intact");
    }

    fn send_completion(&self, run_id: String, activation_cmds: Vec<WorkflowCommand>) {
        self.outgoing_completions
            .send(WorkflowActivationCompletion {
                run_id,
                status: Some(workflow_activation_completion::Status::Successful(
                    workflow_completion::Success {
                        commands: activation_cmds,
                        used_internal_flags: vec![],
                        versioning_behavior: VersioningBehavior::Unspecified.into(),
                    },
                )),
            })
            .expect("Completion channel intact");
    }

    /// Handle update via legacy closure-based registration (RustWfCmd::RegisterUpdate)
    fn handle_legacy_update(
        &mut self,
        u: DoUpdate,
        outgoing_cmds: &mut Vec<WorkflowCommand>,
    ) -> Result<(), Error> {
        if let Some(impls) = self.updates.get_mut(&u.name) {
            let info = UpdateInfo {
                update_id: u.id,
                headers: u.headers,
            };
            let defp = Payload::default();
            let val_res = if u.run_validator {
                match panic::catch_unwind(AssertUnwindSafe(|| {
                    (impls.validator)(&info, u.input.first().unwrap_or(&defp))
                })) {
                    Ok(r) => r,
                    Err(e) => {
                        bail!("Panic in update validator {}", panic_formatter(e));
                    }
                }
            } else {
                Ok(())
            };
            match val_res {
                Ok(_) => {
                    outgoing_cmds.push(
                        update_response(
                            u.protocol_instance_id.clone(),
                            update_response::Response::Accepted(()),
                        )
                        .into(),
                    );
                    let handler_fut = (impls.handler)(
                        UpdateContext {
                            wf_ctx: self.base_ctx.clone(),
                            info,
                        },
                        u.input.first().unwrap_or(&defp),
                    );
                    self.update_futures
                        .push((u.protocol_instance_id, handler_fut));
                }
                Err(e) => {
                    outgoing_cmds.push(
                        update_response(
                            u.protocol_instance_id,
                            update_response::Response::Rejected(e.into()),
                        )
                        .into(),
                    );
                }
            }
        } else {
            outgoing_cmds.push(
                update_response(
                    u.protocol_instance_id,
                    update_response::Response::Rejected(
                        format!("No update handler registered for update name {}", u.name).into(),
                    ),
                )
                .into(),
            );
        }
        Ok(())
    }

    /// Handle a particular workflow activation job.
    ///
    /// Returns Ok(true) if the workflow should be evicted. Returns an error in the event that
    /// the workflow task should be failed.
    ///
    /// Panics if internal assumptions are violated
    fn handle_job(
        &mut self,
        variant: Option<Variant>,
        outgoing_cmds: &mut Vec<WorkflowCommand>,
    ) -> Result<(), Error> {
        if let Some(v) = variant {
            match v {
                Variant::InitializeWorkflow(_) => {
                    // Don't do anything in here. Init workflow is looked at earlier, before
                    // jobs are handled, and may have information taken out of it to avoid clones.
                }
                Variant::FireTimer(FireTimer { seq }) => {
                    self.unblock(UnblockEvent::Timer(seq, TimerResult::Fired))?
                }
                Variant::ResolveActivity(ResolveActivity { seq, result, .. }) => {
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
                Variant::UpdateRandomSeed(rs) => {
                    self.base_ctx.shared.write().random_seed = rs.randomness_seed;
                }
                Variant::QueryWorkflow(q) => {
                    debug!(query_type = %q.query_type, "Query received");
                    let payloads = Payloads {
                        payloads: q.arguments.clone(),
                    };

                    let dispatch_result = match panic::catch_unwind(AssertUnwindSafe(|| {
                        self.execution.dispatch_query(
                            &q.query_type,
                            &payloads,
                            &self.payload_converter,
                        )
                    })) {
                        Ok(r) => r,
                        Err(e) => Some(Err(anyhow!(
                            "Panic in query handler: {}",
                            panic_formatter(e)
                        )
                        .into())),
                    };

                    let response = match dispatch_result {
                        Some(Ok(payload)) => query_result::Variant::Succeeded(QuerySuccess {
                            response: Some(payload),
                        }),
                        // TODO [rust-sdk-branch]: Return list of known queries in error
                        None => query_result::Variant::Failed(Failure {
                            message: format!("No query handler for '{}'", q.query_type),
                            ..Default::default()
                        }),
                        Some(Err(e)) => query_result::Variant::Failed(Failure {
                            message: e.to_string(),
                            ..Default::default()
                        }),
                    };

                    outgoing_cmds.push(
                        workflow_command::Variant::RespondToQuery(QueryResult {
                            query_id: q.query_id.clone(),
                            variant: Some(response),
                        })
                        .into(),
                    );
                }
                Variant::CancelWorkflow(c) => {
                    // TODO: Cancel pending futures, etc
                    self.cancel_sender
                        .send(Some(c.reason))
                        .expect("Cancel rx not dropped");
                }
                Variant::SignalWorkflow(sig) => {
                    info!(signal_name = %sig.signal_name, "Signal received");
                    let payloads = Payloads {
                        payloads: sig.input.clone(),
                    };

                    // Try trait-based dispatch first
                    let dispatch_result = match panic::catch_unwind(AssertUnwindSafe(|| {
                        self.execution.dispatch_signal(
                            &sig.signal_name,
                            payloads,
                            &self.payload_converter,
                        )
                    })) {
                        Ok(r) => r,
                        Err(e) => {
                            bail!("Panic in signal handler: {}", panic_formatter(e));
                        }
                    };

                    match dispatch_result {
                        Some(fut) => {
                            // Signal handler returned - track the future for polling
                            self.signal_futures.push(fut);
                        }
                        None => {
                            // No trait-based handler, fall back to channel-based handling
                            let mut dat = SignalData::new(sig.input);
                            dat.headers = sig.headers;
                            match self.sig_chans.entry(sig.signal_name) {
                                Entry::Occupied(mut o) => match o.get_mut() {
                                    SigChanOrBuffer::Chan(chan) => {
                                        let _ = chan.send(dat);
                                    }
                                    SigChanOrBuffer::Buffer(buf) => buf.push(dat),
                                },
                                Entry::Vacant(v) => {
                                    v.insert(SigChanOrBuffer::Buffer(vec![dat]));
                                }
                            }
                        }
                    }
                }
                Variant::NotifyHasPatch(NotifyHasPatch { patch_id }) => {
                    self.base_ctx.shared.write().changes.insert(patch_id, true);
                }
                Variant::ResolveSignalExternalWorkflow(attrs) => {
                    self.unblock(UnblockEvent::SignalExternal(attrs.seq, attrs.failure))?;
                }
                Variant::ResolveRequestCancelExternalWorkflow(attrs) => {
                    self.unblock(UnblockEvent::CancelExternal(attrs.seq, attrs.failure))?;
                }
                Variant::DoUpdate(u) => {
                    let payloads = Payloads {
                        payloads: u.input.clone(),
                    };

                    // Try trait-based dispatch first
                    let trait_val_result = if u.run_validator {
                        match panic::catch_unwind(AssertUnwindSafe(|| {
                            self.execution.validate_update(
                                &u.name,
                                &payloads,
                                &self.payload_converter,
                            )
                        })) {
                            Ok(r) => r,
                            Err(e) => {
                                bail!("Panic in update validator {}", panic_formatter(e));
                            }
                        }
                    } else {
                        // When not validating, assume handler exists and is valid
                        // Will check handler existence below
                        Some(Ok(()))
                    };

                    match trait_val_result {
                        Some(Ok(())) => {
                            // Trait handler exists and validation passed (or was skipped)
                            // Try to start the handler
                            let start_result = self.execution.start_update(
                                &u.name,
                                payloads.clone(),
                                &self.payload_converter,
                            );
                            if let Some(fut) = start_result {
                                outgoing_cmds.push(
                                    update_response(
                                        u.protocol_instance_id.clone(),
                                        update_response::Response::Accepted(()),
                                    )
                                    .into(),
                                );
                                self.trait_update_futures
                                    .push((u.protocol_instance_id, fut));
                            } else {
                                // No trait handler, fall back to legacy closure-based
                                self.handle_legacy_update(u, outgoing_cmds)?;
                            }
                        }
                        None => {
                            // No trait handler, fall back to legacy closure-based
                            self.handle_legacy_update(u, outgoing_cmds)?;
                        }
                        Some(Err(e)) => {
                            // Validation failed
                            outgoing_cmds.push(
                                update_response(
                                    u.protocol_instance_id,
                                    update_response::Response::Rejected(anyhow!(e).into()),
                                )
                                .into(),
                            );
                        }
                    }
                }
                Variant::ResolveNexusOperationStart(attrs) => {
                    self.unblock(UnblockEvent::NexusOperationStart(
                        attrs.seq,
                        Box::new(
                            attrs
                                .status
                                .context("Nexus operation start must have status")?,
                        ),
                    ))?
                }
                Variant::ResolveNexusOperation(attrs) => {
                    self.unblock(UnblockEvent::NexusOperationComplete(
                        attrs.seq,
                        Box::new(attrs.result.context("Nexus operation must have result")?),
                    ))?
                }
                Variant::RemoveFromCache(_) => {
                    unreachable!("Cache removal should happen higher up");
                }
            }
        } else {
            bail!("Empty activation job variant");
        }

        Ok(())
    }
}

impl Future for WorkflowFuture {
    type Output = WorkflowResult<Payload>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        'activations: loop {
            // WF must always receive an activation first before responding with commands
            let activation = match self.incoming_activations.poll_recv(cx) {
                Poll::Ready(a) => match a {
                    Some(act) => act,
                    None => {
                        return Poll::Ready(Err(anyhow!(
                            "Workflow future's activation channel was lost!"
                        )));
                    }
                },
                Poll::Pending => return Poll::Pending,
            };

            let is_only_eviction = activation.is_only_eviction();
            let run_id = activation.run_id;
            {
                let mut wlock = self.base_ctx.shared.write();
                wlock.is_replaying = activation.is_replaying;
                wlock.wf_time = activation.timestamp.try_into_or_none();
                wlock.history_length = activation.history_length;
                wlock.current_deployment_version = activation
                    .deployment_version_for_current_task
                    .map(Into::into);
            }

            let mut activation_cmds = vec![];
            let mut init_activation_cmds = vec![];
            // Lame hack to avoid hitting "unregistered" update handlers in a situation where
            // the history has no commands until an update is accepted. Will go away w/ SDK redesign
            if activation
                .jobs
                .iter()
                .any(|j| matches!(j.variant, Some(Variant::InitializeWorkflow(_))))
                && activation.jobs.iter().all(|j| {
                    matches!(
                        j.variant,
                        Some(Variant::InitializeWorkflow(_) | Variant::DoUpdate(_))
                    )
                })
            {
                // Poll the workflow future once to get things registered
                if self.poll_wf_future(cx, &run_id, &mut init_activation_cmds)? {
                    continue;
                }
            }

            if is_only_eviction {
                // No need to do anything with the workflow code in this case
                self.outgoing_completions
                    .send(WorkflowActivationCompletion::from_cmds(run_id, vec![]))
                    .expect("Completion channel intact");
                return Ok(WfExitValue::Evicted).into();
            }

            for WorkflowActivationJob { variant } in activation.jobs {
                if let Err(e) = self.handle_job(variant, &mut activation_cmds) {
                    self.fail_wft(run_id, e);
                    continue 'activations;
                }
            }

            // Handle signals first
            let signal_result: Result<Vec<_>, _> = std::mem::take(&mut self.signal_futures)
                .into_iter()
                .filter_map(|mut sig_fut| match sig_fut.poll_unpin(cx) {
                    Poll::Ready(Ok(())) => None,
                    Poll::Ready(Err(e)) => Some(Err(e)),
                    Poll::Pending => Some(Ok(sig_fut)),
                })
                .collect();
            match signal_result {
                Ok(remaining) => self.signal_futures = remaining,
                Err(e) => {
                    self.fail_wft(run_id, anyhow!("Signal handler error: {}", e));
                    continue 'activations;
                }
            }

            // Then updates
            self.update_futures = std::mem::take(&mut self.update_futures)
                .into_iter()
                .filter_map(
                    |(instance_id, mut update_fut)| match update_fut.poll_unpin(cx) {
                        Poll::Ready(v) => {
                            // Push into the command channel here rather than activation_cmds
                            // directly to avoid completing and update before any final un-awaited
                            // commands started from within it.
                            self.base_ctx.send(
                                update_response(
                                    instance_id,
                                    match v {
                                        Ok(v) => update_response::Response::Completed(v),
                                        Err(e) => update_response::Response::Rejected(e.into()),
                                    },
                                )
                                .into(),
                            );
                            None
                        }
                        Poll::Pending => Some((instance_id, update_fut)),
                    },
                )
                .collect();

            self.trait_update_futures = std::mem::take(&mut self.trait_update_futures)
                .into_iter()
                .filter_map(
                    |(instance_id, mut update_fut)| match update_fut.poll_unpin(cx) {
                        Poll::Ready(v) => {
                            self.base_ctx.send(
                                update_response(
                                    instance_id,
                                    match v {
                                        Ok(v) => update_response::Response::Completed(v),
                                        Err(e) => update_response::Response::Rejected(e.into()),
                                    },
                                )
                                .into(),
                            );
                            None
                        }
                        Poll::Pending => Some((instance_id, update_fut)),
                    },
                )
                .collect();

            // The commands from the initial activation should go _after_ the updates run. This
            // is still a bit of hacky nonsense, as the first poll of the WF future should be able
            // to observe any state changed by the update handlers - but that's impossible to do
            // with current handler registration. In the real SDK, this will be obviated by them
            // being functions on a struct.
            activation_cmds.extend(init_activation_cmds);
            if self.poll_wf_future(cx, &run_id, &mut activation_cmds)? {
                continue;
            }

            // TODO: deadlock detector
            // Check if there's nothing to unblock and workflow has not completed.
            // This is different from the assertion that was here before that checked that WF did
            // not produce any commands which is completely viable in the case WF is waiting on
            // multiple completions.

            self.send_completion(run_id, activation_cmds);

            // We don't actually return here, since we could be queried after finishing executing,
            // and it allows us to rely on evictions for death and cache management
        }
    }
}

// Separate impl block down here just to keep it close to the future poll implementation which
// it is specific to.
impl WorkflowFuture {
    /// Returns true if the workflow future polling loop should be continued
    fn poll_wf_future(
        &mut self,
        cx: &mut Context,
        run_id: &str,
        activation_cmds: &mut Vec<WorkflowCommand>,
    ) -> Result<bool, Error> {
        // TODO [rust-sdk-branch]: Make sure this is *actually* safe before un-prototyping rust sdk
        // Poll the execution within the span for proper tracing
        let mut res = {
            let _guard = self.span.enter();
            match panic::catch_unwind(AssertUnwindSafe(|| self.execution.poll_run(cx))) {
                Ok(Poll::Ready(r)) => Poll::Ready(r.map_err(anyhow::Error::from)),
                Ok(Poll::Pending) => Poll::Pending,
                Err(e) => {
                    let errmsg = format!("Workflow function panicked: {}", panic_formatter(e));
                    warn!("{}", errmsg);
                    self.outgoing_completions
                        .send(WorkflowActivationCompletion::fail(
                            run_id,
                            Failure {
                                message: errmsg,
                                ..Default::default()
                            },
                            None,
                        ))
                        .expect("Completion channel intact");
                    // Loop back up because we're about to get evicted
                    return Ok(true);
                }
            }
        };

        while let Ok(cmd) = self.incoming_commands.try_recv() {
            match cmd {
                RustWfCmd::Cancel(cancellable_id) => {
                    let cmd_variant = match cancellable_id {
                        CancellableID::Timer(seq) => {
                            self.unblock(UnblockEvent::Timer(seq, TimerResult::Cancelled))?;
                            // Re-poll wf future since a timer is now unblocked
                            res = self.execution.poll_run(cx).map(|r| r.map_err(|e| e.into()));
                            workflow_command::Variant::CancelTimer(CancelTimer { seq })
                        }
                        CancellableID::Activity(seq) => {
                            workflow_command::Variant::RequestCancelActivity(
                                RequestCancelActivity { seq },
                            )
                        }
                        CancellableID::LocalActivity(seq) => {
                            workflow_command::Variant::RequestCancelLocalActivity(
                                RequestCancelLocalActivity { seq },
                            )
                        }
                        CancellableID::ChildWorkflow { seqnum, reason } => {
                            workflow_command::Variant::CancelChildWorkflowExecution(
                                CancelChildWorkflowExecution {
                                    child_workflow_seq: seqnum,
                                    reason,
                                },
                            )
                        }
                        CancellableID::SignalExternalWorkflow(seq) => {
                            workflow_command::Variant::CancelSignalWorkflow(CancelSignalWorkflow {
                                seq,
                            })
                        }
                        CancellableID::ExternalWorkflow {
                            seqnum,
                            execution,
                            reason,
                        } => workflow_command::Variant::RequestCancelExternalWorkflowExecution(
                            RequestCancelExternalWorkflowExecution {
                                seq: seqnum,
                                workflow_execution: Some(execution),
                                reason,
                            },
                        ),
                        CancellableID::NexusOp(seq) => {
                            workflow_command::Variant::RequestCancelNexusOperation(
                                RequestCancelNexusOperation { seq },
                            )
                        }
                    };
                    activation_cmds.push(cmd_variant.into());
                }

                RustWfCmd::NewCmd(cmd) => {
                    let command_id = match cmd.cmd.variant.as_ref().expect("command variant is set")
                    {
                        workflow_command::Variant::StartTimer(StartTimer { seq, .. }) => {
                            CommandID::Timer(*seq)
                        }
                        workflow_command::Variant::ScheduleActivity(ScheduleActivity {
                            seq,
                            ..
                        })
                        | workflow_command::Variant::ScheduleLocalActivity(
                            ScheduleLocalActivity { seq, .. },
                        ) => CommandID::Activity(*seq),
                        workflow_command::Variant::SetPatchMarker(_) => {
                            panic!("Set patch marker should be a nonblocking command")
                        }
                        workflow_command::Variant::StartChildWorkflowExecution(req) => {
                            let seq = req.seq;
                            CommandID::ChildWorkflowStart(seq)
                        }
                        workflow_command::Variant::SignalExternalWorkflowExecution(req) => {
                            CommandID::SignalExternal(req.seq)
                        }
                        workflow_command::Variant::RequestCancelExternalWorkflowExecution(req) => {
                            CommandID::CancelExternal(req.seq)
                        }
                        workflow_command::Variant::ScheduleNexusOperation(req) => {
                            CommandID::NexusOpStart(req.seq)
                        }
                        _ => unimplemented!("Command type not implemented"),
                    };
                    activation_cmds.push(cmd.cmd);

                    self.command_status.insert(
                        command_id,
                        WFCommandFutInfo {
                            unblocker: cmd.unblocker,
                        },
                    );
                }
                RustWfCmd::NewNonblockingCmd(cmd) => activation_cmds.push(cmd.into()),
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
                    if let Some(SigChanOrBuffer::Buffer(buf)) = self.sig_chans.remove(&signame) {
                        for input in buf {
                            let _ = chan.send(input);
                        }
                        // Re-poll wf future since signals may be unblocked
                        res = self.execution.poll_run(cx).map(|r| r.map_err(|e| e.into()));
                    }
                    self.sig_chans.insert(signame, SigChanOrBuffer::Chan(chan));
                }
                RustWfCmd::ForceWFTFailure(err) => {
                    self.fail_wft(run_id.to_string(), err);
                    return Ok(true);
                }
                RustWfCmd::RegisterUpdate(name, impls) => {
                    self.updates.insert(name, impls);
                }
                RustWfCmd::SubscribeNexusOperationCompletion { seq, unblocker } => {
                    self.command_status.insert(
                        CommandID::NexusOpComplete(seq),
                        WFCommandFutInfo { unblocker },
                    );
                }
            }
        }

        if let Poll::Ready(res) = res {
            // TODO: Auto reply with cancel when cancelled (instead of normal exit value)
            let cmd = match res {
                Ok(exit_val) => match exit_val {
                    // TODO: Generic values
                    WfExitValue::Normal(result) => {
                        workflow_command::Variant::CompleteWorkflowExecution(
                            CompleteWorkflowExecution {
                                result: Some(result),
                            },
                        )
                    }
                    WfExitValue::ContinueAsNew(cmd) => {
                        workflow_command::Variant::ContinueAsNewWorkflowExecution(*cmd)
                    }
                    WfExitValue::Cancelled => workflow_command::Variant::CancelWorkflowExecution(
                        CancelWorkflowExecution {},
                    ),
                    WfExitValue::Evicted => {
                        panic!("Don't explicitly return this")
                    }
                },
                Err(e) => workflow_command::Variant::FailWorkflowExecution(FailWorkflowExecution {
                    failure: Some(Failure {
                        message: e.to_string(),
                        ..Default::default()
                    }),
                }),
            };
            activation_cmds.push(cmd.into())
        }
        Ok(false)
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
    NexusOpStart(u32),
    NexusOpComplete(u32),
}

fn update_response(
    instance_id: String,
    resp: update_response::Response,
) -> workflow_command::Variant {
    UpdateResponse {
        protocol_instance_id: instance_id,
        response: Some(resp),
    }
    .into()
}
