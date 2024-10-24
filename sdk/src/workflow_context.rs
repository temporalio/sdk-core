mod options;

pub use options::{
    ActivityOptions, ChildWorkflowOptions, LocalActivityOptions, Signal, SignalData,
    SignalWorkflowOptions,
};

use crate::{
    workflow_context::options::IntoWorkflowCommand, CancelExternalWfResult, CancellableID,
    CommandCreateRequest, CommandSubscribeChildWorkflowCompletion, IntoUpdateHandlerFunc,
    IntoUpdateValidatorFunc, RustWfCmd, SignalExternalWfResult, TimerResult, UnblockEvent,
    Unblockable, UpdateFunctions,
};
use futures_util::{task::Context, FutureExt, Stream, StreamExt};
use parking_lot::{RwLock, RwLockReadGuard};
use std::{
    collections::HashMap,
    future::Future,
    marker::PhantomData,
    ops::Deref,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, Sender},
        Arc,
    },
    task::Poll,
    time::{Duration, SystemTime},
};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{activity_resolution, ActivityResolution},
        child_workflow::ChildWorkflowResult,
        common::NamespacedWorkflowExecution,
        workflow_activation::resolve_child_workflow_execution_start::Status as ChildWorkflowStartStatus,
        workflow_commands::{
            request_cancel_external_workflow_execution as cancel_we,
            signal_external_workflow_execution as sig_we, workflow_command,
            CancelChildWorkflowExecution, ModifyWorkflowProperties,
            RequestCancelExternalWorkflowExecution, SetPatchMarker,
            SignalExternalWorkflowExecution, StartTimer, UpsertWorkflowSearchAttributes,
        },
    },
    temporal::api::common::v1::{Memo, Payload, SearchAttributes},
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Used within workflows to issue commands, get info, etc.
#[derive(Clone)]
pub struct WfContext {
    namespace: String,
    task_queue: String,
    args: Arc<Vec<Payload>>,

    chan: Sender<RustWfCmd>,
    am_cancelled: watch::Receiver<bool>,
    pub(crate) shared: Arc<RwLock<WfContextSharedData>>,

    seq_nums: Arc<RwLock<WfCtxProtectedDat>>,
}

// TODO: Dataconverter type interface to replace Payloads here. Possibly just use serde
//    traits.
impl WfContext {
    /// Create a new wf context, returning the context itself and a receiver which outputs commands
    /// sent from the workflow.
    pub(super) fn new(
        namespace: String,
        task_queue: String,
        args: Vec<Payload>,
        am_cancelled: watch::Receiver<bool>,
    ) -> (Self, Receiver<RustWfCmd>) {
        // The receiving side is non-async
        let (chan, rx) = std::sync::mpsc::channel();
        (
            Self {
                namespace,
                task_queue,
                args: Arc::new(args),
                chan,
                am_cancelled,
                shared: Arc::new(RwLock::new(Default::default())),
                seq_nums: Arc::new(RwLock::new(WfCtxProtectedDat {
                    next_timer_sequence_number: 1,
                    next_activity_sequence_number: 1,
                    next_child_workflow_sequence_number: 1,
                    next_cancel_external_wf_sequence_number: 1,
                    next_signal_external_wf_sequence_number: 1,
                })),
            },
            rx,
        )
    }

    /// Return the namespace the workflow is executing in
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Return the task queue the workflow is executing in
    pub fn task_queue(&self) -> &str {
        &self.task_queue
    }

    /// Get the arguments provided to the workflow upon execution start
    pub fn get_args(&self) -> &[Payload] {
        self.args.as_slice()
    }

    /// Return the current time according to the workflow (which is not wall-clock time).
    pub fn workflow_time(&self) -> Option<SystemTime> {
        self.shared.read().wf_time
    }

    /// Return the length of history so far at this point in the workflow
    pub fn history_length(&self) -> u32 {
        self.shared.read().history_length
    }

    /// Return the Build ID as it was when this point in the workflow was first reached. If this
    /// code is being executed for the first time, return this Worker's Build ID if it has one.
    pub fn current_build_id(&self) -> Option<String> {
        self.shared.read().current_build_id.clone()
    }

    /// Return current values for workflow search attributes
    pub fn search_attributes(&self) -> impl Deref<Target = SearchAttributes> + '_ {
        RwLockReadGuard::map(self.shared.read(), |s| &s.search_attributes)
    }

    /// Return the workflow's randomness seed
    pub fn random_seed(&self) -> u64 {
        self.shared.read().random_seed
    }

    /// A future that resolves if/when the workflow is cancelled
    pub async fn cancelled(&self) {
        if *self.am_cancelled.borrow() {
            return;
        }
        self.am_cancelled
            .clone()
            .changed()
            .await
            .expect("Cancelled send half not dropped");
    }

    /// Request to create a timer
    pub fn timer(&self, duration: Duration) -> impl CancellableFuture<TimerResult> {
        let seq = self.seq_nums.write().next_timer_seq();
        let (cmd, unblocker) = CancellableWFCommandFut::new(CancellableID::Timer(seq));
        self.send(
            CommandCreateRequest {
                cmd: StartTimer {
                    seq,
                    start_to_fire_timeout: Some(
                        duration
                            .try_into()
                            .expect("Durations must fit into 64 bits"),
                    ),
                }
                .into(),
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Request to run an activity
    pub fn activity(
        &self,
        mut opts: ActivityOptions,
    ) -> impl CancellableFuture<ActivityResolution> {
        if opts.task_queue.is_none() {
            opts.task_queue = Some(self.task_queue.clone());
        }
        let seq = self.seq_nums.write().next_activity_seq();
        let (cmd, unblocker) = CancellableWFCommandFut::new(CancellableID::Activity(seq));
        self.send(
            CommandCreateRequest {
                cmd: opts.into_command(seq).into(),
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Request to run a local activity
    pub fn local_activity(
        &self,
        opts: LocalActivityOptions,
    ) -> impl CancellableFuture<ActivityResolution> + '_ {
        LATimerBackoffFut::new(opts, self)
    }

    /// Request to run a local activity with no implementation of timer-backoff based retrying.
    fn local_activity_no_timer_retry(
        &self,
        opts: LocalActivityOptions,
    ) -> impl CancellableFuture<ActivityResolution> {
        let seq = self.seq_nums.write().next_activity_seq();
        let (cmd, unblocker) = CancellableWFCommandFut::new(CancellableID::LocalActivity(seq));
        self.send(
            CommandCreateRequest {
                cmd: opts.into_command(seq).into(),
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Creates a child workflow stub with the provided options
    pub fn child_workflow(&self, opts: ChildWorkflowOptions) -> ChildWorkflow {
        ChildWorkflow { opts }
    }

    /// Check (or record) that this workflow history was created with the provided patch
    pub fn patched(&self, patch_id: &str) -> bool {
        self.patch_impl(patch_id, false)
    }

    /// Record that this workflow history was created with the provided patch, and it is being
    /// phased out.
    pub fn deprecate_patch(&self, patch_id: &str) -> bool {
        self.patch_impl(patch_id, true)
    }

    fn patch_impl(&self, patch_id: &str, deprecated: bool) -> bool {
        self.send(
            workflow_command::Variant::SetPatchMarker(SetPatchMarker {
                patch_id: patch_id.to_string(),
                deprecated,
            })
            .into(),
        );
        // See if we already know about the status of this change
        if let Some(present) = self.shared.read().changes.get(patch_id) {
            return *present;
        }

        // If we don't already know about the change, that means there is no marker in history,
        // and we should return false if we are replaying
        let res = !self.shared.read().is_replaying;

        self.shared
            .write()
            .changes
            .insert(patch_id.to_string(), res);

        res
    }

    /// Send a signal to an external workflow. May resolve as a failure if the signal didn't work
    /// or was cancelled.
    pub fn signal_workflow(
        &self,
        opts: impl Into<SignalWorkflowOptions>,
    ) -> impl CancellableFuture<SignalExternalWfResult> {
        let options: SignalWorkflowOptions = opts.into();
        let target = sig_we::Target::WorkflowExecution(NamespacedWorkflowExecution {
            namespace: self.namespace.clone(),
            workflow_id: options.workflow_id,
            run_id: options.run_id.unwrap_or_default(),
        });
        self.send_signal_wf(target, options.signal)
    }

    /// Add or create a set of search attributes
    pub fn upsert_search_attributes(&self, attr_iter: impl IntoIterator<Item = (String, Payload)>) {
        self.send(RustWfCmd::NewNonblockingCmd(
            workflow_command::Variant::UpsertWorkflowSearchAttributes(
                UpsertWorkflowSearchAttributes {
                    search_attributes: HashMap::from_iter(attr_iter),
                },
            ),
        ))
    }

    /// Add or create a set of search attributes
    pub fn upsert_memo(&self, attr_iter: impl IntoIterator<Item = (String, Payload)>) {
        self.send(RustWfCmd::NewNonblockingCmd(
            workflow_command::Variant::ModifyWorkflowProperties(ModifyWorkflowProperties {
                upserted_memo: Some(Memo {
                    fields: HashMap::from_iter(attr_iter),
                }),
            }),
        ))
    }

    /// Return a stream that produces values when the named signal is sent to this workflow
    pub fn make_signal_channel(&self, signal_name: impl Into<String>) -> DrainableSignalStream {
        let (tx, rx) = mpsc::unbounded_channel();
        self.send(RustWfCmd::SubscribeSignal(signal_name.into(), tx));
        DrainableSignalStream(UnboundedReceiverStream::new(rx))
    }

    /// Force a workflow task failure (EX: in order to retry on non-sticky queue)
    pub fn force_task_fail(&self, with: anyhow::Error) {
        self.send(with.into());
    }

    /// Request the cancellation of an external workflow. May resolve as a failure if the workflow
    /// was not found or the cancel was otherwise unsendable.
    pub fn cancel_external(
        &self,
        target: NamespacedWorkflowExecution,
    ) -> impl Future<Output = CancelExternalWfResult> {
        let target = cancel_we::Target::WorkflowExecution(target);
        let seq = self.seq_nums.write().next_cancel_external_wf_seq();
        let (cmd, unblocker) = WFCommandFut::new();
        self.send(
            CommandCreateRequest {
                cmd: RequestCancelExternalWorkflowExecution {
                    seq,
                    target: Some(target),
                }
                .into(),
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Register an update handler by providing the handler name, a validator function, and an
    /// update handler. The validator must not mutate workflow state and is synchronous. The handler
    /// may mutate workflow state (though, that's annoying right now in the prototype) and is async.
    ///
    /// Note that if you want a validator that always passes, you will likely need to provide type
    /// annotations to make the compiler happy, like: `|_: &_, _: T| Ok(())`
    pub fn update_handler<Arg, Res>(
        &self,
        name: impl Into<String>,
        validator: impl IntoUpdateValidatorFunc<Arg>,
        handler: impl IntoUpdateHandlerFunc<Arg, Res>,
    ) {
        self.send(RustWfCmd::RegisterUpdate(
            name.into(),
            UpdateFunctions::new(validator, handler),
        ))
    }

    /// Buffer a command to be sent in the activation reply
    pub(crate) fn send(&self, c: RustWfCmd) {
        self.chan.send(c).expect("command channel intact");
    }

    fn send_signal_wf(
        &self,
        target: sig_we::Target,
        signal: Signal,
    ) -> impl CancellableFuture<SignalExternalWfResult> {
        let seq = self.seq_nums.write().next_signal_external_wf_seq();
        let (cmd, unblocker) =
            CancellableWFCommandFut::new(CancellableID::SignalExternalWorkflow(seq));
        self.send(
            CommandCreateRequest {
                cmd: SignalExternalWorkflowExecution {
                    seq,
                    signal_name: signal.signal_name,
                    args: signal.data.input,
                    target: Some(target),
                    headers: signal.data.headers,
                }
                .into(),
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Cancel any cancellable operation by ID
    fn cancel(&self, cancellable_id: CancellableID) {
        self.send(RustWfCmd::Cancel(cancellable_id));
    }
}

struct WfCtxProtectedDat {
    next_timer_sequence_number: u32,
    next_activity_sequence_number: u32,
    next_child_workflow_sequence_number: u32,
    next_cancel_external_wf_sequence_number: u32,
    next_signal_external_wf_sequence_number: u32,
}

impl WfCtxProtectedDat {
    fn next_timer_seq(&mut self) -> u32 {
        let seq = self.next_timer_sequence_number;
        self.next_timer_sequence_number += 1;
        seq
    }
    fn next_activity_seq(&mut self) -> u32 {
        let seq = self.next_activity_sequence_number;
        self.next_activity_sequence_number += 1;
        seq
    }
    fn next_child_workflow_seq(&mut self) -> u32 {
        let seq = self.next_child_workflow_sequence_number;
        self.next_child_workflow_sequence_number += 1;
        seq
    }
    fn next_cancel_external_wf_seq(&mut self) -> u32 {
        let seq = self.next_cancel_external_wf_sequence_number;
        self.next_cancel_external_wf_sequence_number += 1;
        seq
    }
    fn next_signal_external_wf_seq(&mut self) -> u32 {
        let seq = self.next_signal_external_wf_sequence_number;
        self.next_signal_external_wf_sequence_number += 1;
        seq
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct WfContextSharedData {
    /// Maps change ids -> resolved status
    pub(crate) changes: HashMap<String, bool>,
    pub(crate) is_replaying: bool,
    pub(crate) wf_time: Option<SystemTime>,
    pub(crate) history_length: u32,
    pub(crate) current_build_id: Option<String>,
    pub(crate) search_attributes: SearchAttributes,
    pub(crate) random_seed: u64,
}

/// Helper Wrapper that can drain the channel into a Vec<SignalData> in a blocking way.  Useful
/// for making sure channels are empty before ContinueAsNew-ing a workflow
pub struct DrainableSignalStream(UnboundedReceiverStream<SignalData>);

impl DrainableSignalStream {
    pub fn drain_all(self) -> Vec<SignalData> {
        let mut receiver = self.0.into_inner();
        let mut signals = vec![];
        while let Ok(s) = receiver.try_recv() {
            signals.push(s);
        }
        signals
    }

    pub fn drain_ready(&mut self) -> Vec<SignalData> {
        let mut signals = vec![];
        while let Some(s) = self.0.next().now_or_never().flatten() {
            signals.push(s);
        }
        signals
    }
}

impl Stream for DrainableSignalStream {
    type Item = SignalData;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.0).poll_next(cx)
    }
}

/// A Future that can be cancelled.
/// Used in the prototype SDK for cancelling operations like timers and activities.
pub trait CancellableFuture<T>: Future<Output = T> {
    /// Cancel this Future
    fn cancel(&self, cx: &WfContext);
}

struct WFCommandFut<T, D> {
    _unused: PhantomData<T>,
    result_rx: oneshot::Receiver<UnblockEvent>,
    other_dat: Option<D>,
}
impl<T> WFCommandFut<T, ()> {
    fn new() -> (Self, oneshot::Sender<UnblockEvent>) {
        Self::new_with_dat(())
    }
}

impl<T, D> WFCommandFut<T, D> {
    fn new_with_dat(other_dat: D) -> (Self, oneshot::Sender<UnblockEvent>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                _unused: PhantomData,
                result_rx: rx,
                other_dat: Some(other_dat),
            },
            tx,
        )
    }
}

impl<T, D> Unpin for WFCommandFut<T, D> where T: Unblockable<OtherDat = D> {}
impl<T, D> Future for WFCommandFut<T, D>
where
    T: Unblockable<OtherDat = D>,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.result_rx.poll_unpin(cx).map(|x| {
            // SAFETY: Because we can only enter this section once the future has resolved, we
            // know it will never be polled again, therefore consuming the option is OK.
            let od = self
                .other_dat
                .take()
                .expect("Other data must exist when resolving command future");
            Unblockable::unblock(x.unwrap(), od)
        })
    }
}

struct CancellableWFCommandFut<T, D> {
    cmd_fut: WFCommandFut<T, D>,
    cancellable_id: CancellableID,
}
impl<T> CancellableWFCommandFut<T, ()> {
    fn new(cancellable_id: CancellableID) -> (Self, oneshot::Sender<UnblockEvent>) {
        Self::new_with_dat(cancellable_id, ())
    }
}
impl<T, D> CancellableWFCommandFut<T, D> {
    fn new_with_dat(
        cancellable_id: CancellableID,
        other_dat: D,
    ) -> (Self, oneshot::Sender<UnblockEvent>) {
        let (cmd_fut, sender) = WFCommandFut::new_with_dat(other_dat);
        (
            Self {
                cmd_fut,
                cancellable_id,
            },
            sender,
        )
    }
}
impl<T, D> Unpin for CancellableWFCommandFut<T, D> where T: Unblockable<OtherDat = D> {}
impl<T, D> Future for CancellableWFCommandFut<T, D>
where
    T: Unblockable<OtherDat = D>,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.cmd_fut.poll_unpin(cx)
    }
}

impl<T, D> CancellableFuture<T> for CancellableWFCommandFut<T, D>
where
    T: Unblockable<OtherDat = D>,
{
    fn cancel(&self, cx: &WfContext) {
        cx.cancel(self.cancellable_id.clone());
    }
}

struct LATimerBackoffFut<'a> {
    la_opts: LocalActivityOptions,
    current_fut: Pin<Box<dyn CancellableFuture<ActivityResolution> + Send + Unpin + 'a>>,
    timer_fut: Option<Pin<Box<dyn CancellableFuture<TimerResult> + Send + Unpin + 'a>>>,
    ctx: &'a WfContext,
    next_attempt: u32,
    next_sched_time: Option<prost_types::Timestamp>,
    did_cancel: AtomicBool,
}
impl<'a> LATimerBackoffFut<'a> {
    pub(crate) fn new(opts: LocalActivityOptions, ctx: &'a WfContext) -> Self {
        Self {
            la_opts: opts.clone(),
            current_fut: Box::pin(ctx.local_activity_no_timer_retry(opts)),
            timer_fut: None,
            ctx,
            next_attempt: 1,
            next_sched_time: None,
            did_cancel: AtomicBool::new(false),
        }
    }
}
impl<'a> Unpin for LATimerBackoffFut<'a> {}
impl<'a> Future for LATimerBackoffFut<'a> {
    type Output = ActivityResolution;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // If the timer exists, wait for it first
        if let Some(tf) = self.timer_fut.as_mut() {
            return match tf.poll_unpin(cx) {
                Poll::Ready(tr) => {
                    self.timer_fut = None;
                    // Schedule next LA if this timer wasn't cancelled
                    if let TimerResult::Fired = tr {
                        let mut opts = self.la_opts.clone();
                        opts.attempt = Some(self.next_attempt);
                        opts.original_schedule_time
                            .clone_from(&self.next_sched_time);
                        self.current_fut = Box::pin(self.ctx.local_activity_no_timer_retry(opts));
                        Poll::Pending
                    } else {
                        Poll::Ready(ActivityResolution {
                            status: Some(
                                activity_resolution::Status::Cancelled(Default::default()),
                            ),
                        })
                    }
                }
                Poll::Pending => Poll::Pending,
            };
        }
        let poll_res = self.current_fut.poll_unpin(cx);
        if let Poll::Ready(ref r) = poll_res {
            if let Some(activity_resolution::Status::Backoff(b)) = r.status.as_ref() {
                // If we've already said we want to cancel, don't schedule the backoff timer. Just
                // return cancel status. This can happen if cancel comes after the LA says it wants
                // to back off but before we have scheduled the timer.
                if self.did_cancel.load(Ordering::Acquire) {
                    return Poll::Ready(ActivityResolution {
                        status: Some(activity_resolution::Status::Cancelled(Default::default())),
                    });
                }

                let timer_f = self.ctx.timer(
                    b.backoff_duration
                        .expect("Duration is set")
                        .try_into()
                        .expect("duration converts ok"),
                );
                self.timer_fut = Some(Box::pin(timer_f));
                self.next_attempt = b.attempt;
                self.next_sched_time.clone_from(&b.original_schedule_time);
                return Poll::Pending;
            }
        }
        poll_res
    }
}
impl<'a> CancellableFuture<ActivityResolution> for LATimerBackoffFut<'a> {
    fn cancel(&self, ctx: &WfContext) {
        self.did_cancel.store(true, Ordering::Release);
        if let Some(tf) = self.timer_fut.as_ref() {
            tf.cancel(ctx);
        }
        self.current_fut.cancel(ctx);
    }
}

/// A stub representing an unstarted child workflow.
#[derive(Default, Debug, Clone)]
pub struct ChildWorkflow {
    opts: ChildWorkflowOptions,
}

pub(crate) struct ChildWfCommon {
    workflow_id: String,
    result_future: CancellableWFCommandFut<ChildWorkflowResult, ()>,
}

/// Child workflow in pending state
pub struct PendingChildWorkflow {
    /// The status of the child workflow start
    pub status: ChildWorkflowStartStatus,
    pub(crate) common: ChildWfCommon,
}

impl PendingChildWorkflow {
    /// Returns `None` if the child did not start successfully. The returned [StartedChildWorkflow]
    /// can be used to wait on, signal, or cancel the child workflow.
    pub fn into_started(self) -> Option<StartedChildWorkflow> {
        match self.status {
            ChildWorkflowStartStatus::Succeeded(s) => Some(StartedChildWorkflow {
                run_id: s.run_id,
                common: self.common,
            }),
            _ => None,
        }
    }
}

/// Child workflow in started state
pub struct StartedChildWorkflow {
    /// Run ID of the child workflow
    pub run_id: String,
    common: ChildWfCommon,
}

impl ChildWorkflow {
    /// Start the child workflow, the returned Future is cancellable.
    pub fn start(self, cx: &WfContext) -> impl CancellableFuture<PendingChildWorkflow> {
        let child_seq = cx.seq_nums.write().next_child_workflow_seq();
        // Immediately create the command/future for the result, otherwise if the user does
        // not await the result until *after* we receive an activation for it, there will be nothing
        // to match when unblocking.
        let cancel_seq = cx.seq_nums.write().next_cancel_external_wf_seq();
        let (result_cmd, unblocker) =
            CancellableWFCommandFut::new(CancellableID::ExternalWorkflow {
                seqnum: cancel_seq,
                execution: NamespacedWorkflowExecution {
                    workflow_id: self.opts.workflow_id.clone(),
                    ..Default::default()
                },
                only_child: true,
            });
        cx.send(
            CommandSubscribeChildWorkflowCompletion {
                seq: child_seq,
                unblocker,
            }
            .into(),
        );

        let common = ChildWfCommon {
            workflow_id: self.opts.workflow_id.clone(),
            result_future: result_cmd,
        };

        let (cmd, unblocker) =
            CancellableWFCommandFut::new_with_dat(CancellableID::ChildWorkflow(child_seq), common);
        cx.send(
            CommandCreateRequest {
                cmd: self.opts.into_command(child_seq).into(),
                unblocker,
            }
            .into(),
        );

        cmd
    }
}

impl StartedChildWorkflow {
    /// Consumes self and returns a future that will wait until completion of this child workflow
    /// execution
    pub fn result(self) -> impl CancellableFuture<ChildWorkflowResult> {
        self.common.result_future
    }

    /// Cancel the child workflow
    pub fn cancel(&self, cx: &WfContext) {
        cx.send(RustWfCmd::NewNonblockingCmd(
            CancelChildWorkflowExecution {
                child_workflow_seq: self.common.result_future.cancellable_id.seq_num(),
            }
            .into(),
        ));
    }

    /// Signal the child workflow
    pub fn signal(
        &self,
        cx: &WfContext,
        data: impl Into<Signal>,
    ) -> impl CancellableFuture<SignalExternalWfResult> {
        let target = sig_we::Target::ChildWorkflowId(self.common.workflow_id.clone());
        cx.send_signal_wf(target, data.into())
    }
}
