mod options;

pub use options::{ActivityOptions, ChildWorkflowOptions, LocalActivityOptions};

use crate::prototype_rust_sdk::{
    workflow_context::options::IntoWorkflowCommand, CancelExternalWfResult, CancellableID,
    CommandCreateRequest, CommandSubscribeChildWorkflowCompletion, RustWfCmd,
    SignalExternalWfResult, TimerResult, UnblockEvent, Unblockable,
};
use crossbeam::channel::{Receiver, Sender};
use futures::{task::Context, FutureExt, Stream};
use parking_lot::RwLock;
use std::time::SystemTime;
use std::{
    collections::HashMap, future::Future, marker::PhantomData, pin::Pin, sync::Arc, task::Poll,
    time::Duration,
};
use temporal_sdk_core_protos::coresdk::{
    activity_result::ActivityResult,
    child_workflow::ChildWorkflowResult,
    common::{NamespacedWorkflowExecution, Payload},
    workflow_activation::resolve_child_workflow_execution_start::Status as ChildWorkflowStartStatus,
    workflow_commands::{
        request_cancel_external_workflow_execution as cancel_we,
        signal_external_workflow_execution as sig_we, workflow_command,
        RequestCancelExternalWorkflowExecution, SetPatchMarker, SignalExternalWorkflowExecution,
        StartTimer,
    },
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;

/// Used within workflows to issue commands, get info, etc.
pub struct WfContext {
    namespace: String,
    task_queue: String,
    args: Vec<Payload>,

    chan: Sender<RustWfCmd>,
    am_cancelled: watch::Receiver<bool>,
    shared: Arc<RwLock<WfContextSharedData>>,

    next_timer_sequence_number: u32,
    next_activity_sequence_number: u32,
    next_child_workflow_sequence_number: u32,
    next_cancel_external_wf_sequence_number: u32,
    next_signal_external_wf_sequence_number: u32,
}

#[derive(Clone, Debug, Default)]
pub struct WfContextSharedData {
    /// Maps change ids -> resolved status
    pub changes: HashMap<String, bool>,
    pub is_replaying: bool,
    pub wf_time: Option<SystemTime>,
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
        // We need to use a normal std channel since our receiving side is non-async
        let (chan, rx) = crossbeam::channel::unbounded();
        (
            Self {
                namespace,
                task_queue,
                args,
                chan,
                am_cancelled,
                shared: Arc::new(RwLock::new(Default::default())),
                next_timer_sequence_number: 1,
                next_activity_sequence_number: 1,
                next_child_workflow_sequence_number: 1,
                next_cancel_external_wf_sequence_number: 1,
                next_signal_external_wf_sequence_number: 1,
            },
            rx,
        )
    }

    /// Return the namespace the workflow is executing in
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Get the arguments provided to the workflow upon execution start
    pub fn get_args(&self) -> &[Payload] {
        self.args.as_slice()
    }

    /// Return the current time according to the workflow (which is not wall-clock time).
    pub fn workflow_time(&self) -> Option<SystemTime> {
        self.shared.read().wf_time
    }

    pub(crate) fn get_shared_data(&self) -> Arc<RwLock<WfContextSharedData>> {
        self.shared.clone()
    }

    /// A future that resolves if/when the workflow is cancelled
    pub async fn cancelled(&mut self) {
        if *self.am_cancelled.borrow() {
            return;
        }
        self.am_cancelled
            .changed()
            .await
            .expect("Cancelled send half not dropped");
    }

    /// Request to create a timer
    pub fn timer(&mut self, duration: Duration) -> impl CancellableFuture<TimerResult> {
        let seq = self.next_timer_sequence_number;
        self.next_timer_sequence_number += 1;
        let (cmd, unblocker) = CancellableWFCommandFut::new(CancellableID::Timer(seq));
        self.send(
            CommandCreateRequest {
                cmd: StartTimer {
                    seq,
                    start_to_fire_timeout: Some(duration.into()),
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
        &mut self,
        mut opts: ActivityOptions,
    ) -> impl CancellableFuture<ActivityResult> {
        if opts.task_queue.is_empty() {
            opts.task_queue = self.task_queue.clone()
        }
        let seq = self.next_activity_sequence_number;
        self.next_activity_sequence_number += 1;
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
        &mut self,
        opts: LocalActivityOptions,
    ) -> impl CancellableFuture<ActivityResult> {
        let seq = self.next_activity_sequence_number;
        self.next_activity_sequence_number += 1;
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

    /// Request to start a child workflow, Returned future resolves when the child has been started.
    pub fn child_workflow(&mut self, opts: ChildWorkflowOptions) -> ChildWorkflow {
        ChildWorkflow { opts }
    }

    /// Check (or record) that this workflow history was created with the provided patch
    pub fn patched(&self, patch_id: &str) -> bool {
        self.patch_impl(patch_id, false)
    }

    /// Record that this workflow history was created with the provided patch, and it is being
    /// phased out.
    pub fn deprecate_patch(&self, patch_id: &str) {
        self.patch_impl(patch_id, true);
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
        &mut self,
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
        signal_name: impl Into<String>,
        payload: impl Into<Payload>,
    ) -> impl CancellableFuture<SignalExternalWfResult> {
        let target = sig_we::Target::WorkflowExecution(NamespacedWorkflowExecution {
            namespace: self.namespace.clone(),
            workflow_id: workflow_id.into(),
            run_id: run_id.into(),
        });
        self.send_signal_wf(signal_name, payload, target)
    }

    /// Return a stream that produces values when the named signal is sent to this workflow
    pub fn make_signal_channel(
        &mut self,
        signal_name: impl Into<String>,
    ) -> impl Stream<Item = Vec<Payload>> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.send(RustWfCmd::SubscribeSignal(signal_name.into(), tx));
        UnboundedReceiverStream::new(rx)
    }

    /// Force a workflow task failure (EX: in order to retry on non-sticky queue)
    pub fn force_task_fail(&self, with: anyhow::Error) {
        self.send(with.into());
    }

    /// Request the cancellation of an external workflow. May resolve as a failure if the workflow
    /// was not found or the cancel was otherwise unsendable.
    /// TODO: Own result type
    pub fn cancel_external(
        &mut self,
        target: NamespacedWorkflowExecution,
    ) -> impl Future<Output = CancelExternalWfResult> {
        let target = cancel_we::Target::WorkflowExecution(target);
        let seq = self.next_cancel_external_wf_sequence_number;
        self.next_cancel_external_wf_sequence_number += 1;
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

    fn send_signal_wf(
        &mut self,
        signal_name: impl Into<String>,
        payload: impl Into<Payload>,
        target: sig_we::Target,
    ) -> impl CancellableFuture<SignalExternalWfResult> {
        let seq = self.next_signal_external_wf_sequence_number;
        self.next_signal_external_wf_sequence_number += 1;
        let (cmd, unblocker) =
            CancellableWFCommandFut::new(CancellableID::SignalExternalWorkflow(seq));
        self.send(
            CommandCreateRequest {
                cmd: SignalExternalWorkflowExecution {
                    seq,
                    signal_name: signal_name.into(),
                    args: vec![payload.into()],
                    target: Some(target),
                }
                .into(),
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Cancel any cancellable operation by ID
    fn cancel(&mut self, cancellable_id: CancellableID) {
        self.send(RustWfCmd::Cancel(cancellable_id));
    }

    fn send(&self, c: RustWfCmd) {
        self.chan.send(c).unwrap();
    }
}

/// A Future that can be cancelled.
/// Used in the prototype SDK for cancelling operations like timers and activities.
pub trait CancellableFuture<T>: Future<Output = T> {
    /// Cancel this Future
    fn cancel(&self, cx: &mut WfContext);
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
    fn cancel(&self, cx: &mut WfContext) {
        cx.cancel(self.cancellable_id.clone());
    }
}

/// A stub representing an unstarted child workflow.
#[derive(Default, Debug, Clone)]
pub struct ChildWorkflow {
    opts: ChildWorkflowOptions,
}

pub struct ChildWfCommon {
    workflow_id: String,
    result_future: CancellableWFCommandFut<ChildWorkflowResult, ()>,
}

pub struct PendingChildWorkflow {
    pub status: ChildWorkflowStartStatus,
    pub common: ChildWfCommon,
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

pub struct StartedChildWorkflow {
    pub run_id: String,
    common: ChildWfCommon,
}

impl ChildWorkflow {
    /// Start the child workflow, the returned Future is cancellable.
    pub fn start(self, cx: &mut WfContext) -> impl CancellableFuture<PendingChildWorkflow> {
        let child_seq = cx.next_child_workflow_sequence_number;
        cx.next_child_workflow_sequence_number += 1;
        // Immediately create the command/future for the result, otherwise if the user does
        // not await the result until *after* we receive an activation for it, there will be nothing
        // to match when unblocking.
        let cancel_seq = cx.next_cancel_external_wf_sequence_number;
        cx.next_cancel_external_wf_sequence_number += 1;
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
    pub fn cancel(&self, cx: &mut WfContext) -> impl Future<Output = CancelExternalWfResult> {
        let target = NamespacedWorkflowExecution {
            namespace: cx.namespace().to_string(),
            workflow_id: self.common.workflow_id.clone(),
            ..Default::default()
        };
        cx.cancel_external(target)
    }

    /// Signal the child workflow
    pub fn signal(
        &self,
        cx: &mut WfContext,
        signal_name: impl Into<String>,
        payload: impl Into<Payload>,
    ) -> impl CancellableFuture<SignalExternalWfResult> {
        let target = sig_we::Target::ChildWorkflowId(self.common.workflow_id.clone());
        cx.send_signal_wf(signal_name, payload, target)
    }
}
