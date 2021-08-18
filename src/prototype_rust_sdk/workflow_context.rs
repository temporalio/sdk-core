use crate::prototype_rust_sdk::{CancellableID, TimerResult};
use crate::{
    protos::coresdk::{
        activity_result::ActivityResult,
        child_workflow::ChildWorkflowResult,
        common::Payload,
        workflow_activation::resolve_child_workflow_execution_start::Status as ChildWorkflowStartStatus,
        workflow_commands::{
            workflow_command, ActivityCancellationType, ScheduleActivity, SetPatchMarker,
            StartChildWorkflowExecution, StartTimer,
        },
    },
    prototype_rust_sdk::{
        CommandCreateRequest, CommandSubscribeChildWorkflowCompletion, RustWfCmd, UnblockEvent,
    },
};
use crossbeam::channel::{Receiver, Sender};
use futures::{task::Context, FutureExt};
use parking_lot::RwLock;
use std::marker::PhantomData;
use std::time::Duration;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, task::Poll};
use tokio::sync::{oneshot, watch};

/// Used within workflows to issue commands, get info, etc.
pub struct WfContext {
    chan: Sender<RustWfCmd>,
    args: Vec<Payload>,
    am_cancelled: watch::Receiver<bool>,
    shared: Arc<RwLock<WfContextSharedData>>,
    next_timer_sequence_number: u32,
    next_activity_sequence_number: u32,
    next_child_workflow_sequence_number: u32,
}

#[derive(Clone, Debug, Default)]
pub struct WfContextSharedData {
    /// Maps change ids -> resolved status
    pub changes: HashMap<String, bool>,
    pub is_replaying: bool,
}

impl WfContext {
    /// Create a new wf context, returning the context itself, the shared cache for blocked
    /// commands, and a receiver which outputs commands sent from the workflow.
    pub(super) fn new(
        args: Vec<Payload>,
        am_cancelled: watch::Receiver<bool>,
    ) -> (Self, Receiver<RustWfCmd>) {
        // We need to use a normal std channel since our receiving side is non-async
        let (chan, rx) = crossbeam::channel::unbounded();
        (
            Self {
                chan,
                args,
                am_cancelled,
                shared: Arc::new(RwLock::new(Default::default())),
                next_timer_sequence_number: 1,
                next_activity_sequence_number: 1,
                next_child_workflow_sequence_number: 1,
            },
            rx,
        )
    }

    /// Get the arguments provided to the workflow upon execution start
    pub fn get_args(&self) -> &[Payload] {
        self.args.as_slice()
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
            .expect("Cancelled send half not dropped")
    }

    /// Request to create a timer
    pub fn timer(&mut self, duration: Duration) -> impl CancellableFuture<TimerResult> {
        let seq = self.next_timer_sequence_number;
        self.next_timer_sequence_number += 1;
        let (cmd, unblocker) = WFCommandFut::new(CancellableID::Timer(seq));
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
    pub fn activity(&mut self, opts: ActivityOptions) -> impl CancellableFuture<ActivityResult> {
        let seq = self.next_activity_sequence_number;
        self.next_activity_sequence_number += 1;
        let (cmd, unblocker) = WFCommandFut::new(CancellableID::Activity(seq));
        self.send(
            CommandCreateRequest {
                cmd: opts.to_command(seq).into(),
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Request to start a child workflow, Returned future resolves when the child has been started.
    pub fn child_workflow(&mut self, opts: ChildWorkflowOptions) -> ChildWorkflow {
        ChildWorkflow {
            sequence_number: None,
            opts,
        }
    }

    /// Wait on completion of a child workflow execution started with `start_child_workflow`
    pub fn child_workflow_result(
        &mut self,
        seq: u32,
    ) -> impl CancellableFuture<ChildWorkflowResult> {
        let (cmd, unblocker) = WFCommandFut::new(CancellableID::ChildWorkflow(seq));
        self.send(CommandSubscribeChildWorkflowCompletion { seq, unblocker }.into());
        cmd
    }

    /// Cancel any cancellable operation by ID
    fn cancel(&self, cancellable_id: CancellableID) {
        self.send(RustWfCmd::Cancel(cancellable_id))
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

    /// Force a workflow task failure (EX: in order to retry on non-sticky queue)
    pub fn force_task_fail(&self, with: anyhow::Error) {
        self.send(with.into())
    }

    fn send(&self, c: RustWfCmd) {
        self.chan.send(c).unwrap();
    }
}

/// A Future that can be cancelled.
/// Used in the prototype SDK for cancelling operations like timers and activities.
pub trait CancellableFuture<T>: Future<Output = T> {
    /// Cancel this Future
    fn cancel(&self, cx: &WfContext);
}

struct WFCommandFut<T>
where
    T: From<UnblockEvent>,
{
    _unused: PhantomData<T>,
    cancellable_id: CancellableID,
    result_rx: oneshot::Receiver<UnblockEvent>,
}

impl<T> WFCommandFut<T>
where
    T: From<UnblockEvent>,
{
    fn new(cancellable_id: CancellableID) -> (Self, oneshot::Sender<UnblockEvent>) {
        let (tx, rx) = oneshot::channel();
        (
            Self {
                cancellable_id,
                result_rx: rx,
                _unused: PhantomData,
            },
            tx,
        )
    }
}

impl<T> Unpin for WFCommandFut<T> where T: From<UnblockEvent> {}

impl<T> Future for WFCommandFut<T>
where
    T: From<UnblockEvent>,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.result_rx.poll_unpin(cx).map(|x| x.unwrap().into())
    }
}

impl<T> CancellableFuture<T> for WFCommandFut<T>
where
    T: From<UnblockEvent>,
{
    fn cancel(&self, cx: &WfContext) {
        cx.cancel(self.cancellable_id);
    }
}

/// Options for scheduling an activity
#[derive(Default, Debug)]
pub struct ActivityOptions {
    /// Identifier to use for tracking the activity in Workflow history.
    /// The `activityId` can be accessed by the activity function.
    /// Does not need to be unique.
    ///
    /// If `None` use the context's sequence number
    pub activity_id: Option<String>,
    /// Type of activity to schedule
    pub activity_type: String,
    /// Task queue to schedule the activity in
    pub task_queue: String,
    /// Time that the Activity Task can stay in the Task Queue before it is picked up by a Worker.
    /// Do not specify this timeout unless using host specific Task Queues for Activity Tasks are
    /// being used for routing.
    /// `schedule_to_start_timeout` is always non-retryable.
    /// Retrying after this timeout doesn't make sense as it would just put the Activity Task back
    /// into the same Task Queue.
    pub schedule_to_start_timeout: Option<Duration>,
    /// Maximum time of a single Activity execution attempt.
    /// Note that the Temporal Server doesn't detect Worker process failures directly.
    /// It relies on this timeout to detect that an Activity that didn't complete on time.
    /// So this timeout should be as short as the longest possible execution of the Activity body.
    /// Potentially long running Activities must specify `heartbeat_timeout` and heartbeat from the
    /// activity periodically for timely failure detection.
    /// Either this option or `schedule_to_close_timeout` is required.
    pub start_to_close_timeout: Option<Duration>,
    /// Total time that a workflow is willing to wait for Activity to complete.
    /// `schedule_to_close_timeout` limits the total time of an Activity's execution including
    /// retries (use `start_to_close_timeout` to limit the time of a single attempt).
    /// Either this option or `start_to_close_timeout` is required.
    pub schedule_to_close_timeout: Option<Duration>,
    /// Heartbeat interval. Activity must heartbeat before this interval passes after a last
    /// heartbeat or activity start.
    pub heartbeat_timeout: Option<Duration>,
    /// Determines what the SDK does when the Activity is cancelled.
    pub cancellation_type: ActivityCancellationType,
    // Add more fields here as needed
}

impl ActivityOptions {
    /// Turns an `ActivityOptions` struct to a `ScheduleActivity` struct using given `seq`
    pub fn to_command(&self, seq: u32) -> ScheduleActivity {
        ScheduleActivity {
            seq,
            activity_id: match &self.activity_id {
                None => seq.to_string(),
                Some(aid) => aid.clone(),
            },
            activity_type: self.activity_type.clone(),
            task_queue: self.task_queue.clone(),
            schedule_to_close_timeout: self.schedule_to_close_timeout.map(Into::into),
            schedule_to_start_timeout: self.schedule_to_start_timeout.map(Into::into),
            start_to_close_timeout: self.start_to_close_timeout.map(Into::into),
            heartbeat_timeout: self.heartbeat_timeout.map(Into::into),
            cancellation_type: self.cancellation_type as i32,
            ..Default::default()
        }
    }
}

/// Options for scheduling a child workflow
#[derive(Default, Debug)]
pub struct ChildWorkflowOptions {
    /// Workflow ID
    pub workflow_id: String,
    /// Type of workflow to schedule
    pub workflow_type: String,
    /// Input to send the child Workflow
    pub input: Vec<Payload>,
    // Add more fields here as needed
}

impl ChildWorkflowOptions {
    /// Turns a `ChildWorkflowOptions` struct to a `StartChildWorkflowExecution` struct using
    /// given `seq`
    pub fn to_command(&self, seq: u32) -> StartChildWorkflowExecution {
        StartChildWorkflowExecution {
            seq,
            workflow_id: self.workflow_id.clone(),
            workflow_type: self.workflow_type.clone(),
            // TODO: see if there's a way not to clone this
            input: self.input.clone(),
            ..Default::default()
        }
    }
}

/// A stub representing a child workflow.
/// Use its methods to interact with the child.
#[derive(Default, Debug)]
pub struct ChildWorkflow {
    opts: ChildWorkflowOptions,
    sequence_number: Option<u32>,
}

impl ChildWorkflow {
    /// Start the child workflow, the returned Future is cancellable.
    pub fn start(
        &mut self,
        cx: &mut WfContext,
    ) -> impl CancellableFuture<ChildWorkflowStartStatus> {
        let seq = cx.next_child_workflow_sequence_number;
        cx.next_child_workflow_sequence_number += 1;
        self.sequence_number = Some(seq);
        let (cmd, unblocker) = WFCommandFut::new(CancellableID::ChildWorkflow(seq));
        cx.send(
            CommandCreateRequest {
                cmd: self.opts.to_command(seq).into(),
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Wait on completion of a child workflow execution started with `start_child_workflow`.
    /// The returned Future is cancellable.
    pub fn result(&self, cx: &WfContext) -> impl CancellableFuture<ChildWorkflowResult> {
        if let Some(seq) = self.sequence_number {
            let (cmd, unblocker) = WFCommandFut::new(CancellableID::ChildWorkflow(seq));
            cx.send(CommandSubscribeChildWorkflowCompletion { seq, unblocker }.into());
            cmd
        } else {
            panic!("Tried to wait on a result of a child workflow which hasn't been started");
        }
    }
}
