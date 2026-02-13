mod options;

pub use options::{
    ActivityOptions, ChildWorkflowOptions, LocalActivityOptions, NexusOperationOptions, Signal,
    SignalData, SignalWorkflowOptions, TimerOptions,
};

use crate::{
    CancelExternalWfResult, CancellableID, CancellableIDWithReason, CommandCreateRequest,
    CommandSubscribeChildWorkflowCompletion, NexusStartResult, RustWfCmd, SignalExternalWfResult,
    SupportsCancelReason, TimerResult, UnblockEvent, Unblockable,
    workflow_context::options::IntoWorkflowCommand,
};
use futures_util::{FutureExt, future::Shared, task::Context};
use std::{
    cell::{Ref, RefCell},
    collections::HashMap,
    future::{self, Future},
    marker::PhantomData,
    ops::{Deref, DerefMut},
    pin::Pin,
    rc::Rc,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{Receiver, Sender},
    },
    task::Poll,
    time::{Duration, SystemTime},
};
use temporalio_common::{
    ActivityDefinition,
    data_converters::{
        GenericPayloadConverter, PayloadConversionError, PayloadConverter, SerializationContext,
        SerializationContextData, TemporalDeserializable,
    },
    protos::{
        coresdk::{
            activity_result::{ActivityResolution, activity_resolution},
            child_workflow::ChildWorkflowResult,
            common::NamespacedWorkflowExecution,
            nexus::NexusOperationResult,
            workflow_activation::{
                InitializeWorkflow,
                resolve_child_workflow_execution_start::Status as ChildWorkflowStartStatus,
            },
            workflow_commands::{
                CancelChildWorkflowExecution, ModifyWorkflowProperties,
                RequestCancelExternalWorkflowExecution, SetPatchMarker,
                SignalExternalWorkflowExecution, StartTimer, UpsertWorkflowSearchAttributes,
                WorkflowCommand, signal_external_workflow_execution as sig_we, workflow_command,
            },
        },
        temporal::api::{
            common::v1::{Memo, Payload, SearchAttributes},
            failure::v1::Failure,
            sdk::v1::UserMetadata,
        },
    },
    worker::WorkerDeploymentVersion,
};
use tokio::sync::{oneshot, watch};

/// Non-generic base context containing all workflow execution infrastructure.
///
/// This is used internally by futures and commands that don't need typed workflow state.
#[derive(Clone)]
pub struct BaseWorkflowContext {
    inner: Rc<WorkflowContextInner>,
}
impl BaseWorkflowContext {
    pub(crate) fn shared_mut(&self) -> impl DerefMut<Target = WorkflowContextSharedData> {
        self.inner.shared.borrow_mut()
    }

    /// Create a read-only view of this context.
    pub(crate) fn view(&self) -> WorkflowContextView {
        WorkflowContextView::new(
            self.inner.namespace.clone(),
            self.inner.task_queue.clone(),
            self.inner.run_id.clone(),
            &self.inner.inital_information,
        )
    }
}

struct WorkflowContextInner {
    namespace: String,
    task_queue: String,
    run_id: String,
    inital_information: InitializeWorkflow,
    chan: Sender<RustWfCmd>,
    am_cancelled: watch::Receiver<Option<String>>,
    shared: RefCell<WorkflowContextSharedData>,
    seq_nums: RefCell<WfCtxProtectedDat>,
    payload_converter: PayloadConverter,
}

/// Context provided to synchronous signal and update handlers.
///
/// This type provides all workflow context capabilities except `state()`, `state_mut()`,
/// and `wait_condition()`. Those methods are not applicable in sync handler contexts.
///
/// Sync handlers receive `&mut self` directly, so they can reference and mutate workflow state without
/// needing `state()`/`state_mut()`.
pub struct SyncWorkflowContext<W> {
    base: BaseWorkflowContext,
    /// Headers from the current handler invocation (signal, update, etc.)
    headers: Rc<HashMap<String, Payload>>,
    _phantom: PhantomData<W>,
}

impl<W> Clone for SyncWorkflowContext<W> {
    fn clone(&self) -> Self {
        Self {
            base: self.base.clone(),
            headers: self.headers.clone(),
            _phantom: PhantomData,
        }
    }
}

/// Used within workflows to issue commands, get info, etc.
///
/// The type parameter `W` represents the workflow type. This enables type-safe
/// access to workflow state via `state_mut()` for mutations.
pub struct WorkflowContext<W> {
    sync: SyncWorkflowContext<W>,
    /// The workflow instance
    workflow_state: Rc<RefCell<W>>,
}

impl<W> Clone for WorkflowContext<W> {
    fn clone(&self) -> Self {
        Self {
            sync: self.sync.clone(),
            workflow_state: self.workflow_state.clone(),
        }
    }
}

impl<W> Deref for WorkflowContext<W> {
    type Target = SyncWorkflowContext<W>;
    fn deref(&self) -> &SyncWorkflowContext<W> {
        &self.sync
    }
}

impl<W> DerefMut for WorkflowContext<W> {
    fn deref_mut(&mut self) -> &mut SyncWorkflowContext<W> {
        &mut self.sync
    }
}

/// Read-only view of workflow context for use in init and query handlers.
///
/// This provides access to workflow information but cannot issue commands.
#[derive(Clone, Debug)]
pub struct WorkflowContextView {
    /// The workflow's unique identifier
    pub workflow_id: String,
    /// The run id of this workflow execution
    pub run_id: String,
    /// The workflow type name
    pub workflow_type: String,
    /// The task queue this workflow is executing on
    pub task_queue: String,
    /// The namespace this workflow is executing in
    pub namespace: String,

    /// The current attempt number (starting from 1)
    pub attempt: u32,
    /// The run id of the very first execution in the chain
    pub first_execution_run_id: String,
    /// The run id of the previous execution if this is a continuation
    pub continued_from_run_id: Option<String>,

    /// When the workflow execution started
    pub start_time: Option<SystemTime>,
    /// Total workflow execution timeout including retries and continue as new
    pub execution_timeout: Option<Duration>,
    /// Timeout of a single workflow run
    pub run_timeout: Option<Duration>,
    /// Timeout of a single workflow task
    pub task_timeout: Option<Duration>,

    /// Information about the parent workflow, if this is a child workflow
    pub parent: Option<ParentWorkflowInfo>,
    /// Information about the root workflow in the execution chain
    pub root: Option<RootWorkflowInfo>,

    /// The workflow's retry policy
    pub retry_policy: Option<temporalio_common::protos::temporal::api::common::v1::RetryPolicy>,
    /// If this workflow runs on a cron schedule
    pub cron_schedule: Option<String>,
    /// User-defined memo
    pub memo: Option<Memo>,
    /// Initial search attributes
    pub search_attributes: Option<SearchAttributes>,
}

/// Information about a parent workflow.
#[derive(Clone, Debug)]
pub struct ParentWorkflowInfo {
    /// The parent workflow's unique identifier
    pub workflow_id: String,
    /// The parent workflow's run id
    pub run_id: String,
    /// The parent workflow's namespace
    pub namespace: String,
}

/// Information about the root workflow in an execution chain.
#[derive(Clone, Debug)]
pub struct RootWorkflowInfo {
    /// The root workflow's unique identifier
    pub workflow_id: String,
    /// The root workflow's run id
    pub run_id: String,
}

impl WorkflowContextView {
    /// Create a new view from workflow initialization data.
    pub(crate) fn new(
        namespace: String,
        task_queue: String,
        run_id: String,
        init: &InitializeWorkflow,
    ) -> Self {
        let parent = init
            .parent_workflow_info
            .as_ref()
            .map(|p| ParentWorkflowInfo {
                workflow_id: p.workflow_id.clone(),
                run_id: p.run_id.clone(),
                namespace: p.namespace.clone(),
            });

        let root = init.root_workflow.as_ref().map(|r| RootWorkflowInfo {
            workflow_id: r.workflow_id.clone(),
            run_id: r.run_id.clone(),
        });

        let continued_from_run_id = if init.continued_from_execution_run_id.is_empty() {
            None
        } else {
            Some(init.continued_from_execution_run_id.clone())
        };

        let cron_schedule = if init.cron_schedule.is_empty() {
            None
        } else {
            Some(init.cron_schedule.clone())
        };

        Self {
            workflow_id: init.workflow_id.clone(),
            run_id,
            workflow_type: init.workflow_type.clone(),
            task_queue,
            namespace,
            attempt: init.attempt as u32,
            first_execution_run_id: init.first_execution_run_id.clone(),
            continued_from_run_id,
            start_time: init.start_time.and_then(|t| t.try_into().ok()),
            execution_timeout: init
                .workflow_execution_timeout
                .and_then(|d| d.try_into().ok()),
            run_timeout: init.workflow_run_timeout.and_then(|d| d.try_into().ok()),
            task_timeout: init.workflow_task_timeout.and_then(|d| d.try_into().ok()),
            parent,
            root,
            retry_policy: init.retry_policy.clone(),
            cron_schedule,
            memo: init.memo.clone(),
            search_attributes: init.search_attributes.clone(),
        }
    }
}

/// Error type for activity execution outcomes.
#[derive(Debug, thiserror::Error)]
pub enum ActivityExecutionError {
    /// The activity failed with the given failure details.
    #[error("Activity failed: {}", .0.message)]
    Failed(Box<Failure>),
    /// The activity was cancelled.
    #[error("Activity cancelled: {}", .0.message)]
    Cancelled(Box<Failure>),
    // TODO: Timed out variant
    /// Failed to serialize input or deserialize result payload.
    #[error("Payload conversion failed: {0}")]
    Serialization(#[from] PayloadConversionError),
}

impl ActivityExecutionError {
    /// Returns true if this error represents a timeout.
    pub fn is_timeout(&self) -> bool {
        match self {
            ActivityExecutionError::Failed(f) => f.is_timeout().is_some(),
            _ => false,
        }
    }
}

impl BaseWorkflowContext {
    /// Create a new base context, returning the context itself and a receiver which outputs commands
    /// sent from the workflow.
    pub(crate) fn new(
        namespace: String,
        task_queue: String,
        run_id: String,
        init_workflow_job: InitializeWorkflow,
        am_cancelled: watch::Receiver<Option<String>>,
        payload_converter: PayloadConverter,
    ) -> (Self, Receiver<RustWfCmd>) {
        // The receiving side is non-async
        let (chan, rx) = std::sync::mpsc::channel();
        (
            Self {
                inner: Rc::new(WorkflowContextInner {
                    namespace,
                    task_queue,
                    run_id,
                    shared: RefCell::new(WorkflowContextSharedData {
                        random_seed: init_workflow_job.randomness_seed,
                        search_attributes: init_workflow_job
                            .search_attributes
                            .clone()
                            .unwrap_or_default(),
                        ..Default::default()
                    }),
                    inital_information: init_workflow_job,
                    chan,
                    am_cancelled,
                    seq_nums: RefCell::new(WfCtxProtectedDat {
                        next_timer_sequence_number: 1,
                        next_activity_sequence_number: 1,
                        next_child_workflow_sequence_number: 1,
                        next_cancel_external_wf_sequence_number: 1,
                        next_signal_external_wf_sequence_number: 1,
                        next_nexus_op_sequence_number: 1,
                    }),
                    payload_converter,
                }),
            },
            rx,
        )
    }

    /// Buffer a command to be sent in the activation reply
    pub(crate) fn send(&self, c: RustWfCmd) {
        self.inner.chan.send(c).expect("command channel intact");
    }

    /// Cancel any cancellable operation by ID
    fn cancel(&self, cancellable_id: CancellableID) {
        self.send(RustWfCmd::Cancel(cancellable_id));
    }

    /// Request to create a timer
    pub fn timer<T: Into<TimerOptions>>(
        &self,
        opts: T,
    ) -> impl CancellableFuture<TimerResult> + use<T> {
        let opts: TimerOptions = opts.into();
        let seq = self.inner.seq_nums.borrow_mut().next_timer_seq();
        let (cmd, unblocker) =
            CancellableWFCommandFut::new(CancellableID::Timer(seq), self.clone());
        self.send(
            CommandCreateRequest {
                cmd: WorkflowCommand {
                    variant: Some(
                        StartTimer {
                            seq,
                            start_to_fire_timeout: Some(
                                opts.duration
                                    .try_into()
                                    .expect("Durations must fit into 64 bits"),
                            ),
                        }
                        .into(),
                    ),
                    user_metadata: Some(UserMetadata {
                        summary: opts.summary.map(|x| x.as_bytes().into()),
                        details: None,
                    }),
                },
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Request to run an activity
    pub fn start_activity<AD: ActivityDefinition>(
        &self,
        _activity: AD,
        input: AD::Input,
        mut opts: ActivityOptions,
    ) -> impl CancellableFuture<Result<AD::Output, ActivityExecutionError>>
    where
        AD::Output: TemporalDeserializable,
    {
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &self.inner.payload_converter,
        };
        let payload = match self.inner.payload_converter.to_payload(&ctx, &input) {
            Ok(p) => p,
            Err(e) => {
                return ActivityFut::eager(e.into());
            }
        };
        let seq = self.inner.seq_nums.borrow_mut().next_activity_seq();
        let (cmd, unblocker) =
            CancellableWFCommandFut::new(CancellableID::Activity(seq), self.clone());
        if opts.task_queue.is_none() {
            opts.task_queue = Some(self.inner.task_queue.clone());
        }
        self.send(
            CommandCreateRequest {
                cmd: opts.into_command(AD::name().to_string(), payload, seq),
                unblocker,
            }
            .into(),
        );
        ActivityFut::running(cmd, self.inner.payload_converter.clone())
    }

    /// Request to run a local activity
    pub fn start_local_activity<AD: ActivityDefinition>(
        &self,
        _activity: AD,
        input: AD::Input,
        opts: LocalActivityOptions,
    ) -> impl CancellableFuture<Result<AD::Output, ActivityExecutionError>>
    where
        AD::Output: TemporalDeserializable,
    {
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &self.inner.payload_converter,
        };
        let payload = match self.inner.payload_converter.to_payload(&ctx, &input) {
            Ok(p) => p,
            Err(e) => {
                return ActivityFut::eager(e.into());
            }
        };
        ActivityFut::running(
            LATimerBackoffFut::new(AD::name().to_string(), payload, opts, self.clone()),
            self.inner.payload_converter.clone(),
        )
    }

    /// Request to run a local activity with no implementation of timer-backoff based retrying.
    fn local_activity_no_timer_retry(
        self,
        activity_type: String,
        input: Payload,
        opts: LocalActivityOptions,
    ) -> impl CancellableFuture<ActivityResolution> {
        let seq = self.inner.seq_nums.borrow_mut().next_activity_seq();
        let (cmd, unblocker) =
            CancellableWFCommandFut::new(CancellableID::LocalActivity(seq), self.clone());
        self.inner
            .chan
            .send(
                CommandCreateRequest {
                    cmd: opts.into_command(activity_type, input, seq),
                    unblocker,
                }
                .into(),
            )
            .expect("command channel intact");
        cmd
    }

    fn send_signal_wf(
        self,
        target: sig_we::Target,
        signal: Signal,
    ) -> impl CancellableFuture<SignalExternalWfResult> {
        let seq = self
            .inner
            .seq_nums
            .borrow_mut()
            .next_signal_external_wf_seq();
        let (cmd, unblocker) =
            CancellableWFCommandFut::new(CancellableID::SignalExternalWorkflow(seq), self.clone());
        self.send(
            CommandCreateRequest {
                cmd: WorkflowCommand {
                    variant: Some(
                        SignalExternalWorkflowExecution {
                            seq,
                            signal_name: signal.signal_name,
                            args: signal.data.input,
                            target: Some(target),
                            headers: signal.data.headers,
                        }
                        .into(),
                    ),
                    user_metadata: None,
                },
                unblocker,
            }
            .into(),
        );
        cmd
    }
}

impl<W> SyncWorkflowContext<W> {
    /// Return the namespace the workflow is executing in
    pub fn namespace(&self) -> &str {
        &self.base.inner.namespace
    }

    /// Return the task queue the workflow is executing in
    pub fn task_queue(&self) -> &str {
        &self.base.inner.task_queue
    }

    /// Return the current time according to the workflow (which is not wall-clock time).
    pub fn workflow_time(&self) -> Option<SystemTime> {
        self.base.inner.shared.borrow().wf_time
    }

    /// Return the length of history so far at this point in the workflow
    pub fn history_length(&self) -> u32 {
        self.base.inner.shared.borrow().history_length
    }

    /// Return the deployment version, if any,  as it was when this point in the workflow was first
    /// reached. If this code is being executed for the first time, return this Worker's deployment
    /// version if it has one.
    pub fn current_deployment_version(&self) -> Option<WorkerDeploymentVersion> {
        self.base
            .inner
            .shared
            .borrow()
            .current_deployment_version
            .clone()
    }

    /// Return current values for workflow search attributes
    pub fn search_attributes(&self) -> impl Deref<Target = SearchAttributes> + '_ {
        Ref::map(self.base.inner.shared.borrow(), |s| &s.search_attributes)
    }

    /// Return the workflow's randomness seed
    pub fn random_seed(&self) -> u64 {
        self.base.inner.shared.borrow().random_seed
    }

    /// Returns true if the current workflow task is happening under replay
    pub fn is_replaying(&self) -> bool {
        self.base.inner.shared.borrow().is_replaying
    }

    /// Returns the headers for the current handler invocation (signal, update, query, etc.).
    ///
    /// When called from within a signal handler, returns the headers that were sent with that
    /// signal. When called from the main workflow run method, returns an empty map.
    pub fn headers(&self) -> &HashMap<String, Payload> {
        &self.headers
    }

    /// Returns the [PayloadConverter] currently used by the worker running this workflow.
    pub fn payload_converter(&self) -> &PayloadConverter {
        &self.base.inner.payload_converter
    }

    /// Return various information that the workflow was initialized with. Will eventually become
    /// a proper non-proto workflow info struct.
    pub fn workflow_initial_info(&self) -> &InitializeWorkflow {
        &self.base.inner.inital_information
    }

    /// A future that resolves if/when the workflow is cancelled, with the user provided cause
    pub async fn cancelled(&self) -> String {
        if let Some(s) = self.base.inner.am_cancelled.borrow().as_ref() {
            return s.clone();
        }
        self.base
            .inner
            .am_cancelled
            .clone()
            .changed()
            .await
            .expect("Cancelled send half not dropped");
        self.base
            .inner
            .am_cancelled
            .borrow()
            .as_ref()
            .cloned()
            .unwrap_or_default()
    }

    /// Request to create a timer
    pub fn timer<T: Into<TimerOptions>>(&self, opts: T) -> impl CancellableFuture<TimerResult> {
        self.base.timer(opts)
    }

    /// Request to run an activity
    pub fn start_activity<AD: ActivityDefinition>(
        &self,
        activity: AD,
        input: AD::Input,
        opts: ActivityOptions,
    ) -> impl CancellableFuture<Result<AD::Output, ActivityExecutionError>>
    where
        AD::Output: TemporalDeserializable,
    {
        self.base.start_activity(activity, input, opts)
    }

    /// Request to run a local activity
    pub fn start_local_activity<AD: ActivityDefinition>(
        &self,
        activity: AD,
        input: AD::Input,
        opts: LocalActivityOptions,
    ) -> impl CancellableFuture<Result<AD::Output, ActivityExecutionError>>
    where
        AD::Output: TemporalDeserializable,
    {
        self.base.start_local_activity(activity, input, opts)
    }

    /// Creates a child workflow stub with the provided options
    pub fn child_workflow(&self, opts: ChildWorkflowOptions) -> ChildWorkflow {
        ChildWorkflow {
            opts,
            base_ctx: self.base.clone(),
        }
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
        self.base.send(
            workflow_command::Variant::SetPatchMarker(SetPatchMarker {
                patch_id: patch_id.to_string(),
                deprecated,
            })
            .into(),
        );
        // See if we already know about the status of this change
        if let Some(present) = self.base.inner.shared.borrow().changes.get(patch_id) {
            return *present;
        }

        // If we don't already know about the change, that means there is no marker in history,
        // and we should return false if we are replaying
        let res = !self.base.inner.shared.borrow().is_replaying;

        self.base
            .inner
            .shared
            .borrow_mut()
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
            namespace: self.base.inner.namespace.clone(),
            workflow_id: options.workflow_id,
            run_id: options.run_id.unwrap_or_default(),
        });
        self.base.clone().send_signal_wf(target, options.signal)
    }

    /// Add or create a set of search attributes
    pub fn upsert_search_attributes(&self, attr_iter: impl IntoIterator<Item = (String, Payload)>) {
        self.base.send(RustWfCmd::NewNonblockingCmd(
            workflow_command::Variant::UpsertWorkflowSearchAttributes(
                UpsertWorkflowSearchAttributes {
                    search_attributes: Some(SearchAttributes {
                        indexed_fields: HashMap::from_iter(attr_iter),
                    }),
                },
            ),
        ))
    }

    /// Add or create a set of search attributes
    pub fn upsert_memo(&self, attr_iter: impl IntoIterator<Item = (String, Payload)>) {
        self.base.send(RustWfCmd::NewNonblockingCmd(
            workflow_command::Variant::ModifyWorkflowProperties(ModifyWorkflowProperties {
                upserted_memo: Some(Memo {
                    fields: HashMap::from_iter(attr_iter),
                }),
            }),
        ))
    }

    /// Force a workflow task failure (EX: in order to retry on non-sticky queue)
    pub fn force_task_fail(&self, with: anyhow::Error) {
        self.base.send(with.into());
    }

    /// Request the cancellation of an external workflow. May resolve as a failure if the workflow
    /// was not found or the cancel was otherwise unsendable.
    pub fn cancel_external(
        &self,
        target: NamespacedWorkflowExecution,
        reason: String,
    ) -> impl Future<Output = CancelExternalWfResult> {
        let seq = self
            .base
            .inner
            .seq_nums
            .borrow_mut()
            .next_cancel_external_wf_seq();
        let (cmd, unblocker) = WFCommandFut::new();
        self.base.send(
            CommandCreateRequest {
                cmd: WorkflowCommand {
                    variant: Some(
                        RequestCancelExternalWorkflowExecution {
                            seq,
                            workflow_execution: Some(target),
                            reason,
                        }
                        .into(),
                    ),
                    user_metadata: None,
                },
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Start a nexus operation
    pub fn start_nexus_operation(
        &self,
        opts: NexusOperationOptions,
    ) -> impl CancellableFuture<NexusStartResult> {
        let seq = self.base.inner.seq_nums.borrow_mut().next_nexus_op_seq();
        let (result_future, unblocker) = WFCommandFut::new();
        self.base
            .send(RustWfCmd::SubscribeNexusOperationCompletion { seq, unblocker });
        let (cmd, unblocker) = CancellableWFCommandFut::new_with_dat(
            CancellableID::NexusOp(seq),
            NexusUnblockData {
                result_future: result_future.shared(),
                schedule_seq: seq,
                base_ctx: self.base.clone(),
            },
            self.base.clone(),
        );
        self.base.send(
            CommandCreateRequest {
                cmd: opts.into_command(seq),
                unblocker,
            }
            .into(),
        );
        cmd
    }

    /// Create a read-only view of this context.
    pub(crate) fn view(&self) -> WorkflowContextView {
        self.base.view()
    }
}

impl<W> WorkflowContext<W> {
    /// Create a new wf context from a base context and workflow state.
    pub(crate) fn from_base(base: BaseWorkflowContext, workflow_state: Rc<RefCell<W>>) -> Self {
        Self {
            sync: SyncWorkflowContext {
                base,
                headers: Rc::new(HashMap::new()),
                _phantom: PhantomData,
            },
            workflow_state,
        }
    }

    /// Returns a new context with the specified headers set.
    pub(crate) fn with_headers(&self, headers: HashMap<String, Payload>) -> Self {
        Self {
            sync: SyncWorkflowContext {
                base: self.sync.base.clone(),
                headers: Rc::new(headers),
                _phantom: PhantomData,
            },
            workflow_state: self.workflow_state.clone(),
        }
    }

    /// Returns a [`SyncWorkflowContext`] extracted from this context.
    pub(crate) fn sync_context(&self) -> SyncWorkflowContext<W> {
        self.sync.clone()
    }

    /// Access workflow state immutably via closure.
    ///
    /// The borrow is scoped to the closure and cannot escape, preventing
    /// borrows from being held across await points.
    pub fn state<R>(&self, f: impl FnOnce(&W) -> R) -> R {
        f(&*self.workflow_state.borrow())
    }

    /// Access workflow state mutably via closure.
    ///
    /// The borrow is scoped to the closure and cannot escape, preventing
    /// borrows from being held across await points.
    pub fn state_mut<R>(&self, f: impl FnOnce(&mut W) -> R) -> R {
        f(&mut *self.workflow_state.borrow_mut())
    }

    /// Wait for some condition on workflow state to become true, yielding the workflow if not.
    ///
    /// The condition closure receives an immutable reference to the workflow state,
    /// which is borrowed only for the duration of each poll (not across await points).
    pub fn wait_condition<'a>(
        &'a self,
        mut condition: impl FnMut(&W) -> bool + 'a,
    ) -> impl Future<Output = ()> + 'a {
        future::poll_fn(move |_cx: &mut Context<'_>| {
            if condition(&*self.workflow_state.borrow()) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
    }
}

struct WfCtxProtectedDat {
    next_timer_sequence_number: u32,
    next_activity_sequence_number: u32,
    next_child_workflow_sequence_number: u32,
    next_cancel_external_wf_sequence_number: u32,
    next_signal_external_wf_sequence_number: u32,
    next_nexus_op_sequence_number: u32,
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
    fn next_nexus_op_seq(&mut self) -> u32 {
        let seq = self.next_nexus_op_sequence_number;
        self.next_nexus_op_sequence_number += 1;
        seq
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct WorkflowContextSharedData {
    /// Maps change ids -> resolved status
    pub(crate) changes: HashMap<String, bool>,
    pub(crate) is_replaying: bool,
    pub(crate) wf_time: Option<SystemTime>,
    pub(crate) history_length: u32,
    pub(crate) current_deployment_version: Option<WorkerDeploymentVersion>,
    pub(crate) search_attributes: SearchAttributes,
    pub(crate) random_seed: u64,
}

/// A Future that can be cancelled.
/// Used in the prototype SDK for cancelling operations like timers and activities.
pub trait CancellableFuture<T>: Future<Output = T> {
    /// Cancel this Future
    fn cancel(&self);
}

/// A Future that can be cancelled with a reason
pub trait CancellableFutureWithReason<T>: CancellableFuture<T> {
    /// Cancel this Future with a reason
    fn cancel_with_reason(&self, reason: String);
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

struct CancellableWFCommandFut<T, D, ID = CancellableID> {
    cmd_fut: WFCommandFut<T, D>,
    cancellable_id: ID,
    base_ctx: BaseWorkflowContext,
}
impl<T, ID> CancellableWFCommandFut<T, (), ID> {
    fn new(
        cancellable_id: ID,
        base_ctx: BaseWorkflowContext,
    ) -> (Self, oneshot::Sender<UnblockEvent>) {
        Self::new_with_dat(cancellable_id, (), base_ctx)
    }
}
impl<T, D, ID> CancellableWFCommandFut<T, D, ID> {
    fn new_with_dat(
        cancellable_id: ID,
        other_dat: D,
        base_ctx: BaseWorkflowContext,
    ) -> (Self, oneshot::Sender<UnblockEvent>) {
        let (cmd_fut, sender) = WFCommandFut::new_with_dat(other_dat);
        (
            Self {
                cmd_fut,
                cancellable_id,
                base_ctx,
            },
            sender,
        )
    }
}
impl<T, D, ID> Unpin for CancellableWFCommandFut<T, D, ID> where T: Unblockable<OtherDat = D> {}
impl<T, D, ID> Future for CancellableWFCommandFut<T, D, ID>
where
    T: Unblockable<OtherDat = D>,
{
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.cmd_fut.poll_unpin(cx)
    }
}

impl<T, D, ID> CancellableFuture<T> for CancellableWFCommandFut<T, D, ID>
where
    T: Unblockable<OtherDat = D>,
    ID: Clone + Into<CancellableID>,
{
    fn cancel(&self) {
        self.base_ctx.cancel(self.cancellable_id.clone().into());
    }
}
impl<T, D> CancellableFutureWithReason<T> for CancellableWFCommandFut<T, D, CancellableIDWithReason>
where
    T: Unblockable<OtherDat = D>,
{
    fn cancel_with_reason(&self, reason: String) {
        let new_id = self.cancellable_id.clone().with_reason(reason);
        self.base_ctx.cancel(new_id);
    }
}

struct LATimerBackoffFut {
    la_opts: LocalActivityOptions,
    activity_type: String,
    input: Payload,
    current_fut: Pin<Box<dyn CancellableFuture<ActivityResolution> + Unpin>>,
    timer_fut: Option<Pin<Box<dyn CancellableFuture<TimerResult> + Unpin>>>,
    base_ctx: BaseWorkflowContext,
    next_attempt: u32,
    next_sched_time: Option<prost_types::Timestamp>,
    did_cancel: AtomicBool,
}
impl LATimerBackoffFut {
    pub(crate) fn new(
        activity_type: String,
        input: Payload,
        opts: LocalActivityOptions,
        base_ctx: BaseWorkflowContext,
    ) -> Self {
        let current_fut = Box::pin(base_ctx.clone().local_activity_no_timer_retry(
            activity_type.clone(),
            input.clone(),
            opts.clone(),
        ));
        Self {
            la_opts: opts,
            activity_type,
            input,
            current_fut,
            timer_fut: None,
            base_ctx,
            next_attempt: 1,
            next_sched_time: None,
            did_cancel: AtomicBool::new(false),
        }
    }
}
impl Unpin for LATimerBackoffFut {}
impl Future for LATimerBackoffFut {
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
                        self.current_fut =
                            Box::pin(self.base_ctx.clone().local_activity_no_timer_retry(
                                self.activity_type.clone(),
                                self.input.clone(),
                                opts,
                            ));
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
        if let Poll::Ready(ref r) = poll_res
            && let Some(activity_resolution::Status::Backoff(b)) = r.status.as_ref()
        {
            // If we've already said we want to cancel, don't schedule the backoff timer. Just
            // return cancel status. This can happen if cancel comes after the LA says it wants
            // to back off but before we have scheduled the timer.
            if self.did_cancel.load(Ordering::Acquire) {
                return Poll::Ready(ActivityResolution {
                    status: Some(activity_resolution::Status::Cancelled(Default::default())),
                });
            }

            let timer_f = self.base_ctx.timer::<Duration>(
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
        poll_res
    }
}
impl CancellableFuture<ActivityResolution> for LATimerBackoffFut {
    fn cancel(&self) {
        self.did_cancel.store(true, Ordering::Release);
        if let Some(tf) = self.timer_fut.as_ref() {
            tf.cancel();
        }
        self.current_fut.cancel();
    }
}

/// Future for activity results. Either an immediate error or a running activity.
enum ActivityFut<F, Output> {
    /// Immediate error (e.g., input serialization failure). Resolves on first poll.
    Errored {
        error: Option<ActivityExecutionError>,
        _phantom: PhantomData<Output>,
    },
    /// Running activity that will deserialize output on completion.
    Running {
        inner: F,
        payload_converter: PayloadConverter,
        _phantom: PhantomData<Output>,
    },
}

impl<F, Output> ActivityFut<F, Output> {
    fn eager(err: ActivityExecutionError) -> Self {
        Self::Errored {
            error: Some(err),
            _phantom: PhantomData,
        }
    }

    fn running(inner: F, payload_converter: PayloadConverter) -> Self {
        Self::Running {
            inner,
            payload_converter,
            _phantom: PhantomData,
        }
    }
}

impl<F, Output> Unpin for ActivityFut<F, Output> where F: Unpin {}

impl<F, Output> Future for ActivityFut<F, Output>
where
    F: Future<Output = ActivityResolution> + Unpin,
    Output: TemporalDeserializable + 'static,
{
    type Output = Result<Output, ActivityExecutionError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            ActivityFut::Errored { error, .. } => {
                Poll::Ready(Err(error.take().expect("polled after completion")))
            }
            ActivityFut::Running {
                inner,
                payload_converter,
                ..
            } => match Pin::new(inner).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(resolution) => Poll::Ready({
                    let status = resolution.status.ok_or_else(|| {
                        ActivityExecutionError::Failed(Box::new(Failure {
                            message: "Activity completed without a status".to_string(),
                            ..Default::default()
                        }))
                    })?;

                    match status {
                        activity_resolution::Status::Completed(success) => {
                            let payload = success.result.unwrap_or_default();
                            let ctx = SerializationContext {
                                data: &SerializationContextData::Workflow,
                                converter: payload_converter,
                            };
                            payload_converter
                                .from_payload::<Output>(&ctx, payload)
                                .map_err(ActivityExecutionError::Serialization)
                        }
                        activity_resolution::Status::Failed(f) => Err(
                            ActivityExecutionError::Failed(Box::new(f.failure.unwrap_or_default())),
                        ),
                        activity_resolution::Status::Cancelled(c) => {
                            Err(ActivityExecutionError::Cancelled(Box::new(
                                c.failure.unwrap_or_default(),
                            )))
                        }
                        activity_resolution::Status::Backoff(_) => {
                            panic!("DoBackoff should be handled by LATimerBackoffFut")
                        }
                    }
                }),
            },
        }
    }
}

impl<F, Output> CancellableFuture<Result<Output, ActivityExecutionError>> for ActivityFut<F, Output>
where
    F: CancellableFuture<ActivityResolution> + Unpin,
    Output: TemporalDeserializable + 'static,
{
    fn cancel(&self) {
        match self {
            ActivityFut::Errored { .. } => {}
            ActivityFut::Running { inner, .. } => inner.cancel(),
        }
    }
}

/// A stub representing an unstarted child workflow.
#[derive(Clone)]
pub struct ChildWorkflow {
    opts: ChildWorkflowOptions,
    base_ctx: BaseWorkflowContext,
}

pub(crate) struct ChildWfCommon {
    workflow_id: String,
    result_future: CancellableWFCommandFut<ChildWorkflowResult, (), CancellableIDWithReason>,
    base_ctx: BaseWorkflowContext,
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
    pub fn start(self) -> impl CancellableFutureWithReason<PendingChildWorkflow> {
        let child_seq = self
            .base_ctx
            .inner
            .seq_nums
            .borrow_mut()
            .next_child_workflow_seq();
        // Immediately create the command/future for the result, otherwise if the user does
        // not await the result until *after* we receive an activation for it, there will be nothing
        // to match when unblocking.
        let cancel_seq = self
            .base_ctx
            .inner
            .seq_nums
            .borrow_mut()
            .next_cancel_external_wf_seq();
        let (result_cmd, unblocker) = CancellableWFCommandFut::new(
            CancellableIDWithReason::ExternalWorkflow {
                seqnum: cancel_seq,
                execution: NamespacedWorkflowExecution {
                    workflow_id: self.opts.workflow_id.clone(),
                    ..Default::default()
                },
            },
            self.base_ctx.clone(),
        );
        self.base_ctx.send(
            CommandSubscribeChildWorkflowCompletion {
                seq: child_seq,
                unblocker,
            }
            .into(),
        );

        let common = ChildWfCommon {
            workflow_id: self.opts.workflow_id.clone(),
            result_future: result_cmd,
            base_ctx: self.base_ctx.clone(),
        };

        let (cmd, unblocker) = CancellableWFCommandFut::new_with_dat(
            CancellableIDWithReason::ChildWorkflow { seqnum: child_seq },
            common,
            self.base_ctx.clone(),
        );
        self.base_ctx.send(
            CommandCreateRequest {
                cmd: self.opts.into_command(child_seq),
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
    pub fn result(self) -> impl CancellableFutureWithReason<ChildWorkflowResult> {
        self.common.result_future
    }

    /// Cancel the child workflow
    pub fn cancel(&self, reason: String) {
        self.common.base_ctx.send(RustWfCmd::NewNonblockingCmd(
            CancelChildWorkflowExecution {
                child_workflow_seq: self.common.result_future.cancellable_id.seq_num(),
                reason,
            }
            .into(),
        ));
    }

    /// Signal the child workflow
    pub fn signal<S: Into<Signal>>(
        &self,
        data: S,
    ) -> impl CancellableFuture<SignalExternalWfResult> + 'static {
        let target = sig_we::Target::ChildWorkflowId(self.common.workflow_id.clone());
        self.common
            .base_ctx
            .clone()
            .send_signal_wf(target, data.into())
    }
}

#[derive(derive_more::Debug)]
#[debug("StartedNexusOperation{{ operation_token: {operation_token:?} }}")]
pub struct StartedNexusOperation {
    /// The operation token, if the operation started asynchronously
    pub operation_token: Option<String>,
    pub(crate) unblock_dat: NexusUnblockData,
}

pub(crate) struct NexusUnblockData {
    result_future: Shared<WFCommandFut<NexusOperationResult, ()>>,
    schedule_seq: u32,
    base_ctx: BaseWorkflowContext,
}

impl StartedNexusOperation {
    pub async fn result(&self) -> NexusOperationResult {
        self.unblock_dat.result_future.clone().await
    }

    pub fn cancel(&self) {
        self.unblock_dat
            .base_ctx
            .cancel(CancellableID::NexusOp(self.unblock_dat.schedule_seq));
    }
}
