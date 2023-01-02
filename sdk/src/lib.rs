#![warn(missing_docs)] // error if there are missing docs

//! This crate defines an alpha-stage Temporal Rust SDK.
//!
//! Currently defining activities and running an activity-only worker is the most stable code.
//! Workflow definitions exist and running a workflow worker works, but the API is still very
//! unstable.
//!
//! An example of running an activity worker:
//! ```no_run
//! use std::{str::FromStr, sync::Arc};
//! use temporal_sdk::{sdk_client_options, ActContext, Worker};
//! use temporal_sdk_core::{init_worker, Url, CoreRuntime};
//! use temporal_sdk_core_api::{worker::WorkerConfigBuilder, telemetry::TelemetryOptionsBuilder};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let server_options = sdk_client_options(Url::from_str("http://localhost:7233")?).build()?;
//!
//!     let client = server_options.connect("default", None, None).await?;
//!
//!     let telemetry_options = TelemetryOptionsBuilder::default().build()?;
//!     let runtime = CoreRuntime::new_assume_tokio(telemetry_options)?;
//!
//!     let worker_config = WorkerConfigBuilder::default()
//!         .namespace("default")
//!         .task_queue("task_queue")
//!         .build()?;
//!
//!     let core_worker = init_worker(&runtime, worker_config, client)?;
//!
//!     let mut worker = Worker::new_from_core(Arc::new(core_worker), "task_queue");
//!     worker.register_activity(
//!         "echo_activity",
//!         |_ctx: ActContext, echo_me: String| async move { Ok(echo_me) },
//!     );
//!
//!     worker.run().await?;
//!
//!     Ok(())
//! }
//! ```

#[macro_use]
extern crate tracing;

mod activity_context;
mod app_data;
pub mod interceptors;
mod payload_converter;
mod workflow_context;
mod workflow_future;

pub use activity_context::ActContext;
pub use temporal_client::Namespace;
pub use workflow_context::{
    ActivityOptions, CancellableFuture, ChildWorkflow, ChildWorkflowOptions, LocalActivityOptions,
    Signal, SignalData, SignalWorkflowOptions, WfContext,
};

use crate::{
    interceptors::WorkerInterceptor,
    workflow_context::{ChildWfCommon, PendingChildWorkflow},
};
use anyhow::{anyhow, bail, Context};
use app_data::AppData;
use futures::{future::BoxFuture, FutureExt, StreamExt, TryFutureExt, TryStreamExt};
use std::{
    any::{Any, TypeId},
    cell::RefCell,
    collections::HashMap,
    fmt::{Debug, Display, Formatter},
    future::Future,
    panic::AssertUnwindSafe,
    sync::Arc,
};
use temporal_client::ClientOptionsBuilder;
use temporal_sdk_core::Url;
use temporal_sdk_core_api::{
    errors::{PollActivityError, PollWfError},
    Worker as CoreWorker,
};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{ActivityExecutionResult, ActivityResolution},
        activity_task::{activity_task, ActivityTask},
        child_workflow::ChildWorkflowResult,
        common::NamespacedWorkflowExecution,
        workflow_activation::{
            resolve_child_workflow_execution_start::Status as ChildWorkflowStartStatus,
            workflow_activation_job::Variant, WorkflowActivation, WorkflowActivationJob,
        },
        workflow_commands::{workflow_command, ContinueAsNewWorkflowExecution},
        workflow_completion::WorkflowActivationCompletion,
        ActivityTaskCompletion, AsJsonPayloadExt, FromJsonPayloadExt,
    },
    temporal::api::{common::v1::Payload, failure::v1::Failure},
    TaskToken,
};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        oneshot,
    },
    task::JoinError,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Returns a [ClientOptionsBuilder] with required fields set to appropriate values
/// for the Rust SDK.
pub fn sdk_client_options(url: impl Into<Url>) -> ClientOptionsBuilder {
    let mut builder = ClientOptionsBuilder::default();
    builder
        .target_url(url)
        .client_name("rust-sdk".to_string())
        .client_version(VERSION.to_string());

    builder
}

/// A worker that can poll for and respond to workflow tasks by using [WorkflowFunction]s,
/// and activity tasks by using [ActivityFunction]s
pub struct Worker {
    common: CommonWorker,
    workflow_half: WorkflowHalf,
    activity_half: ActivityHalf,
    app_data: Option<AppData>,
}

struct CommonWorker {
    worker: Arc<dyn CoreWorker>,
    task_queue: String,
    worker_interceptor: Option<Box<dyn WorkerInterceptor>>,
}

struct WorkflowHalf {
    /// Maps run id to cached workflow state
    workflows: RefCell<HashMap<String, WorkflowData>>,
    /// Maps workflow type to the function for executing workflow runs with that ID
    workflow_fns: RefCell<HashMap<String, WorkflowFunction>>,
}
struct WorkflowData {
    /// Channel used to send the workflow activations
    activation_chan: UnboundedSender<WorkflowActivation>,
}

struct WorkflowFutureHandle<F: Future<Output = Result<WorkflowResult<()>, JoinError>>> {
    join_handle: F,
    run_id: String,
}

struct ActivityHalf {
    /// Maps activity type to the function for executing activities of that type
    activity_fns: HashMap<String, ActivityFunction>,
    task_tokens_to_cancels: HashMap<TaskToken, CancellationToken>,
}

impl Worker {
    /// Create a new Rust SDK worker from a core worker
    pub fn new_from_core(worker: Arc<dyn CoreWorker>, task_queue: impl Into<String>) -> Self {
        Self {
            common: CommonWorker {
                worker,
                task_queue: task_queue.into(),
                worker_interceptor: None,
            },
            workflow_half: WorkflowHalf {
                workflows: Default::default(),
                workflow_fns: Default::default(),
            },
            activity_half: ActivityHalf {
                activity_fns: Default::default(),
                task_tokens_to_cancels: Default::default(),
            },
            app_data: Some(Default::default()),
        }
    }

    /// Returns the task queue name this worker polls on
    pub fn task_queue(&self) -> &str {
        &self.common.task_queue
    }

    /// Return a handle that can be used to initiate shutdown.
    /// TODO: Doc better after shutdown changes
    pub fn shutdown_handle(&self) -> impl Fn() {
        let w = self.common.worker.clone();
        move || w.initiate_shutdown()
    }

    /// Register a Workflow function to invoke when the Worker is asked to run a workflow of
    /// `workflow_type`
    pub fn register_wf<F: Into<WorkflowFunction>>(
        &mut self,
        workflow_type: impl Into<String>,
        wf_function: F,
    ) {
        self.workflow_half
            .workflow_fns
            .get_mut()
            .insert(workflow_type.into(), wf_function.into());
    }

    /// Register an Activity function to invoke when the Worker is asked to run an activity of
    /// `activity_type`
    pub fn register_activity<A, R, O>(
        &mut self,
        activity_type: impl Into<String>,
        act_function: impl IntoActivityFunc<A, R, O>,
    ) {
        self.activity_half.activity_fns.insert(
            activity_type.into(),
            ActivityFunction {
                act_func: act_function.into_activity_fn(),
            },
        );
    }

    /// Insert Custom App Context for Workflows and Activities
    pub fn insert_app_data<T: Send + Sync + 'static>(&mut self, data: T) {
        self.app_data.as_mut().map(|a| a.insert(data));
    }

    /// Runs the worker. Eventually resolves after the worker has been explicitly shut down,
    /// or may return early with an error in the event of some unresolvable problem.
    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        let shutdown_token = CancellationToken::new();
        let (common, wf_half, act_half, app_data) = self.split_apart();
        let safe_app_data = Arc::new(
            app_data
                .take()
                .ok_or_else(|| anyhow!("app_data should exist on run"))?,
        );
        let (wf_future_tx, wf_future_rx) = unbounded_channel();
        let (completions_tx, completions_rx) = unbounded_channel();
        let wf_future_joiner = async {
            UnboundedReceiverStream::new(wf_future_rx)
                .map(Result::<_, anyhow::Error>::Ok)
                .try_for_each_concurrent(
                    None,
                    |WorkflowFutureHandle {
                         join_handle,
                         run_id,
                     }| {
                        let wf_half = &*wf_half;
                        async move {
                            join_handle.await??;
                            info!(run_id=%run_id, "Removing workflow from cache");
                            wf_half.workflows.borrow_mut().remove(&run_id);
                            Ok(())
                        }
                    },
                )
                .await
                .context("Workflow futures encountered an error")
        };
        let wf_completion_processor = async {
            UnboundedReceiverStream::new(completions_rx)
                .map(Ok)
                .try_for_each_concurrent(None, |completion| async {
                    if let Some(ref i) = common.worker_interceptor {
                        i.on_workflow_activation_completion(&completion).await;
                    }
                    common.worker.complete_workflow_activation(completion).await
                })
                .map_err(anyhow::Error::from)
                .await
                .context("Workflow completions processor encountered an error")
        };
        tokio::try_join!(
            // Workflow polling loop
            async {
                loop {
                    let activation = match common.worker.poll_workflow_activation().await {
                        Err(PollWfError::ShutDown) => {
                            break;
                        }
                        o => o?,
                    };
                    if let Some(ref i) = common.worker_interceptor {
                        i.on_workflow_activation(&activation).await?;
                    }
                    if let Some(wf_fut) = wf_half.workflow_activation_handler(
                        common,
                        shutdown_token.clone(),
                        activation,
                        &completions_tx,
                    )? {
                        if wf_future_tx.send(wf_fut).is_err() {
                            panic!(
                                "Receive half of completion processor channel cannot be dropped"
                            );
                        }
                    }
                }
                // Tell still-alive workflows to evict themselves
                shutdown_token.cancel();
                // It's important to drop these so the future and completion processors will
                // terminate.
                drop(wf_future_tx);
                drop(completions_tx);
                Result::<_, anyhow::Error>::Ok(())
            },
            // Only poll on the activity queue if activity functions have been registered. This
            // makes tests which use mocks dramatically more manageable.
            async {
                if !act_half.activity_fns.is_empty() {
                    loop {
                        let activity = common.worker.poll_activity_task().await;
                        if matches!(activity, Err(PollActivityError::ShutDown)) {
                            break;
                        }
                        act_half.activity_task_handler(
                            common.worker.clone(),
                            safe_app_data.clone(),
                            common.task_queue.clone(),
                            activity?,
                        )?;
                    }
                };
                Result::<_, anyhow::Error>::Ok(())
            },
            wf_future_joiner,
            wf_completion_processor,
        )?;

        info!("Polling loops exited");
        if let Some(i) = self.common.worker_interceptor.as_ref() {
            i.on_shutdown(self);
        }
        self.common.worker.shutdown().await;
        debug!("Worker shutdown complete");
        self.app_data = Some(
            Arc::try_unwrap(safe_app_data)
                .map_err(|_| anyhow!("some references of AppData exist on worker shutdown"))?,
        );
        Ok(())
    }

    /// Set a [WorkerInterceptor]
    pub fn set_worker_interceptor(&mut self, interceptor: Box<dyn WorkerInterceptor>) {
        self.common.worker_interceptor = Some(interceptor);
    }

    /// Turns this rust worker into a new worker with all the same workflows and activities
    /// registered, but with a new underlying core worker. Can be used to swap the worker for
    /// a replay worker, change task queues, etc.
    pub fn with_new_core_worker(&mut self, new_core_worker: Arc<dyn CoreWorker>) {
        self.common.worker = new_core_worker;
    }

    /// Returns number of currently cached workflows as understood by the SDK. Importantly, this
    /// is not the same as understood by core, though they *should* always be in sync.
    pub fn cached_workflows(&self) -> usize {
        self.workflow_half.workflows.borrow().len()
    }

    fn split_apart(
        &mut self,
    ) -> (
        &mut CommonWorker,
        &mut WorkflowHalf,
        &mut ActivityHalf,
        &mut Option<AppData>,
    ) {
        (
            &mut self.common,
            &mut self.workflow_half,
            &mut self.activity_half,
            &mut self.app_data,
        )
    }
}

impl WorkflowHalf {
    fn workflow_activation_handler(
        &self,
        common: &CommonWorker,
        shutdown_token: CancellationToken,
        activation: WorkflowActivation,
        completions_tx: &UnboundedSender<WorkflowActivationCompletion>,
    ) -> Result<
        Option<WorkflowFutureHandle<impl Future<Output = Result<WorkflowResult<()>, JoinError>>>>,
        anyhow::Error,
    > {
        let mut res = None;
        let run_id = activation.run_id.clone();

        // If the activation is to start a workflow, create a new workflow driver for it,
        // using the function associated with that workflow id
        if let Some(WorkflowActivationJob {
            variant: Some(Variant::StartWorkflow(sw)),
        }) = activation.jobs.get(0)
        {
            let workflow_type = &sw.workflow_type;
            let wf_fns_borrow = self.workflow_fns.borrow();
            let wf_function = wf_fns_borrow
                .get(workflow_type)
                .ok_or_else(|| anyhow!("Workflow type {workflow_type} not found"))?;

            let (wff, activations) = wf_function.start_workflow(
                common.worker.get_config().namespace.clone(),
                common.task_queue.clone(),
                // NOTE: Don't clone args if this gets ported to be a non-test rust worker
                sw.arguments.clone(),
                completions_tx.clone(),
            );
            let jh = tokio::spawn(async move {
                tokio::select! {
                    r = wff => r,
                    // TODO: This probably shouldn't abort early, as it could cause an in-progress
                    //  complete to abort. Send synthetic remove activation
                    _ = shutdown_token.cancelled() => {
                        Ok(WfExitValue::Evicted)
                    }
                }
            });
            res = Some(WorkflowFutureHandle {
                join_handle: jh,
                run_id: run_id.clone(),
            });
            self.workflows.borrow_mut().insert(
                run_id.clone(),
                WorkflowData {
                    activation_chan: activations,
                },
            );
        }

        // The activation is expected to apply to some workflow we know about. Use it to
        // unblock things and advance the workflow.
        if let Some(dat) = self.workflows.borrow_mut().get_mut(&run_id) {
            dat.activation_chan
                .send(activation)
                .expect("Workflow should exist if we're sending it an activation");
        } else {
            bail!(
                "Got activation {:?} for unknown workflow {}",
                activation,
                run_id
            );
        };

        Ok(res)
    }
}

impl ActivityHalf {
    /// Spawns off a task to handle the provided activity task
    fn activity_task_handler(
        &mut self,
        worker: Arc<dyn CoreWorker>,
        app_data: Arc<AppData>,
        task_queue: String,
        activity: ActivityTask,
    ) -> Result<(), anyhow::Error> {
        match activity.variant {
            Some(activity_task::Variant::Start(start)) => {
                let act_fn = self
                    .activity_fns
                    .get(&start.activity_type)
                    .ok_or_else(|| {
                        anyhow!(
                            "No function registered for activity type {}",
                            start.activity_type
                        )
                    })?
                    .clone();
                let ct = CancellationToken::new();
                let task_token = activity.task_token;
                self.task_tokens_to_cancels
                    .insert(task_token.clone().into(), ct.clone());

                let (ctx, arg) = ActContext::new(
                    worker.clone(),
                    app_data,
                    ct,
                    task_queue,
                    task_token.clone(),
                    start,
                );
                tokio::spawn(async move {
                    let output = AssertUnwindSafe((act_fn.act_func)(ctx, arg))
                        .catch_unwind()
                        .await;
                    let result = match output {
                        Err(e) => ActivityExecutionResult::fail(Failure::application_failure(
                            format!("Activity function panicked: {}", panic_formatter(e)),
                            true,
                        )),
                        Ok(Ok(ActExitValue::Normal(p))) => ActivityExecutionResult::ok(p),
                        Ok(Ok(ActExitValue::WillCompleteAsync)) => {
                            ActivityExecutionResult::will_complete_async()
                        }
                        Ok(Err(err)) => match err.downcast::<ActivityCancelledError>() {
                            Ok(ce) => ActivityExecutionResult::cancel_from_details(ce.details),
                            Err(other_err) => {
                                match other_err.downcast::<NonRetryableActivityError>() {
                                    Ok(nre) => ActivityExecutionResult::fail(
                                        Failure::application_failure_from_error(nre.into(), true),
                                    ),
                                    Err(other_err) => ActivityExecutionResult::fail(
                                        Failure::application_failure_from_error(other_err, false),
                                    ),
                                }
                            }
                        },
                    };
                    worker
                        .complete_activity_task(ActivityTaskCompletion {
                            task_token,
                            result: Some(result),
                        })
                        .await?;
                    Ok::<_, anyhow::Error>(())
                });
            }
            Some(activity_task::Variant::Cancel(_)) => {
                if let Some(ct) = self.task_tokens_to_cancels.get(&activity.task_token.into()) {
                    ct.cancel();
                }
            }
            None => bail!("Undefined activity task variant"),
        }
        Ok(())
    }
}

#[derive(Debug)]
enum UnblockEvent {
    Timer(u32, TimerResult),
    Activity(u32, Box<ActivityResolution>),
    WorkflowStart(u32, Box<ChildWorkflowStartStatus>),
    WorkflowComplete(u32, Box<ChildWorkflowResult>),
    SignalExternal(u32, Option<Failure>),
    CancelExternal(u32, Option<Failure>),
}

/// Result of awaiting on a timer
#[derive(Debug, Copy, Clone)]
pub enum TimerResult {
    /// The timer was cancelled
    Cancelled,
    /// The timer elapsed and fired
    Fired,
}

/// Successful result of sending a signal to an external workflow
pub struct SignalExternalOk;
/// Result of awaiting on sending a signal to an external workflow
pub type SignalExternalWfResult = Result<SignalExternalOk, Failure>;

/// Successful result of sending a cancel request to an external workflow
pub struct CancelExternalOk;
/// Result of awaiting on sending a cancel request to an external workflow
pub type CancelExternalWfResult = Result<CancelExternalOk, Failure>;

trait Unblockable {
    type OtherDat;

    fn unblock(ue: UnblockEvent, od: Self::OtherDat) -> Self;
}

impl Unblockable for TimerResult {
    type OtherDat = ();
    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::Timer(_, result) => result,
            _ => panic!("Invalid unblock event for timer"),
        }
    }
}

impl Unblockable for ActivityResolution {
    type OtherDat = ();
    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::Activity(_, result) => *result,
            _ => panic!("Invalid unblock event for activity"),
        }
    }
}

impl Unblockable for PendingChildWorkflow {
    // Other data here is workflow id
    type OtherDat = ChildWfCommon;
    fn unblock(ue: UnblockEvent, od: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::WorkflowStart(_, result) => Self {
                status: *result,
                common: od,
            },
            _ => panic!("Invalid unblock event for child workflow start"),
        }
    }
}

impl Unblockable for ChildWorkflowResult {
    type OtherDat = ();
    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::WorkflowComplete(_, result) => *result,
            _ => panic!("Invalid unblock event for child workflow complete"),
        }
    }
}

impl Unblockable for SignalExternalWfResult {
    type OtherDat = ();
    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::SignalExternal(_, maybefail) => {
                maybefail.map_or(Ok(SignalExternalOk), Err)
            }
            _ => panic!("Invalid unblock event for signal external workflow result"),
        }
    }
}

impl Unblockable for CancelExternalWfResult {
    type OtherDat = ();
    fn unblock(ue: UnblockEvent, _: Self::OtherDat) -> Self {
        match ue {
            UnblockEvent::CancelExternal(_, maybefail) => {
                maybefail.map_or(Ok(CancelExternalOk), Err)
            }
            _ => panic!("Invalid unblock event for signal external workflow result"),
        }
    }
}

/// Identifier for cancellable operations
#[derive(Debug, Clone)]
pub enum CancellableID {
    /// Timer sequence number
    Timer(u32),
    /// Activity sequence number
    Activity(u32),
    /// Activity sequence number
    LocalActivity(u32),
    /// Start child sequence number
    ChildWorkflow(u32),
    /// Signal workflow
    SignalExternalWorkflow(u32),
    /// An external workflow identifier as may have been created by a started child workflow
    ExternalWorkflow {
        /// Sequence number which will be used for the cancel command
        seqnum: u32,
        /// Identifying information about the workflow to be cancelled
        execution: NamespacedWorkflowExecution,
        /// Set to true if this workflow is a child of the issuing workflow
        only_child: bool,
    },
}

#[derive(derive_more::From)]
#[allow(clippy::large_enum_variant)]
enum RustWfCmd {
    #[from(ignore)]
    Cancel(CancellableID),
    ForceWFTFailure(anyhow::Error),
    NewCmd(CommandCreateRequest),
    NewNonblockingCmd(workflow_command::Variant),
    SubscribeChildWorkflowCompletion(CommandSubscribeChildWorkflowCompletion),
    SubscribeSignal(String, UnboundedSender<SignalData>),
}

struct CommandCreateRequest {
    cmd: workflow_command::Variant,
    unblocker: oneshot::Sender<UnblockEvent>,
}

struct CommandSubscribeChildWorkflowCompletion {
    seq: u32,
    unblocker: oneshot::Sender<UnblockEvent>,
}

type WfFunc = dyn Fn(WfContext) -> BoxFuture<'static, WorkflowResult<()>> + Send + Sync + 'static;

/// The user's async function / workflow code
pub struct WorkflowFunction {
    wf_func: Box<WfFunc>,
}

impl<F, Fut> From<F> for WorkflowFunction
where
    F: Fn(WfContext) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = WorkflowResult<()>> + Send + 'static,
{
    fn from(wf_func: F) -> Self {
        Self::new(wf_func)
    }
}

impl WorkflowFunction {
    /// Build a workflow function from a closure or function pointer which accepts a [WfContext]
    pub fn new<F, Fut>(wf_func: F) -> Self
    where
        F: Fn(WfContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = WorkflowResult<()>> + Send + 'static,
    {
        Self {
            wf_func: Box::new(move |ctx: WfContext| wf_func(ctx).boxed()),
        }
    }
}

/// The result of running a workflow
pub type WorkflowResult<T> = Result<WfExitValue<T>, anyhow::Error>;

/// Workflow functions may return these values when exiting
#[derive(Debug, derive_more::From)]
pub enum WfExitValue<T: Debug> {
    /// Continue the workflow as a new execution
    #[from(ignore)]
    ContinueAsNew(Box<ContinueAsNewWorkflowExecution>),
    /// Confirm the workflow was cancelled (can be automatic in a more advanced iteration)
    #[from(ignore)]
    Cancelled,
    /// The run was evicted
    #[from(ignore)]
    Evicted,
    /// Finish with a result
    Normal(T),
}

impl<T: Debug> WfExitValue<T> {
    /// Construct a [WfExitValue::ContinueAsNew] variant (handles boxing)
    pub fn continue_as_new(can: ContinueAsNewWorkflowExecution) -> Self {
        Self::ContinueAsNew(Box::new(can))
    }
}

/// Activity functions may return these values when exiting
#[derive(derive_more::From)]
pub enum ActExitValue<T: Debug> {
    /// Completion requires an asynchronous callback
    #[from(ignore)]
    WillCompleteAsync,
    /// Finish with a result
    Normal(T),
}

type BoxActFn = Arc<
    dyn Fn(ActContext, Payload) -> BoxFuture<'static, Result<ActExitValue<Payload>, anyhow::Error>>
        + Send
        + Sync,
>;

/// Container for user-defined activity functions
#[derive(Clone)]
pub struct ActivityFunction {
    act_func: BoxActFn,
}

/// Return this error to indicate your activity is cancelling
#[derive(Debug, Default)]
pub struct ActivityCancelledError {
    details: Option<Payload>,
}
impl ActivityCancelledError {
    /// Include some details as part of concluding the activity as cancelled
    pub fn with_details(payload: Payload) -> Self {
        Self {
            details: Some(payload),
        }
    }
}
impl std::error::Error for ActivityCancelledError {}
impl Display for ActivityCancelledError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Activity cancelled")
    }
}

/// Return this error to indicate that your activity non-retryable
/// this is a transparent wrapper around anyhow Error so essentially any type of error
/// could be used here.
///
/// In your activity function. Return something along the lines of:
/// `Err(NonRetryableActivityError(anyhow::anyhow!("This should *not* be retried")).into())`
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct NonRetryableActivityError(pub anyhow::Error);

/// Closures / functions which can be turned into activity functions implement this trait
pub trait IntoActivityFunc<Args, Res, Out> {
    /// Consume the closure or fn pointer and turned it into a boxed activity function
    fn into_activity_fn(self) -> BoxActFn;
}

impl<A, Rf, R, O, F> IntoActivityFunc<A, Rf, O> for F
where
    F: (Fn(ActContext, A) -> Rf) + Sync + Send + 'static,
    A: FromJsonPayloadExt + Send,
    Rf: Future<Output = Result<R, anyhow::Error>> + Send + 'static,
    R: Into<ActExitValue<O>>,
    O: AsJsonPayloadExt + Debug,
{
    fn into_activity_fn(self) -> BoxActFn {
        let wrapper = move |ctx: ActContext, input: Payload| {
            // Some minor gymnastics are required to avoid needing to clone the function
            match A::from_json_payload(&input) {
                Ok(deser) => (self)(ctx, deser)
                    .map(|r| {
                        r.and_then(|r| {
                            let exit_val: ActExitValue<O> = r.into();
                            Ok(match exit_val {
                                ActExitValue::WillCompleteAsync => ActExitValue::WillCompleteAsync,
                                ActExitValue::Normal(x) => {
                                    ActExitValue::Normal(x.as_json_payload()?)
                                }
                            })
                        })
                    })
                    .boxed(),
                Err(e) => async move { Err(e.into()) }.boxed(),
            }
        };
        Arc::new(wrapper)
    }
}

/// Attempts to turn caught panics into something printable
fn panic_formatter(panic: Box<dyn Any>) -> Box<dyn Display> {
    _panic_formatter::<&str>(panic)
}
fn _panic_formatter<T: 'static + PrintablePanicType>(panic: Box<dyn Any>) -> Box<dyn Display> {
    match panic.downcast::<T>() {
        Ok(d) => d,
        Err(orig) => {
            if TypeId::of::<<T as PrintablePanicType>::NextType>()
                == TypeId::of::<EndPrintingAttempts>()
            {
                return Box::new("Couldn't turn panic into a string");
            }
            _panic_formatter::<T::NextType>(orig)
        }
    }
}
trait PrintablePanicType: Display {
    type NextType: PrintablePanicType;
}
impl PrintablePanicType for &str {
    type NextType = String;
}
impl PrintablePanicType for String {
    type NextType = EndPrintingAttempts;
}
struct EndPrintingAttempts {}
impl Display for EndPrintingAttempts {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Will never be printed")
    }
}
impl PrintablePanicType for EndPrintingAttempts {
    type NextType = EndPrintingAttempts;
}
