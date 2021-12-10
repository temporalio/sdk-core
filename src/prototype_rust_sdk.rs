//! This module is a rough prototype Rust SDK. It can be used to create closures that look sort of
//! like normal workflow code. It should only depend on things in the core crate that are already
//! publicly exposed.
//!
//! Needs lots of love to be production ready but the basis is there

mod conversions;
mod payload_converter;
mod workflow_context;
mod workflow_future;

pub use workflow_context::{
    ActivityOptions, CancellableFuture, ChildWorkflow, ChildWorkflowOptions, LocalActivityOptions,
    WfContext,
};

use crate::{
    prototype_rust_sdk::workflow_context::{ChildWfCommon, PendingChildWorkflow},
    Core, PollActivityError, PollWfError,
};
use anyhow::{anyhow, bail};
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{ActivityExecutionResult, ActivityResolution},
        activity_task::{activity_task, ActivityTask},
        child_workflow::ChildWorkflowResult,
        common::{NamespacedWorkflowExecution, Payload},
        workflow_activation::{
            resolve_child_workflow_execution_start::Status as ChildWorkflowStartStatus,
            wf_activation_job::Variant, WfActivation, WfActivationJob,
        },
        workflow_commands::{workflow_command, ContinueAsNewWorkflowExecution},
        workflow_completion::WfActivationCompletion,
        ActivityTaskCompletion, AsJsonPayloadExt, FromJsonPayloadExt,
    },
    temporal::api::failure::v1::Failure,
};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot, watch,
        watch::Receiver,
    },
    task::JoinError,
};

/// A worker that can poll for and respond to workflow tasks by using [WorkflowFunction]s
pub struct TestRustWorker {
    core: Arc<dyn Core>,
    task_queue: String,
    task_timeout: Option<Duration>,
    workflow_half: WorkflowHalf,
    activity_half: ActivityHalf,
}

struct WorkflowHalf {
    /// Maps run id to the driver
    workflows: HashMap<String, UnboundedSender<WfActivation>>,
    /// Maps workflow type to the function for executing workflow runs with that ID
    workflow_fns: HashMap<String, WorkflowFunction>,
    /// Number of live workflows
    incomplete_workflows: Arc<AtomicUsize>,
    /// Handles for each spawned workflow run are inserted here to be cleaned up when all runs
    /// are finished
    join_handles: FuturesUnordered<BoxFuture<'static, Result<WorkflowResult<()>, JoinError>>>,
}

struct ActivityHalf {
    /// Maps activity type to the function for executing activities of that type
    activity_fns: HashMap<String, ActivityFunction>,
}

impl TestRustWorker {
    /// Create a new rust worker
    pub fn new(core: Arc<dyn Core>, task_queue: String, task_timeout: Option<Duration>) -> Self {
        Self {
            core,
            task_queue,
            task_timeout,
            workflow_half: WorkflowHalf {
                workflows: Default::default(),
                workflow_fns: Default::default(),
                incomplete_workflows: Arc::new(AtomicUsize::new(0)),
                join_handles: FuturesUnordered::new(),
            },
            activity_half: ActivityHalf {
                activity_fns: Default::default(),
            },
        }
    }

    /// Create a workflow, asking the server to start it with the provided workflow ID and using the
    /// provided workflow function.
    ///
    /// Increments the expected Workflow run count.
    ///
    /// Returns the run id of the started workflow
    pub async fn submit_wf(
        &self,
        workflow_id: impl Into<String>,
        workflow_type: impl Into<String>,
        input: Vec<Payload>,
    ) -> Result<String, tonic::Status> {
        let res = self
            .core
            .server_gateway()
            .start_workflow(
                input,
                self.task_queue.clone(),
                workflow_id.into(),
                workflow_type.into(),
                self.task_timeout,
            )
            .await?;

        self.incr_expected_run_count(1);
        Ok(res.run_id)
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
            .insert(workflow_type.into(), wf_function.into());
    }

    /// Register an Activity function to invoke when the Worker is asked to run an activity of
    /// `activity_type`
    pub fn register_activity<A, R>(
        &mut self,
        activity_type: impl Into<String>,
        act_function: impl IntoActivityFunc<A, R>,
    ) {
        self.activity_half.activity_fns.insert(
            activity_type.into(),
            ActivityFunction {
                act_func: act_function.into_activity_fn(),
            },
        );
    }

    /// Increment the expected Workflow run count on this Worker. The Worker tracks the run count
    /// and will resolve `run_until_done` when it goes down to 0.
    /// You do not have to increment if scheduled a Workflow with `submit_wf`.
    pub fn incr_expected_run_count(&self, count: usize) {
        self.workflow_half
            .incomplete_workflows
            .fetch_add(count, Ordering::SeqCst);
    }

    /// Drives all workflows & activities until they have all finished, repeatedly polls server to
    /// fetch work for them.
    pub async fn run_until_done(mut self) -> Result<(), anyhow::Error> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let pollers = async move {
            let (core, task_q, wf_half, act_half) = self.split_apart();
            let (completions_tx, mut completions_rx) = unbounded_channel();
            let (wf_poll_res, act_poll_res) = tokio::join!(
                // Workflow polling loop
                async {
                    loop {
                        let activation = match core.poll_workflow_activation(task_q).await {
                            Err(PollWfError::ShutDown) => {
                                break Result::<_, anyhow::Error>::Ok(());
                            }
                            o => o?,
                        };
                        wf_half
                            .workflow_activation_handler(
                                core,
                                task_q,
                                &shutdown_rx,
                                &completions_tx,
                                &mut completions_rx,
                                activation,
                            )
                            .await?;
                        if wf_half.incomplete_workflows.load(Ordering::SeqCst) == 0 {
                            // Die rebel scum - evict all workflows (which are complete now),
                            // and turn off activity polling.
                            let _ = shutdown_tx.send(true);
                            break Result::<_, anyhow::Error>::Ok(());
                        }
                    }
                },
                // Only poll on the activity queue if activity functions have been registered. This
                // makes tests which use mocks dramatically more manageable.
                async {
                    let mut shutdown_rx = shutdown_rx.clone();
                    if !act_half.activity_fns.is_empty() {
                        loop {
                            tokio::select! {
                                activity = core.poll_activity_task(task_q) => {
                                    if matches!(activity, Err(PollActivityError::ShutDown)) {
                                        break;
                                    }
                                    act_half.activity_task_handler(core, task_q,
                                                                   activity?).await?;
                                },
                                _ = shutdown_rx.changed() => { break }
                            }
                        }
                    };
                    Result::<_, anyhow::Error>::Ok(())
                }
            );
            wf_poll_res?;
            act_poll_res?;
            Result::<_, anyhow::Error>::Ok(self)
        };

        let mut myself = pollers.await?;
        while let Some(h) = myself.workflow_half.join_handles.next().await {
            h??;
        }
        myself.core.shutdown().await;
        Ok(())
    }

    fn split_apart(&mut self) -> (&dyn Core, &str, &mut WorkflowHalf, &mut ActivityHalf) {
        (
            self.core.as_ref(),
            &self.task_queue,
            &mut self.workflow_half,
            &mut self.activity_half,
        )
    }
}

impl WorkflowHalf {
    async fn workflow_activation_handler(
        &mut self,
        core: &dyn Core,
        task_queue: &str,
        shutdown_rx: &Receiver<bool>,
        completions_tx: &UnboundedSender<WfActivationCompletion>,
        completions_rx: &mut UnboundedReceiver<WfActivationCompletion>,
        activation: WfActivation,
    ) -> Result<(), anyhow::Error> {
        // If the activation is to start a workflow, create a new workflow driver for it,
        // using the function associated with that workflow id
        if let Some(WfActivationJob {
            variant: Some(Variant::StartWorkflow(sw)),
        }) = activation.jobs.get(0)
        {
            let wf_function = self
                .workflow_fns
                .get(&sw.workflow_type)
                .ok_or_else(|| anyhow!("Workflow type not found"))?;

            let (wff, activations) = wf_function.start_workflow(
                core.get_init_options().gateway_opts.namespace.clone(),
                task_queue.to_string(),
                // NOTE: Don't clone args if this gets ported to be a non-test rust worker
                sw.arguments.clone(),
                completions_tx.clone(),
            );
            let mut shutdown_rx = shutdown_rx.clone();
            let jh = tokio::spawn(async move {
                tokio::select! {
                    r = wff => r,
                    _ = shutdown_rx.changed() => Ok(WfExitValue::Evicted)
                }
            });
            self.workflows
                .insert(activation.run_id.clone(), activations);
            self.join_handles.push(jh.boxed());
        }

        // The activation is expected to apply to some workflow we know about. Use it to
        // unblock things and advance the workflow.
        if let Some(tx) = self.workflows.get_mut(&activation.run_id) {
            tx.send(activation)
                .expect("Workflow should exist if we're sending it an activation");
        } else {
            bail!("Got activation for unknown workflow");
        };

        let completion = completions_rx.recv().await.expect("No workflows left?");
        if completion.has_execution_ending() {
            debug!("Workflow {} says it's finishing", &completion.run_id);
            self.incomplete_workflows.fetch_sub(1, Ordering::SeqCst);
        }
        core.complete_workflow_activation(completion).await?;
        Ok(())
    }
}

impl ActivityHalf {
    async fn activity_task_handler(
        &mut self,
        core: &dyn Core,
        task_queue: &str,
        activity: ActivityTask,
    ) -> Result<(), anyhow::Error> {
        // TODO: handle cancels, etc.
        match activity.variant {
            Some(activity_task::Variant::Start(start)) => {
                let act_fn = self.activity_fns.get(&start.activity_type).ok_or_else(|| {
                    anyhow!(
                        "No function registered for activity type {}",
                        start.activity_type
                    )
                })?;
                let mut inputs = start.input;
                let arg = inputs.pop().unwrap_or_default();
                let output = (&act_fn.act_func)(arg).await;
                let result = match output {
                    Ok(res) => ActivityExecutionResult::ok(res),
                    Err(err) => ActivityExecutionResult::fail(err.into()),
                };
                core.complete_activity_task(ActivityTaskCompletion {
                    task_token: activity.task_token,
                    task_queue: task_queue.to_string(),
                    result: Some(result),
                })
                .await?;
            }
            Some(activity_task::Variant::Cancel(_)) => {
                unimplemented!("Activity cancels not implemented yet in prototype sdk")
            }
            None => bail!("Undefined activity task variant"),
        }
        Ok(())
    }
}

#[derive(Debug)]
enum UnblockEvent {
    Timer(u32),
    Activity(u32, Box<ActivityResolution>),
    WorkflowStart(u32, Box<ChildWorkflowStartStatus>),
    WorkflowComplete(u32, Box<ChildWorkflowResult>),
    SignalExternal(u32, Option<Failure>),
    CancelExternal(u32, Option<Failure>),
}

/// Result of awaiting on a timer
pub struct TimerResult;

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
            UnblockEvent::Timer(_) => TimerResult,
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
    SubscribeSignal(String, UnboundedSender<Vec<Payload>>),
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

type BoxActFn = Box<dyn Fn(Payload) -> BoxFuture<'static, Result<Payload, anyhow::Error>>>;
/// Container for user-defined activity functions
pub struct ActivityFunction {
    act_func: BoxActFn,
}

/// Closures / functions which can be turned into activity functions implement this trait
pub trait IntoActivityFunc<Args, Res> {
    /// Consume the closure or fn pointer and turned it into a boxed activity function
    fn into_activity_fn(self) -> BoxActFn;
}

impl<A, Rf, R, F> IntoActivityFunc<A, Rf> for F
where
    F: (Fn(A) -> Rf) + Sync + Send + 'static,
    A: FromJsonPayloadExt + Send,
    Rf: Future<Output = Result<R, anyhow::Error>> + Send + 'static,
    R: AsJsonPayloadExt,
{
    fn into_activity_fn(self) -> BoxActFn {
        let wrapper = move |input: Payload| {
            // Some minor gymnastics are required to avoid needing to clone the function
            match A::from_json_payload(&input) {
                Ok(deser) => (self)(deser)
                    .map(|r| r.map(|r| r.as_json_payload())?)
                    .boxed(),
                Err(e) => async move { Err(e.into()) }.boxed(),
            }
        };
        Box::new(wrapper)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_help::{build_fake_core, canned_histories, DEFAULT_WORKFLOW_TYPE, TEST_Q};

    pub async fn timer_wf(ctx: WfContext) -> WorkflowResult<()> {
        ctx.timer(Duration::from_secs(1)).await;
        Ok(().into())
    }

    #[tokio::test]
    async fn new_test_wf_core() {
        let wf_id = "fakeid";
        let wf_type = DEFAULT_WORKFLOW_TYPE;
        let t = canned_histories::single_timer("1");
        let core = build_fake_core(wf_id, t, [2]);
        let mut worker = TestRustWorker::new(Arc::new(core), TEST_Q.to_string(), None);

        worker.register_wf(wf_type.to_owned(), timer_wf);
        worker
            .submit_wf(wf_id.to_owned(), wf_type.to_owned(), vec![])
            .await
            .unwrap();
        worker.run_until_done().await.unwrap();
    }
}
