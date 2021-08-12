//! This module is a rough prototype Rust SDK. It can be used to create closures that look sort of
//! like normal workflow code. It should only depend on things in the core crate that are already
//! publicly exposed.
//!
//! Needs lots of love to be production ready but the basis is there

mod workflow_context;
mod workflow_future;

pub use workflow_context::WfContext;

use crate::{
    protos::coresdk::{
        activity_result::ActivityResult,
        child_workflow::ChildWorkflowResult,
        common::Payload,
        workflow_activation::{
            resolve_child_workflow_execution_start::Status as ChildWorkflowStartStatus,
            wf_activation_job::Variant, WfActivation, WfActivationJob,
        },
        workflow_commands::{workflow_command, ContinueAsNewWorkflowExecution},
    },
    Core,
};
use anyhow::{anyhow, bail};
use dashmap::DashMap;
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use std::{
    fmt::Debug,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::watch;
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        oneshot,
    },
    task::JoinHandle,
};

/// A worker that can poll for and respond to workflow tasks by using [WorkflowFunction]s
pub struct TestRustWorker {
    core: Arc<dyn Core>,
    task_queue: String,
    task_timeout: Option<Duration>,
    /// Maps run id to the driver
    workflows: DashMap<String, UnboundedSender<WfActivation>>,
    /// Maps workflow id to the function for executing workflow runs with that ID
    workflow_fns: DashMap<String, WorkflowFunction>,
    /// Number of live workflows
    incomplete_workflows: Arc<AtomicUsize>,
    /// Handles for each spawned workflow run are inserted here to be cleaned up when all runs
    /// are finished
    join_handles: FuturesUnordered<JoinHandle<WorkflowResult<()>>>,
}
type WfFunc = dyn Fn(WfContext) -> BoxFuture<'static, WorkflowResult<()>> + Send + Sync + 'static;

impl TestRustWorker {
    /// Create a new rust worker using the provided core instance, namespace, and task queue
    pub fn new(core: Arc<dyn Core>, task_queue: String, task_timeout: Option<Duration>) -> Self {
        Self {
            core,
            task_queue,
            task_timeout,
            workflows: Default::default(),
            workflow_fns: Default::default(),
            incomplete_workflows: Arc::new(AtomicUsize::new(0)),
            join_handles: FuturesUnordered::new(),
        }
    }

    /// Create a workflow, asking the server to start it with the provided workflow ID and using the
    /// provided workflow function.
    /// Increments the expected Workflow run count.
    pub async fn submit_wf(
        &self,
        workflow_id: String,
        workflow_type: String,
        input: Vec<Payload>,
    ) -> Result<(), tonic::Status> {
        self.core
            .server_gateway()
            .start_workflow(
                input,
                self.task_queue.clone(),
                workflow_id,
                workflow_type,
                self.task_timeout,
            )
            .await?;

        self.incr_expected_run_count(1);
        Ok(())
    }

    /// Register a Workflow function to invoke when Worker is requested to run `workflow_type`
    pub fn register_wf<F: Into<WorkflowFunction>>(&self, workflow_type: String, wf_function: F) {
        self.workflow_fns.insert(workflow_type, wf_function.into());
    }

    /// Increment the expected Workflow run count on this Worker. The Worker tracks the run count
    /// and will resolve `run_until_done` when it goes down to 0.
    /// You do not have to increment if scheduled a Workflow with `submit_wf`.
    pub fn incr_expected_run_count(&self, count: usize) {
        self.incomplete_workflows.fetch_add(count, Ordering::SeqCst);
    }

    /// Drives all workflows until they have all finished, repeatedly polls server to fetch work
    /// for them.
    pub async fn run_until_done(self) -> Result<(), anyhow::Error> {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let poller = async move {
            let (completions_tx, mut completions_rx) = unbounded_channel();
            loop {
                let activation = self.core.poll_workflow_task(&self.task_queue).await?;

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

                    // NOTE: Don't clone args if this gets ported to be a non-test rust worker
                    let (wff, activations) =
                        wf_function.start_workflow(sw.arguments.clone(), completions_tx.clone());
                    let mut shutdown_rx = shutdown_rx.clone();
                    let jh = tokio::spawn(async move {
                        tokio::select! {
                            r = wff => r,
                            _ = shutdown_rx.changed() => Ok(WfExitValue::Evicted)
                        }
                    });
                    self.workflows
                        .insert(activation.run_id.clone(), activations);
                    self.join_handles.push(jh);
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
                    self.incomplete_workflows.fetch_sub(1, Ordering::SeqCst);
                }
                self.core.complete_workflow_task(completion).await.unwrap();
                if self.incomplete_workflows.load(Ordering::SeqCst) == 0 {
                    break Ok(self);
                }
            }
        };

        let mut myself = poller.await?;

        // Die rebel scum
        let _ = shutdown_tx.send(true);
        while let Some(h) = myself.join_handles.next().await {
            h??;
        }
        Ok(())
    }
}

#[derive(Debug)]
enum UnblockEvent {
    Timer(String),
    Activity(String, Box<ActivityResult>),
    WorkflowStart(String, Box<ChildWorkflowStartStatus>),
    WorkflowComplete(String, Box<ChildWorkflowResult>),
}

#[derive(derive_more::From)]
#[allow(clippy::large_enum_variant)]
enum RustWfCmd {
    #[from(ignore)]
    CancelTimer(String),
    #[from(ignore)]
    CancelActivity(String),
    #[from(ignore)]
    CancelChildWorkflow(String),
    ForceWFTFailure(anyhow::Error),
    NewCmd(CommandCreateRequest),
    NewNonblockingCmd(workflow_command::Variant),
    SubscribeChildWorkflowCompletion(CommandSubscribeChildWorkflowCompletion),
}

struct CommandCreateRequest {
    cmd: workflow_command::Variant,
    unblocker: oneshot::Sender<UnblockEvent>,
}

struct CommandSubscribeChildWorkflowCompletion {
    workflow_id: String,
    unblocker: oneshot::Sender<UnblockEvent>,
}

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
    ContinueAsNew(ContinueAsNewWorkflowExecution),
    /// Confirm the workflow was cancelled (can be automatic in a more advanced iteration)
    #[from(ignore)]
    Cancelled,
    /// TODO: Will go away once we have eviction confirmation
    #[from(ignore)]
    Evicted,
    /// Finish with a result
    Normal(T),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        protos::coresdk::workflow_commands::StartTimer,
        test_help::{build_fake_core, canned_histories, DEFAULT_WORKFLOW_TYPE, TEST_Q},
    };

    pub async fn timer_wf(mut ctx: WfContext) -> WorkflowResult<()> {
        let timer = StartTimer {
            timer_id: "fake_timer".to_string(),
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
        };
        ctx.timer(timer).await;
        Ok(().into())
    }

    #[tokio::test]
    async fn new_test_wf_core() {
        let wf_id = "fakeid";
        let wf_type = DEFAULT_WORKFLOW_TYPE;
        let t = canned_histories::single_timer("fake_timer");
        let core = build_fake_core(wf_id, t, [2]);
        let worker = TestRustWorker::new(Arc::new(core), TEST_Q.to_string(), None);

        worker.register_wf(wf_type.to_owned(), timer_wf);
        worker
            .submit_wf(wf_id.to_owned(), wf_type.to_owned(), vec![])
            .await
            .unwrap();
        worker.run_until_done().await.unwrap();
    }
}
