//! This module is essentially a rough prototype Rust SDK. It can be used to create closures that
//! look sort of like normal workflow code. It should only depend on things in the core crate that
//! are already publicly exposed.
//!
//! As it stands I do *not* like this design. Way too easy to create races and deadlocks. Needs
//! a rethink.

use crate::{
    protos::coresdk::{
        activity_result::ActivityResult,
        common::{Payload, UserCodeFailure},
        workflow_activation::{
            wf_activation_job::Variant, FireTimer, ResolveActivity, WfActivation, WfActivationJob,
        },
        workflow_commands::{
            workflow_command, CancelTimer, CancelWorkflowExecution, CompleteWorkflowExecution,
            ContinueAsNewWorkflowExecution, FailWorkflowExecution, RequestCancelActivity,
            ScheduleActivity, StartTimer,
        },
        workflow_completion::WfActivationCompletion,
    },
    workflow::CommandID,
    Core,
};
use anyhow::{anyhow, bail};
use crossbeam::channel::{Receiver, Sender};
use dashmap::DashMap;
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot, watch,
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
    pub async fn submit_wf<F: Into<WorkflowFunction>>(
        &self,
        input: Vec<Payload>,
        workflow_id: String,
        wf_function: F,
    ) -> Result<(), tonic::Status> {
        self.core
            .server_gateway()
            .start_workflow(
                input,
                self.task_queue.clone(),
                workflow_id.clone(),
                workflow_id.clone(),
                self.task_timeout,
            )
            .await?;

        self.workflow_fns.insert(workflow_id, wf_function.into());
        self.incomplete_workflows.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Drives all workflows until they have all finished, repeatedly polls server to fetch work
    /// for them.
    pub async fn run_until_done(self) -> Result<(), anyhow::Error> {
        let poller = async move {
            let (completions_tx, mut completions_rx) = unbounded_channel();
            loop {
                let activation = self.core.poll_workflow_task(&self.task_queue).await?;
                dbg!(&activation);

                // If the activation is to start a workflow, create a new workflow driver for it,
                // using the function associated with that workflow id
                if let [WfActivationJob {
                    variant: Some(Variant::StartWorkflow(sw)),
                }] = activation.jobs.as_slice()
                {
                    let wf_function = self
                        .workflow_fns
                        .get(&sw.workflow_id)
                        .ok_or_else(|| anyhow!("Workflow id not found"))?;

                    // NOTE: Don't clone args if this gets ported to be a non-test rust worker
                    let (wff, activations) =
                        wf_function.start_workflow(sw.arguments.clone(), completions_tx.clone());
                    let live_wfs = self.incomplete_workflows.clone();
                    let jh = tokio::spawn(async move {
                        let res = dbg!(wff.await);
                        if !matches!(&res, Ok(WfExitValue::Evicted)) {
                            live_wfs.fetch_sub(1, Ordering::SeqCst);
                        }
                        res
                    });
                    self.workflows
                        .insert(activation.run_id.clone(), activations);
                    self.join_handles.push(jh);
                }
                // The activation is expected to apply to some workflow we know about. Use it to
                // unblock things and advance the workflow.
                if let Some(tx) = self.workflows.get_mut(&activation.run_id) {
                    dbg!("Sending activation");
                    tx.send(activation).unwrap();
                } else {
                    bail!("Got activation for unknown workflow");
                }
                dbg!("Activation sent");

                let completion = completions_rx.recv().await.expect("No workflows left?");
                dbg!(&completion);
                self.core.complete_workflow_task(completion).await.unwrap();
                dbg!("Done completing");
                if self.incomplete_workflows.load(Ordering::SeqCst) == 0 {
                    break Ok(self);
                }
            }
        };

        let mut myself = poller.await?;
        while let Some(h) = myself.join_handles.next().await {
            h??;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum UnblockEvent {
    Timer(String),
    Activity { id: String, result: ActivityResult },
}

/// Used within workflows to issue commands, get info, etc.
pub struct WfContext {
    chan: Sender<RustWfCmd>,
    args: Vec<Payload>,
    am_cancelled: watch::Receiver<bool>,
}

impl WfContext {
    /// Create a new wf context, returning the context itself, the shared cache for blocked
    /// commands, and a receiver which outputs commands sent from the workflow.
    fn new(args: Vec<Payload>, am_cancelled: watch::Receiver<bool>) -> (Self, Receiver<RustWfCmd>) {
        // We need to use a normal std channel since our receiving side is non-async
        let (chan, rx) = crossbeam::channel::unbounded();
        (
            Self {
                chan,
                args,
                am_cancelled,
            },
            rx,
        )
    }

    /// Get the arguments provided to the workflow upon execution start
    pub fn get_args(&self) -> &[Payload] {
        self.args.as_slice()
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
    pub fn timer(&mut self, a: StartTimer) -> impl Future<Output = ()> {
        let (cmd, unblocker) = WFCommandFut::new();
        self.send(
            CommandCreateRequest {
                cmd: a.into(),
                unblocker,
            }
            .into(),
        );
        cmd.map(|ue| {
            if let UnblockEvent::Timer(_) = ue {
            } else {
                panic!("Wrong unblock event")
            }
        })
    }

    /// Request to run an activity
    pub fn activity(&mut self, a: ScheduleActivity) -> impl Future<Output = ActivityResult> {
        let (cmd, unblocker) = WFCommandFut::new();
        self.send(
            CommandCreateRequest {
                cmd: a.into(),
                unblocker,
            }
            .into(),
        );
        cmd.map(|ue| {
            if let UnblockEvent::Activity { result, .. } = ue {
                result
            } else {
                panic!("Wrong unblock event")
            }
        })
    }

    /// Cancel a timer
    pub fn cancel_timer(&self, timer_id: &str) {
        self.send(RustWfCmd::CancelTimer(timer_id.to_string()))
    }

    /// Cancel activity
    pub fn cancel_activity(&self, activity_id: &str) {
        self.send(RustWfCmd::CancelActivity(activity_id.to_string()))
    }

    /// Force a workflow task timeout by waiting too long and gumming up the entire runtime
    pub fn force_timeout(&self, by_waiting_for: Duration) {
        self.send(RustWfCmd::ForceTimeout(by_waiting_for))
    }

    fn send(&self, c: RustWfCmd) {
        self.chan.send(c).unwrap();
    }
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
        // TODO: Output should be result
        Fut: Future<Output = WorkflowResult<()>> + Send + 'static,
    {
        Self {
            wf_func: Box::new(move |ctx: WfContext| wf_func(ctx).boxed()),
        }
    }

    pub(crate) fn start_workflow(
        &self,
        args: Vec<Payload>,
        outgoing_completions: UnboundedSender<WfActivationCompletion>,
    ) -> (WorkflowFuture, UnboundedSender<WfActivation>) {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (wf_context, cmd_receiver) = WfContext::new(args, cancel_rx);
        let (tx, incoming_activations) = unbounded_channel();
        (
            WorkflowFuture {
                inner: (self.wf_func)(wf_context).boxed(),
                incoming_commands: cmd_receiver,
                outgoing_completions,
                incoming_activations,
                command_status: Default::default(),
                cancel_sender: cancel_tx,
            },
            tx,
        )
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

pub(crate) struct WorkflowFuture {
    /// Future produced by calling the workflow function
    inner: BoxFuture<'static, WorkflowResult<()>>,
    /// Commands produced inside user's wf code
    incoming_commands: Receiver<RustWfCmd>,
    /// Once blocked or the workflow has finished or errored out, the result is sent here
    outgoing_completions: UnboundedSender<WfActivationCompletion>,
    /// Activations from core TODO: Could be bounded to 1?
    incoming_activations: UnboundedReceiver<WfActivation>,
    /// Commands by ID -> blocked status
    command_status: HashMap<CommandID, WFCommandFutInfo>,
    /// Use to notify workflow code of cancellation
    cancel_sender: watch::Sender<bool>,
}

impl WorkflowFuture {
    fn unblock(&mut self, event: UnblockEvent) {
        let cmd_id = match &event {
            UnblockEvent::Timer(t) => CommandID::Timer(t.clone()),
            UnblockEvent::Activity { id, .. } => CommandID::Activity(id.clone()),
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
        loop {
            // WF must always receive an activation first before responding with commands
            let activation = match self.incoming_activations.poll_recv(cx) {
                Poll::Ready(a) => a.expect("activation channel not dropped"),
                Poll::Pending => return Poll::Pending,
            };

            dbg!("Got activation", &activation);
            let run_id = activation.run_id;

            for WfActivationJob { variant } in activation.jobs {
                if let Some(v) = variant {
                    match v {
                        Variant::StartWorkflow(_) => {
                            // TODO: Can assign randomness seed
                        }
                        Variant::FireTimer(FireTimer { timer_id }) => {
                            self.unblock(UnblockEvent::Timer(timer_id))
                        }
                        Variant::ResolveActivity(ResolveActivity {
                            activity_id,
                            result,
                        }) => self.unblock(UnblockEvent::Activity {
                            id: activity_id,
                            result: result.expect("Activity must have result"),
                        }),
                        Variant::UpdateRandomSeed(_) => {}
                        Variant::QueryWorkflow(_) => {}
                        Variant::CancelWorkflow(_) => {
                            // TODO: Cancel pending futures, etc
                            self.cancel_sender
                                .send(true)
                                .expect("Cancel rx not dropped");
                        }
                        Variant::SignalWorkflow(_) => {}
                        Variant::RemoveFromCache(_) => {
                            // Will make more sense once we have completions for evictions
                            self.outgoing_completions
                                .send(WfActivationCompletion::from_cmds(vec![], run_id))
                                .expect("Completion channel intact");
                            return Ok(WfExitValue::Evicted).into();
                        }
                    }
                } else {
                    return Err(anyhow!("Empty activation job variant")).into();
                }
            }
            dbg!("Done activation");

            // TODO: Trap panics in wf code here?
            let mut res = self.inner.poll_unpin(cx);

            let mut activation_cmds = vec![];
            while let Ok(cmd) = self.incoming_commands.try_recv() {
                match cmd {
                    RustWfCmd::CancelTimer(tid) => {
                        activation_cmds.push(workflow_command::Variant::CancelTimer(CancelTimer {
                            timer_id: tid.clone(),
                        }));
                        self.unblock(UnblockEvent::Timer(tid));
                        // Re-poll wf future since a timer is now unblocked
                        res = self.inner.poll_unpin(cx);
                    }
                    RustWfCmd::CancelActivity(aid) => {
                        activation_cmds.push(workflow_command::Variant::RequestCancelActivity(
                            RequestCancelActivity {
                                activity_id: aid.clone(),
                            },
                        ));
                    }
                    RustWfCmd::NewCmd(cmd) => {
                        activation_cmds.push(cmd.cmd.clone());

                        let command_id = match cmd.cmd {
                            workflow_command::Variant::StartTimer(StartTimer {
                                timer_id, ..
                            }) => CommandID::Timer(timer_id),
                            workflow_command::Variant::ScheduleActivity(ScheduleActivity {
                                activity_id,
                                ..
                            }) => CommandID::Activity(activity_id),
                            _ => unimplemented!("Command type not implemented"),
                        };
                        self.command_status.insert(
                            command_id,
                            WFCommandFutInfo {
                                unblocker: cmd.unblocker,
                            },
                        );
                    }
                    RustWfCmd::ForceTimeout(dur) => {
                        // This is nasty
                        std::thread::sleep(dur);
                    }
                }
            }

            let do_finish = if let Poll::Ready(res) = res {
                // TODO: Auto reply with cancel when cancelled
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
                                failure: Some(UserCodeFailure {
                                    message: e.to_string(),
                                    ..Default::default()
                                }),
                            },
                        ));
                    }
                }
                true
            } else {
                false
            };
            if activation_cmds.is_empty() {
                panic!(
                    "Workflow did not produce any commands or exit, but awaited. This \
                     means it will deadlock. You probably awaited on a non-WfContext future."
                );
            }
            self.outgoing_completions
                .send(WfActivationCompletion::from_cmds(activation_cmds, run_id))
                .expect("Completion channel intact");
            if do_finish {
                return Poll::Ready(Ok(().into()));
            }
        }
    }
}

#[derive(derive_more::From)]
enum RustWfCmd {
    #[from(ignore)]
    CancelTimer(String),
    #[from(ignore)]
    CancelActivity(String),
    #[from(ignore)]
    ForceTimeout(Duration),
    NewCmd(CommandCreateRequest),
}

struct CommandCreateRequest {
    cmd: workflow_command::Variant,
    unblocker: oneshot::Sender<UnblockEvent>,
}

struct WFCommandFutInfo {
    unblocker: oneshot::Sender<UnblockEvent>,
}

struct WFCommandFut {
    result_rx: oneshot::Receiver<UnblockEvent>,
}

impl WFCommandFut {
    fn new() -> (Self, oneshot::Sender<UnblockEvent>) {
        let (tx, rx) = oneshot::channel();
        (Self { result_rx: rx }, tx)
    }
}

impl Future for WFCommandFut {
    type Output = UnblockEvent;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.result_rx.poll_unpin(cx).map(|x| x.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_help::{build_fake_core, canned_histories, TEST_Q};

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
        let t = canned_histories::single_timer("fake_timer");
        let core = build_fake_core(wf_id, t, [2]);
        let worker = TestRustWorker::new(Arc::new(core.inner), TEST_Q.to_string(), None);

        worker
            .submit_wf(vec![], wf_id.to_string(), WorkflowFunction::new(timer_wf))
            .await
            .unwrap();
        worker.run_until_done().await.unwrap();
    }
}
