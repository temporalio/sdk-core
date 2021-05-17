//! This module is essentially a rough prototype Rust SDK. It can be used to create closures that
//! look sort of like normal workflow code. It should only depend on things in the core crate that
//! are already publicly exposed.

use crate::{
    protos::coresdk::{
        activity_result::ActivityResult,
        workflow_activation::{
            wf_activation_job::Variant, FireTimer, ResolveActivity, WfActivation, WfActivationJob,
        },
        workflow_commands::{
            CancelTimer, CompleteWorkflowExecution, RequestCancelActivity, ScheduleActivity,
            StartTimer, WorkflowCommand,
        },
    },
    workflow::CommandID,
    Core, IntoCompletion,
};
use anyhow::bail;
use crossbeam::channel::{Receiver, Sender};
use dashmap::DashMap;
use futures::{stream::FuturesUnordered, StreamExt};
use parking_lot::{Condvar, Mutex};
use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};
use tokio::{
    runtime::Runtime,
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        oneshot,
    },
    task::{JoinError, JoinHandle},
};

/// A worker that can poll for and respond to workflow tasks by using [TestWorkflowDriver]s
pub struct TestRustWorker {
    core: Arc<dyn Core>,
    namespace: String,
    task_queue: String,
    // Maps run id to the driver
    workflows: DashMap<String, UnboundedSender<WfActivation>>,
    join_handles: FuturesUnordered<JoinHandle<Result<(), anyhow::Error>>>,
}

impl TestRustWorker {
    /// Create a new rust worker using the provided core instance, namespace, and task queue
    pub fn new(core: Arc<dyn Core>, namespace: String, task_queue: String) -> Self {
        Self {
            core,
            namespace,
            task_queue,
            workflows: Default::default(),
            join_handles: FuturesUnordered::new(),
        }
    }

    /// Create a workflow, asking the server to start it with the provided workflow ID and using the
    /// provided [TestWorkflowDriver] as the workflow code.
    pub async fn submit_wf(
        &self,
        workflow_id: String,
        mut twd: TestWorkflowDriver,
    ) -> Result<(), tonic::Status> {
        let res = self
            .core
            .server_gateway()
            .start_workflow(
                self.namespace.clone(),
                self.task_queue.clone(),
                workflow_id.clone(),
                workflow_id.clone(),
                None,
            )
            .await?;
        let (tx, mut rx) = unbounded_channel::<WfActivation>();
        let core = self.core.clone();
        let jh = tokio::spawn(async move {
            while let Some(activation) = rx.recv().await {
                for WfActivationJob { variant } in activation.jobs {
                    if let Some(v) = variant {
                        match v {
                            Variant::StartWorkflow(_) => {}
                            Variant::FireTimer(FireTimer { timer_id }) => {
                                twd.unblock(UnblockEvent::Timer(timer_id))
                            }
                            Variant::ResolveActivity(ResolveActivity {
                                activity_id,
                                result,
                            }) => twd.unblock(UnblockEvent::Activity {
                                id: activity_id,
                                result: result.expect("Activity must have result"),
                            }),
                            Variant::UpdateRandomSeed(_) => {}
                            Variant::QueryWorkflow(_) => {}
                            Variant::CancelWorkflow(_) => {}
                            Variant::SignalWorkflow(_) => {}
                            Variant::RemoveFromCache(_) => {}
                        }
                    } else {
                        bail!("Empty activation job variant");
                    }
                }

                // After the workflow has been advanced, grab any outgoing commands and send them to
                // the server.

                // Since waiting until the iteration is done may block and isn't async, we need
                // to use block_in_place here.
                let wf_is_done = tokio::task::block_in_place(|| twd.wait_until_wf_iteration_done());
                let outgoing = twd.drain_pending_commands();
                core.complete_workflow_task(outgoing.into_completion(activation.task_token))
                    .await?;
                if wf_is_done {
                    break;
                }
            }
            Ok(())
        });
        self.workflows.insert(res.run_id, tx);
        self.join_handles.push(jh);
        Ok(())
    }

    /// Drives all workflows until they have all finished, repeatedly polls server to fetch work
    /// for them.
    pub async fn run_until_done(self) -> Result<(), anyhow::Error> {
        // Need to deconstruct self
        let mut handles = self.join_handles;
        let core = self.core;
        let workflows = self.workflows;
        let tq = self.task_queue;

        let poller = async {
            loop {
                let activation = core.poll_workflow_task(&tq).await?;
                // The activation is expected to apply to some workflow we know about. Use it to
                // unblock things and advance the workflow.
                if let Some(tx) = workflows.get_mut(&activation.run_id) {
                    tx.send(activation).unwrap();
                } else {
                    bail!("Got activation for unknown workflow");
                }
            }
        };
        let shutdown_checker = async {
            // Because we consume self, it is impossible that any new workflows will be added while
            // we are draining here.
            while handles.next().await.is_some() {}
        };
        tokio::select!(
            r = poller => r,
            _ = shutdown_checker => Ok(())
        )
    }
}

pub(crate) enum UnblockEvent {
    Timer(String),
    Activity { id: String, result: ActivityResult },
}

/// Allows implementing workflows in a reasonably natural looking way in Rust
pub struct TestWorkflowDriver {
    join_handle: Option<JoinHandle<()>>,
    commands_from_wf: Receiver<WorkflowCommand>,
    cache: Arc<TestWfDriverCache>,
    _runtime: Option<Runtime>,
}

impl TestWorkflowDriver {
    /// Create a new test workflow driver from a workflow "function" which is really a closure
    /// that returns an async block.
    ///
    /// Expects to be called within the context of a tokio runtime, since it spawns the workflow
    /// code into a new task.
    pub fn new<F, Fut>(workflow_fn: F) -> Self
    where
        F: Fn(CommandSender) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let twd_cache = Arc::new(TestWfDriverCache::default());
        let (sender, receiver) = CommandSender::new(twd_cache.clone());

        let twd_clone = twd_cache.clone();
        let wf_inner_fut = workflow_fn(sender);
        let wf_future = async move {
            wf_inner_fut.await;

            let mut bc = twd_clone.blocking_info.lock();
            bc.wf_is_done = true;
            // Wake up the fetcher thread, since we have finished the workflow and that would mean
            // we've finished sending what we can.
            twd_clone.condvar.notify_one();
        };

        // This allows us to use the test workflow driver from inside an async context, or not.
        // If we are not in an async context we create a new tokio runtime and store it in ourselves
        // to run the workflow future. If we are in one, we use that instead.
        let (maybe_rt, join_handle) = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            (None, Some(handle.spawn(wf_future)))
        } else {
            let runtime = Runtime::new().unwrap();
            let jh = runtime.spawn(wf_future);
            (Some(runtime), Some(jh))
        };

        Self {
            join_handle,
            commands_from_wf: receiver,
            cache: twd_cache,
            _runtime: maybe_rt,
        }
    }

    /// Drains all pending commands that the workflow has produced since the last time this was
    /// called.
    pub fn drain_pending_commands(&mut self) -> impl Iterator<Item = WorkflowCommand> + '_ {
        self.commands_from_wf.try_iter()
    }

    /// Given the id of a command, indicate to the workflow code that it is now unblocked
    pub(crate) fn unblock(&mut self, unblock_evt: UnblockEvent) {
        self.cache.unblock(unblock_evt);
    }

    /// If there are no commands, and the workflow isn't done, we need to wait for one of those
    /// things to become true before we fetch commands. Otherwise, time out via panic.
    ///
    /// Returns true if the workflow function exited
    // TODO: When we try to deal with spawning concurrent tasks inside a workflow, we will
    //  somehow need to check that specifically the top-level task (the main wf function) is
    //  blocked waiting on a command. If the workflow spawns a concurrent task, and it blocks
    //  on a command before the main wf code path does, it will cause a spurious wakeup.
    pub fn wait_until_wf_iteration_done(&mut self) -> bool {
        let mut bc_lock = self.cache.blocking_info.lock();
        while !bc_lock.wf_is_done && bc_lock.num_blocked_cmds() == 0 {
            let timeout_res = self
                .cache
                .condvar
                .wait_for(&mut bc_lock, Duration::from_secs(1));
            if timeout_res.timed_out() {
                panic!("Workflow deadlocked (1 second)")
            }
        }
        bc_lock.wf_is_done
    }

    /// Wait for the test workflow to exit
    pub async fn join(&mut self) -> Result<(), JoinError> {
        if let Some(jh) = self.join_handle.take() {
            jh.await
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Default)]
struct TestWfDriverCache {
    blocking_info: Mutex<BlockingCondInfo>,
    condvar: Condvar,
}

macro_rules! add_sent_cmd_decl {
    ($method_name:ident, $map_name:ident, $resolve_type:ty) => {
        /// Track a new command that the wf has sent down the command sink. The command starts in
        /// [CommandStatus::Sent] and will be marked blocked once it is `await`ed
        fn $method_name(&self, id: String) -> oneshot::Receiver<$resolve_type> {
            let (tx, rx) = oneshot::channel();
            let mut bc = self.blocking_info.lock();
            let ic = IssuedCommand {
                unblocker: tx,
                status: CommandStatus::Sent,
            };
            bc.$map_name.insert(id, ic);
            rx
        }
    };
}

impl TestWfDriverCache {
    /// Unblock a command
    fn unblock(&self, unblock_evt: UnblockEvent) {
        let mut bc = self.blocking_info.lock();
        match unblock_evt {
            UnblockEvent::Timer(id) => {
                if let Some(t) = bc.issued_timers.remove(&id) {
                    t.unblocker.send(()).unwrap();
                };
            }
            UnblockEvent::Activity { id, result } => {
                if let Some(t) = bc.issued_activities.remove(&id) {
                    t.unblocker.send(result).unwrap();
                }
            }
        };
    }

    /// Cancel a timer by ID. Timers get some special handling here since they are always
    /// removed from the "lang" side without needing a response from core.
    fn cancel_timer(&self, id: &str) {
        let mut bc = self.blocking_info.lock();
        bc.issued_timers.remove(id);
    }

    /// Cancel activity by ID.
    /// TODO: Support different cancel types
    fn cancel_activity(&self, id: &str) {
        let mut bc = self.blocking_info.lock();
        bc.issued_activities.remove(id);
    }

    add_sent_cmd_decl!(add_sent_timer, issued_timers, ());
    add_sent_cmd_decl!(add_sent_activity, issued_activities, ActivityResult);

    /// Indicate that a command is being `await`ed
    fn set_cmd_blocked(&self, id: CommandID) {
        let mut bc = self.blocking_info.lock();
        match &id {
            CommandID::Timer(id) => {
                if let Some(cmd) = bc.issued_timers.get_mut(id) {
                    cmd.status = CommandStatus::Blocked;
                }
            }
            CommandID::Activity(id) => {
                if let Some(cmd) = bc.issued_activities.get_mut(id) {
                    cmd.status = CommandStatus::Blocked;
                }
            }
        }
        // Wake up the fetcher thread, since we have blocked on a command and that would mean we've
        // finished sending what we can.
        self.condvar.notify_one();
    }
}

/// Contains the info needed to know if workflow code is "done" being iterated or not. A workflow
/// iteration is considered complete if the workflow exits, or the top level task (the main codepath
/// of the workflow) is blocked waiting on a command).
#[derive(Default, Debug)]
struct BlockingCondInfo {
    /// Holds a mapping of timer ids -> oneshot channel to resolve them
    issued_timers: HashMap<String, IssuedCommand<()>>,
    /// Holds a mapping of activity ids -> oneshot channel to resolve them
    issued_activities: HashMap<String, IssuedCommand<ActivityResult>>,
    wf_is_done: bool,
}

impl BlockingCondInfo {
    fn num_blocked_cmds(&self) -> usize {
        let tc = self
            .issued_timers
            .values()
            .filter(|ic| ic.status == CommandStatus::Blocked)
            .count();
        let ac = self
            .issued_activities
            .values()
            .filter(|ic| ic.status == CommandStatus::Blocked)
            .count();
        tc + ac
    }
}

#[derive(Debug)]
struct IssuedCommand<Out> {
    unblocker: oneshot::Sender<Out>,
    status: CommandStatus,
}

#[derive(Debug, PartialEq)]
enum CommandStatus {
    Sent,
    Blocked,
}

/// Used within workflows to issue commands
pub struct CommandSender {
    chan: Sender<WorkflowCommand>,
    twd_cache: Arc<TestWfDriverCache>,
}

impl CommandSender {
    fn new(twd_cache: Arc<TestWfDriverCache>) -> (Self, Receiver<WorkflowCommand>) {
        // We need to use a normal std channel since our receiving side is non-async
        let (chan, rx) = crossbeam::channel::unbounded();
        (Self { chan, twd_cache }, rx)
    }

    /// Request to create a timer
    pub fn timer(&mut self, a: StartTimer) -> impl Future {
        let id = a.timer_id.clone();
        self.send_blocking_cmd(
            CommandID::Timer(id.clone()),
            WorkflowCommand {
                variant: Some(a.into()),
            },
            |s: &Self| s.twd_cache.add_sent_timer(id),
        )
    }

    /// Request to run an activity
    pub fn activity(
        &mut self,
        a: ScheduleActivity,
    ) -> impl Future<Output = Option<ActivityResult>> {
        let id = a.activity_id.clone();
        self.send_blocking_cmd(
            CommandID::Activity(id.clone()),
            WorkflowCommand {
                variant: Some(a.into()),
            },
            |s: &Self| s.twd_cache.add_sent_activity(id),
        )
    }

    fn send_blocking_cmd<O>(
        &mut self,
        id: CommandID,
        c: WorkflowCommand,
        sent_adder: impl FnOnce(&Self) -> oneshot::Receiver<O>,
    ) -> impl Future<Output = Option<O>> {
        self.send(c);
        let rx = sent_adder(&self);
        let cache_clone = self.twd_cache.clone();
        async move {
            cache_clone.set_cmd_blocked(id);
            rx.await.ok()
        }
    }

    /// Cancel a timer
    pub fn cancel_timer(&self, timer_id: &str) {
        let c = WorkflowCommand {
            variant: Some(
                CancelTimer {
                    timer_id: timer_id.to_owned(),
                }
                .into(),
            ),
        };
        self.twd_cache.cancel_timer(timer_id);
        self.send(c);
    }

    /// Cancel activity
    pub fn cancel_activity(&self, activity_id: &str) {
        let c = WorkflowCommand {
            variant: Some(
                RequestCancelActivity {
                    activity_id: activity_id.to_string(),
                    ..Default::default()
                }
                .into(),
            ),
        };
        self.twd_cache.cancel_activity(activity_id);
        self.send(c);
    }

    /// Finish the workflow execution
    // TODO: Make automatic
    pub fn complete_workflow_execution(&self) {
        let c = WorkflowCommand {
            variant: Some(CompleteWorkflowExecution::default().into()),
        };
        self.send(c);
    }

    fn send(&self, c: WorkflowCommand) {
        self.chan.send(c).unwrap();
    }
}
