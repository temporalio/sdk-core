//! This module is essentially a rough prototype Rust SDK. It can be used to create closures that
//! look sort of like normal workflow code. It should only depend on things in the core crate that
//! are already publicly exposed.

use crate::{
    protos::coresdk::{
        activity_result::ActivityResult,
        common::Payload,
        workflow_activation::{
            wf_activation_job::Variant, FireTimer, ResolveActivity, WfActivation, WfActivationJob,
        },
        workflow_commands::{
            CancelTimer, CompleteWorkflowExecution, ContinueAsNewWorkflowExecution,
            RequestCancelActivity, ScheduleActivity, StartTimer, WorkflowCommand,
        },
    },
    workflow::CommandID,
    CompleteWfError, Core, IntoCompletion,
};
use anyhow::{anyhow, bail};
use crossbeam::channel::{Receiver, Sender};
use dashmap::DashMap;
use futures::{future::BoxFuture, stream::FuturesUnordered, FutureExt, StreamExt};
use parking_lot::{Condvar, Mutex};
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    runtime::Runtime,
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        oneshot, Notify,
    },
    task::{JoinError, JoinHandle},
};

/// A worker that can poll for and respond to workflow tasks by using [TestWorkflowDriver]s
pub struct TestRustWorker {
    core: Arc<dyn Core>,
    task_queue: String,
    deadlock_override: Option<Duration>,
    /// Maps run id to the driver
    workflows: DashMap<String, UnboundedSender<WfActivation>>,
    /// Maps workflow id to the function for executing workflow runs with that ID
    workflow_fns: DashMap<String, Box<WfFunc>>,
    /// Number of live workflows
    incomplete_workflows: Arc<AtomicUsize>,
    /// Awoken each time an activation is completed by a workflow run
    activation_done_notify: Arc<Notify>,
    /// Handles for each spawned workflow run are inserted here to be cleaned up when all runs
    /// are finished
    join_handles: FuturesUnordered<JoinHandle<Result<(), anyhow::Error>>>,
}
type WfFunc = dyn Fn(WfContext) -> BoxFuture<'static, ()> + Send + Sync + 'static;

impl TestRustWorker {
    /// Create a new rust worker using the provided core instance, namespace, and task queue
    pub fn new(core: Arc<dyn Core>, task_queue: String) -> Self {
        Self {
            core,
            task_queue,
            workflows: Default::default(),
            workflow_fns: Default::default(),
            incomplete_workflows: Arc::new(AtomicUsize::new(0)),
            activation_done_notify: Arc::new(Notify::new()),
            deadlock_override: None,
            join_handles: FuturesUnordered::new(),
        }
    }

    /// Force the workflow deadlock timeout to a different value
    pub fn override_deadlock(&mut self, new_time: Duration) {
        self.deadlock_override = Some(new_time);
    }

    /// Create a workflow, asking the server to start it with the provided workflow ID and using the
    /// provided [TestWorkflowDriver] as the workflow code.
    pub async fn submit_wf<F, Fut>(
        &self,
        input: Vec<Payload>,
        workflow_id: String,
        wf_function: F,
    ) -> Result<(), tonic::Status>
    where
        F: Fn(WfContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.core
            .server_gateway()
            .start_workflow(
                input,
                self.task_queue.clone(),
                workflow_id.clone(),
                workflow_id.clone(),
                None,
            )
            .await?;

        self.workflow_fns.insert(
            workflow_id,
            Box::new(move |ctx: WfContext| wf_function(ctx).boxed()),
        );
        self.incomplete_workflows.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Actually run the workflow function. Once complete, the workflow will remove itself
    /// from the function map, indicating that the submitted workflow has finished.
    fn start_wf(&self, mut twd: TestWorkflowDriver, run_id: String) {
        let (tx, mut rx) = unbounded_channel::<WfActivation>();
        let core = self.core.clone();
        let live_wfs = self.incomplete_workflows.clone();
        let act_done_notify = self.activation_done_notify.clone();
        let jh = tokio::spawn(async move {
            let mut workflow_really_finished = false;
            let mut retval = Ok(());

            'rcvloop: while let Some(activation) = rx.recv().await {
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
                            Variant::RemoveFromCache(_) => {
                                twd.kill().await;
                                break 'rcvloop;
                            }
                        }
                    } else {
                        retval = Err(anyhow!("Empty activation job variant"));
                        break 'rcvloop;
                    }
                }

                match twd
                    .complete_wf_task(core.as_ref(), activation.run_id)
                    .await?
                {
                    WfExitKind::NotYetDone => {}
                    WfExitKind::Fail | WfExitKind::Complete => {
                        workflow_really_finished = true;
                        break;
                    }
                    WfExitKind::ContinueAsNew => {
                        break;
                    }
                }
                act_done_notify.notify_one();
            }
            if workflow_really_finished {
                live_wfs.fetch_sub(1, Ordering::SeqCst);
            }
            act_done_notify.notify_one();
            retval
        });
        self.workflows.insert(run_id, tx);
        self.join_handles.push(jh);
    }

    /// Drives all workflows until they have all finished, repeatedly polls server to fetch work
    /// for them.
    pub async fn run_until_done(self) -> Result<(), anyhow::Error> {
        let poller = async move {
            loop {
                let activation = self.core.poll_workflow_task(&self.task_queue).await?;

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
                    let mut twd =
                        TestWorkflowDriver::new(sw.arguments.clone(), wf_function.as_ref());
                    if let Some(or) = self.deadlock_override {
                        twd.override_deadlock(or);
                    }
                    self.start_wf(twd, activation.run_id.clone());
                }
                // The activation is expected to apply to some workflow we know about. Use it to
                // unblock things and advance the workflow.
                if let Some(tx) = self.workflows.get_mut(&activation.run_id) {
                    // Error could happen b/c of eviction notification after wf exits. We don't care
                    // about that in these tests.
                    let _ = tx.send(activation);
                } else {
                    bail!("Got activation for unknown workflow");
                }

                self.activation_done_notify.notified().await;
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

pub(crate) enum UnblockEvent {
    Timer(String),
    Activity { id: String, result: ActivityResult },
}

/// Allows implementing workflows in a reasonably natural looking way in Rust
pub struct TestWorkflowDriver {
    join_handle: Option<JoinHandle<()>>,
    commands_from_wf: Receiver<WorkflowCommand>,
    cache: Arc<TestWfDriverCache>,
    kill_notifier: Arc<Notify>,
    deadlock_time: Duration,
    _runtime: Option<Runtime>,
}

impl TestWorkflowDriver {
    /// Create a new test workflow driver from a workflow function which must be async and accepts
    /// a [WfContext]
    ///
    /// Expects to be called within the context of a tokio runtime, since it spawns the workflow
    /// code into a new task.
    pub fn new<F, Fut>(args: Vec<Payload>, workflow_fn: F) -> Self
    where
        F: Fn(WfContext) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let twd_cache = Arc::new(TestWfDriverCache::default());
        let (sender, receiver) = WfContext::new(twd_cache.clone(), args);

        let twd_clone = twd_cache.clone();
        let wf_inner_fut = workflow_fn(sender);
        let kill_notifier = Arc::new(Notify::new());
        let killed = kill_notifier.clone();
        let wf_future = async move {
            tokio::select! {
                biased;

                _ = killed.notified() => {
                    return;
                }
                _ = wf_inner_fut => {},
            }

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
            kill_notifier,
            deadlock_time: Duration::from_secs(1),
            _runtime: maybe_rt,
        }
    }

    /// Useful for forcing WFT timeouts
    pub fn override_deadlock(&mut self, new_time: Duration) {
        self.deadlock_time = new_time;
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
                .wait_for(&mut bc_lock, self.deadlock_time);
            if timeout_res.timed_out() {
                panic!("Workflow deadlocked (1 second)")
            }
        }
        bc_lock.wf_is_done
    }

    /// After the workflow has been advanced, grab any outgoing commands and send them to the
    /// server. Returns how the workflow exited.
    async fn complete_wf_task(
        &mut self,
        core: &dyn Core,
        run_id: String,
    ) -> Result<WfExitKind, CompleteWfError> {
        // Since waiting until the iteration is done may block and isn't async, we need to use
        // block_in_place here.
        let wf_is_done = tokio::task::block_in_place(|| self.wait_until_wf_iteration_done());
        let outgoing = self.drain_pending_commands();
        let completion = outgoing.into_completion(run_id);
        let wf_exit = if wf_is_done {
            if completion.has_complete_workflow_execution() {
                WfExitKind::Complete
            } else if completion.has_continue_as_new() {
                WfExitKind::ContinueAsNew
            } else {
                WfExitKind::Fail
            }
        } else {
            WfExitKind::NotYetDone
        };
        core.complete_workflow_task(completion).await?;
        Ok(wf_exit)
    }

    async fn kill(mut self) {
        self.kill_notifier.notify_one();
        self.join().await.unwrap();
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

enum WfExitKind {
    NotYetDone,
    Fail,
    Complete,
    ContinueAsNew,
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
        self.unblock(UnblockEvent::Timer(id.to_owned()));
    }

    /// Cancel activity by ID.
    /// TODO: Support different cancel types
    fn cancel_activity(&self, id: &str) {
        self.unblock(UnblockEvent::Activity {
            id: id.to_owned(),
            result: ActivityResult::cancel_from_details(None),
        });
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

/// Used within workflows to issue commands, get info, etc.
pub struct WfContext {
    chan: Sender<WorkflowCommand>,
    twd_cache: Arc<TestWfDriverCache>,
    args: Vec<Payload>,
}

impl WfContext {
    fn new(
        twd_cache: Arc<TestWfDriverCache>,
        args: Vec<Payload>,
    ) -> (Self, Receiver<WorkflowCommand>) {
        // We need to use a normal std channel since our receiving side is non-async
        let (chan, rx) = crossbeam::channel::unbounded();
        (
            Self {
                chan,
                twd_cache,
                args,
            },
            rx,
        )
    }

    /// Get the arguments provided to the workflow upon execution start
    pub fn get_args(&self) -> &[Payload] {
        self.args.as_slice()
    }

    /// Request to create a timer
    pub fn timer(&mut self, a: StartTimer) -> impl Future<Output = Option<()>> {
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
            // Dropping the handle can happen on an "eviction", and we don't want to spew unwrap
            // panics when that happens, so `ok` is used.
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
    // TODO: Make automatic if wf exits w/o other "final" command like fail or continue as new
    pub fn complete_workflow_execution(&self) {
        let c = WorkflowCommand {
            variant: Some(CompleteWorkflowExecution::default().into()),
        };
        self.send(c);
    }

    /// Continue as new
    pub fn continue_as_new(&self, command: ContinueAsNewWorkflowExecution) {
        let c = WorkflowCommand {
            variant: Some(command.into()),
        };
        self.send(c);
    }

    fn send(&self, c: WorkflowCommand) {
        self.chan.send(c).unwrap();
    }
}
