use crate::{
    machines::{workflow_machines::CommandID, WFCommand},
    protos::coresdk::{
        workflow_activation::{
            wf_activation_job, wf_activation_job::Variant, FireTimer, ResolveActivity,
        },
        workflow_commands::{CancelTimer, RequestCancelActivity, ScheduleActivity, StartTimer},
    },
    workflow::{ActivationListener, WorkflowFetcher},
};
use futures::channel::oneshot;
use parking_lot::{Condvar, Mutex};
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc,
    },
    time::Duration,
};
use tokio::{
    runtime::Runtime,
    task::{JoinError, JoinHandle},
};

pub struct TestWorkflowDriver {
    join_handle: Option<JoinHandle<()>>,
    commands_from_wf: Receiver<WFCommand>,
    cache: Arc<TestWfDriverCache>,
    runtime: Runtime,
}

#[derive(Debug)]
struct TestWfDriverCache {
    blocking_condvar: Arc<(Mutex<BlockingCondInfo>, Condvar)>,
}

impl TestWfDriverCache {
    /// Unblock a command by ID
    fn unblock(&self, id: CommandID) {
        let mut bc = self.blocking_condvar.0.lock();
        if let Some(t) = bc.issued_commands.remove(&id) {
            t.unblocker.send(()).unwrap()
        };
    }

    /// Cancel a timer by ID. Timers get some special handling here since they are always
    /// removed from the "lang" side without needing a response from core.
    fn cancel_timer(&self, id: &str) {
        let mut bc = self.blocking_condvar.0.lock();
        bc.issued_commands.remove(&CommandID::Timer(id.to_owned()));
    }

    /// Cancel activity by ID.
    fn cancel_activity(&self, id: &str) {
        let mut bc = self.blocking_condvar.0.lock();
        bc.issued_commands
            .remove(&CommandID::Activity(id.to_owned()));
    }

    /// Track a new command that the wf has sent down the command sink. The command starts in
    /// [CommandStatus::Sent] and will be marked blocked once it is `await`ed
    fn add_sent_cmd(&self, id: CommandID) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        let mut bc = self.blocking_condvar.0.lock();
        bc.issued_commands.insert(
            id,
            IssuedCommand {
                unblocker: tx,
                status: CommandStatus::Sent,
            },
        );
        rx
    }

    /// Indicate that a command is being `await`ed
    fn set_cmd_blocked(&self, id: CommandID) {
        let mut bc = self.blocking_condvar.0.lock();
        if let Some(cmd) = bc.issued_commands.get_mut(&id) {
            cmd.status = CommandStatus::Blocked;
        }
        // Wake up the fetcher thread, since we have blocked on a command and that would mean we've
        // finished sending what we can.
        self.blocking_condvar.1.notify_one();
    }
}

/// Contains the info needed to know if workflow code is "done" being iterated or not. A workflow
/// iteration is considered complete if the workflow exits, or the top level task (the main codepath
/// of the workflow) is blocked waiting on a command).
#[derive(Default, Debug)]
struct BlockingCondInfo {
    /// Holds a mapping of timer id -> oneshot channel to resolve it
    issued_commands: HashMap<CommandID, IssuedCommand>,
    wf_is_done: bool,
}

impl BlockingCondInfo {
    fn num_blocked_cmds(&self) -> usize {
        self.issued_commands
            .values()
            .filter(|ic| ic.status == CommandStatus::Blocked)
            .count()
    }
}

#[derive(Debug)]
struct IssuedCommand {
    unblocker: oneshot::Sender<()>,
    status: CommandStatus,
}

#[derive(Debug, PartialEq)]
enum CommandStatus {
    Sent,
    Blocked,
}

pub struct CommandSender {
    chan: Sender<WFCommand>,
    twd_cache: Arc<TestWfDriverCache>,
}

impl CommandSender {
    fn new(twd_cache: Arc<TestWfDriverCache>) -> (Self, Receiver<WFCommand>) {
        // We need to use a normal std channel since our receiving side is non-async
        let (chan, rx) = mpsc::channel();
        (Self { chan, twd_cache }, rx)
    }

    pub fn send(&self, c: WFCommand) {
        self.chan.send(c).unwrap();
    }

    /// Request to create a timer
    pub fn timer(&mut self, a: StartTimer) -> impl Future {
        self.send_blocking_cmd(CommandID::Timer(a.timer_id.clone()), WFCommand::AddTimer(a))
    }

    /// Request to run an activity
    pub fn activity(&mut self, a: ScheduleActivity) -> impl Future {
        self.send_blocking_cmd(
            CommandID::Activity(a.activity_id.clone()),
            WFCommand::AddActivity(a),
        )
    }

    fn send_blocking_cmd(&mut self, id: CommandID, c: WFCommand) -> impl Future {
        self.send(c);
        let rx = self.twd_cache.add_sent_cmd(id.clone());
        let cache_clone = self.twd_cache.clone();
        async move {
            cache_clone.set_cmd_blocked(id);
            rx.await
        }
    }

    /// Cancel a timer
    pub fn cancel_timer(&self, timer_id: &str) {
        let c = WFCommand::CancelTimer(CancelTimer {
            timer_id: timer_id.to_owned(),
        });
        self.twd_cache.cancel_timer(timer_id);
        self.send(c);
    }

    /// Cancel activity
    pub fn cancel_activity(&self, activity_id: &str) {
        let c = WFCommand::RequestCancelActivity(RequestCancelActivity {
            activity_id: activity_id.to_string(),
            ..Default::default()
        });
        self.twd_cache.cancel_activity(activity_id);
        self.send(c);
    }
}

impl TestWorkflowDriver {
    /// Create a new test workflow driver from a workflow "function" which is really a closure
    /// that returns an async block.
    ///
    /// Creates a tokio runtime to execute the workflow on.
    pub fn new<F, Fut>(workflow_fn: F) -> Self
    where
        F: Fn(CommandSender) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let blocking_condvar = Arc::new((Default::default(), Default::default()));
        let bc_clone = blocking_condvar.clone();
        let twd_cache = Arc::new(TestWfDriverCache { blocking_condvar });
        let (sender, receiver) = CommandSender::new(twd_cache.clone());

        let wf_inner_fut = workflow_fn(sender);
        let wf_future = async move {
            wf_inner_fut.await;

            let mut bc = bc_clone.0.lock();
            bc.wf_is_done = true;
            // Wake up the fetcher thread, since we have finished the  workflow and that would mean
            // we've finished sending what we can.
            bc_clone.1.notify_one();
        };
        let runtime = Runtime::new().unwrap();
        let join_handle = Some(runtime.spawn(wf_future));
        Self {
            join_handle,
            commands_from_wf: receiver,
            cache: twd_cache,
            runtime,
        }
    }

    /// If there are no commands, and the workflow isn't done, we need to wait for one of those
    /// things to become true before we fetch commands. Otherwise, time out via panic.
    ///
    /// Returns true if the workflow function exited
    // TODO: When we try to deal with spawning concurrent tasks inside a workflow, we will
    //  somehow need to check that specifically the top-level task (the main wf function) is
    //  blocked waiting on a command. If the workflow spawns a concurrent task, and it blocks
    //  on a command before the main wf code path does, it will cause a spurious wakeup.
    fn wait_until_wf_iteration_done(&mut self) -> bool {
        let mut bc_lock = self.cache.blocking_condvar.0.lock();
        while !bc_lock.wf_is_done && bc_lock.num_blocked_cmds() == 0 {
            let timeout_res = self
                .cache
                .blocking_condvar
                .1
                .wait_for(&mut bc_lock, Duration::from_secs(1));
            if timeout_res.timed_out() {
                panic!("Workflow deadlocked (1 second)")
            }
        }
        bc_lock.wf_is_done
    }

    /// Wait for the test workflow to exit
    fn join(&mut self) -> Result<(), JoinError> {
        if let Some(jh) = self.join_handle.take() {
            self.runtime.block_on(jh)
        } else {
            Ok(())
        }
    }
}

impl WorkflowFetcher for TestWorkflowDriver {
    fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand> {
        let mut emit_these = vec![];

        let wf_is_done = self.wait_until_wf_iteration_done();

        for c in self.commands_from_wf.try_iter() {
            emit_these.push(c);
        }

        if wf_is_done {
            // TODO: Eventually upgrade to return workflow failures on panic
            self.join().expect("Workflow completes without panic");
        }

        debug!(emit_these = ?emit_these, "Test wf driver emitting");

        emit_these
    }
}

impl ActivationListener for TestWorkflowDriver {
    fn on_activation_job(&mut self, activation: &wf_activation_job::Variant) {
        match activation {
            Variant::FireTimer(FireTimer { timer_id }) => {
                self.cache.unblock(CommandID::Timer(timer_id.to_owned()));
            }
            Variant::ResolveActivity(ResolveActivity {
                activity_id,
                result: _result,
            }) => {
                self.cache
                    .unblock(CommandID::Activity(activity_id.to_owned()));
            }
            _ => {}
        }
    }
}
