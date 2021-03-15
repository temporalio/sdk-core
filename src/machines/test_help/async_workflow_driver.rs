use crate::{
    machines::WFCommand,
    protos::{
        coresdk::{wf_activation_job, CancelTimer, FireTimer},
        temporal::api::command::v1::{CancelTimerCommandAttributes, StartTimerCommandAttributes},
    },
    workflow::{ActivationListener, WorkflowFetcher},
};
use dashmap::DashMap;
use futures::channel::oneshot;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
        Arc,
    },
    time::Duration,
};
use tokio::task::{JoinError, JoinHandle};

pub struct TestWorkflowDriver {
    join_handle: JoinHandle<()>,
    commands_from_wf: Receiver<WFCommand>,
    cache: Arc<TestWfDriverCache>,
    is_done: Arc<AtomicBool>,
}

#[derive(Default, Debug)]
pub struct TestWfDriverCache {
    /// Holds a mapping of timer id -> oneshot channel to resolve it
    timer_futures: DashMap<String, oneshot::Sender<()>>,
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
    pub fn timer(&mut self, a: StartTimerCommandAttributes) -> impl Future {
        let (tx, rx) = oneshot::channel();
        self.twd_cache.timer_futures.insert(a.timer_id.clone(), tx);
        let c = WFCommand::AddTimer(a);
        dbg!("Send add timer");
        self.send(c.into());
        rx
    }

    /// Cancel a timer
    pub fn cancel_timer(&self, timer_id: &str) {
        let c = WFCommand::CancelTimer(CancelTimerCommandAttributes {
            timer_id: timer_id.to_string(),
        });
        // Timer cancellation immediately unblocks awaiting a timer
        self.twd_cache
            .timer_futures
            .remove(timer_id)
            .map(|t| t.1.send(()).unwrap());
        self.send(c.into());
    }
}

impl TestWorkflowDriver {
    /// Create a new test workflow driver from a workflow "function" which is really a closure
    /// that returns an async block.
    ///
    /// This function, though not async itself, must be called from an async context as it depends
    /// on the tokio runtime to spawn the workflow future.
    pub fn new<F, Fut>(workflow_fn: F) -> Self
    where
        F: Fn(CommandSender) -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let twd_cache = Arc::new(TestWfDriverCache::default());
        let (sender, receiver) = CommandSender::new(twd_cache.clone());
        let send_half_drop_on_wf_exit = sender.chan.clone();
        let is_done = Arc::new(AtomicBool::new(false));
        let is_done_in_fut = is_done.clone();
        let wf_inner_fut = workflow_fn(sender);
        let wf_future = async move {
            wf_inner_fut.await;
            is_done_in_fut.store(true, Ordering::SeqCst);
            drop(send_half_drop_on_wf_exit);
            dbg!("Exiting wf future");
        };
        let join_handle = tokio::spawn(wf_future);
        Self {
            join_handle,
            commands_from_wf: receiver,
            is_done,
            cache: twd_cache,
        }
    }

    // TODO: Needs to be returned separately from new
    /// Wait for the test workflow to exit
    pub async fn join(self) -> Result<(), JoinError> {
        self.join_handle.await
    }
}

impl WorkflowFetcher for TestWorkflowDriver {
    fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand> {
        let mut emit_these = vec![];

        // TODO: We really want this to be like a condvar, where we block here until either
        //  the wf is done or we are blocking on a command, doing deadlock if neither happens after
        //  a timeout.
        // let is_blocking_on_cmds = self.cache.blocked_commands.load(Ordering::SeqCst) > 0;

        // If the workflow is blocking on one or more commands, we will perform a blocking wait
        // for it to send us new commands, with a timeout. If we are not blocking on any commands,
        // and we hit the timeout, the workflow is spinning or deadlocked.

        // futures::executor::block_on(self.cache.cmd_notifier.notified());
        loop {
            match self
                .commands_from_wf
                .recv_timeout(Duration::from_millis(10))
            {
                Ok(c) => emit_these.push(c),
                Err(RecvTimeoutError::Timeout) => {
                    dbg!("Timeout");
                    break;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    if self.is_done.load(Ordering::SeqCst) {
                        break;
                    }
                    unreachable!("Workflow done flag must be set properly")
                }
            }
        }

        debug!(emit_these = ?emit_these, "Test wf driver emitting");

        emit_these
    }
}
impl ActivationListener for TestWorkflowDriver {
    fn on_activation_job(&mut self, activation: &wf_activation_job::Variant) {
        if let wf_activation_job::Variant::FireTimer(FireTimer { timer_id })
        | wf_activation_job::Variant::CancelTimer(CancelTimer { timer_id }) = activation
        {
            if let Some(tx) = self.cache.timer_futures.remove(timer_id) {
                tx.1.send(()).unwrap()
            }
        }
    }
}
