use super::Result;
use crate::{
    machines::{DrivenWorkflow, WFCommand},
    protos::temporal::api::{
        command::v1::StartTimerCommandAttributes,
        history::v1::{
            WorkflowExecutionCanceledEventAttributes, WorkflowExecutionSignaledEventAttributes,
            WorkflowExecutionStartedEventAttributes,
        },
    },
};
use futures::{channel::oneshot, executor::block_on, lock::Mutex, Future, FutureExt};
use std::sync::Arc;
use std::{
    collections::{hash_map, HashMap},
    sync::{
        mpsc::{self, Receiver, Sender},
        RwLock,
    },
};
use tracing::Level;

// TODO: Mutex is not necessary
type TimerMap = HashMap<String, Arc<futures::lock::Mutex<oneshot::Receiver<bool>>>>;

/// This is a test only implementation of a [DrivenWorkflow] which has finer-grained control
/// over when commands are returned than a normal workflow would.
///
/// It replaces "TestEnitityTestListenerBase" in java which is pretty hard to follow.
#[derive(Debug)]
pub(in crate::machines) struct TestWorkflowDriver<F> {
    wf_function: F,

    /// Holds completion channels for timers so we don't recreate them by accident. Key is timer id.
    ///
    /// I don't love that this is pretty duplicative of workflow machines
    timers: RwLock<TimerMap>,
}

impl<F, Fut> TestWorkflowDriver<F>
where
    F: Fn(CommandSender<'_>) -> Fut,
    Fut: Future<Output = ()>,
{
    pub(in crate::machines) fn new(workflow_fn: F) -> Self {
        Self {
            wf_function: workflow_fn,
            timers: RwLock::new(HashMap::default()),
        }
    }
}

impl<F, Fut> DrivenWorkflow for TestWorkflowDriver<F>
where
    F: Fn(CommandSender<'_>) -> Fut,
    Fut: Future<Output = ()>,
{
    #[instrument(skip(self))]
    fn start(
        &self,
        _attribs: WorkflowExecutionStartedEventAttributes,
    ) -> Result<Vec<WFCommand>, anyhow::Error> {
        Ok(vec![])
    }

    #[instrument(skip(self))]
    fn iterate_wf(&self) -> Result<Vec<WFCommand>, anyhow::Error> {
        dbg!("iterate");
        let (sender, receiver) = CommandSender::new(&self.timers);
        // Call the closure that produces the workflow future
        let wf_future = (self.wf_function)(sender);

        // TODO: This block_on might cause issues later
        block_on(wf_future);
        let cmds = receiver.into_iter();
        event!(Level::DEBUG, msg = "Test wf driver emitting", ?cmds);

        let mut last_cmd = None;
        for cmd in cmds {
            match cmd {
                TestWFCommand::WFCommand(c) => last_cmd = Some(c),
                TestWFCommand::Waiting => {
                    // Ignore further commands since we're waiting on something
                    break;
                }
            }
        }

        // Return only the last command. TODO: Later this will need to account for the wf
        Ok(if let Some(c) = dbg!(last_cmd) {
            vec![c]
        } else {
            vec![]
        })
    }

    fn signal(
        &self,
        _attribs: WorkflowExecutionSignaledEventAttributes,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    fn cancel(
        &self,
        _attribs: WorkflowExecutionCanceledEventAttributes,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

#[derive(Debug, derive_more::From)]
pub enum TestWFCommand {
    WFCommand(WFCommand),
    /// When a test workflow wants to await something like a timer or an activity, we will
    /// ignore all commands produced after the wait, since they couldn't have actually happened
    /// in a real workflow, since you'd be stuck waiting
    Waiting,
}

pub struct CommandSender<'a> {
    chan: Sender<TestWFCommand>,
    timer_map: &'a RwLock<TimerMap>,
}

impl<'a> CommandSender<'a> {
    fn new(timer_map: &'a RwLock<TimerMap>) -> (Self, Receiver<TestWFCommand>) {
        let (chan, rx) = mpsc::channel();
        (Self { chan, timer_map }, rx)
    }

    /// Request to create a timer, returning future which resolves when the timer completes
    pub fn timer(&mut self, a: StartTimerCommandAttributes) -> impl Future + '_ {
        dbg!("Timer call", &a.timer_id);
        let mut rx = match self.timer_map.write().unwrap().entry(a.timer_id.clone()) {
            hash_map::Entry::Occupied(existing) => {
                dbg!("occupied");
                existing.get().clone()
            }
            hash_map::Entry::Vacant(v) => {
                dbg!("vacant");
                let (tx, rx) = oneshot::channel();
                let c = WFCommand::AddTimer(a, tx);
                // In theory we should send this in both branches
                self.chan.send(c.into()).unwrap();
                v.insert(Arc::new(Mutex::new(rx))).clone()
            }
        };
        let chan = &mut self.chan;
        async move {
            // let mut lock = rx.lock().await.fuse();
            futures::select! {
                // If the timer is done, nothing to do.
                _ = &(*rx.lock().await) => {
                    dbg!("He done");
                }
                // Timer is not done. Send waiting command
                default => {
                    dbg!("He waitin");
                    chan.send(TestWFCommand::Waiting).unwrap();
                }
            }
        }
    }

    pub fn send(&mut self, c: WFCommand) {
        self.chan.send(c.into()).unwrap();
    }
}
