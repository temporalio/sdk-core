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
use futures::Future;
use std::{
    collections::{hash_map, HashMap},
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, Sender},
        Arc, RwLock,
    },
};
use tracing::Level;

type TimerMap = HashMap<String, Arc<AtomicBool>>;

/// This is a test only implementation of a [DrivenWorkflow] which has finer-grained control
/// over when commands are returned than a normal workflow would.
///
/// It replaces "TestEnitityTestListenerBase" in java which is pretty hard to follow.
#[derive(Debug)]
pub(in crate::machines) struct TestWorkflowDriver<F> {
    wf_function: F,

    /// Holds status for timers so we don't recreate them by accident. Key is timer id, value is
    /// true if it is complete.
    ///
    /// I don't love that this is pretty duplicative of workflow machines, nor the nasty sync
    /// involved. Would be good to eliminate.
    timers: Arc<RwLock<TimerMap>>,
}

impl<F, Fut> TestWorkflowDriver<F>
where
    F: Fn(CommandSender) -> Fut,
    Fut: Future<Output = ()>,
{
    pub(in crate::machines) fn new(workflow_fn: F) -> Self {
        Self {
            wf_function: workflow_fn,
            timers: Arc::new(RwLock::new(HashMap::default())),
        }
    }
}

impl<F, Fut> DrivenWorkflow for TestWorkflowDriver<F>
where
    F: Fn(CommandSender) -> Fut,
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
        let (sender, receiver) = CommandSender::new(self.timers.clone());
        // Call the closure that produces the workflow future
        let wf_future = (self.wf_function)(sender);

        // Create a tokio runtime to block on the future
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(wf_future);

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
        Ok(if let Some(c) = last_cmd {
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

pub struct CommandSender {
    chan: Sender<TestWFCommand>,
    timer_map: Arc<RwLock<TimerMap>>,
}

impl<'a> CommandSender {
    fn new(timer_map: Arc<RwLock<TimerMap>>) -> (Self, Receiver<TestWFCommand>) {
        let (chan, rx) = mpsc::channel();
        (Self { chan, timer_map }, rx)
    }

    /// Request to create a timer, returning future which resolves when the timer completes
    pub fn timer(&mut self, a: StartTimerCommandAttributes) -> impl Future + '_ {
        let finished = match self.timer_map.write().unwrap().entry(a.timer_id.clone()) {
            hash_map::Entry::Occupied(existing) => existing.get().load(Ordering::SeqCst),
            hash_map::Entry::Vacant(v) => {
                let atomic = Arc::new(AtomicBool::new(false));

                let c = WFCommand::AddTimer(a, atomic.clone());
                // In theory we should send this in both branches
                self.chan.send(c.into()).unwrap();
                v.insert(atomic);
                false
            }
        };
        if !finished {
            self.chan.send(TestWFCommand::Waiting).unwrap();
        }
        futures::future::ready(())
    }

    pub fn send(&mut self, c: WFCommand) {
        self.chan.send(c.into()).unwrap();
    }
}
