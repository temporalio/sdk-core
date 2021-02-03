use super::Result;
use crate::{
    machines::{ActivationListener, DrivenWorkflow, WFCommand},
    protos::{
        coresdk::{wf_activation_job::Attributes, TimerFiredTaskAttributes},
        temporal::api::{
            command::v1::StartTimerCommandAttributes,
            history::v1::{
                WorkflowExecutionCanceledEventAttributes, WorkflowExecutionSignaledEventAttributes,
                WorkflowExecutionStartedEventAttributes,
            },
        },
    },
};
use dashmap::DashMap;
use futures::Future;
use std::sync::{
    mpsc::{self, Receiver, Sender},
    Arc,
};
use tracing::Level;

/// This is a test only implementation of a [DrivenWorkflow] which has finer-grained control
/// over when commands are returned than a normal workflow would.
///
/// It replaces "TestEnitityTestListenerBase" in java which is pretty hard to follow.
pub(in crate::machines) struct TestWorkflowDriver<F> {
    wf_function: F,
    cache: Arc<TestWfDriverCache>,
}

#[derive(Default, Debug)]
pub(in crate::machines) struct TestWfDriverCache {
    /// Holds a mapping of timer id -> is finished
    ///
    /// It can and should be eliminated by not recreating CommandSender on every iteration, which
    /// means keeping the workflow suspended in a future somewhere. I tried this and it was hard,
    /// but ultimately it's how real workflows will need to work.
    unblocked_timers: DashMap<String, bool>,
}

impl<F, Fut> TestWorkflowDriver<F>
where
    F: Fn(CommandSender) -> Fut,
    Fut: Future<Output = ()>,
{
    /// Create a new test workflow driver from a workflow "function" which is really a closure
    /// that returns an async block.
    ///
    /// In an ideal world, the workflow fn would actually be a generator which can yield commands,
    /// and we may well end up doing something like that later.
    pub(in crate::machines) fn new(workflow_fn: F) -> Self {
        Self {
            wf_function: workflow_fn,
            cache: Default::default(),
        }
    }
}

impl<F> ActivationListener for TestWorkflowDriver<F> {
    fn on_activation_job(&mut self, activation: &Attributes) {
        if let Attributes::TimerFired(TimerFiredTaskAttributes { timer_id }) = activation {
            Arc::get_mut(&mut self.cache)
                .unwrap()
                .unblocked_timers
                .insert(timer_id.clone(), true);
        }
    }
}

impl<F, Fut> DrivenWorkflow for TestWorkflowDriver<F>
where
    F: Fn(CommandSender) -> Fut + Send + Sync,
    Fut: Future<Output = ()>,
{
    #[instrument(skip(self))]
    fn start(
        &mut self,
        _attribs: WorkflowExecutionStartedEventAttributes,
    ) -> Result<Vec<WFCommand>, anyhow::Error> {
        Ok(vec![])
    }

    #[instrument(skip(self))]
    fn iterate_wf(&mut self) -> Result<Vec<WFCommand>, anyhow::Error> {
        let (sender, receiver) = CommandSender::new(self.cache.clone());
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

        // Return only the last command, since that's what would've been yielded in a real wf
        Ok(if let Some(c) = last_cmd {
            vec![c]
        } else {
            vec![]
        })
    }

    fn signal(
        &mut self,
        _attribs: WorkflowExecutionSignaledEventAttributes,
    ) -> Result<(), anyhow::Error> {
        Ok(())
    }

    fn cancel(
        &mut self,
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
    twd_cache: Arc<TestWfDriverCache>,
}

impl CommandSender {
    fn new(twd_cache: Arc<TestWfDriverCache>) -> (Self, Receiver<TestWFCommand>) {
        let (chan, rx) = mpsc::channel();
        (Self { chan, twd_cache }, rx)
    }

    /// Request to create a timer, returning future which resolves when the timer completes
    pub fn timer(&mut self, a: StartTimerCommandAttributes) -> impl Future + '_ {
        let finished = match self.twd_cache.unblocked_timers.entry(a.timer_id.clone()) {
            dashmap::mapref::entry::Entry::Occupied(existing) => *existing.get(),
            dashmap::mapref::entry::Entry::Vacant(v) => {
                let c = WFCommand::AddTimer(a);
                self.chan.send(c.into()).unwrap();
                v.insert(false);
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
