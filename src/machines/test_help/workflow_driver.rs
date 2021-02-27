use crate::protos::temporal::api::command::v1::CancelTimerCommandAttributes;
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
///
/// It is important to understand that this driver doesn't work like a real workflow in the sense
/// that nothing in it ever blocks, or ever should block. Every workflow task will run through the
/// *entire* workflow, but any commands given to the sink after a `Waiting` command are simply
/// ignored, allowing you to simulate blocking without ever actually blocking.
pub(in crate::machines) struct TestWorkflowDriver<F> {
    wf_function: F,
    cache: Arc<TestWfDriverCache>,
    /// Set to true if a workflow execution completed/failed/cancelled/etc has been issued
    sent_final_execution: bool,
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
            sent_final_execution: false,
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
    fn start(&mut self, _attribs: WorkflowExecutionStartedEventAttributes) {
        event!(Level::DEBUG, msg = "Test WF driver start called");
    }

    fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand> {
        // If we have already sent the command to complete the workflow, we don't want
        // to re-run the workflow again.
        // TODO: This would be better to solve by actually pausing the workflow properly rather
        //   than doing the re-run the whole thing every time deal.
        if self.sent_final_execution {
            return vec![];
        }

        let (sender, receiver) = CommandSender::new(self.cache.clone());
        // Call the closure that produces the workflow future
        let wf_future = (self.wf_function)(sender);

        // TODO: This is pointless right now -- either actually use async and suspend on awaits
        //   or just remove it.
        // Create a tokio runtime to block on the future
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(wf_future);

        let cmds = receiver.into_iter();

        let mut emit_these = vec![];
        for cmd in cmds {
            match cmd {
                TestWFCommand::WFCommand(c) => {
                    if let WFCommand::CompleteWorkflow(_) = &c {
                        self.sent_final_execution = true;
                    }
                    emit_these.push(c);
                }
                TestWFCommand::Waiting => {
                    // Ignore further commands since we're waiting on something
                    break;
                }
            }
        }

        event!(Level::DEBUG, msg = "Test wf driver emitting", ?emit_these);

        emit_these
    }

    fn signal(&mut self, _attribs: WorkflowExecutionSignaledEventAttributes) {}

    fn cancel(&mut self, _attribs: WorkflowExecutionCanceledEventAttributes) {}
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

    /// Request to create a timer. Returns true if the timer has fired, false if it hasn't yet.
    ///
    /// If `do_wait` is true, issue a waiting command if the timer is not finished.
    pub fn timer(&mut self, a: StartTimerCommandAttributes, do_wait: bool) -> bool {
        let finished = match self.twd_cache.unblocked_timers.entry(a.timer_id.clone()) {
            dashmap::mapref::entry::Entry::Occupied(existing) => *existing.get(),
            dashmap::mapref::entry::Entry::Vacant(v) => {
                let c = WFCommand::AddTimer(a);
                self.chan.send(c.into()).unwrap();
                v.insert(false);
                false
            }
        };
        if !finished && do_wait {
            self.chan.send(TestWFCommand::Waiting).unwrap();
        }
        finished
    }

    pub fn cancel_timer(&mut self, timer_id: &str) {
        let c = WFCommand::CancelTimer(CancelTimerCommandAttributes {
            timer_id: timer_id.to_string(),
        });
        self.chan.send(c.into()).unwrap();
    }

    pub fn send(&mut self, c: WFCommand) {
        self.chan.send(c.into()).unwrap();
    }
}
