use super::Result;
use crate::{
    machines::{DrivenWorkflow, WFCommand},
    protos::temporal::api::history::v1::{
        WorkflowExecutionCanceledEventAttributes, WorkflowExecutionSignaledEventAttributes,
        WorkflowExecutionStartedEventAttributes,
    },
};
use futures::{
    channel::mpsc::{channel, Sender},
    executor::block_on,
    Future, StreamExt,
};
use tracing::Level;

/// This is a test only implementation of a [DrivenWorkflow] which has finer-grained control
/// over when commands are returned than a normal workflow would.
///
/// It replaces "TestEnitityTestListenerBase" in java which is pretty hard to follow.
#[derive(Debug)]
pub(in crate::machines) struct TestWorkflowDriver<F> {
    wf_function: F,
}

impl<F, Fut> TestWorkflowDriver<F>
where
    F: Fn(Sender<WFCommand>) -> Fut,
    Fut: Future<Output = ()>,
{
    pub(in crate::machines) fn new(workflow_fn: F) -> Self {
        Self {
            wf_function: workflow_fn,
        }
    }
    // TODO: Need some way to make the await return immediately for timer future even when it
    //   hasn't completed, when it should produce the "thunk" for waiting
    // pub(crate) fn new_timer(
    //     &self,
    //     attribs: StartTimerCommandAttributes,
    // ) -> (CancellableCommand, impl Future) {
    // }
}

impl<F, Fut> DrivenWorkflow for TestWorkflowDriver<F>
where
    F: Fn(Sender<WFCommand>) -> Fut,
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
        let (sender, receiver) = channel(1_000);
        // Call the closure that produces the workflow future
        let wf_future = (self.wf_function)(sender);

        // TODO: This block_on might cause issues later
        block_on(wf_future);
        let cmds: Vec<_> = block_on(async { receiver.collect().await });
        event!(Level::DEBUG, msg = "Test wf driver emitting", ?cmds);

        // Return only the last command
        Ok(vec![cmds.into_iter().last().unwrap()])
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
