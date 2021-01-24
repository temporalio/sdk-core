use super::Result;
use crate::{
    machines::{DrivenWorkflow, WFCommand},
    protos::temporal::api::history::v1::{
        WorkflowExecutionCanceledEventAttributes, WorkflowExecutionSignaledEventAttributes,
        WorkflowExecutionStartedEventAttributes,
    },
};
use std::{
    sync::mpsc::{channel, Receiver, Sender},
    time::Duration,
};
use tracing::Level;

// TODO: This will need to be broken out into it's own place and evolved / made more generic as
//   we learn more. It replaces "TestEnitityTestListenerBase" in java which is pretty hard to
//   follow.
#[derive(Debug)]
pub(crate) struct TestWorkflowDriver {
    /// A queue of command lists to return upon calls to [DrivenWorkflow::iterate_wf]. This
    /// gives us more manual control than actually running the workflow for real would, for
    /// example allowing us to simulate nondeterminism.
    iteration_results: Receiver<Vec<WFCommand>>,
    iteration_sender: Sender<Vec<WFCommand>>,

    /// The entire history passed in when we were constructed
    full_history: Vec<Vec<WFCommand>>,
}

impl TestWorkflowDriver {
    pub(in crate::machines) fn new<I>(iteration_results: I) -> Self
    where
        I: IntoIterator<Item = Vec<WFCommand>>,
    {
        let (sender, receiver) = channel();
        Self {
            iteration_results: receiver,
            iteration_sender: sender,
            full_history: iteration_results.into_iter().collect(),
        }
    }
}

impl DrivenWorkflow for TestWorkflowDriver {
    #[instrument]
    fn start(
        &self,
        attribs: WorkflowExecutionStartedEventAttributes,
    ) -> Result<Vec<WFCommand>, anyhow::Error> {
        self.full_history
            .iter()
            .for_each(|cmds| self.iteration_sender.send(cmds.to_vec()).unwrap());
        Ok(vec![])
    }

    #[instrument]
    fn iterate_wf(&self) -> Result<Vec<WFCommand>, anyhow::Error> {
        // Timeout exists just to make blocking obvious. We should never block.
        let cmd = self
            .iteration_results
            .recv_timeout(Duration::from_millis(10))?;
        event!(Level::DEBUG, msg = "Test wf driver emitting", ?cmd);
        Ok(cmd)
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
