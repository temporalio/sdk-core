use crate::{
    telemetry::VecDisplayer,
    worker::workflow::{WFCommand, WorkflowFetcher},
};
use std::sync::mpsc::{self, Receiver, Sender};

/// The [DrivenWorkflow] trait expects to be called to make progress, but the [CoreSDKService]
/// expects to be polled by the lang sdk. This struct acts as the bridge between the two, buffering
/// output from calls to [DrivenWorkflow] and offering them to [CoreSDKService]
#[derive(Debug)]
pub(crate) struct WorkflowBridge {
    incoming_commands: Receiver<Vec<WFCommand>>,
}

impl WorkflowBridge {
    /// Create a new bridge, returning it and the sink used to send commands to it.
    pub(crate) fn new() -> (Self, Sender<Vec<WFCommand>>) {
        let (tx, rx) = mpsc::channel();
        (
            Self {
                incoming_commands: rx,
            },
            tx,
        )
    }
}

#[async_trait::async_trait]
impl WorkflowFetcher for WorkflowBridge {
    async fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand> {
        let in_cmds = self.incoming_commands.try_recv();

        let in_cmds = in_cmds.unwrap_or_else(|_| vec![WFCommand::NoCommandsFromLang]);
        debug!(in_cmds = %in_cmds.display(), "wf bridge iteration fetch");
        in_cmds
    }
}
