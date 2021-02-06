use crate::{
    machines::{ActivationListener, DrivenWorkflow, WFCommand},
    protos::temporal::api::history::v1::WorkflowExecutionCanceledEventAttributes,
    protos::temporal::api::history::v1::WorkflowExecutionSignaledEventAttributes,
    protos::temporal::api::history::v1::WorkflowExecutionStartedEventAttributes,
};
use std::sync::mpsc::{self, Receiver, Sender};

/// The [DrivenWorkflow] trait expects to be called to make progress, but the [CoreSDKService]
/// expects to be polled by the lang sdk. This struct acts as the bridge between the two, buffering
/// output from calls to [DrivenWorkflow] and offering them to [CoreSDKService]
#[derive(Debug)]
pub(crate) struct WorkflowBridge {
    // does wf id belong in here?
    started_attrs: Option<WorkflowExecutionStartedEventAttributes>,
    incoming_commands: Receiver<Vec<WFCommand>>,
}

impl WorkflowBridge {
    /// Create a new bridge, returning it and the sink used to send commands to it.
    pub(crate) fn new() -> (Self, Sender<Vec<WFCommand>>) {
        let (tx, rx) = mpsc::channel();
        (
            Self {
                started_attrs: None,
                incoming_commands: rx,
            },
            tx,
        )
    }
}

impl DrivenWorkflow for WorkflowBridge {
    #[instrument]
    fn start(&mut self, attribs: WorkflowExecutionStartedEventAttributes) -> Vec<WFCommand> {
        self.started_attrs = Some(attribs);
        vec![]
    }

    #[instrument]
    fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand> {
        self.incoming_commands
            .try_recv()
            .unwrap_or_else(|_| vec![WFCommand::NoCommandsFromLang])
    }

    fn signal(&mut self, _attribs: WorkflowExecutionSignaledEventAttributes) {
        unimplemented!()
    }

    fn cancel(&mut self, _attribs: WorkflowExecutionCanceledEventAttributes) {
        unimplemented!()
    }
}

// Real bridge doesn't actually need to listen
impl ActivationListener for WorkflowBridge {}
