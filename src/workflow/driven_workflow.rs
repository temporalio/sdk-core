use crate::{
    machines::WFCommand,
    protos::coresdk::wf_activation_job,
    protos::temporal::api::history::v1::{
        WorkflowExecutionCanceledEventAttributes, WorkflowExecutionSignaledEventAttributes,
        WorkflowExecutionStartedEventAttributes,
    },
};

/// Abstracts away the concept of an actual workflow implementation
///
/// TODO: More
pub struct DrivenWorkflow {
    started_attrs: Option<WorkflowExecutionStartedEventAttributes>,
    fetcher: Box<dyn WorkflowFetcher>,
}

impl<WF> From<Box<WF>> for DrivenWorkflow
where
    WF: WorkflowFetcher + 'static,
{
    fn from(wf: Box<WF>) -> Self {
        Self {
            started_attrs: None,
            fetcher: wf,
        }
    }
}

impl DrivenWorkflow {
    /// Start the workflow
    pub fn start(&mut self, attribs: WorkflowExecutionStartedEventAttributes) {}

    /// Signal the workflow
    fn signal(&mut self, attribs: WorkflowExecutionSignaledEventAttributes) {}

    /// Cancel the workflow
    fn cancel(&mut self, attribs: WorkflowExecutionCanceledEventAttributes) {}
}

impl WorkflowFetcher for DrivenWorkflow {
    fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand> {
        self.fetcher.fetch_workflow_iteration_output()
    }
}

impl ActivationListener for DrivenWorkflow {
    fn on_activation_job(&mut self, a: &wf_activation_job::Variant) {
        self.fetcher.on_activation_job(a)
    }
}

/// Implementors of this trait represent a way to fetch output from executing/iterating some
/// workflow code (or a mocked workflow).
pub trait WorkflowFetcher: ActivationListener + Send {
    /// Obtain any output from the workflow's recent execution(s). Because the lang sdk is
    /// responsible for calling workflow code as a result of receiving tasks from
    /// [crate::Core::poll_task], we cannot directly iterate it here. Thus implementations of this
    /// trait are expected to either buffer output or otherwise produce it on demand when this
    /// function is called.
    ///
    /// In the case of the real [WorkflowBridge] implementation, commands are simply pulled from
    /// a buffer that the language side sinks into when it calls [crate::Core::complete_task]
    fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand>;
}

/// Allows observers to listen to newly generated outgoing activation jobs. Used for testing, where
/// some activations must be handled before outgoing commands are issued to avoid deadlocking.
pub trait ActivationListener {
    fn on_activation_job(&mut self, _activation: &wf_activation_job::Variant) {}
}
