use crate::machines::WFCommand;
use std::collections::VecDeque;
use temporal_sdk_core_protos::{
    coresdk::workflow_activation::{
        wf_activation_job, CancelWorkflow, SignalWorkflow, WfActivationJob,
    },
    temporal::api::history::v1::WorkflowExecutionStartedEventAttributes,
};

/// Abstracts away the concept of an actual workflow implementation, handling sending it new
/// jobs and fetching output from it.
pub struct DrivenWorkflow {
    started_attrs: Option<WorkflowExecutionStartedEventAttributes>,
    fetcher: Box<dyn WorkflowFetcher>,
    /// Outgoing activation jobs that need to be sent to the lang sdk
    outgoing_wf_activation_jobs: VecDeque<wf_activation_job::Variant>,
}

impl<WF> From<Box<WF>> for DrivenWorkflow
where
    WF: WorkflowFetcher + 'static,
{
    fn from(wf: Box<WF>) -> Self {
        Self {
            started_attrs: None,
            fetcher: wf,
            outgoing_wf_activation_jobs: Default::default(),
        }
    }
}

impl DrivenWorkflow {
    /// Start the workflow
    pub fn start(&mut self, attribs: WorkflowExecutionStartedEventAttributes) {
        debug!(run_id = %attribs.original_execution_run_id, "Driven WF start");
        self.started_attrs = Some(attribs);
    }

    /// Enqueue a new job to be sent to the driven workflow
    pub fn send_job(&mut self, job: wf_activation_job::Variant) {
        self.outgoing_wf_activation_jobs.push_back(job);
    }

    /// Drain all pending jobs, so that they may be sent to the driven workflow
    pub fn drain_jobs(&mut self) -> Vec<WfActivationJob> {
        self.outgoing_wf_activation_jobs
            .drain(..)
            .map(Into::into)
            .collect()
    }

    /// Signal the workflow
    pub fn signal(&mut self, signal: SignalWorkflow) {
        self.send_job(wf_activation_job::Variant::SignalWorkflow(signal));
    }

    /// Cancel the workflow
    pub fn cancel(&mut self, attribs: CancelWorkflow) {
        self.send_job(wf_activation_job::Variant::CancelWorkflow(attribs));
    }
}

#[async_trait::async_trait]
impl WorkflowFetcher for DrivenWorkflow {
    async fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand> {
        self.fetcher.fetch_workflow_iteration_output().await
    }
}

/// Implementors of this trait represent a way to fetch output from executing/iterating some
/// workflow code (or a mocked workflow).
#[async_trait::async_trait]
pub trait WorkflowFetcher: Send {
    /// Obtain any output from the workflow's recent execution(s). Because the lang sdk is
    /// responsible for calling workflow code as a result of receiving tasks from
    /// [crate::Core::poll_task], we cannot directly iterate it here. Thus implementations of this
    /// trait are expected to either buffer output or otherwise produce it on demand when this
    /// function is called.
    ///
    /// In the case of the real [WorkflowBridge] implementation, commands are simply pulled from
    /// a buffer that the language side sinks into when it calls [crate::Core::complete_task]
    async fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand>;
}
