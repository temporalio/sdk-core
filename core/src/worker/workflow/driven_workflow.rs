use crate::worker::workflow::{OutgoingJob, WFCommand, WorkflowStartedInfo};
use prost_types::Timestamp;
use temporal_sdk_core_protos::{
    coresdk::workflow_activation::{start_workflow_from_attribs, WorkflowActivationJob},
    temporal::api::history::v1::WorkflowExecutionStartedEventAttributes,
    utilities::TryIntoOrNone,
};

/// Abstracts away the concept of an actual workflow implementation, handling sending it new
/// jobs and fetching output from it.
pub struct DrivenWorkflow {
    started_attrs: Option<WorkflowStartedInfo>,
    fetcher: Box<dyn WorkflowFetcher>,
    /// Outgoing activation jobs that need to be sent to the lang sdk
    outgoing_wf_activation_jobs: Vec<OutgoingJob>,
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
    pub fn start(
        &mut self,
        workflow_id: String,
        randomness_seed: u64,
        start_time: Timestamp,
        attribs: WorkflowExecutionStartedEventAttributes,
    ) {
        debug!(run_id = %attribs.original_execution_run_id, "Driven WF start");
        let started_info = WorkflowStartedInfo {
            workflow_task_timeout: attribs.workflow_task_timeout.clone().try_into_or_none(),
            workflow_execution_timeout: attribs
                .workflow_execution_timeout
                .clone()
                .try_into_or_none(),
            memo: attribs.memo.clone(),
            search_attrs: attribs.search_attributes.clone(),
            retry_policy: attribs.retry_policy.clone(),
        };
        self.send_job(
            start_workflow_from_attribs(attribs, workflow_id, randomness_seed, start_time).into(),
        );
        self.started_attrs = Some(started_info);
    }

    /// Return the attributes from the workflow execution started event if this workflow has started
    pub fn get_started_info(&self) -> Option<&WorkflowStartedInfo> {
        self.started_attrs.as_ref()
    }

    /// Enqueue a new job to be sent to the driven workflow
    pub(super) fn send_job(&mut self, job: OutgoingJob) {
        self.outgoing_wf_activation_jobs.push(job);
    }

    /// Observe pending jobs
    pub(super) fn peek_pending_jobs(&self) -> &[OutgoingJob] {
        self.outgoing_wf_activation_jobs.as_slice()
    }

    /// Drain all pending jobs, so that they may be sent to the driven workflow
    pub fn drain_jobs(&mut self) -> Vec<WorkflowActivationJob> {
        self.outgoing_wf_activation_jobs
            .drain(..)
            .map(Into::into)
            .collect()
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
