//! Worker-specific client needs

pub(crate) mod mocks;

use std::{
    borrow::Borrow,
    ops::{Deref, DerefMut},
};
use temporal_client::{ClientOptions, WorkflowClientTrait, WorkflowTaskCompletion};
use temporal_sdk_core_protos::{
    coresdk::workflow_commands::QueryResult,
    temporal::api::{
        common::v1::Payloads, enums::v1::WorkflowTaskFailedCause, failure::v1::Failure,
        workflowservice::v1::*,
    },
    TaskToken,
};

type Result<T, E = tonic::Status> = std::result::Result<T, E>;

/// Contains everything a worker needs to interact with the server
pub(crate) struct WorkerClientBag {
    client: Box<dyn WorkerClient>,
    namespace: String,
    options: ClientOptions,
}

impl WorkerClientBag {
    pub fn new(client: Box<dyn WorkerClient>, namespace: String, options: ClientOptions) -> Self {
        Self {
            client,
            namespace,
            options,
        }
    }
    pub fn options(&self) -> &ClientOptions {
        &self.options
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }
}
impl Deref for WorkerClientBag {
    type Target = dyn WorkerClient;

    fn deref(&self) -> &Self::Target {
        &*self.client
    }
}
impl DerefMut for WorkerClientBag {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut *self.client
    }
}
#[cfg(test)]
impl<T> From<T> for WorkerClientBag
where
    T: WorkerClient + 'static,
{
    fn from(c: T) -> Self {
        use crate::worker::client::mocks::fake_sg_opts;
        use temporal_sdk_core_test_utils::NAMESPACE;

        WorkerClientBag::new(Box::new(c), NAMESPACE.to_string(), fake_sg_opts())
    }
}

/// This trait contains everything workers need to interact with Temporal, and hence provides a
/// minimal mocking surface.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub(crate) trait WorkerClient: Sync + Send {
    /// Fetch new workflow tasks from the provided queue. Should block indefinitely if there is no
    /// work.
    async fn poll_workflow_task(
        &self,
        task_queue: String,
        is_sticky: bool,
    ) -> Result<PollWorkflowTaskQueueResponse>;

    /// Fetch new activity tasks from the provided queue. Should block indefinitely if there is no
    /// work.
    async fn poll_activity_task(
        &self,
        task_queue: String,
        max_tasks_per_sec: Option<f64>,
    ) -> Result<PollActivityTaskQueueResponse>;

    /// Complete a workflow activation.
    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> Result<RespondWorkflowTaskCompletedResponse>;

    /// Complete activity task by sending response to the server. `task_token` contains activity
    /// identifier that would've been received from polling for an activity task. `result` is a blob
    /// that contains activity response.
    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse>;

    /// Report activity task heartbeat by sending details to the server. `task_token` contains
    /// activity identifier that would've been received from polling for an activity task. `result`
    /// contains `cancel_requested` flag, which if set to true indicates that activity has been
    /// cancelled.
    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse>;

    /// Cancel activity task by sending response to the server. `task_token` contains activity
    /// identifier that would've been received from polling for an activity task. `details` is a
    /// blob that provides arbitrary user defined cancellation info.
    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse>;

    /// Fail activity task by sending response to the server. `task_token` contains activity
    /// identifier that would've been received from polling for an activity task. `failure` provides
    /// failure details, such as message, cause and stack trace.
    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse>;

    /// Fail task by sending the failure to the server. `task_token` is the task token that would've
    /// been received from polling for a workflow activation.
    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse>;

    /// Get history for a particular workflow run
    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse>;

    /// Respond to a legacy query-only workflow task
    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: QueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse>;
}

#[async_trait::async_trait]
impl<'a, T> WorkerClient for T
where
    T: Borrow<dyn WorkflowClientTrait + 'a + Send + Sync> + Send + Sync,
{
    async fn poll_workflow_task(
        &self,
        task_queue: String,
        is_sticky: bool,
    ) -> Result<PollWorkflowTaskQueueResponse> {
        WorkflowClientTrait::poll_workflow_task(self.borrow(), task_queue, is_sticky).await
    }

    async fn poll_activity_task(
        &self,
        task_queue: String,
        max_tasks_per_sec: Option<f64>,
    ) -> Result<PollActivityTaskQueueResponse> {
        WorkflowClientTrait::poll_activity_task(self.borrow(), task_queue, max_tasks_per_sec).await
    }

    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> Result<RespondWorkflowTaskCompletedResponse> {
        WorkflowClientTrait::complete_workflow_task(self.borrow(), request).await
    }

    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse> {
        WorkflowClientTrait::complete_activity_task(self.borrow(), task_token, result).await
    }

    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse> {
        WorkflowClientTrait::record_activity_heartbeat(self.borrow(), task_token, details).await
    }

    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse> {
        WorkflowClientTrait::cancel_activity_task(self.borrow(), task_token, details).await
    }

    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse> {
        WorkflowClientTrait::fail_activity_task(self.borrow(), task_token, failure).await
    }

    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse> {
        WorkflowClientTrait::fail_workflow_task(self.borrow(), task_token, cause, failure).await
    }

    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse> {
        WorkflowClientTrait::get_workflow_execution_history(
            self.borrow(),
            workflow_id,
            run_id,
            page_token,
        )
        .await
    }

    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: QueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse> {
        WorkflowClientTrait::respond_legacy_query(self.borrow(), task_token, query_result).await
    }
}
