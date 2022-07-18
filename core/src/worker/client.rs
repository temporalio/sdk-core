//! Worker-specific client needs

pub(crate) mod mocks;

use std::{
    borrow::Borrow,
    ops::{Deref, DerefMut},
};
use temporal_client::{WorkflowClientTrait, WorkflowTaskCompletion, RETRYABLE_ERROR_CODES};
use temporal_sdk_core_protos::{
    coresdk::workflow_commands::QueryResult,
    temporal::api::{
        common::v1::Payloads, enums::v1::WorkflowTaskFailedCause, failure::v1::Failure,
        workflowservice::v1::*,
    },
    TaskToken,
};
use tonic::Code;

type Result<T, E = tonic::Status> = std::result::Result<T, E>;

/// Returns true if the network error should not be reported to lang. This can happen if we've
/// exceeded the max number of retries, and we prefer to just warn rather than blowing up lang.
pub(crate) fn should_swallow_net_error(err: &tonic::Status) -> bool {
    RETRYABLE_ERROR_CODES.contains(&err.code())
        || matches!(err.code(), Code::Cancelled | Code::DeadlineExceeded)
}

/// Contains everything a worker needs to interact with the server
pub(crate) struct WorkerClientBag {
    client: Box<dyn WorkerClient>,
    namespace: String,
}

impl WorkerClientBag {
    pub fn new(client: Box<dyn WorkerClient>, namespace: String) -> Self {
        Self { client, namespace }
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
        use temporal_sdk_core_test_utils::NAMESPACE;

        WorkerClientBag::new(Box::new(c), NAMESPACE.to_string())
    }
}

/// This trait contains everything workers need to interact with Temporal, and hence provides a
/// minimal mocking surface. Delegates to [WorkflowClientTrait] so see that for details.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub(crate) trait WorkerClient: Sync + Send {
    async fn poll_workflow_task(
        &self,
        task_queue: String,
        is_sticky: bool,
    ) -> Result<PollWorkflowTaskQueueResponse>;
    async fn poll_activity_task(
        &self,
        task_queue: String,
        max_tasks_per_sec: Option<f64>,
    ) -> Result<PollActivityTaskQueueResponse>;
    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> Result<RespondWorkflowTaskCompletedResponse>;
    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse>;
    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse>;
    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse>;
    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse>;
    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse>;
    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse>;
    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: QueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse>;
}

#[async_trait::async_trait]
impl<'a, T> WorkerClient for T
where
    // TODO: This should be workflow service... no reason to marry worker trait to sdk client trait
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
