//! Worker-specific client needs

pub(crate) mod mocks;

use std::ops::{Deref, DerefMut};
use temporal_client::{
    RawClientLike, WorkflowService, WorkflowTaskCompletion, RETRYABLE_ERROR_CODES,
};
use temporal_sdk_core_protos::{
    coresdk::workflow_commands::QueryResult,
    temporal::api::{
        common::v1::Payloads,
        enums::v1::{TaskQueueKind, WorkflowTaskFailedCause},
        failure::v1::Failure,
        taskqueue::v1::TaskQueue,
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
impl<T, SVC> WorkerClient for T
where
    T: RawClientLike<SvcType = SVC> + Send + Sync + 'static,
    // Look! Everyone's favorite giant block of tonic generic bounds!
    SVC: Send + Sync + Clone + 'static,
    SVC: tonic::client::GrpcService<tonic::body::BoxBody> + Send + Clone + 'static,
    SVC::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    SVC::Error: Into<tonic::codegen::StdError>,
    <SVC::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
    SVC::Future: Send,
{
    async fn poll_workflow_task(
        &self,
        task_queue: String,
        is_sticky: bool,
    ) -> Result<PollWorkflowTaskQueueResponse> {
        let request = PollWorkflowTaskQueueRequest {
            namespace: todo!().to_string(),
            task_queue: Some(TaskQueue {
                name: task_queue,
                kind: if is_sticky {
                    TaskQueueKind::Sticky
                } else {
                    TaskQueueKind::Normal
                } as i32,
            }),
            identity: todo!().to_string(),
            binary_checksum: todo!().to_string(),
            worker_versioning_build_id: todo!().to_string(),
        };

        Ok(self.poll_workflow_task_queue(request).await?.into_inner())
    }

    async fn poll_activity_task(
        &self,
        task_queue: String,
        max_tasks_per_sec: Option<f64>,
    ) -> Result<PollActivityTaskQueueResponse> {
        todo!()
    }

    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> Result<RespondWorkflowTaskCompletedResponse> {
        todo!()
    }

    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse> {
        todo!()
    }

    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse> {
        todo!()
    }

    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse> {
        todo!()
    }

    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse> {
        todo!()
    }

    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse> {
        todo!()
    }

    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse> {
        todo!()
    }

    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: QueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse> {
        todo!()
    }
}
