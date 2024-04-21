use super::*;
use futures::Future;
use once_cell::sync::Lazy;
use std::sync::Arc;
use temporal_client::SlotManager;

static DEFAULT_WORKERS_REGISTRY: Lazy<Arc<SlotManager>> =
    Lazy::new(|| Arc::new(SlotManager::new()));

pub(crate) static DEFAULT_TEST_CAPABILITIES: &Capabilities = &Capabilities {
    signal_and_query_header: true,
    internal_error_differentiation: true,
    activity_failure_include_heartbeat: true,
    supports_schedules: true,
    encoded_failure_attributes: true,
    build_id_based_versioning: true,
    upsert_memo: true,
    eager_workflow_start: true,
    sdk_metadata: true,
    count_group_by_execution_status: false,
};

#[cfg(test)]
/// Create a mock client primed with basic necessary expectations
pub(crate) fn mock_workflow_client() -> MockWorkerClient {
    let mut r = MockWorkerClient::new();
    r.expect_capabilities()
        .returning(|| Some(DEFAULT_TEST_CAPABILITIES.clone()));
    r.expect_workers()
        .returning(|| DEFAULT_WORKERS_REGISTRY.clone());
    r.expect_is_mock().returning(|| true);
    r
}

/// Create a mock manual client primed with basic necessary expectations
pub(crate) fn mock_manual_workflow_client() -> MockManualWorkerClient {
    let mut r = MockManualWorkerClient::new();
    r.expect_capabilities()
        .returning(|| Some(DEFAULT_TEST_CAPABILITIES.clone()));
    r.expect_workers()
        .returning(|| DEFAULT_WORKERS_REGISTRY.clone());
    r.expect_is_mock().returning(|| true);
    r
}

// Need a version of the mock that can return futures so we can return potentially pending
// results. This is really annoying b/c of the async trait stuff. Need
// https://github.com/asomers/mockall/issues/189 to be fixed for it to go away.
mockall::mock! {
    pub(crate) ManualWorkerClient {}
    #[allow(unused)]
    impl WorkerClient for ManualWorkerClient {
        fn poll_workflow_task<'a, 'b>(&'a self, task_queue: TaskQueue)
            -> impl Future<Output = Result<PollWorkflowTaskQueueResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn poll_activity_task<'a, 'b>(&self, task_queue: String, max_tasks_per_sec: Option<f64>)
            -> impl Future<Output = Result<PollActivityTaskQueueResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn complete_workflow_task<'a, 'b>(
            &self,
            request: WorkflowTaskCompletion,
        ) -> impl Future<Output = Result<RespondWorkflowTaskCompletedResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn complete_activity_task<'a, 'b>(
            &self,
            task_token: TaskToken,
            result: Option<Payloads>,
        ) -> impl Future<Output = Result<RespondActivityTaskCompletedResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn cancel_activity_task<'a, 'b>(
            &self,
            task_token: TaskToken,
            details: Option<Payloads>,
        ) -> impl Future<Output = Result<RespondActivityTaskCanceledResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn fail_activity_task<'a, 'b>(
            &self,
            task_token: TaskToken,
            failure: Option<Failure>,
        ) -> impl Future<Output = Result<RespondActivityTaskFailedResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn fail_workflow_task<'a, 'b>(
            &self,
            task_token: TaskToken,
            cause: WorkflowTaskFailedCause,
            failure: Option<Failure>,
        ) -> impl Future<Output = Result<RespondWorkflowTaskFailedResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn record_activity_heartbeat<'a, 'b>(
           &self,
           task_token: TaskToken,
           details: Option<Payloads>,
        ) -> impl Future<Output = Result<RecordActivityTaskHeartbeatResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn get_workflow_execution_history<'a, 'b>(
            &self,
            workflow_id: String,
            run_id: Option<String>,
            page_token: Vec<u8>
        ) -> impl Future<Output = Result<GetWorkflowExecutionHistoryResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn respond_legacy_query<'a, 'b>(
            &self,
            task_token: TaskToken,
            query_result: QueryResult,
        ) -> impl Future<Output = Result<RespondQueryTaskCompletedResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn replace_client(&self, new_client: RetryClient<Client>);

        fn capabilities(&self) -> Option<get_system_info_response::Capabilities>;

        fn workers(&self) -> Arc<SlotManager>;

        fn is_mock(&self) -> bool;
    }
}
