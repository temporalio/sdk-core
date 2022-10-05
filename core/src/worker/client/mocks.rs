use super::*;
use futures::Future;

#[cfg(test)]
/// Create a mock client primed with basic necessary expectations
pub(crate) fn mock_workflow_client() -> MockWorkerClient {
    MockWorkerClient::new()
}

/// Create a mock manual client primed with basic necessary expectations
pub(crate) fn mock_manual_workflow_client() -> MockManualWorkerClient {
    MockManualWorkerClient::new()
}

// Need a version of the mock that can return futures so we can return potentially pending
// results. This is really annoying b/c of the async trait stuff. Need
// https://github.com/asomers/mockall/issues/189 to be fixed for it to go away.
mockall::mock! {
    pub(crate) ManualWorkerClient {}
    #[allow(unused)]
    impl WorkerClient for ManualWorkerClient {
        fn poll_workflow_task<'a, 'b>(&'a self, task_queue: String, is_sticky: bool)
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
    }
}
