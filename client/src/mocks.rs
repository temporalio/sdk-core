//! Helpers for mocking

use super::*;
use crate::{MockServerGatewayApis, ServerGatewayOptions, ServerGatewayOptionsBuilder};
use std::str::FromStr;
use url::Url;

/// Create a mock client primed with basic necessary expectations
pub fn mock_gateway() -> MockServerGatewayApis {
    let mut mg = MockServerGatewayApis::new();
    mg.expect_get_options().return_const(fake_sg_opts());
    mg.expect_namespace()
        .return_const("fake_namespace".to_string());
    mg
}

/// Create a mock manual client primed with basic necessary expectations
pub fn mock_manual_gateway() -> MockManualGateway {
    let mut mg = MockManualGateway::new();
    mg.expect_get_options().return_const(fake_sg_opts());
    mg.expect_namespace()
        .return_const("fake_namespace".to_string());
    mg
}

/// Returns some totally fake client options for use with mock clients
pub fn fake_sg_opts() -> ServerGatewayOptions {
    ServerGatewayOptionsBuilder::default()
        .target_url(Url::from_str("https://fake").unwrap())
        .client_name("fake_client".to_string())
        .client_version("fake_version".to_string())
        .worker_binary_id("fake_binid".to_string())
        .build()
        .unwrap()
}

// Need a version of the mock that can return futures so we can return potentially pending
// results. This is really annoying b/c of the async trait stuff. Need
// https://github.com/asomers/mockall/issues/189 to be fixed for it to go away.
mockall::mock! {
    pub ManualGateway {}
    impl ServerGatewayApis for ManualGateway {
        fn start_workflow<'a, 'b>(
            &self,
            input: Vec<Payload>,
            task_queue: String,
            workflow_id: String,
            workflow_type: String,
            task_timeout: Option<Duration>,
        ) -> impl Future<Output = Result<StartWorkflowExecutionResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn poll_workflow_task<'a, 'b>(&'a self, task_queue: String, is_sticky: bool)
            -> impl Future<Output = Result<PollWorkflowTaskQueueResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn poll_activity_task<'a, 'b>(&self, task_queue: String)
            -> impl Future<Output = Result<PollActivityTaskQueueResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn reset_sticky_task_queue<'a, 'b>(
            &self,
            workflow_id: String,
            run_id: String,
        ) -> impl Future<Output = Result<ResetStickyTaskQueueResponse>> + Send + 'b
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

        fn signal_workflow_execution<'a, 'b>(
            &self,
            workflow_id: String,
            run_id: String,
            signal_name: String,
            payloads: Option<Payloads>,
        ) -> impl Future<Output = Result<SignalWorkflowExecutionResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn record_activity_heartbeat<'a, 'b>(
           &self,
           task_token: TaskToken,
           details: Option<Payloads>,
        ) -> impl Future<Output = Result<RecordActivityTaskHeartbeatResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn query_workflow_execution<'a, 'b>(
            &self,
            workflow_id: String,
            run_id: String,
            query: WorkflowQuery,
        ) -> impl Future<Output = Result<QueryWorkflowResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn describe_workflow_execution<'a, 'b>(
            &self,
            workflow_id: String,
            run_id: Option<String>,
        ) -> impl Future<Output = Result<DescribeWorkflowExecutionResponse>> + Send + 'b
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

        fn cancel_workflow_execution<'a, 'b>(
            &self,
            workflow_id: String,
            run_id: Option<String>,
        ) -> impl Future<Output = Result<RequestCancelWorkflowExecutionResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn terminate_workflow_execution<'a, 'b>(
            &self,
            workflow_id: String,
            run_id: Option<String>,
        ) -> impl Future<Output = Result<TerminateWorkflowExecutionResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn list_namespaces<'a, 'b>(
            &self,
        ) -> impl Future<Output = Result<ListNamespacesResponse>> + Send + 'b
            where 'a: 'b, Self: 'b;

        fn get_options(&self) -> &ServerGatewayOptions;
        fn namespace(&self) -> &str;
    }
}
