use crate::{
    ClientOptions, RawClientLikeUser, Result, RetryConfig, WorkflowClientTrait, WorkflowOptions,
    WorkflowTaskCompletion,
};
use backoff::{backoff::Backoff, ExponentialBackoff};
use futures_retry::{ErrorHandler, FutureRetry, RetryPolicy};
use std::{fmt::Debug, future::Future, time::Duration};
use temporal_sdk_core_protos::{
    coresdk::workflow_commands::QueryResult,
    temporal::api::{
        common::v1::{Payload, Payloads},
        enums::v1::WorkflowTaskFailedCause,
        failure::v1::Failure,
        query::v1::WorkflowQuery,
        workflowservice::v1::*,
    },
    TaskToken,
};
use tonic::Code;

/// List of gRPC error codes that client will retry.
pub const RETRYABLE_ERROR_CODES: [Code; 7] = [
    Code::DataLoss,
    Code::Internal,
    Code::Unknown,
    Code::ResourceExhausted,
    Code::Aborted,
    Code::OutOfRange,
    Code::Unavailable,
];

/// A wrapper for a [WorkflowClientTrait] or [crate::WorkflowService] implementor which performs
/// auto-retries
#[derive(Debug, Clone)]
pub struct RetryClient<SG> {
    client: SG,
    retry_config: RetryConfig,
}

impl<SG> RetryClient<SG> {
    /// Use the provided retry config with the provided client
    pub const fn new(client: SG, retry_config: RetryConfig) -> Self {
        Self {
            client,
            retry_config,
        }
    }
}

impl<SG> RetryClient<SG> {
    /// Return the inner client type
    pub fn get_client(&self) -> &SG {
        &self.client
    }

    /// Return the inner client type mutably
    pub fn get_client_mut(&mut self) -> &mut SG {
        &mut self.client
    }

    /// Disable retry and return the inner client type
    pub fn into_inner(self) -> SG {
        self.client
    }

    /// Wraps a call to the underlying client with retry capability.
    ///
    /// This is the "old" path used by higher-level [WorkflowClientTrait] implementors
    pub(crate) async fn call_with_retry<R, F, Fut>(
        &self,
        factory: F,
        call_name: &'static str,
    ) -> Result<R>
    where
        F: Fn() -> Fut + Unpin,
        Fut: Future<Output = Result<R>>,
    {
        let rtc = self.get_retry_config(call_name);
        let res = Self::make_future_retry(rtc, factory, call_name).await;
        Ok(res.map_err(|(e, _attempt)| e)?.0)
    }

    pub(crate) fn get_retry_config(&self, call_name: &'static str) -> RetryConfig {
        let call_type = Self::determine_call_type(call_name);
        match call_type {
            CallType::Normal => self.retry_config.clone(),
            CallType::LongPoll => RetryConfig::poll_retry_policy(),
        }
    }

    pub(crate) fn make_future_retry<R, F, Fut>(
        rtc: RetryConfig,
        factory: F,
        call_name: &'static str,
    ) -> FutureRetry<F, TonicErrorHandler>
    where
        F: FnMut() -> Fut + Unpin,
        Fut: Future<Output = Result<R>>,
    {
        let call_type = Self::determine_call_type(call_name);
        FutureRetry::new(factory, TonicErrorHandler::new(rtc, call_type, call_name))
    }

    fn determine_call_type(call_name: &str) -> CallType {
        match call_name {
            "poll_workflow_task" | "poll_activity_task" => CallType::LongPoll,
            _ => CallType::Normal,
        }
    }
}

#[derive(Debug)]
pub(crate) struct TonicErrorHandler {
    backoff: ExponentialBackoff,
    max_retries: usize,
    call_type: CallType,
    call_name: &'static str,
}
impl TonicErrorHandler {
    fn new(cfg: RetryConfig, call_type: CallType, call_name: &'static str) -> Self {
        Self {
            max_retries: cfg.max_retries,
            call_type,
            call_name,
            backoff: cfg.into(),
        }
    }

    const fn should_log_retry_warning(&self, cur_attempt: usize) -> bool {
        // Warn on more than 5 retries for unlimited retrying
        if self.max_retries == 0 && cur_attempt > 5 {
            return true;
        }
        // Warn if the attempts are more than 50% of max retries
        if self.max_retries > 0 && cur_attempt * 2 >= self.max_retries {
            return true;
        }
        false
    }
}
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum CallType {
    Normal,
    LongPoll,
}

impl ErrorHandler<tonic::Status> for TonicErrorHandler {
    type OutError = tonic::Status;

    fn handle(&mut self, current_attempt: usize, e: tonic::Status) -> RetryPolicy<tonic::Status> {
        // 0 max retries means unlimited retries
        if self.max_retries > 0 && current_attempt >= self.max_retries {
            return RetryPolicy::ForwardError(e);
        }

        // Long polls are OK with being cancelled or running into the timeout because there's
        // nothing to do but retry anyway
        let long_poll_allowed = self.call_type == CallType::LongPoll
            && [Code::Cancelled, Code::DeadlineExceeded].contains(&e.code());

        if RETRYABLE_ERROR_CODES.contains(&e.code()) || long_poll_allowed {
            if current_attempt == 1 {
                debug!(error=?e, "gRPC call {} failed on first attempt", self.call_name);
            } else if self.should_log_retry_warning(current_attempt) {
                warn!(error=?e, "gRPC call {} retried {} times", self.call_name, current_attempt);
            }

            match self.backoff.next_backoff() {
                None => RetryPolicy::ForwardError(e), // None is returned when we've ran out of time
                Some(backoff) => {
                    if cfg!(test) {
                        // Allow unit tests to do lots of retries quickly. This does *not* apply
                        // during integration testing, importantly.
                        RetryPolicy::WaitRetry(Duration::from_millis(1))
                    } else {
                        RetryPolicy::WaitRetry(backoff)
                    }
                }
            }
        } else {
            RetryPolicy::ForwardError(e)
        }
    }
}

macro_rules! retry_call {
    ($myself:ident, $call_name:ident) => { retry_call!($myself, $call_name,) };
    ($myself:ident, $call_name:ident, $($args:expr),*) => {{
        let call_name_str = stringify!($call_name);
        let fact = || { $myself.get_client().$call_name($($args,)*)};
        $myself.call_with_retry(fact, call_name_str).await
    }}
}

// Ideally, this would be auto-implemented for anything that implements the raw client, but that
// breaks all our retry clients which use a mock since it's based on this trait currently. Ideally
// we would create an automock for the WorkflowServiceClient copy-paste trait and use that, but
// that's a huge pain. Maybe one day tonic will provide traits.
#[async_trait::async_trait]
impl<SG> WorkflowClientTrait for RetryClient<SG>
where
    SG: WorkflowClientTrait + Send + Sync + 'static,
{
    async fn start_workflow(
        &self,
        input: Vec<Payload>,
        task_queue: String,
        workflow_id: String,
        workflow_type: String,
        options: WorkflowOptions,
    ) -> Result<StartWorkflowExecutionResponse> {
        retry_call!(
            self,
            start_workflow,
            input.clone(),
            task_queue.clone(),
            workflow_id.clone(),
            workflow_type.clone(),
            options.clone()
        )
    }

    async fn poll_workflow_task(
        &self,
        task_queue: String,
        is_sticky: bool,
    ) -> Result<PollWorkflowTaskQueueResponse> {
        retry_call!(self, poll_workflow_task, task_queue.clone(), is_sticky)
    }

    async fn poll_activity_task(
        &self,
        task_queue: String,
        max_tasks_per_sec: Option<f64>,
    ) -> Result<PollActivityTaskQueueResponse> {
        retry_call!(
            self,
            poll_activity_task,
            task_queue.clone(),
            max_tasks_per_sec
        )
    }

    async fn reset_sticky_task_queue(
        &self,
        workflow_id: String,
        run_id: String,
    ) -> Result<ResetStickyTaskQueueResponse> {
        retry_call!(
            self,
            reset_sticky_task_queue,
            workflow_id.clone(),
            run_id.clone()
        )
    }

    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> Result<RespondWorkflowTaskCompletedResponse> {
        retry_call!(self, complete_workflow_task, request.clone())
    }

    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse> {
        retry_call!(
            self,
            complete_activity_task,
            task_token.clone(),
            result.clone()
        )
    }

    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse> {
        retry_call!(
            self,
            record_activity_heartbeat,
            task_token.clone(),
            details.clone()
        )
    }

    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse> {
        retry_call!(
            self,
            cancel_activity_task,
            task_token.clone(),
            details.clone()
        )
    }

    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse> {
        retry_call!(
            self,
            fail_activity_task,
            task_token.clone(),
            failure.clone()
        )
    }

    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse> {
        retry_call!(
            self,
            fail_workflow_task,
            task_token.clone(),
            cause,
            failure.clone()
        )
    }

    async fn signal_workflow_execution(
        &self,
        workflow_id: String,
        run_id: String,
        signal_name: String,
        payloads: Option<Payloads>,
    ) -> Result<SignalWorkflowExecutionResponse> {
        retry_call!(
            self,
            signal_workflow_execution,
            workflow_id.clone(),
            run_id.clone(),
            signal_name.clone(),
            payloads.clone()
        )
    }

    async fn query_workflow_execution(
        &self,
        workflow_id: String,
        run_id: String,
        query: WorkflowQuery,
    ) -> Result<QueryWorkflowResponse> {
        retry_call!(
            self,
            query_workflow_execution,
            workflow_id.clone(),
            run_id.clone(),
            query.clone()
        )
    }

    async fn describe_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<DescribeWorkflowExecutionResponse> {
        retry_call!(
            self,
            describe_workflow_execution,
            workflow_id.clone(),
            run_id.clone()
        )
    }

    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse> {
        retry_call!(
            self,
            get_workflow_execution_history,
            workflow_id.clone(),
            run_id.clone(),
            page_token.clone()
        )
    }

    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: QueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse> {
        retry_call!(
            self,
            respond_legacy_query,
            task_token.clone(),
            query_result.clone()
        )
    }

    async fn cancel_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        reason: String,
    ) -> Result<RequestCancelWorkflowExecutionResponse> {
        retry_call!(
            self,
            cancel_workflow_execution,
            workflow_id.clone(),
            run_id.clone(),
            reason.clone()
        )
    }

    async fn terminate_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<TerminateWorkflowExecutionResponse> {
        retry_call!(
            self,
            terminate_workflow_execution,
            workflow_id.clone(),
            run_id.clone()
        )
    }

    async fn list_namespaces(&self) -> Result<ListNamespacesResponse> {
        retry_call!(self, list_namespaces,)
    }

    fn get_options(&self) -> &ClientOptions {
        self.client.get_options()
    }

    fn namespace(&self) -> &str {
        self.client.namespace()
    }
}

impl<C> RawClientLikeUser for RetryClient<C>
where
    C: RawClientLikeUser,
{
    type RawClientT = C::RawClientT;

    fn wf_svc(&self) -> Self::RawClientT {
        self.client.wf_svc()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MockWorkflowClientTrait;
    use tonic::Status;

    #[tokio::test]
    async fn non_retryable_errors() {
        for code in [
            Code::InvalidArgument,
            Code::NotFound,
            Code::AlreadyExists,
            Code::PermissionDenied,
            Code::FailedPrecondition,
            Code::Cancelled,
            Code::DeadlineExceeded,
            Code::Unauthenticated,
            Code::Unimplemented,
        ] {
            let mut mock_client = MockWorkflowClientTrait::new();
            mock_client
                .expect_cancel_activity_task()
                .returning(move |_, _| Err(Status::new(code, "non-retryable failure")))
                .times(1);
            let retry_client = RetryClient::new(mock_client, Default::default());
            let result = retry_client
                .cancel_activity_task(vec![1].into(), None)
                .await;
            // Expecting an error after a single attempt, since there was a non-retryable error.
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn long_poll_non_retryable_errors() {
        for code in [
            Code::InvalidArgument,
            Code::NotFound,
            Code::AlreadyExists,
            Code::PermissionDenied,
            Code::FailedPrecondition,
            Code::Unauthenticated,
            Code::Unimplemented,
        ] {
            let mut mock_client = MockWorkflowClientTrait::new();
            mock_client
                .expect_poll_workflow_task()
                .returning(move |_, _| Err(Status::new(code, "non-retryable failure")))
                .times(1);
            mock_client
                .expect_poll_activity_task()
                .returning(move |_, _| Err(Status::new(code, "non-retryable failure")))
                .times(1);
            let retry_client = RetryClient::new(mock_client, Default::default());
            let result = retry_client
                .poll_workflow_task("tq".to_string(), false)
                .await;
            assert!(result.is_err());
            let result = retry_client
                .poll_activity_task("tq".to_string(), None)
                .await;
            assert!(result.is_err());
        }
    }

    #[tokio::test]
    async fn retryable_errors() {
        for code in RETRYABLE_ERROR_CODES {
            let mut mock_client = MockWorkflowClientTrait::new();
            mock_client
                .expect_cancel_activity_task()
                .returning(move |_, _| Err(Status::new(code, "retryable failure")))
                .times(3);
            mock_client
                .expect_cancel_activity_task()
                .returning(|_, _| Ok(Default::default()))
                .times(1);

            let retry_client = RetryClient::new(mock_client, Default::default());
            let result = retry_client
                .cancel_activity_task(vec![1].into(), None)
                .await;
            // Expecting successful response after retries
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn long_poll_retries_forever() {
        let mut mock_client = MockWorkflowClientTrait::new();
        mock_client
            .expect_poll_workflow_task()
            .returning(move |_, _| Err(Status::new(Code::Unknown, "retryable failure")))
            .times(50);
        mock_client
            .expect_poll_workflow_task()
            .returning(|_, _| Ok(Default::default()))
            .times(1);
        mock_client
            .expect_poll_activity_task()
            .returning(move |_, _| Err(Status::new(Code::Unknown, "retryable failure")))
            .times(50);
        mock_client
            .expect_poll_activity_task()
            .returning(|_, _| Ok(Default::default()))
            .times(1);

        let retry_client = RetryClient::new(mock_client, Default::default());

        let result = retry_client
            .poll_workflow_task("tq".to_string(), false)
            .await;
        assert!(result.is_ok());
        let result = retry_client
            .poll_activity_task("tq".to_string(), None)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn long_poll_retries_deadline_exceeded() {
        // For some reason we will get cancelled in these situations occasionally (always?) too
        for code in [Code::Cancelled, Code::DeadlineExceeded] {
            let mut mock_client = MockWorkflowClientTrait::new();
            mock_client
                .expect_poll_workflow_task()
                .returning(move |_, _| Err(Status::new(code, "retryable failure")))
                .times(5);
            mock_client
                .expect_poll_workflow_task()
                .returning(|_, _| Ok(Default::default()))
                .times(1);
            mock_client
                .expect_poll_activity_task()
                .returning(move |_, _| Err(Status::new(code, "retryable failure")))
                .times(5);
            mock_client
                .expect_poll_activity_task()
                .returning(|_, _| Ok(Default::default()))
                .times(1);

            let retry_client = RetryClient::new(mock_client, Default::default());

            let result = retry_client
                .poll_workflow_task("tq".to_string(), false)
                .await;
            assert!(result.is_ok());
            let result = retry_client
                .poll_activity_task("tq".to_string(), None)
                .await;
            assert!(result.is_ok());
        }
    }
}
