use crate::{
    ClientOptions, ListClosedFilters, ListOpenFilters, Namespace, Result, RetryConfig,
    StartTimeFilter, WorkflowClientTrait, WorkflowOptions,
};
use backoff::{backoff::Backoff, exponential::ExponentialBackoff, Clock, SystemClock};
use futures_retry::{ErrorHandler, FutureRetry, RetryPolicy};
use std::{fmt::Debug, future::Future, sync::Arc, time::Duration};
use temporal_sdk_core_protos::{
    coresdk::workflow_commands::QueryResult,
    temporal::api::{
        common::v1::{Header, Payload, Payloads},
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
const LONG_POLL_FATAL_GRACE: Duration = Duration::from_secs(60);
/// Must match the method name in [crate::raw::WorkflowService]
const POLL_WORKFLOW_METH_NAME: &str = "poll_workflow_task_queue";
/// Must match the method name in [crate::raw::WorkflowService]
const POLL_ACTIVITY_METH_NAME: &str = "poll_activity_task_queue";

/// A wrapper for a [WorkflowClientTrait] or [crate::WorkflowService] implementor which performs
/// auto-retries
#[derive(Debug, Clone)]
pub struct RetryClient<SG> {
    client: SG,
    retry_config: Arc<RetryConfig>,
}

impl<SG> RetryClient<SG> {
    /// Use the provided retry config with the provided client
    pub fn new(client: SG, retry_config: RetryConfig) -> Self {
        Self {
            client,
            retry_config: Arc::new(retry_config),
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
        match CallType::from_call_name(call_name) {
            CallType::Normal => (*self.retry_config).clone(),
            CallType::LongPoll => RetryConfig::poll_retry_policy(),
        }
    }

    pub(crate) fn make_future_retry<R, F, Fut>(
        rtc: RetryConfig,
        factory: F,
        call_name: &'static str,
    ) -> FutureRetry<F, TonicErrorHandler<SystemClock>>
    where
        F: FnMut() -> Fut + Unpin,
        Fut: Future<Output = Result<R>>,
    {
        FutureRetry::new(
            factory,
            TonicErrorHandler::new(rtc, RetryConfig::throttle_retry_policy(), call_name),
        )
    }
}

#[derive(Debug)]
pub(crate) struct TonicErrorHandler<C: Clock> {
    backoff: ExponentialBackoff<C>,
    throttle_backoff: ExponentialBackoff<C>,
    max_retries: usize,
    call_type: CallType,
    call_name: &'static str,
}
impl TonicErrorHandler<SystemClock> {
    fn new(cfg: RetryConfig, throttle_cfg: RetryConfig, call_name: &'static str) -> Self {
        Self::new_with_clock(
            cfg,
            throttle_cfg,
            call_name,
            SystemClock::default(),
            SystemClock::default(),
        )
    }
}
impl<C> TonicErrorHandler<C>
where
    C: Clock,
{
    fn new_with_clock(
        cfg: RetryConfig,
        throttle_cfg: RetryConfig,
        call_name: &'static str,
        clock: C,
        throttle_clock: C,
    ) -> Self {
        Self {
            max_retries: cfg.max_retries,
            call_type: CallType::from_call_name(call_name),
            call_name,
            backoff: cfg.into_exp_backoff(clock),
            throttle_backoff: throttle_cfg.into_exp_backoff(throttle_clock),
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
impl CallType {
    fn from_call_name(call_name: &str) -> Self {
        match call_name {
            POLL_WORKFLOW_METH_NAME | POLL_ACTIVITY_METH_NAME => CallType::LongPoll,
            _ => CallType::Normal,
        }
    }
}

impl<C> ErrorHandler<tonic::Status> for TonicErrorHandler<C>
where
    C: Clock,
{
    type OutError = tonic::Status;

    fn handle(&mut self, current_attempt: usize, e: tonic::Status) -> RetryPolicy<tonic::Status> {
        // 0 max retries means unlimited retries
        if self.max_retries > 0 && current_attempt >= self.max_retries {
            return RetryPolicy::ForwardError(e);
        }

        let is_long_poll = self.call_type == CallType::LongPoll;
        // Long polls are OK with being cancelled or running into the timeout because there's
        // nothing to do but retry anyway
        let long_poll_allowed =
            is_long_poll && [Code::Cancelled, Code::DeadlineExceeded].contains(&e.code());

        if RETRYABLE_ERROR_CODES.contains(&e.code()) || long_poll_allowed {
            if current_attempt == 1 {
                debug!(error=?e, "gRPC call {} failed on first attempt", self.call_name);
            } else if self.should_log_retry_warning(current_attempt) {
                warn!(error=?e, "gRPC call {} retried {} times", self.call_name, current_attempt);
            }

            match self.backoff.next_backoff() {
                None => RetryPolicy::ForwardError(e), // None is returned when we've ran out of time
                Some(backoff) => {
                    // We treat ResourceExhausted as a special case and backoff more
                    // so we don't overload the server
                    if e.code() == Code::ResourceExhausted {
                        let extended_backoff =
                            backoff.max(self.throttle_backoff.next_backoff().unwrap_or_default());
                        RetryPolicy::WaitRetry(extended_backoff)
                    } else {
                        RetryPolicy::WaitRetry(backoff)
                    }
                }
            }
        } else if is_long_poll && self.backoff.get_elapsed_time() <= LONG_POLL_FATAL_GRACE {
            // We permit "fatal" errors while long polling for a while, because some proxies return
            // stupid error codes while getting ready, among other weird infra issues
            RetryPolicy::WaitRetry(self.backoff.max_interval)
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
        request_id: Option<String>,
        options: WorkflowOptions,
    ) -> Result<StartWorkflowExecutionResponse> {
        retry_call!(
            self,
            start_workflow,
            input.clone(),
            task_queue.clone(),
            workflow_id.clone(),
            workflow_type.clone(),
            request_id.clone(),
            options.clone()
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
        request_id: Option<String>,
    ) -> Result<SignalWorkflowExecutionResponse> {
        retry_call!(
            self,
            signal_workflow_execution,
            workflow_id.clone(),
            run_id.clone(),
            signal_name.clone(),
            payloads.clone(),
            request_id.clone()
        )
    }

    async fn signal_with_start_workflow_execution(
        &self,
        input: Option<Payloads>,
        task_queue: String,
        workflow_id: String,
        workflow_type: String,
        request_id: Option<String>,
        options: WorkflowOptions,
        signal_name: String,
        signal_input: Option<Payloads>,
        signal_header: Option<Header>,
    ) -> Result<SignalWithStartWorkflowExecutionResponse> {
        retry_call!(
            self,
            signal_with_start_workflow_execution,
            input.clone(),
            task_queue.clone(),
            workflow_id.clone(),
            workflow_type.clone(),
            request_id.clone(),
            options.clone(),
            signal_name.clone(),
            signal_input.clone(),
            signal_header.clone()
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
        request_id: Option<String>,
    ) -> Result<RequestCancelWorkflowExecutionResponse> {
        retry_call!(
            self,
            cancel_workflow_execution,
            workflow_id.clone(),
            run_id.clone(),
            reason.clone(),
            request_id.clone()
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

    async fn describe_namespace(&self, namespace: Namespace) -> Result<DescribeNamespaceResponse> {
        retry_call!(self, describe_namespace, namespace.clone())
    }

    async fn list_open_workflow_executions(
        &self,
        maximum_page_size: i32,
        next_page_token: Vec<u8>,
        start_time_filter: Option<StartTimeFilter>,
        filters: Option<ListOpenFilters>,
    ) -> Result<ListOpenWorkflowExecutionsResponse> {
        retry_call!(
            self,
            list_open_workflow_executions,
            maximum_page_size,
            next_page_token.clone(),
            start_time_filter.clone(),
            filters.clone()
        )
    }

    async fn list_closed_workflow_executions(
        &self,
        maximum_page_size: i32,
        next_page_token: Vec<u8>,
        start_time_filter: Option<StartTimeFilter>,
        filters: Option<ListClosedFilters>,
    ) -> Result<ListClosedWorkflowExecutionsResponse> {
        retry_call!(
            self,
            list_closed_workflow_executions,
            maximum_page_size,
            next_page_token.clone(),
            start_time_filter.clone(),
            filters.clone()
        )
    }

    async fn list_workflow_executions(
        &self,
        page_size: i32,
        next_page_token: Vec<u8>,
        query: String,
    ) -> Result<ListWorkflowExecutionsResponse> {
        retry_call!(
            self,
            list_workflow_executions,
            page_size,
            next_page_token.clone(),
            query.clone()
        )
    }

    fn get_options(&self) -> &ClientOptions {
        self.client.get_options()
    }

    fn namespace(&self) -> &str {
        self.client.namespace()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MockWorkflowClientTrait;
    use assert_matches::assert_matches;
    use backoff::Clock;
    use std::{ops::Add, time::Instant};
    use tonic::Status;

    /// Predefined retry configs with low durations to make unit tests faster
    const TEST_RETRY_CONFIG: RetryConfig = RetryConfig {
        initial_interval: Duration::from_millis(1),
        randomization_factor: 0.0,
        multiplier: 1.1,
        max_interval: Duration::from_millis(2),
        max_elapsed_time: None,
        max_retries: 10,
    };

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
            let retry_client = RetryClient::new(mock_client, TEST_RETRY_CONFIG);
            let result = retry_client
                .cancel_activity_task(vec![1].into(), None)
                .await;
            // Expecting an error after a single attempt, since there was a non-retryable error.
            assert!(result.is_err());
        }
    }

    struct FixedClock(Instant);
    impl Clock for FixedClock {
        fn now(&self) -> Instant {
            self.0
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
            for call_name in [POLL_WORKFLOW_METH_NAME, POLL_ACTIVITY_METH_NAME] {
                let mut err_handler = TonicErrorHandler {
                    max_retries: TEST_RETRY_CONFIG.max_retries,
                    call_type: CallType::LongPoll,
                    call_name,
                    backoff: TEST_RETRY_CONFIG.into_exp_backoff(FixedClock(Instant::now())),
                    throttle_backoff: TEST_RETRY_CONFIG
                        .into_exp_backoff(FixedClock(Instant::now())),
                };
                let result = err_handler.handle(1, Status::new(code, "Ahh"));
                assert_matches!(result, RetryPolicy::WaitRetry(_));
                err_handler.backoff.clock.0 = err_handler
                    .backoff
                    .clock
                    .0
                    .add(LONG_POLL_FATAL_GRACE + Duration::from_secs(1));
                let result = err_handler.handle(2, Status::new(code, "Ahh"));
                assert_matches!(result, RetryPolicy::ForwardError(_));
            }
        }
    }

    #[tokio::test]
    async fn long_poll_retryable_errors_never_fatal() {
        for code in RETRYABLE_ERROR_CODES {
            for call_name in [POLL_WORKFLOW_METH_NAME, POLL_ACTIVITY_METH_NAME] {
                let mut err_handler = TonicErrorHandler {
                    max_retries: TEST_RETRY_CONFIG.max_retries,
                    call_type: CallType::LongPoll,
                    call_name,
                    backoff: TEST_RETRY_CONFIG.into_exp_backoff(FixedClock(Instant::now())),
                    throttle_backoff: TEST_RETRY_CONFIG
                        .into_exp_backoff(FixedClock(Instant::now())),
                };
                let result = err_handler.handle(1, Status::new(code, "Ahh"));
                assert_matches!(result, RetryPolicy::WaitRetry(_));
                err_handler.backoff.clock.0 = err_handler
                    .backoff
                    .clock
                    .0
                    .add(LONG_POLL_FATAL_GRACE + Duration::from_secs(1));
                let result = err_handler.handle(2, Status::new(code, "Ahh"));
                assert_matches!(result, RetryPolicy::WaitRetry(_));
            }
        }
    }

    #[tokio::test]
    async fn retryable_errors() {
        // Take out retry exhausted since it gets a special policy which would make this take ages
        for code in RETRYABLE_ERROR_CODES
            .iter()
            .copied()
            .filter(|p| p != &Code::ResourceExhausted)
        {
            let mut mock_client = MockWorkflowClientTrait::new();
            mock_client
                .expect_cancel_activity_task()
                .returning(move |_, _| Err(Status::new(code, "retryable failure")))
                .times(3);
            mock_client
                .expect_cancel_activity_task()
                .returning(|_, _| Ok(Default::default()))
                .times(1);

            let retry_client = RetryClient::new(mock_client, TEST_RETRY_CONFIG);
            let result = retry_client
                .cancel_activity_task(vec![1].into(), None)
                .await;
            // Expecting successful response after retries
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn retry_resource_exhausted() {
        let mut err_handler = TonicErrorHandler {
            max_retries: TEST_RETRY_CONFIG.max_retries,
            call_type: CallType::Normal,
            call_name: POLL_WORKFLOW_METH_NAME,
            backoff: TEST_RETRY_CONFIG.into_exp_backoff(FixedClock(Instant::now())),
            throttle_backoff: RetryConfig {
                initial_interval: Duration::from_millis(2),
                randomization_factor: 0.0,
                multiplier: 4.0,
                max_interval: Duration::from_millis(10),
                max_elapsed_time: None,
                max_retries: 10,
            }
            .into_exp_backoff(FixedClock(Instant::now())),
        };
        let result = err_handler.handle(1, Status::new(Code::ResourceExhausted, "leave me alone"));
        match result {
            RetryPolicy::WaitRetry(duration) => assert_eq!(duration, Duration::from_millis(2)),
            _ => panic!(),
        }
        err_handler.backoff.clock.0 = err_handler.backoff.clock.0.add(Duration::from_millis(10));
        err_handler.throttle_backoff.clock.0 = err_handler
            .throttle_backoff
            .clock
            .0
            .add(Duration::from_millis(10));
        let result = err_handler.handle(2, Status::new(Code::ResourceExhausted, "leave me alone"));
        match result {
            RetryPolicy::WaitRetry(duration) => assert_eq!(duration, Duration::from_millis(8)),
            _ => panic!(),
        }
    }

    #[tokio::test]
    async fn long_poll_retries_forever() {
        // A bit odd, but we don't need a real client to test the retry client passes through the
        // correct retry config
        let fake_retry = RetryClient::new((), TEST_RETRY_CONFIG);
        for i in 1..=50 {
            for call in [POLL_WORKFLOW_METH_NAME, POLL_ACTIVITY_METH_NAME] {
                let mut err_handler = TonicErrorHandler::new(
                    fake_retry.get_retry_config(call),
                    fake_retry.get_retry_config(call),
                    call,
                );
                let result = err_handler.handle(i, Status::new(Code::Unknown, "Ahh"));
                assert_matches!(result, RetryPolicy::WaitRetry(_));
            }
        }
    }

    #[tokio::test]
    async fn long_poll_retries_deadline_exceeded() {
        let fake_retry = RetryClient::new((), TEST_RETRY_CONFIG);
        // For some reason we will get cancelled in these situations occasionally (always?) too
        for code in [Code::Cancelled, Code::DeadlineExceeded] {
            for call in [POLL_WORKFLOW_METH_NAME, POLL_ACTIVITY_METH_NAME] {
                let mut err_handler = TonicErrorHandler::new(
                    fake_retry.get_retry_config(call),
                    fake_retry.get_retry_config(call),
                    call,
                );
                for i in 1..=5 {
                    let result = err_handler.handle(i, Status::new(code, "retryable failure"));
                    assert_matches!(result, RetryPolicy::WaitRetry(_));
                }
            }
        }
    }
}
