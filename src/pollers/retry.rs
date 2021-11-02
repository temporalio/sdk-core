use crate::{
    pollers::{
        gateway::{RetryConfig, ServerGatewayApis},
        Result, RETRYABLE_ERROR_CODES,
    },
    protosext::WorkflowTaskCompletion,
    task_token::TaskToken,
};
use backoff::{backoff::Backoff, ExponentialBackoff};
use futures_retry::{ErrorHandler, FutureRetry, RetryPolicy};
use std::{fmt::Debug, future::Future, time::Duration};
use temporal_sdk_core_protos::{
    coresdk::{common::Payload, workflow_commands::QueryResult},
    temporal::api::{
        common::v1::Payloads, enums::v1::WorkflowTaskFailedCause, failure::v1::Failure,
        query::v1::WorkflowQuery, workflowservice::v1::*,
    },
};
use tonic::Code;

#[derive(Debug)]
/// A wrapper for a [ServerGatewayApis] implementor which performs auto-retries
pub struct RetryGateway<SG> {
    gateway: SG,
    retry_config: RetryConfig,
}

impl<SG> RetryGateway<SG> {
    /// Use the provided retry config with the provided gateway
    pub fn new(gateway: SG, retry_config: RetryConfig) -> Self {
        Self {
            gateway,
            retry_config,
        }
    }
}

impl<SG: ServerGatewayApis + Send + Sync + 'static> RetryGateway<SG> {
    async fn call_with_retry<R, F, Fut>(&self, factory: F) -> Result<R>
    where
        F: Fn() -> Fut + Unpin,
        Fut: Future<Output = Result<R>>,
    {
        self.call_type_with_retry(factory, CallType::Normal).await
    }

    async fn long_poll_call_with_retry<R, F, Fut>(&self, factory: F) -> Result<R>
    where
        F: Fn() -> Fut + Unpin,
        Fut: Future<Output = Result<R>>,
    {
        self.call_type_with_retry(factory, CallType::LongPoll).await
    }

    async fn call_type_with_retry<R, F, Fut>(&self, factory: F, ct: CallType) -> Result<R>
    where
        F: Fn() -> Fut + Unpin,
        Fut: Future<Output = Result<R>>,
    {
        let rtc = match ct {
            CallType::Normal => self.retry_config.clone(),
            CallType::LongPoll => RetryConfig::poll_retry_policy(),
        };
        Ok(FutureRetry::new(factory, TonicErrorHandler::new(rtc, ct))
            .await
            .map_err(|(e, _attempt)| e)?
            .0)
    }
}

#[derive(Debug)]
struct TonicErrorHandler {
    backoff: ExponentialBackoff,
    max_retries: usize,
    call_type: CallType,
}
impl TonicErrorHandler {
    fn new(cfg: RetryConfig, call_type: CallType) -> Self {
        Self {
            max_retries: cfg.max_retries,
            backoff: cfg.into(),
            call_type,
        }
    }

    fn should_log_retry_warning(&self, cur_attempt: usize) -> bool {
        // Warn on more than 5 retries for unlimited retrying
        if self.max_retries == 0 && cur_attempt > 5 {
            return true;
        }
        // Warn if the attempts are more than 50% of max retries
        if cur_attempt * 2 >= self.max_retries {
            return true;
        }
        false
    }
}
#[derive(Debug, Eq, PartialEq, Hash)]
enum CallType {
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

        if current_attempt == 1 {
            debug!(error=?e, "gRPC call failed on first attempt")
        } else if self.should_log_retry_warning(current_attempt) {
            warn!(error=?e, "gRPC call retried {} times", current_attempt)
        }

        // Long polls are OK with being cancelled or running into the timeout because there's
        // nothing to do but retry anyway
        let long_poll_allowed = self.call_type == CallType::LongPoll
            && [Code::Cancelled, Code::DeadlineExceeded].contains(&e.code());

        if RETRYABLE_ERROR_CODES.contains(&e.code()) || long_poll_allowed {
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

#[async_trait::async_trait]
impl<SG: ServerGatewayApis + Send + Sync + 'static> ServerGatewayApis for RetryGateway<SG> {
    async fn start_workflow(
        &self,
        input: Vec<Payload>,
        task_queue: String,
        workflow_id: String,
        workflow_type: String,
        task_timeout: Option<Duration>,
    ) -> Result<StartWorkflowExecutionResponse> {
        let input = &input;
        let factory = move || {
            self.gateway.start_workflow(
                input.to_vec(),
                task_queue.clone(),
                workflow_id.clone(),
                workflow_type.clone(),
                task_timeout,
            )
        };
        self.call_with_retry(factory).await
    }

    async fn poll_workflow_task(
        &self,
        task_queue: String,
    ) -> Result<PollWorkflowTaskQueueResponse> {
        let factory = move || self.gateway.poll_workflow_task(task_queue.clone());
        self.long_poll_call_with_retry(factory).await
    }

    async fn poll_activity_task(
        &self,
        task_queue: String,
    ) -> Result<PollActivityTaskQueueResponse> {
        let factory = move || self.gateway.poll_activity_task(task_queue.clone());
        self.long_poll_call_with_retry(factory).await
    }

    async fn reset_sticky_task_queue(
        &self,
        workflow_id: String,
        run_id: String,
    ) -> Result<ResetStickyTaskQueueResponse> {
        let factory = move || {
            self.gateway
                .reset_sticky_task_queue(workflow_id.clone(), run_id.clone())
        };
        self.call_with_retry(factory).await
    }

    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> Result<RespondWorkflowTaskCompletedResponse> {
        let factory = move || self.gateway.complete_workflow_task(request.clone());
        self.call_with_retry(factory).await
    }

    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse> {
        let factory = move || {
            self.gateway
                .complete_activity_task(task_token.clone(), result.clone())
        };
        self.call_with_retry(factory).await
    }

    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse> {
        let factory = move || {
            self.gateway
                .record_activity_heartbeat(task_token.clone(), details.clone())
        };
        self.call_with_retry(factory).await
    }

    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse> {
        let factory = move || {
            self.gateway
                .cancel_activity_task(task_token.clone(), details.clone())
        };
        self.call_with_retry(factory).await
    }

    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse> {
        let factory = move || {
            self.gateway
                .fail_activity_task(task_token.clone(), failure.clone())
        };
        self.call_with_retry(factory).await
    }

    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse> {
        let factory = move || {
            self.gateway
                .fail_workflow_task(task_token.clone(), cause, failure.clone())
        };
        self.call_with_retry(factory).await
    }

    async fn signal_workflow_execution(
        &self,
        workflow_id: String,
        run_id: String,
        signal_name: String,
        payloads: Option<Payloads>,
    ) -> Result<SignalWorkflowExecutionResponse> {
        let factory = move || {
            self.gateway.signal_workflow_execution(
                workflow_id.clone(),
                run_id.clone(),
                signal_name.clone(),
                payloads.clone(),
            )
        };
        self.call_with_retry(factory).await
    }

    async fn query_workflow_execution(
        &self,
        workflow_id: String,
        run_id: String,
        query: WorkflowQuery,
    ) -> Result<QueryWorkflowResponse> {
        let factory = move || {
            self.gateway.query_workflow_execution(
                workflow_id.clone(),
                run_id.clone(),
                query.clone(),
            )
        };
        self.call_with_retry(factory).await
    }

    async fn describe_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<DescribeWorkflowExecutionResponse> {
        let factory = move || {
            self.gateway
                .describe_workflow_execution(workflow_id.clone(), run_id.clone())
        };
        self.call_with_retry(factory).await
    }

    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse> {
        let factory = move || {
            self.gateway.get_workflow_execution_history(
                workflow_id.clone(),
                run_id.clone(),
                page_token.clone(),
            )
        };
        self.call_with_retry(factory).await
    }

    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: QueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse> {
        let factory = move || {
            self.gateway
                .respond_legacy_query(task_token.clone(), query_result.clone())
        };
        self.call_with_retry(factory).await
    }

    async fn cancel_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<RequestCancelWorkflowExecutionResponse> {
        let factory = move || {
            self.gateway
                .cancel_workflow_execution(workflow_id.clone(), run_id.clone())
        };
        self.call_with_retry(factory).await
    }

    async fn terminate_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<TerminateWorkflowExecutionResponse> {
        let factory = move || {
            self.gateway
                .terminate_workflow_execution(workflow_id.clone(), run_id.clone())
        };
        self.call_with_retry(factory).await
    }

    async fn list_namespaces(&self) -> Result<ListNamespacesResponse> {
        let factory = move || self.gateway.list_namespaces();
        self.call_with_retry(factory).await
    }
}
