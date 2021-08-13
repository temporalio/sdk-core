use crate::pollers::gateway::ServerGatewayApis;
use crate::{
    pollers::Result,
    protos::{
        coresdk::common::Payload,
        coresdk::workflow_commands::QueryResult,
        temporal::api::{
            common::v1::Payloads, enums::v1::WorkflowTaskFailedCause, failure::v1::Failure,
            query::v1::WorkflowQuery, workflowservice::v1::*,
        },
    },
    protosext::WorkflowTaskCompletion,
    task_token::TaskToken,
};
use futures_retry::{ErrorHandler, FutureRetry, RetryPolicy};
use std::future::Future;
use std::{fmt::Debug, time::Duration};
use tonic::Code;

#[derive(Debug)]
pub struct RetryGateway<SG> {
    gateway: SG,
    error_handler: TonicErrorHandler<String>,
}

#[derive(Clone, Debug)]
pub struct TonicErrorHandler<D> {
    max_attempts: usize,
    display_name: D,
}

impl<D> TonicErrorHandler<D> {
    pub fn new(max_attempts: usize, display_name: D) -> Self {
        TonicErrorHandler {
            max_attempts,
            display_name,
        }
    }
}

impl<D> ErrorHandler<tonic::Status> for TonicErrorHandler<D>
where
    D: ::std::fmt::Display,
{
    type OutError = tonic::Status;

    fn handle(&mut self, current_attempt: usize, e: tonic::Status) -> RetryPolicy<tonic::Status> {
        if current_attempt >= self.max_attempts {
            eprintln!(
                "[{}] All attempts ({}) have been used",
                self.display_name, self.max_attempts
            );
            return RetryPolicy::ForwardError(e);
        }
        eprintln!(
            "[{}] Attempt {}/{} has failed",
            self.display_name, current_attempt, self.max_attempts
        );
        match e.code() {
            Code::Cancelled
            | Code::DataLoss
            | Code::DeadlineExceeded
            | Code::Internal
            | Code::Unknown
            | Code::ResourceExhausted
            | Code::Aborted
            | Code::OutOfRange
            | Code::Unimplemented
            | Code::Unavailable
            | Code::Unauthenticated => RetryPolicy::WaitRetry(Duration::from_millis(200)),
            _ => RetryPolicy::ForwardError(e),
        }
    }
}

impl<SG> RetryGateway<SG> {
    pub fn new(gateway: SG, error_handler: TonicErrorHandler<String>) -> Self {
        Self {
            gateway,
            error_handler,
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
        Ok(FutureRetry::new(
            move || {
                self.gateway.start_workflow(
                    input.to_vec(),
                    task_queue.clone(),
                    workflow_id.clone(),
                    workflow_type.clone(),
                    task_timeout,
                )
            },
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn poll_workflow_task(
        &self,
        task_queue: String,
    ) -> Result<PollWorkflowTaskQueueResponse> {
        Ok(FutureRetry::new(
            move || self.gateway.poll_workflow_task(task_queue.clone()),
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn poll_activity_task(
        &self,
        task_queue: String,
    ) -> Result<PollActivityTaskQueueResponse> {
        Ok(FutureRetry::new(
            move || self.gateway.poll_activity_task(task_queue.clone()),
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
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

impl<SG: ServerGatewayApis + Send + Sync + 'static> RetryGateway<SG> {
    async fn call_with_retry<R, F, Fut>(&self, factory: F) -> Result<R>
    where
        F: Fn() -> Fut + Unpin,
        Fut: Future<Output = Result<R>>,
    {
        Ok(FutureRetry::new(factory, self.error_handler.clone())
            .await
            .map_err(|(e, _attempt)| e)?
            .0)
    }
}
