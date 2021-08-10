use crate::pollers::gateway::{ServerGateway, ServerGatewayApis};
use crate::{
    pollers::Result,
    protos::{
        coresdk::common::Payload,
        coresdk::workflow_commands::QueryResult,
        temporal::api::{
            common::v1::{Payloads, WorkflowExecution, WorkflowType},
            enums::v1::{TaskQueueKind, WorkflowTaskFailedCause},
            failure::v1::Failure,
            query::v1::{WorkflowQuery, WorkflowQueryResult},
            taskqueue::v1::TaskQueue,
            workflowservice::v1::{workflow_service_client::WorkflowServiceClient, *},
        },
    },
    protosext::WorkflowTaskCompletion,
    task_token::TaskToken,
    CoreInitError,
};
use futures_retry::{ErrorHandler, FutureRetry, RetryPolicy};
use std::{
    fmt::{Debug, Formatter},
    time::Duration,
};
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
            | Code::Unauthenticated => RetryPolicy::WaitRetry(Duration::from_secs(5)),
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
                    task_timeout.clone(),
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
        Ok(FutureRetry::new(
            move || {
                self.gateway
                    .reset_sticky_task_queue(workflow_id.clone(), run_id.clone())
            },
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn complete_workflow_task(
        &self,
        request: WorkflowTaskCompletion,
    ) -> Result<RespondWorkflowTaskCompletedResponse> {
        Ok(FutureRetry::new(
            move || self.gateway.complete_workflow_task(request.clone()),
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn complete_activity_task(
        &self,
        task_token: TaskToken,
        result: Option<Payloads>,
    ) -> Result<RespondActivityTaskCompletedResponse> {
        Ok(FutureRetry::new(
            move || {
                self.gateway
                    .complete_activity_task(task_token.clone(), result.clone())
            },
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn record_activity_heartbeat(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RecordActivityTaskHeartbeatResponse> {
        Ok(FutureRetry::new(
            move || {
                self.gateway
                    .record_activity_heartbeat(task_token.clone(), details.clone())
            },
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn cancel_activity_task(
        &self,
        task_token: TaskToken,
        details: Option<Payloads>,
    ) -> Result<RespondActivityTaskCanceledResponse> {
        Ok(FutureRetry::new(
            move || {
                self.gateway
                    .cancel_activity_task(task_token.clone(), details.clone())
            },
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn fail_activity_task(
        &self,
        task_token: TaskToken,
        failure: Option<Failure>,
    ) -> Result<RespondActivityTaskFailedResponse> {
        Ok(FutureRetry::new(
            move || {
                self.gateway
                    .fail_activity_task(task_token.clone(), failure.clone())
            },
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn fail_workflow_task(
        &self,
        task_token: TaskToken,
        cause: WorkflowTaskFailedCause,
        failure: Option<Failure>,
    ) -> Result<RespondWorkflowTaskFailedResponse> {
        Ok(FutureRetry::new(
            move || {
                self.gateway
                    .fail_workflow_task(task_token.clone(), cause.clone(), failure.clone())
            },
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn signal_workflow_execution(
        &self,
        workflow_id: String,
        run_id: String,
        signal_name: String,
        payloads: Option<Payloads>,
    ) -> Result<SignalWorkflowExecutionResponse> {
        Ok(FutureRetry::new(
            move || {
                self.gateway.signal_workflow_execution(
                    workflow_id.clone(),
                    run_id.clone(),
                    signal_name.clone(),
                    payloads.clone(),
                )
            },
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn query_workflow_execution(
        &self,
        workflow_id: String,
        run_id: String,
        query: WorkflowQuery,
    ) -> Result<QueryWorkflowResponse> {
        Ok(FutureRetry::new(
            move || {
                self.gateway.query_workflow_execution(
                    workflow_id.clone(),
                    run_id.clone(),
                    query.clone(),
                )
            },
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn describe_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<DescribeWorkflowExecutionResponse> {
        Ok(FutureRetry::new(
            move || {
                self.gateway
                    .describe_workflow_execution(workflow_id.clone(), run_id.clone())
            },
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn get_workflow_execution_history(
        &self,
        workflow_id: String,
        run_id: Option<String>,
        page_token: Vec<u8>,
    ) -> Result<GetWorkflowExecutionHistoryResponse> {
        Ok(FutureRetry::new(
            move || {
                self.gateway.get_workflow_execution_history(
                    workflow_id.clone(),
                    run_id.clone(),
                    page_token.clone(),
                )
            },
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn respond_legacy_query(
        &self,
        task_token: TaskToken,
        query_result: QueryResult,
    ) -> Result<RespondQueryTaskCompletedResponse> {
        Ok(FutureRetry::new(
            move || {
                self.gateway
                    .respond_legacy_query(task_token.clone(), query_result.clone())
            },
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn cancel_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<RequestCancelWorkflowExecutionResponse> {
        Ok(FutureRetry::new(
            move || {
                self.gateway
                    .cancel_workflow_execution(workflow_id.clone(), run_id.clone())
            },
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn terminate_workflow_execution(
        &self,
        workflow_id: String,
        run_id: Option<String>,
    ) -> Result<TerminateWorkflowExecutionResponse> {
        Ok(FutureRetry::new(
            move || {
                self.gateway
                    .terminate_workflow_execution(workflow_id.clone(), run_id.clone())
            },
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }

    async fn list_namespaces(&self) -> Result<ListNamespacesResponse> {
        Ok(FutureRetry::new(
            move || self.gateway.list_namespaces(),
            self.error_handler.clone(),
        )
        .await
        .map_err(|(e, _attempt)| e)?
        .0)
    }
}
