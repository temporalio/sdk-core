//! We need a way to trait-ify the raw grpc client because there is no other way to get the
//! information we need via interceptors. This module contains the necessary stuff to make that
//! happen.

use crate::{
    metrics::{namespace_kv, task_queue_kv},
    raw::sealed::RawClientLike,
    Client, ConfiguredClient, InterceptedMetricsSvc, RetryClient, TemporalServiceClient,
    LONG_POLL_TIMEOUT,
};
use futures::{future::BoxFuture, FutureExt, TryFutureExt};
use temporal_sdk_core_protos::{
    grpc::health::v1::{health_client::HealthClient, *},
    temporal::api::{
        operatorservice::v1::{operator_service_client::OperatorServiceClient, *},
        taskqueue::v1::TaskQueue,
        testservice::v1::{test_service_client::TestServiceClient, *},
        workflowservice::v1::{workflow_service_client::WorkflowServiceClient, *},
    },
};
use tonic::{
    body::BoxBody, client::GrpcService, metadata::KeyAndValueRef, Request, Response, Status,
};

pub(super) mod sealed {
    use super::*;

    /// Something that has a workflow service client
    #[async_trait::async_trait]
    pub trait RawClientLike: Send {
        type SvcType: Send + Sync + Clone + 'static;

        /// Return the workflow service client instance
        fn workflow_client(&mut self) -> &mut WorkflowServiceClient<Self::SvcType>;

        /// Return the operator service client instance
        fn operator_client(&mut self) -> &mut OperatorServiceClient<Self::SvcType>;

        /// Return the test service client instance
        fn test_client(&mut self) -> &mut TestServiceClient<Self::SvcType>;

        /// Return the health service client instance
        fn health_client(&mut self) -> &mut HealthClient<Self::SvcType>;

        async fn call<F, Req, Resp>(
            &mut self,
            _call_name: &'static str,
            mut callfn: F,
            req: Request<Req>,
        ) -> Result<Response<Resp>, Status>
        where
            Req: Clone + Unpin + Send + Sync + 'static,
            F: FnMut(&mut Self, Request<Req>) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
            F: Send + Sync + Unpin + 'static,
        {
            callfn(self, req).await
        }
    }
}

// Here we implement retry on anything that is already RawClientLike
#[async_trait::async_trait]
impl<RC, T> RawClientLike for RetryClient<RC>
where
    RC: RawClientLike<SvcType = T> + 'static,
    T: Send + Sync + Clone + 'static,
{
    type SvcType = T;

    fn workflow_client(&mut self) -> &mut WorkflowServiceClient<Self::SvcType> {
        self.get_client_mut().workflow_client()
    }

    fn operator_client(&mut self) -> &mut OperatorServiceClient<Self::SvcType> {
        self.get_client_mut().operator_client()
    }

    fn test_client(&mut self) -> &mut TestServiceClient<Self::SvcType> {
        self.get_client_mut().test_client()
    }

    fn health_client(&mut self) -> &mut HealthClient<Self::SvcType> {
        self.get_client_mut().health_client()
    }

    async fn call<F, Req, Resp>(
        &mut self,
        call_name: &'static str,
        mut callfn: F,
        req: Request<Req>,
    ) -> Result<Response<Resp>, Status>
    where
        Req: Clone + Unpin + Send + Sync + 'static,
        F: FnMut(&mut Self, Request<Req>) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
        F: Send + Sync + Unpin + 'static,
    {
        let rtc = self.get_retry_config(call_name);
        let fact = || {
            let req_clone = req_cloner(&req);
            callfn(self, req_clone)
        };
        let res = Self::make_future_retry(rtc, fact, call_name);
        res.map_err(|(e, _attempt)| e).map_ok(|x| x.0).await
    }
}

impl<T> RawClientLike for TemporalServiceClient<T>
where
    T: Send + Sync + Clone + 'static,
    T: GrpcService<BoxBody> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    type SvcType = T;

    fn workflow_client(&mut self) -> &mut WorkflowServiceClient<Self::SvcType> {
        self.workflow_svc_mut()
    }

    fn operator_client(&mut self) -> &mut OperatorServiceClient<Self::SvcType> {
        self.operator_svc_mut()
    }

    fn test_client(&mut self) -> &mut TestServiceClient<Self::SvcType> {
        self.test_svc_mut()
    }

    fn health_client(&mut self) -> &mut HealthClient<Self::SvcType> {
        self.health_svc_mut()
    }
}

impl<T> RawClientLike for ConfiguredClient<TemporalServiceClient<T>>
where
    T: Send + Sync + Clone + 'static,
    T: GrpcService<BoxBody> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    type SvcType = T;

    fn workflow_client(&mut self) -> &mut WorkflowServiceClient<Self::SvcType> {
        self.client.workflow_client()
    }

    fn operator_client(&mut self) -> &mut OperatorServiceClient<Self::SvcType> {
        self.client.operator_client()
    }

    fn test_client(&mut self) -> &mut TestServiceClient<Self::SvcType> {
        self.client.test_client()
    }

    fn health_client(&mut self) -> &mut HealthClient<Self::SvcType> {
        self.client.health_client()
    }
}

impl RawClientLike for Client {
    type SvcType = InterceptedMetricsSvc;

    fn workflow_client(&mut self) -> &mut WorkflowServiceClient<Self::SvcType> {
        self.inner.workflow_client()
    }

    fn operator_client(&mut self) -> &mut OperatorServiceClient<Self::SvcType> {
        self.inner.operator_client()
    }

    fn test_client(&mut self) -> &mut TestServiceClient<Self::SvcType> {
        self.inner.test_client()
    }

    fn health_client(&mut self) -> &mut HealthClient<Self::SvcType> {
        self.inner.health_client()
    }
}

/// Helper for cloning a tonic request as long as the inner message may be cloned.
/// We drop extensions, so, lang bridges can't pass those in :shrug:
fn req_cloner<T: Clone>(cloneme: &Request<T>) -> Request<T> {
    let msg = cloneme.get_ref().clone();
    let mut new_req = Request::new(msg);
    let new_met = new_req.metadata_mut();
    for kv in cloneme.metadata().iter() {
        match kv {
            KeyAndValueRef::Ascii(k, v) => {
                new_met.insert(k, v.clone());
            }
            KeyAndValueRef::Binary(k, v) => {
                new_met.insert_bin(k, v.clone());
            }
        }
    }
    new_req
}

#[derive(Debug)]
pub(super) struct AttachMetricLabels {
    pub(super) labels: Vec<opentelemetry::KeyValue>,
}
impl AttachMetricLabels {
    pub fn new(kvs: impl Into<Vec<opentelemetry::KeyValue>>) -> Self {
        Self { labels: kvs.into() }
    }
    pub fn namespace(ns: impl Into<String>) -> Self {
        AttachMetricLabels::new(vec![namespace_kv(ns.into())])
    }
    pub fn task_q(&mut self, tq: Option<TaskQueue>) -> &mut Self {
        if let Some(tq) = tq {
            self.task_q_str(tq.name);
        }
        self
    }
    pub fn task_q_str(&mut self, tq: impl Into<String>) -> &mut Self {
        self.labels.push(task_queue_kv(tq.into()));
        self
    }
}

// Blanket impl the trait for all raw-client-like things. Since the trait default-implements
// everything, there's nothing to actually implement.
impl<RC, T> WorkflowService for RC
where
    RC: RawClientLike<SvcType = T>,
    T: GrpcService<BoxBody> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
}
impl<RC, T> OperatorService for RC
where
    RC: RawClientLike<SvcType = T>,
    T: GrpcService<BoxBody> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
}
impl<RC, T> TestService for RC
where
    RC: RawClientLike<SvcType = T>,
    T: GrpcService<BoxBody> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
}
impl<RC, T> HealthService for RC
where
    RC: RawClientLike<SvcType = T>,
    T: GrpcService<BoxBody> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
}

/// Helps re-declare gRPC client methods
macro_rules! proxy {
    ($client_type:tt, $client_meth:ident, $method:ident, $req:ty, $resp:ty $(, $closure:expr)?) => {
        #[doc = concat!("See [", stringify!($client_type), "::", stringify!($method), "]")]
        fn $method(
            &mut self,
            request: impl tonic::IntoRequest<$req>,
        ) -> BoxFuture<Result<tonic::Response<$resp>, tonic::Status>> {
            #[allow(unused_mut)]
            let fact = |c: &mut Self, mut req: tonic::Request<$req>| {
                $( type_closure_arg(&mut req, $closure); )*
                let mut c = c.$client_meth().clone();
                async move { c.$method(req).await }.boxed()
            };
            self.call(stringify!($method), fact, request.into_request())
        }
    };
}
macro_rules! proxier {
    ( $trait_name:ident; $impl_list_name:ident; $client_type:tt; $client_meth:ident;
      $(($method:ident, $req:ty, $resp:ty $(, $closure:expr)?  );)* ) => {
        #[cfg(test)]
        const $impl_list_name: &'static [&'static str] = &[$(stringify!($method)),*];
        /// Trait version of the generated client with modifications to attach appropriate metric
        /// labels or whatever else to requests
        pub trait $trait_name: RawClientLike
        where
            // Yo this is wild
            <Self as RawClientLike>::SvcType: GrpcService<BoxBody> + Send + Clone + 'static,
            <<Self as RawClientLike>::SvcType as GrpcService<BoxBody>>::ResponseBody:
                tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
            <<Self as RawClientLike>::SvcType as GrpcService<BoxBody>>::Error:
                Into<tonic::codegen::StdError>,
            <<Self as RawClientLike>::SvcType as GrpcService<BoxBody>>::Future: Send,
            <<<Self as RawClientLike>::SvcType as GrpcService<BoxBody>>::ResponseBody
                as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
        {
            $(
               proxy!($client_type, $client_meth, $method, $req, $resp $(,$closure)*);
            )*
        }
    };
}

// Nice little trick to avoid the callsite asking to type the closure parameter
fn type_closure_arg<T, R>(arg: T, f: impl FnOnce(T) -> R) -> R {
    f(arg)
}

proxier! {
    WorkflowService; ALL_IMPLEMENTED_WORKFLOW_SERVICE_RPCS; WorkflowServiceClient; workflow_client;
    (
        register_namespace,
        RegisterNamespaceRequest,
        RegisterNamespaceResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_namespace,
        DescribeNamespaceRequest,
        DescribeNamespaceResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_namespaces,
        ListNamespacesRequest,
        ListNamespacesResponse
    );
    (
        update_namespace,
        UpdateNamespaceRequest,
        UpdateNamespaceResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        deprecate_namespace,
        DeprecateNamespaceRequest,
        DeprecateNamespaceResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        start_workflow_execution,
        StartWorkflowExecutionRequest,
        StartWorkflowExecutionResponse,
        |r| {
            let mut labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_workflow_execution_history,
        GetWorkflowExecutionHistoryRequest,
        GetWorkflowExecutionHistoryResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_workflow_execution_history_reverse,
        GetWorkflowExecutionHistoryReverseRequest,
        GetWorkflowExecutionHistoryReverseResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        poll_workflow_task_queue,
        PollWorkflowTaskQueueRequest,
        PollWorkflowTaskQueueResponse,
        |r| {
            let mut labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
            r.set_timeout(LONG_POLL_TIMEOUT);
        }
    );
    (
        respond_workflow_task_completed,
        RespondWorkflowTaskCompletedRequest,
        RespondWorkflowTaskCompletedResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_workflow_task_failed,
        RespondWorkflowTaskFailedRequest,
        RespondWorkflowTaskFailedResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        poll_activity_task_queue,
        PollActivityTaskQueueRequest,
        PollActivityTaskQueueResponse,
        |r| {
            let mut labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
            r.set_timeout(LONG_POLL_TIMEOUT);
        }
    );
    (
        record_activity_task_heartbeat,
        RecordActivityTaskHeartbeatRequest,
        RecordActivityTaskHeartbeatResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        record_activity_task_heartbeat_by_id,
        RecordActivityTaskHeartbeatByIdRequest,
        RecordActivityTaskHeartbeatByIdResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_activity_task_completed,
        RespondActivityTaskCompletedRequest,
        RespondActivityTaskCompletedResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_activity_task_completed_by_id,
        RespondActivityTaskCompletedByIdRequest,
        RespondActivityTaskCompletedByIdResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );

    (
        respond_activity_task_failed,
        RespondActivityTaskFailedRequest,
        RespondActivityTaskFailedResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_activity_task_failed_by_id,
        RespondActivityTaskFailedByIdRequest,
        RespondActivityTaskFailedByIdResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_activity_task_canceled,
        RespondActivityTaskCanceledRequest,
        RespondActivityTaskCanceledResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_activity_task_canceled_by_id,
        RespondActivityTaskCanceledByIdRequest,
        RespondActivityTaskCanceledByIdResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        request_cancel_workflow_execution,
        RequestCancelWorkflowExecutionRequest,
        RequestCancelWorkflowExecutionResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        signal_workflow_execution,
        SignalWorkflowExecutionRequest,
        SignalWorkflowExecutionResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        signal_with_start_workflow_execution,
        SignalWithStartWorkflowExecutionRequest,
        SignalWithStartWorkflowExecutionResponse,
        |r| {
            let mut labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        reset_workflow_execution,
        ResetWorkflowExecutionRequest,
        ResetWorkflowExecutionResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        terminate_workflow_execution,
        TerminateWorkflowExecutionRequest,
        TerminateWorkflowExecutionResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        delete_workflow_execution,
        DeleteWorkflowExecutionRequest,
        DeleteWorkflowExecutionResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_open_workflow_executions,
        ListOpenWorkflowExecutionsRequest,
        ListOpenWorkflowExecutionsResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_closed_workflow_executions,
        ListClosedWorkflowExecutionsRequest,
        ListClosedWorkflowExecutionsResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_workflow_executions,
        ListWorkflowExecutionsRequest,
        ListWorkflowExecutionsResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_archived_workflow_executions,
        ListArchivedWorkflowExecutionsRequest,
        ListArchivedWorkflowExecutionsResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        scan_workflow_executions,
        ScanWorkflowExecutionsRequest,
        ScanWorkflowExecutionsResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        count_workflow_executions,
        CountWorkflowExecutionsRequest,
        CountWorkflowExecutionsResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_search_attributes,
        GetSearchAttributesRequest,
        GetSearchAttributesResponse
    );
    (
        respond_query_task_completed,
        RespondQueryTaskCompletedRequest,
        RespondQueryTaskCompletedResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        reset_sticky_task_queue,
        ResetStickyTaskQueueRequest,
        ResetStickyTaskQueueResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        query_workflow,
        QueryWorkflowRequest,
        QueryWorkflowResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_workflow_execution,
        DescribeWorkflowExecutionRequest,
        DescribeWorkflowExecutionResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_task_queue,
        DescribeTaskQueueRequest,
        DescribeTaskQueueResponse,
        |r| {
            let mut labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_cluster_info,
        GetClusterInfoRequest,
        GetClusterInfoResponse
    );
    (
        get_system_info,
        GetSystemInfoRequest,
        GetSystemInfoResponse
    );
    (
        list_task_queue_partitions,
        ListTaskQueuePartitionsRequest,
        ListTaskQueuePartitionsResponse,
        |r| {
            let mut labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        create_schedule,
        CreateScheduleRequest,
        CreateScheduleResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_schedule,
        DescribeScheduleRequest,
        DescribeScheduleResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_schedule,
        UpdateScheduleRequest,
        UpdateScheduleResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        patch_schedule,
        PatchScheduleRequest,
        PatchScheduleResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_schedule_matching_times,
        ListScheduleMatchingTimesRequest,
        ListScheduleMatchingTimesResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        delete_schedule,
        DeleteScheduleRequest,
        DeleteScheduleResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_schedules,
        ListSchedulesRequest,
        ListSchedulesResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_worker_build_id_ordering,
        UpdateWorkerBuildIdOrderingRequest,
        UpdateWorkerBuildIdOrderingResponse,
        |r| {
            let mut labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            labels.task_q_str(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_worker_build_id_ordering,
        GetWorkerBuildIdOrderingRequest,
        GetWorkerBuildIdOrderingResponse,
        |r| {
            let mut labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            labels.task_q_str(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_workflow,
        UpdateWorkflowRequest,
        UpdateWorkflowResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        start_batch_operation,
        StartBatchOperationRequest,
        StartBatchOperationResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        stop_batch_operation,
        StopBatchOperationRequest,
        StopBatchOperationResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_batch_operation,
        DescribeBatchOperationRequest,
        DescribeBatchOperationResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_batch_operations,
        ListBatchOperationsRequest,
        ListBatchOperationsResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
}

proxier! {
    OperatorService; ALL_IMPLEMENTED_OPERATOR_SERVICE_RPCS; OperatorServiceClient; operator_client;
    (add_search_attributes, AddSearchAttributesRequest, AddSearchAttributesResponse);
    (remove_search_attributes, RemoveSearchAttributesRequest, RemoveSearchAttributesResponse);
    (list_search_attributes, ListSearchAttributesRequest, ListSearchAttributesResponse);
    (delete_namespace, DeleteNamespaceRequest, DeleteNamespaceResponse,
        |r| {
            let labels = AttachMetricLabels::namespace(r.get_ref().namespace.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (add_or_update_remote_cluster, AddOrUpdateRemoteClusterRequest, AddOrUpdateRemoteClusterResponse);
    (remove_remote_cluster, RemoveRemoteClusterRequest, RemoveRemoteClusterResponse);
    (list_clusters, ListClustersRequest, ListClustersResponse);
}

proxier! {
    TestService; ALL_IMPLEMENTED_TEST_SERVICE_RPCS; TestServiceClient; test_client;
    (lock_time_skipping, LockTimeSkippingRequest, LockTimeSkippingResponse);
    (unlock_time_skipping, UnlockTimeSkippingRequest, UnlockTimeSkippingResponse);
    (sleep, SleepRequest, SleepResponse);
    (sleep_until, SleepUntilRequest, SleepResponse);
    (unlock_time_skipping_with_sleep, SleepRequest, SleepResponse);
    (get_current_time, (), GetCurrentTimeResponse);
}

proxier! {
    HealthService; ALL_IMPLEMENTED_HEALTH_SERVICE_RPCS; HealthClient; health_client;
    (check, HealthCheckRequest, HealthCheckResponse);
    (watch, HealthCheckRequest, tonic::codec::Streaming<HealthCheckResponse>);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ClientOptionsBuilder, RetryClient};
    use std::collections::HashSet;
    use temporal_sdk_core_protos::temporal::api::{
        operatorservice::v1::DeleteNamespaceRequest, workflowservice::v1::ListNamespacesRequest,
    };

    // Just to help make sure some stuff compiles. Not run.
    #[allow(dead_code)]
    async fn raw_client_retry_compiles() {
        let opts = ClientOptionsBuilder::default().build().unwrap();
        let raw_client = opts.connect_no_namespace(None, None).await.unwrap();
        let mut retry_client = RetryClient::new(raw_client, opts.retry_config);

        let list_ns_req = ListNamespacesRequest::default();
        let fact = |c: &mut RetryClient<_>, req| {
            let mut c = c.workflow_client().clone();
            async move { c.list_namespaces(req).await }.boxed()
        };
        retry_client
            .call("whatever", fact, Request::new(list_ns_req.clone()))
            .await
            .unwrap();

        // Operator svc method
        let del_ns_req = DeleteNamespaceRequest::default();
        let fact = |c: &mut RetryClient<_>, req| {
            let mut c = c.operator_client().clone();
            async move { c.delete_namespace(req).await }.boxed()
        };
        retry_client
            .call("whatever", fact, Request::new(del_ns_req.clone()))
            .await
            .unwrap();

        // Verify calling through traits works
        retry_client.list_namespaces(list_ns_req).await.unwrap();
        retry_client.delete_namespace(del_ns_req).await.unwrap();
        retry_client.get_current_time(()).await.unwrap();
        retry_client
            .check(HealthCheckRequest::default())
            .await
            .unwrap();
    }

    fn verify_methods(proto_def_str: &str, impl_list: &[&str]) {
        let methods: Vec<_> = proto_def_str
            .lines()
            .map(|l| l.trim())
            .filter(|l| l.starts_with("rpc"))
            .map(|l| {
                let stripped = l.strip_prefix("rpc ").unwrap();
                (stripped[..stripped.find('(').unwrap()]).trim()
            })
            .collect();
        let no_underscores: HashSet<_> = impl_list.iter().map(|x| x.replace('_', "")).collect();
        for method in methods {
            if !no_underscores.contains(&method.to_lowercase()) {
                panic!("RPC method {} is not implemented by raw client", method)
            }
        }
    }
    #[test]
    fn verify_all_workflow_service_methods_implemented() {
        // This is less work than trying to hook into the codegen process
        let proto_def =
            include_str!("../../protos/api_upstream/temporal/api/workflowservice/v1/service.proto");
        verify_methods(proto_def, ALL_IMPLEMENTED_WORKFLOW_SERVICE_RPCS);
    }

    #[test]
    fn verify_all_operator_service_methods_implemented() {
        let proto_def =
            include_str!("../../protos/api_upstream/temporal/api/operatorservice/v1/service.proto");
        verify_methods(proto_def, ALL_IMPLEMENTED_OPERATOR_SERVICE_RPCS);
    }

    #[test]
    fn verify_all_test_service_methods_implemented() {
        let proto_def =
            include_str!("../../protos/testsrv_upstream/temporal/api/testservice/v1/service.proto");
        verify_methods(proto_def, ALL_IMPLEMENTED_TEST_SERVICE_RPCS);
    }

    #[test]
    fn verify_all_health_service_methods_implemented() {
        let proto_def = include_str!("../../protos/grpc/health/v1/health.proto");
        verify_methods(proto_def, ALL_IMPLEMENTED_HEALTH_SERVICE_RPCS);
    }
}
