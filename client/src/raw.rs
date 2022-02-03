//! We need a way to trait-ify the raw grpc client because there is no other way to get the
//! information we need via interceptors. This module contains the necessary stuff to make that
//! happen.

use crate::{metrics::namespace_kv, raw::sealed::RawClientLike, LONG_POLL_TIMEOUT};
use futures::{future::BoxFuture, FutureExt};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::workflow_service_client::WorkflowServiceClient;
use tonic::metadata::KeyAndValueRef;

pub(super) mod sealed {
    use super::*;
    use crate::RetryGateway;
    use futures::TryFutureExt;
    use tonic::{Request, Response, Status};

    /// Something that has a workflow service client
    #[async_trait::async_trait]
    pub trait RawClientLike<T> {
        /// Return the actual client instance
        fn client(&mut self) -> &mut WorkflowServiceClient<T>;

        async fn do_call<F, Req, Resp>(
            &mut self,
            call_name: &'static str,
            mut callfn: F,
            req: tonic::Request<Req>,
        ) -> Result<tonic::Response<Resp>, tonic::Status>
        where
            Req: Clone + Unpin + Send + Sync + 'static,
            F: FnMut(
                &mut WorkflowServiceClient<T>,
                tonic::Request<Req>,
            )
                -> BoxFuture<'static, Result<tonic::Response<Resp>, tonic::Status>>,
            F: Send + Sync + Unpin + 'static;
    }

    #[async_trait::async_trait]
    impl<T> RawClientLike<T> for RetryGateway<WorkflowServiceClient<T>>
    where
        T: Send + Sync + Clone + 'static,
    {
        fn client(&mut self) -> &mut WorkflowServiceClient<T> {
            self.get_client_mut()
        }

        async fn do_call<F, Req, Resp>(
            &mut self,
            call_name: &'static str,
            mut callfn: F,
            req: Request<Req>,
        ) -> Result<Response<Resp>, Status>
        where
            Req: Clone + Unpin + Send + Sync + 'static,
            F: FnMut(
                &mut WorkflowServiceClient<T>,
                Request<Req>,
            ) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
            F: Send + Sync + Unpin + 'static,
        {
            let rtc = self.get_retry_config(call_name);
            let req = req_cloner(&req);
            let fact = || {
                let req_clone = req_cloner(&req);
                callfn(self.client(), req_clone)
            };
            let res = Self::make_future_retry(rtc, fact, call_name);
            res.map_err(|(e, _attempt)| e).map_ok(|x| x.0).await
        }
    }

    #[async_trait::async_trait]
    impl<T> RawClientLike<T> for WorkflowServiceClient<T>
    where
        T: Send + Sync + Clone + 'static,
    {
        fn client(&mut self) -> &mut WorkflowServiceClient<T> {
            self
        }

        async fn do_call<F, Req, Resp>(
            &mut self,
            _call_name: &'static str,
            mut callfn: F,
            req: Request<Req>,
        ) -> Result<Response<Resp>, Status>
        where
            Req: Clone + Unpin + Send + Sync,
            F: FnMut(
                &mut WorkflowServiceClient<T>,
                Request<Req>,
            ) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
            F: Send + Sync + Unpin + 'static,
        {
            callfn(self.client(), req).await
        }
    }
}

/// Helper for cloning a tonic request as long as the inner message may be cloned.
/// We drop extensions, so, lang bridges can't pass those in :shrug:
fn req_cloner<T: Clone>(cloneme: &tonic::Request<T>) -> tonic::Request<T> {
    let msg = cloneme.get_ref().clone();
    let mut new_req = tonic::Request::new(msg);
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
}

// Blanket impl the trait for all raw-client-like things. Since the trait default-implements
// everything, there's nothing to actually implement.
impl<RC, T> WorkflowService<T> for RC
where
    RC: RawClientLike<T>,
    T: tonic::client::GrpcService<tonic::body::BoxBody> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
}

/// Helps re-declare gRPC client methods
macro_rules! proxy {
    ($method:ident, $req:ident, $resp:ident $(, $closure:expr)?) => {
        #[doc = concat!("See [WorkflowServiceClient::", stringify!($method), "]")]
        fn $method(
            &mut self,
            request: impl tonic::IntoRequest<super::$req>,
        ) -> BoxFuture<Result<tonic::Response<super::$resp>, tonic::Status>> {
            #[allow(unused_mut)]
            let fact = |c: &mut WorkflowServiceClient<T>, mut req: tonic::Request<super::$req>| {
                $( type_closure_arg(&mut req, $closure); )*
                let mut c = c.clone();
                async move { c.$method(req).await }.boxed()
            };
            self.do_call(stringify!($method), fact, request.into_request())
        }
    };
}
// Nice little trick to avoid the callsite asking to type the closure parameter
fn type_closure_arg<T, R>(arg: T, f: impl FnOnce(T) -> R) -> R {
    f(arg)
}

/// Trait version of the generated workflow service client with modifications to attach appropriate
/// metric labels or whatever else to requests
#[async_trait::async_trait]
pub trait WorkflowService<T>: RawClientLike<T>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + Send + Clone + 'static,
    T::ResponseBody: tonic::codegen::Body + Send + 'static,
    T::Error: Into<tonic::codegen::StdError>,
    T::Future: Send,
    <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
{
    proxy!(
        register_namespace,
        RegisterNamespaceRequest,
        RegisterNamespaceResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        describe_namespace,
        DescribeNamespaceRequest,
        DescribeNamespaceResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        list_namespaces,
        ListNamespacesRequest,
        ListNamespacesResponse
    );
    proxy!(
        update_namespace,
        UpdateNamespaceRequest,
        UpdateNamespaceResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        deprecate_namespace,
        DeprecateNamespaceRequest,
        DeprecateNamespaceResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        start_workflow_execution,
        StartWorkflowExecutionRequest,
        StartWorkflowExecutionResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        get_workflow_execution_history,
        GetWorkflowExecutionHistoryRequest,
        GetWorkflowExecutionHistoryResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        poll_workflow_task_queue,
        PollWorkflowTaskQueueRequest,
        PollWorkflowTaskQueueResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
            r.set_timeout(LONG_POLL_TIMEOUT);
        }
    );
    proxy!(
        respond_workflow_task_completed,
        RespondWorkflowTaskCompletedRequest,
        RespondWorkflowTaskCompletedResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        respond_workflow_task_failed,
        RespondWorkflowTaskFailedRequest,
        RespondWorkflowTaskFailedResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        poll_activity_task_queue,
        PollActivityTaskQueueRequest,
        PollActivityTaskQueueResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
            r.set_timeout(LONG_POLL_TIMEOUT);
        }
    );
    proxy!(
        record_activity_task_heartbeat,
        RecordActivityTaskHeartbeatRequest,
        RecordActivityTaskHeartbeatResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        record_activity_task_heartbeat_by_id,
        RecordActivityTaskHeartbeatByIdRequest,
        RecordActivityTaskHeartbeatByIdResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        respond_activity_task_completed,
        RespondActivityTaskCompletedRequest,
        RespondActivityTaskCompletedResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        respond_activity_task_completed_by_id,
        RespondActivityTaskCompletedByIdRequest,
        RespondActivityTaskCompletedByIdResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );

    proxy!(
        respond_activity_task_failed,
        RespondActivityTaskFailedRequest,
        RespondActivityTaskFailedResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        respond_activity_task_failed_by_id,
        RespondActivityTaskFailedByIdRequest,
        RespondActivityTaskFailedByIdResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        respond_activity_task_canceled,
        RespondActivityTaskCanceledRequest,
        RespondActivityTaskCanceledResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        respond_activity_task_canceled_by_id,
        RespondActivityTaskCanceledByIdRequest,
        RespondActivityTaskCanceledByIdResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        request_cancel_workflow_execution,
        RequestCancelWorkflowExecutionRequest,
        RequestCancelWorkflowExecutionResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        signal_workflow_execution,
        SignalWorkflowExecutionRequest,
        SignalWorkflowExecutionResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        signal_with_start_workflow_execution,
        SignalWithStartWorkflowExecutionRequest,
        SignalWithStartWorkflowExecutionResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        reset_workflow_execution,
        ResetWorkflowExecutionRequest,
        ResetWorkflowExecutionResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        terminate_workflow_execution,
        TerminateWorkflowExecutionRequest,
        TerminateWorkflowExecutionResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        list_open_workflow_executions,
        ListOpenWorkflowExecutionsRequest,
        ListOpenWorkflowExecutionsResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        list_closed_workflow_executions,
        ListClosedWorkflowExecutionsRequest,
        ListClosedWorkflowExecutionsResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        list_workflow_executions,
        ListWorkflowExecutionsRequest,
        ListWorkflowExecutionsResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        list_archived_workflow_executions,
        ListArchivedWorkflowExecutionsRequest,
        ListArchivedWorkflowExecutionsResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        scan_workflow_executions,
        ScanWorkflowExecutionsRequest,
        ScanWorkflowExecutionsResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        count_workflow_executions,
        CountWorkflowExecutionsRequest,
        CountWorkflowExecutionsResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        get_search_attributes,
        GetSearchAttributesRequest,
        GetSearchAttributesResponse
    );
    proxy!(
        respond_query_task_completed,
        RespondQueryTaskCompletedRequest,
        RespondQueryTaskCompletedResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        reset_sticky_task_queue,
        ResetStickyTaskQueueRequest,
        ResetStickyTaskQueueResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        query_workflow,
        QueryWorkflowRequest,
        QueryWorkflowResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        describe_workflow_execution,
        DescribeWorkflowExecutionRequest,
        DescribeWorkflowExecutionResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        describe_task_queue,
        DescribeTaskQueueRequest,
        DescribeTaskQueueResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
    proxy!(
        get_cluster_info,
        GetClusterInfoRequest,
        GetClusterInfoResponse
    );
    proxy!(
        list_task_queue_partitions,
        ListTaskQueuePartitionsRequest,
        ListTaskQueuePartitionsResponse,
        |r| {
            let labels = AttachMetricLabels::new(vec![namespace_kv(r.get_ref().namespace.clone())]);
            r.extensions_mut().insert(labels);
        }
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mocks::fake_sg_opts;
    use crate::{RetryGateway, WorkflowServiceClientWithMetrics};
    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::ListNamespacesRequest;

    // Just to help me make sure some stuff compiles
    #[allow(dead_code)]
    async fn raw_client_retry_compiles() {
        let opts = fake_sg_opts();
        let raw_client = opts.connect_raw(None).await.unwrap();
        let mut retry_client = RetryGateway::new(raw_client, opts.retry_config);

        let the_request = ListNamespacesRequest::default();
        let fact = |c: &mut WorkflowServiceClientWithMetrics, req| {
            let mut c = c.clone();
            async move { c.list_namespaces(req).await }.boxed()
        };
        retry_client
            .do_call("whatever", fact, tonic::Request::new(the_request))
            .await
            .unwrap();
    }
}
