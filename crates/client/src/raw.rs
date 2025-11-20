//! We need a way to trait-ify the raw grpc client because there is no other way to get the
//! information we need via interceptors. This module contains the necessary stuff to make that
//! happen.

use crate::{
    Client, ConfiguredClient, LONG_POLL_TIMEOUT, RequestExt, RetryClient, SharedReplaceableClient,
    TEMPORAL_NAMESPACE_HEADER_KEY, TemporalServiceClient,
    metrics::namespace_kv,
    worker_registry::{ClientWorkerSet, Slot},
};
use dyn_clone::DynClone;
use futures_util::{FutureExt, TryFutureExt, future::BoxFuture};
use std::{any::Any, marker::PhantomData, sync::Arc};
use temporalio_common::{
    protos::{
        grpc::health::v1::{health_client::HealthClient, *},
        temporal::api::{
            cloud::cloudservice::{v1 as cloudreq, v1::cloud_service_client::CloudServiceClient},
            operatorservice::v1::{operator_service_client::OperatorServiceClient, *},
            taskqueue::v1::TaskQueue,
            testservice::v1::{test_service_client::TestServiceClient, *},
            workflowservice::v1::{workflow_service_client::WorkflowServiceClient, *},
        },
    },
    telemetry::metrics::MetricKeyValue,
};
use tonic::{
    Request, Response, Status,
    body::Body,
    client::GrpcService,
    metadata::{AsciiMetadataValue, KeyAndValueRef},
};

/// Something that has access to the raw grpc services
trait RawClientProducer {
    /// Returns information about workers associated with this client. Implementers outside of
    /// core can safely return `None`.
    fn get_workers_info(&self) -> Option<Arc<ClientWorkerSet>>;

    /// Return a workflow service client instance
    fn workflow_client(&mut self) -> Box<dyn WorkflowService>;

    /// Return a mutable ref to the operator service client instance
    fn operator_client(&mut self) -> Box<dyn OperatorService>;

    /// Return a mutable ref to the cloud service client instance
    fn cloud_client(&mut self) -> Box<dyn CloudService>;

    /// Return a mutable ref to the test service client instance
    fn test_client(&mut self) -> Box<dyn TestService>;

    /// Return a mutable ref to the health service client instance
    fn health_client(&mut self) -> Box<dyn HealthService>;
}

/// Any client that can make gRPC calls. The default implementation simply invokes the passed-in
/// function. Implementers may override this to provide things like retry behavior, ex:
/// [RetryClient].
#[async_trait::async_trait]
trait RawGrpcCaller: Send + Sync + 'static {
    async fn call<F, Req, Resp>(
        &mut self,
        _call_name: &'static str,
        mut callfn: F,
        req: Request<Req>,
    ) -> Result<Response<Resp>, Status>
    where
        Req: Clone + Unpin + Send + Sync + 'static,
        Resp: Send + 'static,
        F: Send + Sync + Unpin + 'static,
        for<'a> F:
            FnMut(&'a mut Self, Request<Req>) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
    {
        callfn(self, req).await
    }
}

trait ErasedRawClient: Send + Sync + 'static {
    fn erased_call(
        &mut self,
        call_name: &'static str,
        op: &mut dyn ErasedCallOp,
    ) -> BoxFuture<'static, Result<Response<Box<dyn Any + Send>>, Status>>;
}

trait ErasedCallOp: Send {
    fn invoke(
        &mut self,
        raw: &mut dyn ErasedRawClient,
        call_name: &'static str,
    ) -> BoxFuture<'static, Result<Response<Box<dyn Any + Send>>, Status>>;
}

struct CallShim<F, Req, Resp> {
    callfn: F,
    seed_req: Option<Request<Req>>,
    _resp: PhantomData<Resp>,
}

impl<F, Req, Resp> CallShim<F, Req, Resp> {
    fn new(callfn: F, seed_req: Request<Req>) -> Self {
        Self {
            callfn,
            seed_req: Some(seed_req),
            _resp: PhantomData,
        }
    }
}
impl<F, Req, Resp> ErasedCallOp for CallShim<F, Req, Resp>
where
    Req: Clone + Unpin + Send + Sync + 'static,
    Resp: Send + 'static,
    F: Send + Sync + Unpin + 'static,
    for<'a> F: FnMut(
        &'a mut dyn ErasedRawClient,
        Request<Req>,
    ) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
{
    fn invoke(
        &mut self,
        raw: &mut dyn ErasedRawClient,
        _call_name: &'static str,
    ) -> BoxFuture<'static, Result<Response<Box<dyn Any + Send>>, Status>> {
        (self.callfn)(
            raw,
            self.seed_req
                .take()
                .expect("CallShim must have request populated"),
        )
        .map(|res| res.map(|payload| payload.map(|t| Box::new(t) as Box<dyn Any + Send>)))
        .boxed()
    }
}

#[async_trait::async_trait]
impl RawGrpcCaller for dyn ErasedRawClient {
    async fn call<F, Req, Resp>(
        &mut self,
        call_name: &'static str,
        callfn: F,
        req: Request<Req>,
    ) -> Result<Response<Resp>, Status>
    where
        Req: Clone + Unpin + Send + Sync + 'static,
        Resp: Send + 'static,
        F: Send + Sync + Unpin + 'static,
        for<'a> F: FnMut(
            &'a mut dyn ErasedRawClient,
            Request<Req>,
        ) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
    {
        let mut shim = CallShim::new(callfn, req);
        let erased_resp = ErasedRawClient::erased_call(self, call_name, &mut shim).await?;
        Ok(erased_resp.map(|boxed| {
            *boxed
                .downcast()
                .expect("RawGrpcCaller erased response type mismatch")
        }))
    }
}

impl<T> ErasedRawClient for T
where
    T: RawGrpcCaller + 'static,
{
    fn erased_call(
        &mut self,
        call_name: &'static str,
        op: &mut dyn ErasedCallOp,
    ) -> BoxFuture<'static, Result<Response<Box<dyn Any + Send>>, Status>> {
        let raw: &mut dyn ErasedRawClient = self;
        op.invoke(raw, call_name)
    }
}

impl<RC> RawClientProducer for RetryClient<RC>
where
    RC: RawClientProducer + 'static,
{
    fn get_workers_info(&self) -> Option<Arc<ClientWorkerSet>> {
        self.get_client().get_workers_info()
    }

    fn workflow_client(&mut self) -> Box<dyn WorkflowService> {
        self.get_client_mut().workflow_client()
    }

    fn operator_client(&mut self) -> Box<dyn OperatorService> {
        self.get_client_mut().operator_client()
    }

    fn cloud_client(&mut self) -> Box<dyn CloudService> {
        self.get_client_mut().cloud_client()
    }

    fn test_client(&mut self) -> Box<dyn TestService> {
        self.get_client_mut().test_client()
    }

    fn health_client(&mut self) -> Box<dyn HealthService> {
        self.get_client_mut().health_client()
    }
}

#[async_trait::async_trait]
impl<RC> RawGrpcCaller for RetryClient<RC>
where
    RC: RawGrpcCaller + 'static,
{
    async fn call<F, Req, Resp>(
        &mut self,
        call_name: &'static str,
        mut callfn: F,
        mut req: Request<Req>,
    ) -> Result<Response<Resp>, Status>
    where
        Req: Clone + Unpin + Send + Sync + 'static,
        F: FnMut(&mut Self, Request<Req>) -> BoxFuture<'static, Result<Response<Resp>, Status>>,
        F: Send + Sync + Unpin + 'static,
    {
        let info = self.get_call_info(call_name, Some(&req));
        req.extensions_mut().insert(info.call_type);
        if info.call_type.is_long() {
            req.set_default_timeout(LONG_POLL_TIMEOUT);
        }
        let fact = || {
            let req_clone = req_cloner(&req);
            callfn(self, req_clone)
        };
        let res = Self::make_future_retry(info, fact);
        res.map_err(|(e, _attempt)| e).map_ok(|x| x.0).await
    }
}

/// Helper for cloning a tonic request as long as the inner message may be cloned.
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
    *new_req.extensions_mut() = cloneme.extensions().clone();
    new_req
}

impl<RC> RawClientProducer for SharedReplaceableClient<RC>
where
    RC: RawClientProducer + Clone + Send + Sync + 'static,
{
    fn get_workers_info(&self) -> Option<Arc<ClientWorkerSet>> {
        self.inner_cow().get_workers_info()
    }
    fn workflow_client(&mut self) -> Box<dyn WorkflowService> {
        self.inner_mut_refreshed().workflow_client()
    }

    fn operator_client(&mut self) -> Box<dyn OperatorService> {
        self.inner_mut_refreshed().operator_client()
    }

    fn cloud_client(&mut self) -> Box<dyn CloudService> {
        self.inner_mut_refreshed().cloud_client()
    }

    fn test_client(&mut self) -> Box<dyn TestService> {
        self.inner_mut_refreshed().test_client()
    }

    fn health_client(&mut self) -> Box<dyn HealthService> {
        self.inner_mut_refreshed().health_client()
    }
}

#[async_trait::async_trait]
impl<RC> RawGrpcCaller for SharedReplaceableClient<RC> where
    RC: RawGrpcCaller + Clone + Sync + 'static
{
}

impl RawClientProducer for TemporalServiceClient {
    fn get_workers_info(&self) -> Option<Arc<ClientWorkerSet>> {
        None
    }

    fn workflow_client(&mut self) -> Box<dyn WorkflowService> {
        self.workflow_svc()
    }

    fn operator_client(&mut self) -> Box<dyn OperatorService> {
        self.operator_svc()
    }

    fn cloud_client(&mut self) -> Box<dyn CloudService> {
        self.cloud_svc()
    }

    fn test_client(&mut self) -> Box<dyn TestService> {
        self.test_svc()
    }

    fn health_client(&mut self) -> Box<dyn HealthService> {
        self.health_svc()
    }
}

impl RawGrpcCaller for TemporalServiceClient {}

impl RawClientProducer for ConfiguredClient<TemporalServiceClient> {
    fn get_workers_info(&self) -> Option<Arc<ClientWorkerSet>> {
        Some(self.workers())
    }

    fn workflow_client(&mut self) -> Box<dyn WorkflowService> {
        self.client.workflow_client()
    }

    fn operator_client(&mut self) -> Box<dyn OperatorService> {
        self.client.operator_client()
    }

    fn cloud_client(&mut self) -> Box<dyn CloudService> {
        self.client.cloud_client()
    }

    fn test_client(&mut self) -> Box<dyn TestService> {
        self.client.test_client()
    }

    fn health_client(&mut self) -> Box<dyn HealthService> {
        self.client.health_client()
    }
}

impl RawGrpcCaller for ConfiguredClient<TemporalServiceClient> {}

impl RawClientProducer for Client {
    fn get_workers_info(&self) -> Option<Arc<ClientWorkerSet>> {
        self.inner.get_workers_info()
    }

    fn workflow_client(&mut self) -> Box<dyn WorkflowService> {
        self.inner.workflow_client()
    }

    fn operator_client(&mut self) -> Box<dyn OperatorService> {
        self.inner.operator_client()
    }

    fn cloud_client(&mut self) -> Box<dyn CloudService> {
        self.inner.cloud_client()
    }

    fn test_client(&mut self) -> Box<dyn TestService> {
        self.inner.test_client()
    }

    fn health_client(&mut self) -> Box<dyn HealthService> {
        self.inner.health_client()
    }
}

impl RawGrpcCaller for Client {}

#[derive(Clone, Debug)]
pub(super) struct AttachMetricLabels {
    pub(super) labels: Vec<MetricKeyValue>,
    pub(super) normal_task_queue: Option<String>,
    pub(super) sticky_task_queue: Option<String>,
}
impl AttachMetricLabels {
    pub(super) fn new(kvs: impl Into<Vec<MetricKeyValue>>) -> Self {
        Self {
            labels: kvs.into(),
            normal_task_queue: None,
            sticky_task_queue: None,
        }
    }
    pub(super) fn namespace(ns: impl Into<String>) -> Self {
        AttachMetricLabels::new(vec![namespace_kv(ns.into())])
    }
    pub(super) fn task_q(&mut self, tq: Option<TaskQueue>) -> &mut Self {
        if let Some(tq) = tq {
            if !tq.normal_name.is_empty() {
                self.sticky_task_queue = Some(tq.name);
                self.normal_task_queue = Some(tq.normal_name);
            } else {
                self.normal_task_queue = Some(tq.name);
            }
        }
        self
    }
    pub(super) fn task_q_str(&mut self, tq: impl Into<String>) -> &mut Self {
        self.normal_task_queue = Some(tq.into());
        self
    }
}

/// A request extension that, when set, should make the [RetryClient] consider this call to be a
/// [super::retry::CallType::UserLongPoll]
#[derive(Copy, Clone, Debug)]
pub(super) struct IsUserLongPoll;

macro_rules! proxy_def {
    ($client_type:tt, $client_meth:ident, $method:ident, $req:ty, $resp:ty, defaults) => {
        #[doc = concat!("See [", stringify!($client_type), "::", stringify!($method), "]")]
        fn $method(
            &mut self,
            _request: tonic::Request<$req>,
        ) -> BoxFuture<'_, Result<tonic::Response<$resp>, tonic::Status>> {
            async { Ok(tonic::Response::new(<$resp>::default())) }.boxed()
        }
    };
    ($client_type:tt, $client_meth:ident, $method:ident, $req:ty, $resp:ty) => {
        #[doc = concat!("See [", stringify!($client_type), "::", stringify!($method), "]")]
        fn $method(
            &mut self,
            _request: tonic::Request<$req>,
        ) -> BoxFuture<'_, Result<tonic::Response<$resp>, tonic::Status>>;
    };
}
/// Helps re-declare gRPC client methods
///
/// There are four forms:
///
/// * The first takes a closure that can modify the request. This is only called once, before the
///   actual rpc call is made, and before determinations are made about the kind of call (long poll
///   or not) and retry policy.
/// * The second takes three closures. The first can modify the request like in the first form.
///   The second can modify the request and return a value, and is called right before every call
///   (including on retries). The third is called with the response to the call after it resolves.
/// * The third and fourth are equivalents of the above that skip calling through the `call` method
///   and are implemented directly on the generated gRPC clients (IE: the bottom of the stack).
macro_rules! proxy_impl {
    ($client_type:tt, $client_meth:ident, $method:ident, $req:ty, $resp:ty $(, $closure:expr)?) => {
        #[doc = concat!("See [", stringify!($client_type), "::", stringify!($method), "]")]
        fn $method(
            &mut self,
            #[allow(unused_mut)]
            mut request: tonic::Request<$req>,
        ) -> BoxFuture<'_, Result<tonic::Response<$resp>, tonic::Status>> {
            $( type_closure_arg(&mut request, $closure); )*
            #[allow(unused_mut)]
            let fact = |c: &mut Self, mut req: tonic::Request<$req>| {
                let mut c = c.$client_meth();
                async move { c.$method(req).await }.boxed()
            };
            self.call(stringify!($method), fact, request)
        }
    };
    ($client_type:tt, $client_meth:ident, $method:ident, $req:ty, $resp:ty,
     $closure_request:expr, $closure_before:expr, $closure_after:expr) => {
        #[doc = concat!("See [", stringify!($client_type), "::", stringify!($method), "]")]
        fn $method(
            &mut self,
            mut request: tonic::Request<$req>,
        ) -> BoxFuture<'_, Result<tonic::Response<$resp>, tonic::Status>> {
            type_closure_arg(&mut request, $closure_request);
            #[allow(unused_mut)]
            let fact = |c: &mut Self, mut req: tonic::Request<$req>| {
                let data = type_closure_two_arg(&mut req, c.get_workers_info(), $closure_before);
                let mut c = c.$client_meth();
                async move {
                    type_closure_two_arg(c.$method(req).await, data, $closure_after)
                }.boxed()
            };
            self.call(stringify!($method), fact, request)
        }
    };
    ($client_type:tt, $method:ident, $req:ty, $resp:ty $(, $closure:expr)?) => {
        #[doc = concat!("See [", stringify!($client_type), "::", stringify!($method), "]")]
        fn $method(
            &mut self,
            #[allow(unused_mut)]
            mut request: tonic::Request<$req>,
        ) -> BoxFuture<'_, Result<tonic::Response<$resp>, tonic::Status>> {
            $( type_closure_arg(&mut request, $closure); )*
            async move { <$client_type<_>>::$method(self, request).await }.boxed()
        }
    };
    ($client_type:tt, $method:ident, $req:ty, $resp:ty,
     $closure_request:expr, $closure_before:expr, $closure_after:expr) => {
        #[doc = concat!("See [", stringify!($client_type), "::", stringify!($method), "]")]
        fn $method(
            &mut self,
            mut request: tonic::Request<$req>,
        ) -> BoxFuture<'_, Result<tonic::Response<$resp>, tonic::Status>> {
            type_closure_arg(&mut request, $closure_request);
            let data = type_closure_two_arg(&mut request, Option::<Arc<ClientWorkerSet>>::None,
                                            $closure_before);
            async move {
                type_closure_two_arg(<$client_type<_>>::$method(self, request).await,
                                     data, $closure_after)
            }.boxed()
        }
    };
}
macro_rules! proxier_impl {
    ($trait_name:ident; $impl_list_name:ident; $client_type:tt; $client_meth:ident;
     [$( proxy_def!($($def_args:tt)*); )*];
     $(($method:ident, $req:ty, $resp:ty
       $(, $closure:expr $(, $closure_before:expr, $closure_after:expr)?)? );)* ) => {
        #[cfg(test)]
        const $impl_list_name: &'static [&'static str] = &[$(stringify!($method)),*];

        #[doc = concat!("Trait version of [", stringify!($client_type), "]")]
        pub trait $trait_name: Send + Sync + DynClone
        {
            $( proxy_def!($($def_args)*); )*
        }
        dyn_clone::clone_trait_object!($trait_name);

        impl<RC> $trait_name for RC
        where
            RC: RawGrpcCaller + RawClientProducer + Clone,
        {
            $(
                proxy_impl!($client_type, $client_meth, $method, $req, $resp
                            $(,$closure $(,$closure_before, $closure_after)*)*);
            )*
        }

        impl<T: Send + Sync + 'static> RawGrpcCaller for $client_type<T> {}

        impl<T> $trait_name for $client_type<T>
        where
            T: GrpcService<Body> + Clone + Send + Sync + 'static,
            T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
            T::Error: Into<tonic::codegen::StdError>,
            <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
            <T as tonic::client::GrpcService<Body>>::Future: Send
        {
            $(
                proxy_impl!($client_type, $method, $req, $resp
                            $(,$closure $(,$closure_before, $closure_after)*)*);
            )*
        }
    };
}

macro_rules! proxier {
    ( $trait_name:ident; $impl_list_name:ident; $client_type:tt; $client_meth:ident;
      $(($method:ident, $req:ty, $resp:ty
         $(, $closure:expr $(, $closure_before:expr, $closure_after:expr)?)? );)* ) => {
        proxier_impl!($trait_name; $impl_list_name; $client_type; $client_meth;
                      [$(proxy_def!($client_type, $client_meth, $method, $req, $resp);)*];
                      $(($method, $req, $resp $(, $closure $(, $closure_before, $closure_after)?)?);)*);
    };
    ( $trait_name:ident; $impl_list_name:ident; $client_type:tt; $client_meth:ident; defaults;
      $(($method:ident, $req:ty, $resp:ty
         $(, $closure:expr $(, $closure_before:expr, $closure_after:expr)?)? );)* ) => {
        proxier_impl!($trait_name; $impl_list_name; $client_type; $client_meth;
                      [$(proxy_def!($client_type, $client_meth, $method, $req, $resp, defaults);)*];
                      $(($method, $req, $resp $(, $closure $(, $closure_before, $closure_after)?)?);)*);
    };
}

macro_rules! namespaced_request {
    ($req:ident) => {{
        let ns_str = $req.get_ref().namespace.clone();
        // Attach namespace header
        $req.metadata_mut().insert(
            TEMPORAL_NAMESPACE_HEADER_KEY,
            ns_str.parse().unwrap_or_else(|e| {
                warn!("Unable to parse namespace for header: {e:?}");
                AsciiMetadataValue::from_static("")
            }),
        );
        // Init metric labels
        AttachMetricLabels::namespace(ns_str)
    }};
}

// Nice little trick to avoid the callsite asking to type the closure parameter
fn type_closure_arg<T, R>(arg: T, f: impl FnOnce(T) -> R) -> R {
    f(arg)
}

fn type_closure_two_arg<T, R, S>(arg1: R, arg2: T, f: impl FnOnce(R, T) -> S) -> S {
    f(arg1, arg2)
}

proxier! {
    WorkflowService; ALL_IMPLEMENTED_WORKFLOW_SERVICE_RPCS; WorkflowServiceClient; workflow_client; defaults;
    (
        register_namespace,
        RegisterNamespaceRequest,
        RegisterNamespaceResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_namespace,
        DescribeNamespaceRequest,
        DescribeNamespaceResponse,
        |r| {
            let labels = namespaced_request!(r);
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
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        deprecate_namespace,
        DeprecateNamespaceRequest,
        DeprecateNamespaceResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        start_workflow_execution,
        StartWorkflowExecutionRequest,
        StartWorkflowExecutionResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        },
        |r, workers| {
            if let Some(workers) = workers {
                let mut slot: Option<Box<dyn Slot + Send>> = None;
                let req_mut = r.get_mut();
                if req_mut.request_eager_execution {
                    let namespace = req_mut.namespace.clone();
                    let task_queue = req_mut.task_queue.as_ref()
                                        .map(|tq| tq.name.clone()).unwrap_or_default();
                    match workers.try_reserve_wft_slot(namespace, task_queue) {
                        Some(reservation) => {
                            // Populate eager_worker_deployment_options from the slot reservation
                            if let Some(opts) = reservation.deployment_options {
                                req_mut.eager_worker_deployment_options = Some(temporalio_common::protos::temporal::api::deployment::v1::WorkerDeploymentOptions {
                                    deployment_name: opts.version.deployment_name,
                                    build_id: opts.version.build_id,
                                    worker_versioning_mode: if opts.use_worker_versioning {
                                        temporalio_common::protos::temporal::api::enums::v1::WorkerVersioningMode::Versioned.into()
                                    } else {
                                        temporalio_common::protos::temporal::api::enums::v1::WorkerVersioningMode::Unversioned.into()
                                    },
                                });                            }
                            slot = Some(reservation.slot);
                        }
                        None => req_mut.request_eager_execution = false
                    }
                }
                slot
            } else {
                None
            }
        },
        |resp, slot| {
            if let Some(s) = slot
                && let Ok(response) = resp.as_ref()
                    && let Some(task) = response.get_ref().clone().eager_workflow_task
                        && let Err(e) = s.schedule_wft(task) {
                            // This is a latency issue, i.e., the client does not need to handle
                            //  this error, because the WFT will be retried after a timeout.
                            warn!(details = ?e, "Eager workflow task rejected by worker.");
                        }
            resp
        }
    );
    (
        get_workflow_execution_history,
        GetWorkflowExecutionHistoryRequest,
        GetWorkflowExecutionHistoryResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
            if r.get_ref().wait_new_event {
                r.extensions_mut().insert(IsUserLongPoll);
            }
        }
    );
    (
        get_workflow_execution_history_reverse,
        GetWorkflowExecutionHistoryReverseRequest,
        GetWorkflowExecutionHistoryReverseResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        poll_workflow_task_queue,
        PollWorkflowTaskQueueRequest,
        PollWorkflowTaskQueueResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_workflow_task_completed,
        RespondWorkflowTaskCompletedRequest,
        RespondWorkflowTaskCompletedResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_workflow_task_failed,
        RespondWorkflowTaskFailedRequest,
        RespondWorkflowTaskFailedResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        poll_activity_task_queue,
        PollActivityTaskQueueRequest,
        PollActivityTaskQueueResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        record_activity_task_heartbeat,
        RecordActivityTaskHeartbeatRequest,
        RecordActivityTaskHeartbeatResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        record_activity_task_heartbeat_by_id,
        RecordActivityTaskHeartbeatByIdRequest,
        RecordActivityTaskHeartbeatByIdResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_activity_task_completed,
        RespondActivityTaskCompletedRequest,
        RespondActivityTaskCompletedResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_activity_task_completed_by_id,
        RespondActivityTaskCompletedByIdRequest,
        RespondActivityTaskCompletedByIdResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );

    (
        respond_activity_task_failed,
        RespondActivityTaskFailedRequest,
        RespondActivityTaskFailedResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_activity_task_failed_by_id,
        RespondActivityTaskFailedByIdRequest,
        RespondActivityTaskFailedByIdResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_activity_task_canceled,
        RespondActivityTaskCanceledRequest,
        RespondActivityTaskCanceledResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_activity_task_canceled_by_id,
        RespondActivityTaskCanceledByIdRequest,
        RespondActivityTaskCanceledByIdResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        request_cancel_workflow_execution,
        RequestCancelWorkflowExecutionRequest,
        RequestCancelWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        signal_workflow_execution,
        SignalWorkflowExecutionRequest,
        SignalWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        signal_with_start_workflow_execution,
        SignalWithStartWorkflowExecutionRequest,
        SignalWithStartWorkflowExecutionResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        reset_workflow_execution,
        ResetWorkflowExecutionRequest,
        ResetWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        terminate_workflow_execution,
        TerminateWorkflowExecutionRequest,
        TerminateWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        delete_workflow_execution,
        DeleteWorkflowExecutionRequest,
        DeleteWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_open_workflow_executions,
        ListOpenWorkflowExecutionsRequest,
        ListOpenWorkflowExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_closed_workflow_executions,
        ListClosedWorkflowExecutionsRequest,
        ListClosedWorkflowExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_workflow_executions,
        ListWorkflowExecutionsRequest,
        ListWorkflowExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_archived_workflow_executions,
        ListArchivedWorkflowExecutionsRequest,
        ListArchivedWorkflowExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        scan_workflow_executions,
        ScanWorkflowExecutionsRequest,
        ScanWorkflowExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        count_workflow_executions,
        CountWorkflowExecutionsRequest,
        CountWorkflowExecutionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        create_workflow_rule,
        CreateWorkflowRuleRequest,
        CreateWorkflowRuleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_workflow_rule,
        DescribeWorkflowRuleRequest,
        DescribeWorkflowRuleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        delete_workflow_rule,
        DeleteWorkflowRuleRequest,
        DeleteWorkflowRuleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_workflow_rules,
        ListWorkflowRulesRequest,
        ListWorkflowRulesResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        trigger_workflow_rule,
        TriggerWorkflowRuleRequest,
        TriggerWorkflowRuleResponse,
        |r| {
            let labels = namespaced_request!(r);
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
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        reset_sticky_task_queue,
        ResetStickyTaskQueueRequest,
        ResetStickyTaskQueueResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        query_workflow,
        QueryWorkflowRequest,
        QueryWorkflowResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_workflow_execution,
        DescribeWorkflowExecutionRequest,
        DescribeWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_task_queue,
        DescribeTaskQueueRequest,
        DescribeTaskQueueResponse,
        |r| {
            let mut labels = namespaced_request!(r);
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
            let mut labels = namespaced_request!(r);
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        create_schedule,
        CreateScheduleRequest,
        CreateScheduleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_schedule,
        DescribeScheduleRequest,
        DescribeScheduleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_schedule,
        UpdateScheduleRequest,
        UpdateScheduleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        patch_schedule,
        PatchScheduleRequest,
        PatchScheduleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_schedule_matching_times,
        ListScheduleMatchingTimesRequest,
        ListScheduleMatchingTimesResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        delete_schedule,
        DeleteScheduleRequest,
        DeleteScheduleResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_schedules,
        ListSchedulesRequest,
        ListSchedulesResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_worker_build_id_compatibility,
        UpdateWorkerBuildIdCompatibilityRequest,
        UpdateWorkerBuildIdCompatibilityResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q_str(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_worker_build_id_compatibility,
        GetWorkerBuildIdCompatibilityRequest,
        GetWorkerBuildIdCompatibilityResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q_str(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_worker_task_reachability,
        GetWorkerTaskReachabilityRequest,
        GetWorkerTaskReachabilityResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_workflow_execution,
        UpdateWorkflowExecutionRequest,
        UpdateWorkflowExecutionResponse,
        |r| {
            let labels = namespaced_request!(r);
            let exts = r.extensions_mut();
            exts.insert(labels);
            exts.insert(IsUserLongPoll);
        }
    );
    (
        poll_workflow_execution_update,
        PollWorkflowExecutionUpdateRequest,
        PollWorkflowExecutionUpdateResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        start_batch_operation,
        StartBatchOperationRequest,
        StartBatchOperationResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        stop_batch_operation,
        StopBatchOperationRequest,
        StopBatchOperationResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_batch_operation,
        DescribeBatchOperationRequest,
        DescribeBatchOperationResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_deployment,
        DescribeDeploymentRequest,
        DescribeDeploymentResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_batch_operations,
        ListBatchOperationsRequest,
        ListBatchOperationsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_deployments,
        ListDeploymentsRequest,
        ListDeploymentsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        execute_multi_operation,
        ExecuteMultiOperationRequest,
        ExecuteMultiOperationResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_current_deployment,
        GetCurrentDeploymentRequest,
        GetCurrentDeploymentResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_deployment_reachability,
        GetDeploymentReachabilityRequest,
        GetDeploymentReachabilityResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        get_worker_versioning_rules,
        GetWorkerVersioningRulesRequest,
        GetWorkerVersioningRulesResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q_str(&r.get_ref().task_queue);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_worker_versioning_rules,
        UpdateWorkerVersioningRulesRequest,
        UpdateWorkerVersioningRulesResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q_str(&r.get_ref().task_queue);
            r.extensions_mut().insert(labels);
        }
    );
    (
        poll_nexus_task_queue,
        PollNexusTaskQueueRequest,
        PollNexusTaskQueueResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_nexus_task_completed,
        RespondNexusTaskCompletedRequest,
        RespondNexusTaskCompletedResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        respond_nexus_task_failed,
        RespondNexusTaskFailedRequest,
        RespondNexusTaskFailedResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        set_current_deployment,
        SetCurrentDeploymentRequest,
        SetCurrentDeploymentResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        shutdown_worker,
        ShutdownWorkerRequest,
        ShutdownWorkerResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_activity_options,
        UpdateActivityOptionsRequest,
        UpdateActivityOptionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        pause_activity,
        PauseActivityRequest,
        PauseActivityResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        unpause_activity,
        UnpauseActivityRequest,
        UnpauseActivityResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_workflow_execution_options,
        UpdateWorkflowExecutionOptionsRequest,
        UpdateWorkflowExecutionOptionsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        reset_activity,
        ResetActivityRequest,
        ResetActivityResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        delete_worker_deployment,
        DeleteWorkerDeploymentRequest,
        DeleteWorkerDeploymentResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        delete_worker_deployment_version,
        DeleteWorkerDeploymentVersionRequest,
        DeleteWorkerDeploymentVersionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_worker_deployment,
        DescribeWorkerDeploymentRequest,
        DescribeWorkerDeploymentResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_worker_deployment_version,
        DescribeWorkerDeploymentVersionRequest,
        DescribeWorkerDeploymentVersionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_worker_deployments,
        ListWorkerDeploymentsRequest,
        ListWorkerDeploymentsResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        set_worker_deployment_current_version,
        SetWorkerDeploymentCurrentVersionRequest,
        SetWorkerDeploymentCurrentVersionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        set_worker_deployment_ramping_version,
        SetWorkerDeploymentRampingVersionRequest,
        SetWorkerDeploymentRampingVersionResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_worker_deployment_version_metadata,
        UpdateWorkerDeploymentVersionMetadataRequest,
        UpdateWorkerDeploymentVersionMetadataResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        list_workers,
        ListWorkersRequest,
        ListWorkersResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        record_worker_heartbeat,
        RecordWorkerHeartbeatRequest,
        RecordWorkerHeartbeatResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_task_queue_config,
        UpdateTaskQueueConfigRequest,
        UpdateTaskQueueConfigResponse,
        |r| {
            let mut labels = namespaced_request!(r);
            labels.task_q_str(r.get_ref().task_queue.clone());
            r.extensions_mut().insert(labels);
        }
    );
    (
        fetch_worker_config,
        FetchWorkerConfigRequest,
        FetchWorkerConfigResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        update_worker_config,
        UpdateWorkerConfigRequest,
        UpdateWorkerConfigResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        describe_worker,
        DescribeWorkerRequest,
        DescribeWorkerResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (
        set_worker_deployment_manager,
        SetWorkerDeploymentManagerRequest,
        SetWorkerDeploymentManagerResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
}

proxier! {
    OperatorService; ALL_IMPLEMENTED_OPERATOR_SERVICE_RPCS; OperatorServiceClient; operator_client; defaults;
    (add_search_attributes, AddSearchAttributesRequest, AddSearchAttributesResponse);
    (remove_search_attributes, RemoveSearchAttributesRequest, RemoveSearchAttributesResponse);
    (list_search_attributes, ListSearchAttributesRequest, ListSearchAttributesResponse);
    (delete_namespace, DeleteNamespaceRequest, DeleteNamespaceResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (add_or_update_remote_cluster, AddOrUpdateRemoteClusterRequest, AddOrUpdateRemoteClusterResponse);
    (remove_remote_cluster, RemoveRemoteClusterRequest, RemoveRemoteClusterResponse);
    (list_clusters, ListClustersRequest, ListClustersResponse);
    (get_nexus_endpoint, GetNexusEndpointRequest, GetNexusEndpointResponse);
    (create_nexus_endpoint, CreateNexusEndpointRequest, CreateNexusEndpointResponse);
    (update_nexus_endpoint, UpdateNexusEndpointRequest, UpdateNexusEndpointResponse);
    (delete_nexus_endpoint, DeleteNexusEndpointRequest, DeleteNexusEndpointResponse);
    (list_nexus_endpoints, ListNexusEndpointsRequest, ListNexusEndpointsResponse);
}

proxier! {
    CloudService; ALL_IMPLEMENTED_CLOUD_SERVICE_RPCS; CloudServiceClient; cloud_client; defaults;
    (get_users, cloudreq::GetUsersRequest, cloudreq::GetUsersResponse);
    (get_user, cloudreq::GetUserRequest, cloudreq::GetUserResponse);
    (create_user, cloudreq::CreateUserRequest, cloudreq::CreateUserResponse);
    (update_user, cloudreq::UpdateUserRequest, cloudreq::UpdateUserResponse);
    (delete_user, cloudreq::DeleteUserRequest, cloudreq::DeleteUserResponse);
    (set_user_namespace_access, cloudreq::SetUserNamespaceAccessRequest, cloudreq::SetUserNamespaceAccessResponse);
    (get_async_operation, cloudreq::GetAsyncOperationRequest, cloudreq::GetAsyncOperationResponse);
    (create_namespace, cloudreq::CreateNamespaceRequest, cloudreq::CreateNamespaceResponse);
    (get_namespaces, cloudreq::GetNamespacesRequest, cloudreq::GetNamespacesResponse);
    (get_namespace, cloudreq::GetNamespaceRequest, cloudreq::GetNamespaceResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (update_namespace, cloudreq::UpdateNamespaceRequest, cloudreq::UpdateNamespaceResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (rename_custom_search_attribute, cloudreq::RenameCustomSearchAttributeRequest, cloudreq::RenameCustomSearchAttributeResponse);
    (delete_namespace, cloudreq::DeleteNamespaceRequest, cloudreq::DeleteNamespaceResponse,
        |r| {
            let labels = namespaced_request!(r);
            r.extensions_mut().insert(labels);
        }
    );
    (failover_namespace_region, cloudreq::FailoverNamespaceRegionRequest, cloudreq::FailoverNamespaceRegionResponse);
    (add_namespace_region, cloudreq::AddNamespaceRegionRequest, cloudreq::AddNamespaceRegionResponse);
    (delete_namespace_region, cloudreq::DeleteNamespaceRegionRequest, cloudreq::DeleteNamespaceRegionResponse);
    (get_regions, cloudreq::GetRegionsRequest, cloudreq::GetRegionsResponse);
    (get_region, cloudreq::GetRegionRequest, cloudreq::GetRegionResponse);
    (get_api_keys, cloudreq::GetApiKeysRequest, cloudreq::GetApiKeysResponse);
    (get_api_key, cloudreq::GetApiKeyRequest, cloudreq::GetApiKeyResponse);
    (create_api_key, cloudreq::CreateApiKeyRequest, cloudreq::CreateApiKeyResponse);
    (update_api_key, cloudreq::UpdateApiKeyRequest, cloudreq::UpdateApiKeyResponse);
    (delete_api_key, cloudreq::DeleteApiKeyRequest, cloudreq::DeleteApiKeyResponse);
    (get_nexus_endpoints, cloudreq::GetNexusEndpointsRequest, cloudreq::GetNexusEndpointsResponse);
    (get_nexus_endpoint, cloudreq::GetNexusEndpointRequest, cloudreq::GetNexusEndpointResponse);
    (create_nexus_endpoint, cloudreq::CreateNexusEndpointRequest, cloudreq::CreateNexusEndpointResponse);
    (update_nexus_endpoint, cloudreq::UpdateNexusEndpointRequest, cloudreq::UpdateNexusEndpointResponse);
    (delete_nexus_endpoint, cloudreq::DeleteNexusEndpointRequest, cloudreq::DeleteNexusEndpointResponse);
    (get_user_groups, cloudreq::GetUserGroupsRequest, cloudreq::GetUserGroupsResponse);
    (get_user_group, cloudreq::GetUserGroupRequest, cloudreq::GetUserGroupResponse);
    (create_user_group, cloudreq::CreateUserGroupRequest, cloudreq::CreateUserGroupResponse);
    (update_user_group, cloudreq::UpdateUserGroupRequest, cloudreq::UpdateUserGroupResponse);
    (delete_user_group, cloudreq::DeleteUserGroupRequest, cloudreq::DeleteUserGroupResponse);
    (add_user_group_member, cloudreq::AddUserGroupMemberRequest, cloudreq::AddUserGroupMemberResponse);
    (remove_user_group_member, cloudreq::RemoveUserGroupMemberRequest, cloudreq::RemoveUserGroupMemberResponse);
    (get_user_group_members, cloudreq::GetUserGroupMembersRequest, cloudreq::GetUserGroupMembersResponse);
    (set_user_group_namespace_access, cloudreq::SetUserGroupNamespaceAccessRequest, cloudreq::SetUserGroupNamespaceAccessResponse);
    (create_service_account, cloudreq::CreateServiceAccountRequest, cloudreq::CreateServiceAccountResponse);
    (get_service_account, cloudreq::GetServiceAccountRequest, cloudreq::GetServiceAccountResponse);
    (get_service_accounts, cloudreq::GetServiceAccountsRequest, cloudreq::GetServiceAccountsResponse);
    (update_service_account, cloudreq::UpdateServiceAccountRequest, cloudreq::UpdateServiceAccountResponse);
    (delete_service_account, cloudreq::DeleteServiceAccountRequest, cloudreq::DeleteServiceAccountResponse);
    (get_usage, cloudreq::GetUsageRequest, cloudreq::GetUsageResponse);
    (get_account, cloudreq::GetAccountRequest, cloudreq::GetAccountResponse);
    (update_account, cloudreq::UpdateAccountRequest, cloudreq::UpdateAccountResponse);
    (create_namespace_export_sink, cloudreq::CreateNamespaceExportSinkRequest, cloudreq::CreateNamespaceExportSinkResponse);
    (get_namespace_export_sink, cloudreq::GetNamespaceExportSinkRequest, cloudreq::GetNamespaceExportSinkResponse);
    (get_namespace_export_sinks, cloudreq::GetNamespaceExportSinksRequest, cloudreq::GetNamespaceExportSinksResponse);
    (update_namespace_export_sink, cloudreq::UpdateNamespaceExportSinkRequest, cloudreq::UpdateNamespaceExportSinkResponse);
    (delete_namespace_export_sink, cloudreq::DeleteNamespaceExportSinkRequest, cloudreq::DeleteNamespaceExportSinkResponse);
    (validate_namespace_export_sink, cloudreq::ValidateNamespaceExportSinkRequest, cloudreq::ValidateNamespaceExportSinkResponse);
    (update_namespace_tags, cloudreq::UpdateNamespaceTagsRequest, cloudreq::UpdateNamespaceTagsResponse);
    (create_connectivity_rule, cloudreq::CreateConnectivityRuleRequest, cloudreq::CreateConnectivityRuleResponse);
    (get_connectivity_rule, cloudreq::GetConnectivityRuleRequest, cloudreq::GetConnectivityRuleResponse);
    (get_connectivity_rules, cloudreq::GetConnectivityRulesRequest, cloudreq::GetConnectivityRulesResponse);
    (delete_connectivity_rule, cloudreq::DeleteConnectivityRuleRequest, cloudreq::DeleteConnectivityRuleResponse);
    (set_service_account_namespace_access, cloudreq::SetServiceAccountNamespaceAccessRequest, cloudreq::SetServiceAccountNamespaceAccessResponse);
    (validate_account_audit_log_sink, cloudreq::ValidateAccountAuditLogSinkRequest, cloudreq::ValidateAccountAuditLogSinkResponse);
}

proxier! {
    TestService; ALL_IMPLEMENTED_TEST_SERVICE_RPCS; TestServiceClient; test_client; defaults;
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
    use temporalio_common::protos::temporal::api::{
        operatorservice::v1::DeleteNamespaceRequest, workflowservice::v1::ListNamespacesRequest,
    };
    use temporalio_common::worker::WorkerTaskTypes;
    use tonic::IntoRequest;
    use uuid::Uuid;

    // Just to help make sure some stuff compiles. Not run.
    #[allow(dead_code)]
    async fn raw_client_retry_compiles() {
        let opts = ClientOptionsBuilder::default().build().unwrap();
        let raw_client = opts.connect_no_namespace(None).await.unwrap();
        let mut retry_client = RetryClient::new(raw_client, opts.retry_config);

        let list_ns_req = ListNamespacesRequest::default();
        let fact = |c: &mut RetryClient<_>, req| {
            let mut c = c.workflow_client();
            async move { c.list_namespaces(req).await }.boxed()
        };
        retry_client
            .call("whatever", fact, Request::new(list_ns_req.clone()))
            .await
            .unwrap();

        // Operator svc method
        let op_del_ns_req = DeleteNamespaceRequest::default();
        let fact = |c: &mut RetryClient<_>, req| {
            let mut c = c.operator_client();
            async move { c.delete_namespace(req).await }.boxed()
        };
        retry_client
            .call("whatever", fact, Request::new(op_del_ns_req.clone()))
            .await
            .unwrap();

        // Cloud svc method
        let cloud_del_ns_req = cloudreq::DeleteNamespaceRequest::default();
        let fact = |c: &mut RetryClient<_>, req| {
            let mut c = c.cloud_client();
            async move { c.delete_namespace(req).await }.boxed()
        };
        retry_client
            .call("whatever", fact, Request::new(cloud_del_ns_req.clone()))
            .await
            .unwrap();

        // Verify calling through traits works
        retry_client
            .list_namespaces(list_ns_req.into_request())
            .await
            .unwrap();
        // Have to disambiguate operator and cloud service
        OperatorService::delete_namespace(&mut retry_client, op_del_ns_req.into_request())
            .await
            .unwrap();
        CloudService::delete_namespace(&mut retry_client, cloud_del_ns_req.into_request())
            .await
            .unwrap();
        retry_client
            .get_current_time(().into_request())
            .await
            .unwrap();
        retry_client
            .check(HealthCheckRequest::default().into_request())
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
                stripped[..stripped.find('(').unwrap()].trim()
            })
            .collect();
        let no_underscores: HashSet<_> = impl_list.iter().map(|x| x.replace('_', "")).collect();
        let mut not_implemented = vec![];
        for method in methods {
            if !no_underscores.contains(&method.to_lowercase()) {
                not_implemented.push(method);
            }
        }
        if !not_implemented.is_empty() {
            panic!(
                "The following RPC methods are not implemented by raw client: {not_implemented:?}"
            );
        }
    }
    #[test]
    fn verify_all_workflow_service_methods_implemented() {
        // This is less work than trying to hook into the codegen process
        let proto_def = include_str!(
            "../../common/protos/api_upstream/temporal/api/workflowservice/v1/service.proto"
        );
        verify_methods(proto_def, ALL_IMPLEMENTED_WORKFLOW_SERVICE_RPCS);
    }

    #[test]
    fn verify_all_operator_service_methods_implemented() {
        let proto_def = include_str!(
            "../../common/protos/api_upstream/temporal/api/operatorservice/v1/service.proto"
        );
        verify_methods(proto_def, ALL_IMPLEMENTED_OPERATOR_SERVICE_RPCS);
    }

    #[test]
    fn verify_all_cloud_service_methods_implemented() {
        let proto_def = include_str!(
            "../../common/protos/api_cloud_upstream/temporal/api/cloud/cloudservice/v1/service.proto"
        );
        verify_methods(proto_def, ALL_IMPLEMENTED_CLOUD_SERVICE_RPCS);
    }

    #[test]
    fn verify_all_test_service_methods_implemented() {
        let proto_def = include_str!(
            "../../common/protos/testsrv_upstream/temporal/api/testservice/v1/service.proto"
        );
        verify_methods(proto_def, ALL_IMPLEMENTED_TEST_SERVICE_RPCS);
    }

    #[test]
    fn verify_all_health_service_methods_implemented() {
        let proto_def = include_str!("../../common/protos/grpc/health/v1/health.proto");
        verify_methods(proto_def, ALL_IMPLEMENTED_HEALTH_SERVICE_RPCS);
    }

    #[tokio::test]
    async fn can_mock_services() {
        #[derive(Clone)]
        struct MyFakeServices {}
        impl RawGrpcCaller for MyFakeServices {}
        impl WorkflowService for MyFakeServices {
            fn list_namespaces(
                &mut self,
                _request: Request<ListNamespacesRequest>,
            ) -> BoxFuture<'_, Result<Response<ListNamespacesResponse>, Status>> {
                async {
                    Ok(Response::new(ListNamespacesResponse {
                        namespaces: vec![DescribeNamespaceResponse {
                            failover_version: 12345,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }))
                }
                .boxed()
            }
        }
        impl OperatorService for MyFakeServices {}
        impl CloudService for MyFakeServices {}
        impl TestService for MyFakeServices {}
        // Health service isn't possible to create a default impl for.
        impl HealthService for MyFakeServices {
            fn check(
                &mut self,
                _request: tonic::Request<HealthCheckRequest>,
            ) -> BoxFuture<'_, Result<tonic::Response<HealthCheckResponse>, tonic::Status>>
            {
                todo!()
            }
            fn watch(
                &mut self,
                _request: tonic::Request<HealthCheckRequest>,
            ) -> BoxFuture<
                '_,
                Result<
                    tonic::Response<tonic::codec::Streaming<HealthCheckResponse>>,
                    tonic::Status,
                >,
            > {
                todo!()
            }
        }
        let mut mocked_client = TemporalServiceClient::from_services(
            Box::new(MyFakeServices {}),
            Box::new(MyFakeServices {}),
            Box::new(MyFakeServices {}),
            Box::new(MyFakeServices {}),
            Box::new(MyFakeServices {}),
        );
        let r = mocked_client
            .list_namespaces(ListNamespacesRequest::default().into_request())
            .await
            .unwrap();
        assert_eq!(r.into_inner().namespaces[0].failover_version, 12345);
    }

    #[rstest::rstest]
    #[case::with_versioning(true)]
    #[case::without_versioning(false)]
    #[tokio::test]
    async fn eager_reservations_attach_deployment_options(#[case] use_worker_versioning: bool) {
        use crate::worker_registry::{MockClientWorker, MockSlot};
        use temporalio_common::{
            protos::temporal::api::enums::v1::WorkerVersioningMode,
            worker::{WorkerDeploymentOptions, WorkerDeploymentVersion},
        };

        let expected_mode = if use_worker_versioning {
            WorkerVersioningMode::Versioned
        } else {
            WorkerVersioningMode::Unversioned
        };

        #[derive(Clone)]
        struct MyFakeServices {
            client_worker_set: Arc<ClientWorkerSet>,
            expected_mode: WorkerVersioningMode,
        }
        impl RawGrpcCaller for MyFakeServices {}
        impl RawClientProducer for MyFakeServices {
            fn get_workers_info(&self) -> Option<Arc<ClientWorkerSet>> {
                Some(self.client_worker_set.clone())
            }
            fn workflow_client(&mut self) -> Box<dyn WorkflowService> {
                Box::new(MyFakeWfClient {
                    expected_mode: self.expected_mode,
                })
            }
            fn operator_client(&mut self) -> Box<dyn OperatorService> {
                unimplemented!()
            }
            fn cloud_client(&mut self) -> Box<dyn CloudService> {
                unimplemented!()
            }
            fn test_client(&mut self) -> Box<dyn TestService> {
                unimplemented!()
            }
            fn health_client(&mut self) -> Box<dyn HealthService> {
                unimplemented!()
            }
        }

        let deployment_opts = WorkerDeploymentOptions {
            version: WorkerDeploymentVersion {
                deployment_name: "test-deployment".to_string(),
                build_id: "test-build-123".to_string(),
            },
            use_worker_versioning,
            default_versioning_behavior: None,
        };

        let mut mock_provider = MockClientWorker::new();
        mock_provider
            .expect_namespace()
            .return_const("test-namespace".to_string());
        mock_provider
            .expect_task_queue()
            .return_const("test-task-queue".to_string());
        let mut mock_slot = MockSlot::new();
        mock_slot.expect_schedule_wft().returning(|_| Ok(()));
        mock_provider
            .expect_try_reserve_wft_slot()
            .return_once(|| Some(Box::new(mock_slot)));
        mock_provider
            .expect_deployment_options()
            .return_const(Some(deployment_opts.clone()));
        mock_provider.expect_heartbeat_enabled().return_const(false);
        let uuid = Uuid::new_v4();
        mock_provider
            .expect_worker_instance_key()
            .return_const(uuid);
        mock_provider
            .expect_worker_task_types()
            .return_const(WorkerTaskTypes {
                enable_workflows: true,
                enable_local_activities: true,
                enable_remote_activities: true,
                enable_nexus: true,
            });

        let client_worker_set = Arc::new(ClientWorkerSet::new());
        client_worker_set
            .register_worker(Arc::new(mock_provider), true)
            .unwrap();

        #[derive(Clone)]
        struct MyFakeWfClient {
            expected_mode: WorkerVersioningMode,
        }
        impl WorkflowService for MyFakeWfClient {
            fn start_workflow_execution(
                &mut self,
                request: tonic::Request<StartWorkflowExecutionRequest>,
            ) -> BoxFuture<'_, Result<tonic::Response<StartWorkflowExecutionResponse>, tonic::Status>>
            {
                let req = request.into_inner();
                let expected_mode = self.expected_mode;

                assert!(
                    req.eager_worker_deployment_options.is_some(),
                    "eager_worker_deployment_options should be populated"
                );

                let opts = req.eager_worker_deployment_options.as_ref().unwrap();
                assert_eq!(opts.deployment_name, "test-deployment");
                assert_eq!(opts.build_id, "test-build-123");
                assert_eq!(opts.worker_versioning_mode, expected_mode as i32);

                async { Ok(Response::new(StartWorkflowExecutionResponse::default())) }.boxed()
            }
        }

        let mut mfs = MyFakeServices {
            client_worker_set,
            expected_mode,
        };

        // Create a request with eager execution enabled
        let req = StartWorkflowExecutionRequest {
            namespace: "test-namespace".to_string(),
            workflow_id: "test-wf-id".to_string(),
            workflow_type: Some(
                temporalio_common::protos::temporal::api::common::v1::WorkflowType {
                    name: "test-workflow".to_string(),
                },
            ),
            task_queue: Some(TaskQueue {
                name: "test-task-queue".to_string(),
                kind: 0,
                normal_name: String::new(),
            }),
            request_eager_execution: true,
            ..Default::default()
        };

        mfs.start_workflow_execution(req.into_request())
            .await
            .unwrap();
    }
}
