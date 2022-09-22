use crate::{AttachMetricLabels, LONG_POLL_METHOD_NAMES};
use futures::{future::BoxFuture, FutureExt};
use opentelemetry::{
    metrics::{Counter, Histogram, Meter},
    KeyValue,
};
use std::{
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tonic::{body::BoxBody, transport::Channel};
use tower::Service;

/// Used to track context associated with metrics, and record/update them
// Possible improvement: make generic over some type tag so that methods are only exposed if the
// appropriate k/vs have already been set.
#[derive(Clone, Debug)]
pub struct MetricsContext {
    ctx: opentelemetry::Context,
    kvs: Arc<Vec<KeyValue>>,
    poll_is_long: bool,

    svc_request: Counter<u64>,
    svc_request_failed: Counter<u64>,
    long_svc_request: Counter<u64>,
    long_svc_request_failed: Counter<u64>,

    svc_request_latency: Histogram<u64>,
    long_svc_request_latency: Histogram<u64>,
}

impl MetricsContext {
    pub(crate) fn new(kvs: Vec<KeyValue>, meter: &Meter) -> Self {
        Self {
            ctx: opentelemetry::Context::current(),
            kvs: Arc::new(kvs),
            poll_is_long: false,
            svc_request: meter.u64_counter("request").init(),
            svc_request_failed: meter.u64_counter("request_failure").init(),
            long_svc_request: meter.u64_counter("long_request").init(),
            long_svc_request_failed: meter.u64_counter("long_request_failure").init(),
            svc_request_latency: meter.u64_histogram("request_latency").init(),
            long_svc_request_latency: meter.u64_histogram("long_request_latency").init(),
        }
    }

    /// Extend an existing metrics context with new attributes, returning a new one
    pub(crate) fn with_new_attrs(&self, new_kvs: impl IntoIterator<Item = KeyValue>) -> Self {
        let mut r = self.clone();
        r.add_new_attrs(new_kvs);
        r
    }

    /// Add new attributes to the context, mutating it
    pub(crate) fn add_new_attrs(&mut self, new_kvs: impl IntoIterator<Item = KeyValue>) {
        Arc::make_mut(&mut self.kvs).extend(new_kvs);
    }

    pub(crate) fn set_is_long_poll(&mut self) {
        self.poll_is_long = true;
    }

    /// A request to the temporal service was made
    pub(crate) fn svc_request(&self) {
        if self.poll_is_long {
            self.long_svc_request.add(&self.ctx, 1, &self.kvs);
        } else {
            self.svc_request.add(&self.ctx, 1, &self.kvs);
        }
    }

    /// A request to the temporal service failed
    pub(crate) fn svc_request_failed(&self) {
        if self.poll_is_long {
            self.long_svc_request_failed.add(&self.ctx, 1, &self.kvs);
        } else {
            self.svc_request_failed.add(&self.ctx, 1, &self.kvs);
        }
    }

    /// Record service request latency
    pub(crate) fn record_svc_req_latency(&self, dur: Duration) {
        if self.poll_is_long {
            self.long_svc_request_latency
                .record(&self.ctx, dur.as_millis() as u64, &self.kvs);
        } else {
            self.svc_request_latency
                .record(&self.ctx, dur.as_millis() as u64, &self.kvs);
        }
    }
}

const KEY_NAMESPACE: &str = "namespace";
const KEY_SVC_METHOD: &str = "operation";
const KEY_TASK_QUEUE: &str = "task_queue";

pub(crate) fn namespace_kv(ns: String) -> KeyValue {
    KeyValue::new(KEY_NAMESPACE, ns)
}

pub(crate) fn task_queue_kv(tq: String) -> KeyValue {
    KeyValue::new(KEY_TASK_QUEUE, tq)
}

pub(crate) fn svc_operation(op: String) -> KeyValue {
    KeyValue::new(KEY_SVC_METHOD, op)
}

/// Implements metrics functionality for gRPC (really, any http) calls
#[derive(Debug, Clone)]
pub struct GrpcMetricSvc {
    pub(crate) inner: Channel,
    // If set to none, metrics are a no-op
    pub(crate) metrics: Option<MetricsContext>,
}

impl Service<http::Request<BoxBody>> for GrpcMetricSvc {
    type Response = http::Response<tonic::transport::Body>;
    type Error = tonic::transport::Error;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, mut req: http::Request<BoxBody>) -> Self::Future {
        let metrics = self
            .metrics
            .clone()
            .map(|m| {
                // Attach labels from client wrapper
                if let Some(other_labels) = req.extensions_mut().remove::<AttachMetricLabels>() {
                    m.with_new_attrs(other_labels.labels)
                } else {
                    m
                }
            })
            .and_then(|mut metrics| {
                // Attach method name label if possible
                req.uri().to_string().rsplit_once('/').map(|split_tup| {
                    let method_name = split_tup.1;
                    metrics.add_new_attrs([svc_operation(method_name.to_string())]);
                    if LONG_POLL_METHOD_NAMES.contains(&method_name) {
                        metrics.set_is_long_poll();
                    }
                    metrics.svc_request();
                    metrics
                })
            });
        let callfut = self.inner.call(req);
        async move {
            let started = Instant::now();
            let res = callfut.await;
            if let Some(metrics) = metrics {
                metrics.record_svc_req_latency(started.elapsed());
                if res.is_err() {
                    metrics.svc_request_failed();
                }
            }
            res
        }
        .boxed()
    }
}
