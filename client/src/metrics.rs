use crate::{AttachMetricLabels, LONG_POLL_METHOD_NAMES};
use futures::{future::BoxFuture, FutureExt};
use std::{
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use temporal_sdk_core_api::telemetry::metrics::{
    CoreMeter, Counter, HistogramDuration, MetricAttributes, MetricKeyValue, MetricParameters,
    TemporalMeter,
};
use tonic::{body::BoxBody, transport::Channel};
use tower::Service;

/// Used to track context associated with metrics, and record/update them
// Possible improvement: make generic over some type tag so that methods are only exposed if the
// appropriate k/vs have already been set.
#[derive(Clone, derive_more::DebugCustom)]
#[debug(fmt = "MetricsContext {{ attribs: {kvs:?}, poll_is_long: {poll_is_long} }}")]
pub(crate) struct MetricsContext {
    meter: Arc<dyn CoreMeter>,
    kvs: MetricAttributes,
    poll_is_long: bool,

    svc_request: Arc<dyn Counter>,
    svc_request_failed: Arc<dyn Counter>,
    long_svc_request: Arc<dyn Counter>,
    long_svc_request_failed: Arc<dyn Counter>,

    svc_request_latency: Arc<dyn HistogramDuration>,
    long_svc_request_latency: Arc<dyn HistogramDuration>,
}

impl MetricsContext {
    pub(crate) fn new(tm: TemporalMeter) -> Self {
        let meter = tm.inner;
        Self {
            kvs: meter.new_attributes(tm.default_attribs),
            poll_is_long: false,
            svc_request: meter.counter(MetricParameters {
                name: "request".into(),
                description: "Count of client request successes by rpc name".into(),
                unit: "".into(),
            }),
            svc_request_failed: meter.counter(MetricParameters {
                name: "request_failure".into(),
                description: "Count of client request failures by rpc name".into(),
                unit: "".into(),
            }),
            long_svc_request: meter.counter(MetricParameters {
                name: "long_request".into(),
                description: "Count of long-poll request successes by rpc name".into(),
                unit: "".into(),
            }),
            long_svc_request_failed: meter.counter(MetricParameters {
                name: "long_request_failure".into(),
                description: "Count of long-poll request failures by rpc name".into(),
                unit: "".into(),
            }),
            svc_request_latency: meter.histogram_duration(MetricParameters {
                name: "request_latency".into(),
                unit: "duration".into(),
                description: "Histogram of client request latencies".into(),
            }),
            long_svc_request_latency: meter.histogram_duration(MetricParameters {
                name: "long_request_latency".into(),
                unit: "duration".into(),
                description: "Histogram of client long-poll request latencies".into(),
            }),
            meter,
        }
    }

    /// Mutate this metrics context with new attributes
    pub(crate) fn with_new_attrs(&mut self, new_kvs: impl IntoIterator<Item = MetricKeyValue>) {
        self.kvs = self
            .meter
            .extend_attributes(self.kvs.clone(), new_kvs.into());
    }

    pub(crate) fn set_is_long_poll(&mut self) {
        self.poll_is_long = true;
    }

    /// A request to the temporal service was made
    pub(crate) fn svc_request(&self) {
        if self.poll_is_long {
            self.long_svc_request.add(1, &self.kvs);
        } else {
            self.svc_request.add(1, &self.kvs);
        }
    }

    /// A request to the temporal service failed
    pub(crate) fn svc_request_failed(&self) {
        if self.poll_is_long {
            self.long_svc_request_failed.add(1, &self.kvs);
        } else {
            self.svc_request_failed.add(1, &self.kvs);
        }
    }

    /// Record service request latency
    pub(crate) fn record_svc_req_latency(&self, dur: Duration) {
        if self.poll_is_long {
            self.long_svc_request_latency.record(dur, &self.kvs);
        } else {
            self.svc_request_latency.record(dur, &self.kvs);
        }
    }
}

const KEY_NAMESPACE: &str = "namespace";
const KEY_SVC_METHOD: &str = "operation";
const KEY_TASK_QUEUE: &str = "task_queue";

pub(crate) fn namespace_kv(ns: String) -> MetricKeyValue {
    MetricKeyValue::new(KEY_NAMESPACE, ns)
}

pub(crate) fn task_queue_kv(tq: String) -> MetricKeyValue {
    MetricKeyValue::new(KEY_TASK_QUEUE, tq)
}

pub(crate) fn svc_operation(op: String) -> MetricKeyValue {
    MetricKeyValue::new(KEY_SVC_METHOD, op)
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
            .map(|mut m| {
                // Attach labels from client wrapper
                if let Some(other_labels) = req.extensions_mut().remove::<AttachMetricLabels>() {
                    m.with_new_attrs(other_labels.labels)
                }
                m
            })
            .and_then(|mut metrics| {
                // Attach method name label if possible
                req.uri().to_string().rsplit_once('/').map(|split_tup| {
                    let method_name = split_tup.1;
                    metrics.with_new_attrs([svc_operation(method_name.to_string())]);
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
