use crate::{AttachMetricLabels, CallType};
use futures_util::{FutureExt, future::BoxFuture};
use std::{
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use temporal_sdk_core_api::telemetry::metrics::{
    CoreMeter, Counter, HistogramDuration, MetricAttributes, MetricKeyValue, MetricParameters,
    TemporalMeter,
};
use tonic::{Code, body::BoxBody, transport::Channel};
use tower::Service;

/// The string name (which may be prefixed) for this metric
pub static REQUEST_LATENCY_HISTOGRAM_NAME: &str = "request_latency";
/// The string name (which may be prefixed) for this metric
pub static LONG_REQUEST_LATENCY_HISTOGRAM_NAME: &str = "long_request_latency";

/// Used to track context associated with metrics, and record/update them
// Possible improvement: make generic over some type tag so that methods are only exposed if the
// appropriate k/vs have already been set.
#[derive(Clone, derive_more::Debug)]
#[debug("MetricsContext {{ attribs: {kvs:?}, poll_is_long: {poll_is_long} }}")]
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
                name: REQUEST_LATENCY_HISTOGRAM_NAME.into(),
                unit: "duration".into(),
                description: "Histogram of client request latencies".into(),
            }),
            long_svc_request_latency: meter.histogram_duration(MetricParameters {
                name: LONG_REQUEST_LATENCY_HISTOGRAM_NAME.into(),
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
    pub(crate) fn svc_request_failed(&self, code: Option<Code>) {
        let refme: MetricAttributes;
        let kvs = if let Some(c) = code {
            refme = self
                .meter
                .extend_attributes(self.kvs.clone(), [status_code_kv(c)].into());
            &refme
        } else {
            &self.kvs
        };
        if self.poll_is_long {
            self.long_svc_request_failed.add(1, kvs);
        } else {
            self.svc_request_failed.add(1, kvs);
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
const KEY_STATUS_CODE: &str = "status_code";

pub(crate) fn namespace_kv(ns: String) -> MetricKeyValue {
    MetricKeyValue::new(KEY_NAMESPACE, ns)
}

pub(crate) fn task_queue_kv(tq: String) -> MetricKeyValue {
    MetricKeyValue::new(KEY_TASK_QUEUE, tq)
}

pub(crate) fn svc_operation(op: String) -> MetricKeyValue {
    MetricKeyValue::new(KEY_SVC_METHOD, op)
}

pub(crate) fn status_code_kv(code: Code) -> MetricKeyValue {
    MetricKeyValue::new(KEY_STATUS_CODE, code_as_screaming_snake(&code))
}

/// This is done to match the way Java sdk labels these codes (and also matches gRPC spec)
fn code_as_screaming_snake(code: &Code) -> &'static str {
    match code {
        Code::Ok => "OK",
        Code::Cancelled => "CANCELLED",
        Code::Unknown => "UNKNOWN",
        Code::InvalidArgument => "INVALID_ARGUMENT",
        Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
        Code::NotFound => "NOT_FOUND",
        Code::AlreadyExists => "ALREADY_EXISTS",
        Code::PermissionDenied => "PERMISSION_DENIED",
        Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
        Code::FailedPrecondition => "FAILED_PRECONDITION",
        Code::Aborted => "ABORTED",
        Code::OutOfRange => "OUT_OF_RANGE",
        Code::Unimplemented => "UNIMPLEMENTED",
        Code::Internal => "INTERNAL",
        Code::Unavailable => "UNAVAILABLE",
        Code::DataLoss => "DATA_LOSS",
        Code::Unauthenticated => "UNAUTHENTICATED",
    }
}

/// Implements metrics functionality for gRPC (really, any http) calls
#[derive(Debug, Clone)]
pub struct GrpcMetricSvc {
    pub(crate) inner: Channel,
    // If set to none, metrics are a no-op
    pub(crate) metrics: Option<MetricsContext>,
    pub(crate) disable_errcode_label: bool,
}

impl Service<http::Request<BoxBody>> for GrpcMetricSvc {
    type Response = http::Response<BoxBody>;
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
                if let Some(ct) = req.extensions().get::<CallType>() {
                    if ct.is_long() {
                        m.set_is_long_poll();
                    }
                }
                m
            })
            .and_then(|mut metrics| {
                // Attach method name label if possible
                req.uri().to_string().rsplit_once('/').map(|split_tup| {
                    let method_name = split_tup.1;
                    metrics.with_new_attrs([svc_operation(method_name.to_string())]);
                    metrics.svc_request();
                    metrics
                })
            });
        let callfut = self.inner.call(req);
        let errcode_label_disabled = self.disable_errcode_label;
        async move {
            let started = Instant::now();
            let res = callfut.await;
            if let Some(metrics) = metrics {
                metrics.record_svc_req_latency(started.elapsed());
                if let Ok(ref ok_res) = res {
                    if let Some(number) = ok_res
                        .headers()
                        .get("grpc-status")
                        .and_then(|s| s.to_str().ok())
                        .and_then(|s| s.parse::<i32>().ok())
                    {
                        let code = Code::from(number);
                        if code != Code::Ok {
                            let code = if errcode_label_disabled {
                                None
                            } else {
                                Some(code)
                            };
                            metrics.svc_request_failed(code);
                        }
                    }
                }
            }
            res
        }
        .boxed()
    }
}
