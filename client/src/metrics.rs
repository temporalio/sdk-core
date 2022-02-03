use opentelemetry::{
    metrics::{Counter, Meter, ValueRecorder},
    KeyValue,
};
use std::{sync::Arc, time::Duration};

/// Used to track context associated with metrics, and record/update them
// Possible improvement: make generic over some type tag so that methods are only exposed if the
// appropriate k/vs have already been set.
#[derive(Clone, Debug)]
pub struct MetricsContext {
    kvs: Arc<Vec<KeyValue>>,
    poll_is_long: bool,

    svc_request: Counter<u64>,
    svc_request_failed: Counter<u64>,
    long_svc_request: Counter<u64>,
    long_svc_request_failed: Counter<u64>,

    svc_request_latency: ValueRecorder<u64>,
    long_svc_request_latency: ValueRecorder<u64>,
}

impl MetricsContext {
    pub(crate) fn new(kvs: Vec<KeyValue>, meter: &Meter) -> Self {
        Self {
            kvs: Arc::new(kvs),
            poll_is_long: false,
            svc_request: meter.u64_counter("request").init(),
            svc_request_failed: meter.u64_counter("request_failure").init(),
            long_svc_request: meter.u64_counter("long_request").init(),
            long_svc_request_failed: meter.u64_counter("long_request_failure").init(),
            svc_request_latency: meter.u64_value_recorder("request_latency").init(),
            long_svc_request_latency: meter.u64_value_recorder("long_request_latency").init(),
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
            self.long_svc_request_latency
                .record(dur.as_millis() as u64, &self.kvs);
        } else {
            self.svc_request_latency
                .record(dur.as_millis() as u64, &self.kvs);
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
