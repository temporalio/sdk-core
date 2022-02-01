use opentelemetry::{
    metrics::{Counter, Meter, ValueRecorder},
    KeyValue,
};
use std::{sync::Arc, time::Duration};

/// Used to track context associated with metrics, and record/update them
// Possible improvement: make generic over some type tag so that methods are only exposed if the
// appropriate k/vs have already been set.
#[derive(Clone, Debug)]
pub(crate) struct MetricsContext {
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
    fn new(kvs: Vec<KeyValue>, meter: &Meter) -> Self {
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

    pub(crate) fn top_level(namespace: String, meter: &Meter) -> Self {
        Self::new(vec![KeyValue::new(KEY_NAMESPACE, namespace)], meter)
    }

    /// Extend an existing metrics context with new attributes
    pub(crate) fn with_new_attrs(&self, new_kvs: impl IntoIterator<Item = KeyValue>) -> Self {
        let mut r = self.clone();
        Arc::make_mut(&mut r.kvs).extend(new_kvs);
        r
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

pub(crate) fn svc_operation(op: String) -> KeyValue {
    KeyValue::new(KEY_SVC_METHOD, op)
}
