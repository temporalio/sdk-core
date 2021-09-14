use super::TELEM_SERVICE_NAME;
use opentelemetry::{
    global,
    metrics::{Counter, Descriptor, InstrumentKind, Meter, ValueRecorder},
    sdk::{
        export::metrics::{Aggregator, AggregatorSelector},
        metrics::aggregators::{histogram, last_value, sum},
    },
    KeyValue,
};
use std::sync::Arc;

/// Used to track context associated with metrics, and record/update them
///
/// Possible improvement: make generic over some type tag so that methods are only exposed if the
/// appropriate k/vs have already been set.
#[derive(Default, Clone, Debug)]
pub(crate) struct MetricsContext {
    kvs: Arc<Vec<KeyValue>>,
    // TODO: Ideally this would hold bound metrics, but using them is basically impossible because
    //   of lifetime issues: https://github.com/open-telemetry/opentelemetry-rust/issues/629
    //   Use once fixed.
}

impl MetricsContext {
    pub(crate) fn top_level(namespace: String) -> Self {
        Self::new(vec![KeyValue::new(KEY_NAMESPACE, namespace)])
    }

    /// Extend an existing metrics context with new attributes
    pub(crate) fn with_new_attrs(&self, new_kvs: impl IntoIterator<Item = KeyValue>) -> Self {
        let mut kvs = self.kvs.clone();
        Arc::make_mut(&mut kvs).extend(new_kvs);
        Self { kvs }
    }

    fn new(kvs: Vec<KeyValue>) -> Self {
        Self { kvs: Arc::new(kvs) }
    }

    /// A workflow task queue poll succeeded
    pub(crate) fn wf_tq_poll_ok(&self) {
        WF_TASK_QUEUE_POLL_SUCCEED_COUNTER.add(1, &self.kvs);
    }

    /// A workflow task queue poll timed out / had empty response
    pub(crate) fn wf_tq_poll_empty(&self) {
        WF_TASK_QUEUE_POLL_EMPTY_COUNTER.add(1, &self.kvs);
    }

    /// A workflow task execution failed
    pub(crate) fn wf_task_failed(&self) {
        WF_TASK_EXECUTION_FAILURE_COUNTER.add(1, &self.kvs);
    }

    /// A workflow completed successfully
    pub(crate) fn wf_completed(&self) {
        WF_COMPLETED_COUNTER.add(1, &self.kvs);
    }

    /// A workflow ended cancelled
    pub(crate) fn wf_canceled(&self) {
        WF_CANCELED_COUNTER.add(1, &self.kvs);
    }

    /// A workflow ended failed
    pub(crate) fn wf_failed(&self) {
        WF_FAILED_COUNTER.add(1, &self.kvs);
    }

    /// A workflow continued as new
    pub(crate) fn wf_continued_as_new(&self) {
        WF_CONT_COUNTER.add(1, &self.kvs);
    }

    /// Record workflow total execution time in milliseconds
    pub(crate) fn wf_e2e_latency(&self, duration_millis: u64) {
        WF_E2E_LATENCY.record(duration_millis, &self.kvs);
    }

    /// Record workflow task schedule to start time in millis
    pub(crate) fn wf_task_sched_to_start_latency(&self, duration_millis: u64) {
        WF_TASK_SCHED_TO_START_LATENCY.record(duration_millis, &self.kvs);
    }

    /// Record workflow task execution time in milliseconds
    pub(crate) fn wf_task_latency(&self, duration_millis: u64) {
        WF_TASK_EXECUTION_LATENCY.record(duration_millis, &self.kvs);
    }

    /// Record time it takes to catch up on replaying a WFT
    pub(crate) fn wf_task_replay_latency(&self, duration_millis: u64) {
        WF_TASK_REPLAY_LATENCY.record(duration_millis, &self.kvs);
    }

    /// A workflow task found a cached workflow to run against
    pub(crate) fn sticky_cache_hit(&self) {
        STICKY_CACHE_HIT.add(1, &self.kvs);
    }

    /// A workflow task did not find a cached workflow
    pub(crate) fn sticky_cache_miss(&self) {
        STICKY_CACHE_MISS.add(1, &self.kvs);
    }

    /// Record current cache size (in number of wfs, not bytes)
    pub(crate) fn cache_size(&self, size: u64) {
        STICKY_CACHE_SIZE.record(size, &self.kvs);
    }
}

lazy_static::lazy_static! {
    static ref METRIC_METER: Meter = global::meter(TELEM_SERVICE_NAME);
}

/// Define a temporal metric. All metrics are kept private to this file, and should be accessed
/// through functions on the [MetricsContext]
macro_rules! tm {
    (ctr, $ident:ident, $name:expr) => {
        lazy_static::lazy_static! {
            static ref $ident: Counter<u64> = {
                METRIC_METER.u64_counter($name).init()
            };
        }
    };
    (vr_u64, $ident:ident, $name:expr) => {
        lazy_static::lazy_static! {
            static ref $ident: ValueRecorder<u64> = {
                METRIC_METER.u64_value_recorder($name).init()
            };
        }
    };
}

pub(crate) const KEY_NAMESPACE: &str = "namespace";
pub(crate) const KEY_WF_TYPE: &str = "workflow_type";

tm!(ctr, WF_COMPLETED_COUNTER, "workflow_completed");
tm!(ctr, WF_CANCELED_COUNTER, "workflow_canceled");
tm!(ctr, WF_FAILED_COUNTER, "workflow_failed");
tm!(ctr, WF_CONT_COUNTER, "workflow_continue_as_new");
const WF_E2E_LATENCY_NAME: &str = "workflow_endtoend_latency";
tm!(vr_u64, WF_E2E_LATENCY, WF_E2E_LATENCY_NAME);

tm!(
    ctr,
    WF_TASK_QUEUE_POLL_EMPTY_COUNTER,
    "workflow_task_queue_poll_empty"
);
tm!(
    ctr,
    WF_TASK_QUEUE_POLL_SUCCEED_COUNTER,
    "workflow_task_queue_poll_succeed"
);
tm!(
    ctr,
    WF_TASK_EXECUTION_FAILURE_COUNTER,
    "workflow_task_execution_failed"
);
const WF_TASK_SCHED_TO_START_LATENCY_NAME: &str = "workflow_task_schedule_to_start_latency";
tm!(
    vr_u64,
    WF_TASK_SCHED_TO_START_LATENCY,
    WF_TASK_SCHED_TO_START_LATENCY_NAME
);
const WF_TASK_REPLAY_LATENCY_NAME: &str = "workflow_task_replay_latency";
tm!(vr_u64, WF_TASK_REPLAY_LATENCY, WF_TASK_REPLAY_LATENCY_NAME);
const WF_TASK_EXECUTION_LATENCY_NAME: &str = "workflow_task_execution_latency";
tm!(
    vr_u64,
    WF_TASK_EXECUTION_LATENCY,
    WF_TASK_EXECUTION_LATENCY_NAME
);

tm!(ctr, STICKY_CACHE_HIT, "sticky_cache_hit");
tm!(ctr, STICKY_CACHE_MISS, "sticky_cache_miss");
const STICKY_CACHE_SIZE_NAME: &str = "sticky_cache_size";
tm!(vr_u64, STICKY_CACHE_SIZE, STICKY_CACHE_SIZE_NAME);

/// Artisanal, handcrafted latency buckets for workflow e2e latency which should expose a useful
/// set of buckets for < 1 day runtime workflows. Beyond that, this metric probably isn't very
/// helpful
static WF_LATENCY_MS_BUCKETS: &[f64] = &[
    100.,
    500.,
    1000.,
    1500.,
    2000.,
    5000.,
    10_000.,
    30_000.,
    60_000.,
    120_000.,
    300_000.,
    600_000.,
    1_800_000.,  // 30 min
    3_600_000.,  //  1 hr
    30_600_000., // 10 hrs
    8.64e7,      // 24 hrs
];

/// Task latencies are expected to be fast, no longer than a second which was generally the deadlock
/// timeout in old SDKs. Here it's a bit different since a WFT may represent multiple activations.
static WF_TASK_MS_BUCKETS: &[f64] = &[1., 10., 20., 50., 100., 200., 500., 1000.];

/// Default buckets. Should never really be used as they will be meaningless for many things, but
/// broadly it's trying to represent latencies in millis.
static DEFAULT_BUCKETS: &[f64] = &[50., 100., 500., 1000., 2500., 10_000.];

/// Chooses appropriate aggregators for our metrics
#[derive(Debug)]
pub struct SDKAggSelector;

impl AggregatorSelector for SDKAggSelector {
    fn aggregator_for(&self, descriptor: &Descriptor) -> Option<Arc<dyn Aggregator + Send + Sync>> {
        // Observers are always last value
        if *descriptor.instrument_kind() == InstrumentKind::ValueObserver {
            return Some(Arc::new(last_value()));
        }

        if *descriptor.instrument_kind() == InstrumentKind::ValueRecorder {
            // Some recorders are just gauges
            match descriptor.name() {
                STICKY_CACHE_SIZE_NAME => return Some(Arc::new(last_value())),
                _ => (),
            }

            // Other recorders will select their appropriate buckets
            let buckets = match descriptor.name() {
                WF_E2E_LATENCY_NAME => WF_LATENCY_MS_BUCKETS,
                WF_TASK_EXECUTION_LATENCY_NAME => WF_TASK_MS_BUCKETS,
                WF_TASK_REPLAY_LATENCY_NAME => WF_TASK_MS_BUCKETS,
                _ => DEFAULT_BUCKETS,
            };
            return Some(Arc::new(histogram(descriptor, buckets)));
        }

        Some(Arc::new(sum()))
    }
}
