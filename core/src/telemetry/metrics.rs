use super::TELEM_SERVICE_NAME;
use crate::telemetry::GLOBAL_TELEM_DAT;
use opentelemetry::{
    global,
    metrics::{Counter, Descriptor, InstrumentKind, Meter, ValueRecorder},
    sdk::{
        export::metrics::{Aggregator, AggregatorSelector},
        metrics::aggregators::{histogram, last_value, sum},
    },
    KeyValue,
};
use std::{borrow::Cow, sync::Arc, time::Duration};

/// Used to track context associated with metrics, and record/update them
///
/// Possible improvement: make generic over some type tag so that methods are only exposed if the
/// appropriate k/vs have already been set.
#[derive(Default, Clone, Debug)]
pub(crate) struct MetricsContext {
    kvs: Arc<Vec<KeyValue>>,
}

impl MetricsContext {
    fn new(kvs: Vec<KeyValue>) -> Self {
        Self { kvs: Arc::new(kvs) }
    }

    pub(crate) fn top_level(namespace: String) -> Self {
        Self::new(vec![KeyValue::new(KEY_NAMESPACE, namespace)])
    }

    pub(crate) fn with_task_q(mut self, tq: String) -> Self {
        Arc::make_mut(&mut self.kvs).push(task_queue(tq));
        self
    }

    /// Extend an existing metrics context with new attributes
    pub(crate) fn with_new_attrs(&self, new_kvs: impl IntoIterator<Item = KeyValue>) -> Self {
        let mut kvs = self.kvs.clone();
        Arc::make_mut(&mut kvs).extend(new_kvs);
        Self { kvs }
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
    pub(crate) fn wf_e2e_latency(&self, dur: Duration) {
        WF_E2E_LATENCY.record(dur.as_millis() as u64, &self.kvs);
    }

    /// Record workflow task schedule to start time in millis
    pub(crate) fn wf_task_sched_to_start_latency(&self, dur: Duration) {
        WF_TASK_SCHED_TO_START_LATENCY.record(dur.as_millis() as u64, &self.kvs);
    }

    /// Record workflow task execution time in milliseconds
    pub(crate) fn wf_task_latency(&self, dur: Duration) {
        WF_TASK_EXECUTION_LATENCY.record(dur.as_millis() as u64, &self.kvs);
    }

    /// Record time it takes to catch up on replaying a WFT
    pub(crate) fn wf_task_replay_latency(&self, dur: Duration) {
        WF_TASK_REPLAY_LATENCY.record(dur.as_millis() as u64, &self.kvs);
    }

    /// An activity long poll timed out
    pub(crate) fn act_poll_timeout(&self) {
        ACT_POLL_NO_TASK.add(1, &self.kvs);
    }

    /// An activity execution failed
    pub(crate) fn act_execution_failed(&self) {
        ACT_EXECUTION_FAILED.add(1, &self.kvs);
    }

    /// Record activity task schedule to start time in millis
    pub(crate) fn act_sched_to_start_latency(&self, dur: Duration) {
        ACT_SCHED_TO_START_LATENCY.record(dur.as_millis() as u64, &self.kvs);
    }

    /// Record time it took to complete activity execution, from the time core generated the
    /// activity task, to the time lang responded with a completion (failure or success).
    pub(crate) fn act_execution_latency(&self, dur: Duration) {
        ACT_EXEC_LATENCY.record(dur.as_millis() as u64, &self.kvs);
    }

    /// A worker was registered
    pub(crate) fn worker_registered(&self) {
        WORKER_REGISTERED.add(1, &self.kvs);
    }

    /// Record current number of available task slots. Context should have worker type set.
    pub(crate) fn available_task_slots(&self, num: usize) {
        TASK_SLOTS_AVAILABLE.record(num as u64, &self.kvs)
    }

    /// Record current number of pollers. Context should include poller type / task queue tag.
    pub(crate) fn record_num_pollers(&self, num: usize) {
        NUM_POLLERS.record(num as u64, &self.kvs);
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
    pub(crate) static ref METRIC_METER: Meter = {
        #[cfg(not(test))]
        if crate::telemetry::GLOBAL_TELEM_DAT.get().is_none() {
            panic!("Tried to use a metric but telemetry has not been initialized")
        }
        global::meter(TELEM_SERVICE_NAME)
    };
}
fn metric_prefix() -> &'static str {
    GLOBAL_TELEM_DAT
        .get()
        .map(|gtd| {
            if gtd.no_temporal_prefix_for_metrics {
                ""
            } else {
                "temporal_"
            }
        })
        .unwrap_or("")
}

/// Define a temporal metric. All metrics are kept private to this file, and should be accessed
/// through functions on the [MetricsContext]
macro_rules! tm {
    (ctr, $ident:ident, $name:expr) => {
        lazy_static::lazy_static! {
            static ref $ident: Counter<u64> = {
                METRIC_METER.u64_counter(metric_prefix().to_string() + $name).init()
            };
        }
    };
    (vr_u64, $ident:ident, $name:expr) => {
        lazy_static::lazy_static! {
            static ref $ident: ValueRecorder<u64> = {
                METRIC_METER.u64_value_recorder(metric_prefix().to_string() + $name).init()
            };
        }
    };
}

const KEY_NAMESPACE: &str = "namespace";
const KEY_WF_TYPE: &str = "workflow_type";
const KEY_TASK_QUEUE: &str = "task_queue";
const KEY_ACT_TYPE: &str = "activity_type";
const KEY_POLLER_TYPE: &str = "poller_type";
const KEY_WORKER_TYPE: &str = "worker_type";

pub(crate) fn workflow_poller() -> KeyValue {
    KeyValue::new(KEY_POLLER_TYPE, "workflow_task")
}
pub(crate) fn workflow_sticky_poller() -> KeyValue {
    KeyValue::new(KEY_POLLER_TYPE, "sticky_workflow_task")
}
pub(crate) fn activity_poller() -> KeyValue {
    KeyValue::new(KEY_POLLER_TYPE, "activity_task")
}
pub(crate) fn task_queue(tq: String) -> KeyValue {
    KeyValue::new(KEY_TASK_QUEUE, tq)
}
pub(crate) fn activity_type(ty: String) -> KeyValue {
    KeyValue::new(KEY_ACT_TYPE, ty)
}
pub(crate) fn workflow_type(ty: String) -> KeyValue {
    KeyValue::new(KEY_WF_TYPE, ty)
}
pub(crate) const fn workflow_worker_type() -> KeyValue {
    KeyValue {
        key: opentelemetry::Key::from_static_str(KEY_WORKER_TYPE),
        value: opentelemetry::Value::String(Cow::Borrowed("WorkflowWorker")),
    }
}
pub(crate) const fn activity_worker_type() -> KeyValue {
    KeyValue {
        key: opentelemetry::Key::from_static_str(KEY_WORKER_TYPE),
        value: opentelemetry::Value::String(Cow::Borrowed("ActivityWorker")),
    }
}
pub(crate) const fn local_activity_worker_type() -> KeyValue {
    KeyValue {
        key: opentelemetry::Key::from_static_str(KEY_WORKER_TYPE),
        value: opentelemetry::Value::String(Cow::Borrowed("LocalActivityWorker")),
    }
}

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

tm!(ctr, ACT_POLL_NO_TASK, "activity_poll_no_task");
tm!(ctr, ACT_EXECUTION_FAILED, "activity_execution_failed");
// Act task unregistered can't be known by core right now since it's not well defined as an
// activity result. We could add a flag to the failed activity result if desired.
const ACT_SCHED_TO_START_LATENCY_NAME: &str = "activity_schedule_to_start_latency";
tm!(
    vr_u64,
    ACT_SCHED_TO_START_LATENCY,
    ACT_SCHED_TO_START_LATENCY_NAME
);
const ACT_EXEC_LATENCY_NAME: &str = "activity_execution_latency";
tm!(vr_u64, ACT_EXEC_LATENCY, ACT_EXEC_LATENCY_NAME);

// name kept as worker start for compat with old sdk / what users expect
tm!(ctr, WORKER_REGISTERED, "worker_start");
const NUM_POLLERS_NAME: &str = "num_pollers";
tm!(vr_u64, NUM_POLLERS, NUM_POLLERS_NAME);
const TASK_SLOTS_AVAILABLE_NAME: &str = "worker_task_slots_available";
tm!(vr_u64, TASK_SLOTS_AVAILABLE, TASK_SLOTS_AVAILABLE_NAME);

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

/// Activity are generally expected to take at least a little time, and sometimes quite a while,
/// since they're doing side-effecty things, etc.
static ACT_EXE_MS_BUCKETS: &[f64] = &[50., 100., 500., 1000., 5000., 10_000., 60_000.];

/// Schedule-to-start latency buckets for both WFT and AT
static TASK_SCHED_TO_START_MS_BUCKETS: &[f64] = &[100., 500., 1000., 5000., 10_000.];

/// Default buckets. Should never really be used as they will be meaningless for many things, but
/// broadly it's trying to represent latencies in millis.
pub(super) static DEFAULT_MS_BUCKETS: &[f64] = &[50., 100., 500., 1000., 2500., 10_000.];

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
            let dname = descriptor
                .name()
                .strip_prefix(metric_prefix())
                .unwrap_or_else(|| descriptor.name());
            // Some recorders are just gauges
            match dname {
                STICKY_CACHE_SIZE_NAME | NUM_POLLERS_NAME | TASK_SLOTS_AVAILABLE_NAME => {
                    return Some(Arc::new(last_value()))
                }
                _ => (),
            }

            // Other recorders will select their appropriate buckets
            let buckets = match dname {
                WF_E2E_LATENCY_NAME => WF_LATENCY_MS_BUCKETS,
                WF_TASK_EXECUTION_LATENCY_NAME | WF_TASK_REPLAY_LATENCY_NAME => WF_TASK_MS_BUCKETS,
                WF_TASK_SCHED_TO_START_LATENCY_NAME | ACT_SCHED_TO_START_LATENCY_NAME => {
                    TASK_SCHED_TO_START_MS_BUCKETS
                }
                ACT_EXEC_LATENCY_NAME => ACT_EXE_MS_BUCKETS,
                _ => DEFAULT_MS_BUCKETS,
            };
            return Some(Arc::new(histogram(descriptor, buckets)));
        }

        Some(Arc::new(sum()))
    }
}
