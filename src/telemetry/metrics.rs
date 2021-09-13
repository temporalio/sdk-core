use super::TELEM_SERVICE_NAME;
use opentelemetry::{
    global,
    metrics::{Counter, Descriptor, InstrumentKind, Meter, ValueRecorder},
    sdk::{
        export::metrics::{Aggregator, AggregatorSelector},
        metrics::aggregators::{histogram, last_value, sum},
    },
};
use std::sync::Arc;

lazy_static::lazy_static! {
    static ref METRIC_METER: Meter = global::meter(TELEM_SERVICE_NAME);
}

/// Define a temporal metric
macro_rules! tm {
    (ctr, $ident:ident, $name:expr) => {
        lazy_static::lazy_static! {
            pub(crate) static ref $ident: Counter<u64> = {
                METRIC_METER.u64_counter($name).init()
            };
        }
    };
    (vr_u64, $ident:ident, $name:expr) => {
        lazy_static::lazy_static! {
            pub(crate) static ref $ident: ValueRecorder<u64> = {
                METRIC_METER.u64_value_recorder($name).init()
            };
        }
    };
}

pub(crate) const KEY_NAMESPACE: &str = "namespace";

tm!(ctr, WF_COMPLETED_COUNTER, "workflow_completed");
tm!(ctr, WF_CANCELED_COUNTER, "workflow_canceled");
tm!(ctr, WF_FAILED_COUNTER, "workflow_failed");
tm!(ctr, WF_CONT_COUNTER, "workflow_continue_as_new");
const WF_E2E_LATENCY_NAME: &str = "workflow_endtoend_latency";
// Workflow total execution time in milliseconds
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
tm!(
    ctr,
    WF_TASK_NO_COMPLETION_COUNTER,
    "workflow_task_no_completion"
);
const WF_TASK_REPLAY_LATENCY_NAME: &str = "workflow_task_replay_latency";
// TODO: Doc
tm!(vr_u64, WF_TASK_REPLAY_LATENCY, WF_TASK_REPLAY_LATENCY_NAME);

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

        // Recorders will select their appropriate buckets
        if *descriptor.instrument_kind() == InstrumentKind::ValueRecorder {
            let buckets = match descriptor.name() {
                WF_E2E_LATENCY_NAME => WF_LATENCY_MS_BUCKETS,
                _ => DEFAULT_BUCKETS,
            };
            return Some(Arc::new(histogram(descriptor, buckets)));
        }

        Some(Arc::new(sum()))
    }
}
