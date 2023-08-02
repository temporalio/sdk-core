use crate::telemetry::TelemetryInstance;
use opentelemetry::{
    self,
    metrics::{noop::NoopMeterProvider, Counter, Histogram, Meter, MeterProvider},
    KeyValue,
};
use opentelemetry_sdk::metrics::{
    new_view,
    reader::{AggregationSelector, DefaultAggregationSelector},
    Aggregation, Instrument, InstrumentKind, MeterProviderBuilder, View,
};
use std::{borrow::Cow, ops::Deref, sync::Arc, time::Duration};
use temporal_client::ClientMetricProvider;

/// Used to track context associated with metrics, and record/update them
///
/// Possible improvement: make generic over some type tag so that methods are only exposed if the
/// appropriate k/vs have already been set.
#[derive(Clone)]
pub(crate) struct MetricsContext {
    kvs: Arc<Vec<KeyValue>>,
    instruments: Arc<Instruments>,
}

/// Wraps OTel's [Meter] to ensure we name our metrics properly, or any other temporal-specific
/// metrics customizations
#[derive(derive_more::Constructor)]
pub struct TemporalMeter<'a> {
    inner: &'a Meter,
    metrics_prefix: &'static str,
    kvs: Arc<Vec<KeyValue>>,
}

impl<'a> TemporalMeter<'a> {
    pub(crate) fn counter(&self, name: &'static str) -> Counter<u64> {
        self.inner
            .u64_counter(self.metrics_prefix.to_string() + name)
            .init()
    }

    pub(crate) fn histogram(&self, name: &'static str) -> Histogram<u64> {
        self.inner
            .u64_histogram(self.metrics_prefix.to_string() + name)
            .init()
    }

    pub(crate) fn gauge(&self, name: &'static str) -> MemoryGaugeU64 {
        MemoryGaugeU64::new(self.metrics_prefix.to_string() + name, self.inner)
    }
}

impl<'a> ClientMetricProvider for TemporalMeter<'a> {
    fn counter(&self, name: &'static str) -> Counter<u64> {
        self.counter(name)
    }

    fn histogram(&self, name: &'static str) -> Histogram<u64> {
        self.histogram(name)
    }

    fn fixed_labels(&self) -> &[KeyValue] {
        self.kvs.as_slice()
    }
}

impl<'a> Deref for TemporalMeter<'a> {
    type Target = dyn ClientMetricProvider + 'a;

    fn deref(&self) -> &Self::Target {
        self as &Self::Target
    }
}

struct Instruments {
    wf_completed_counter: Counter<u64>,
    wf_canceled_counter: Counter<u64>,
    wf_failed_counter: Counter<u64>,
    wf_cont_counter: Counter<u64>,
    wf_e2e_latency: Histogram<u64>,
    wf_task_queue_poll_empty_counter: Counter<u64>,
    wf_task_queue_poll_succeed_counter: Counter<u64>,
    wf_task_execution_failure_counter: Counter<u64>,
    wf_task_sched_to_start_latency: Histogram<u64>,
    wf_task_replay_latency: Histogram<u64>,
    wf_task_execution_latency: Histogram<u64>,
    act_poll_no_task: Counter<u64>,
    act_task_received_counter: Counter<u64>,
    act_execution_failed: Counter<u64>,
    act_sched_to_start_latency: Histogram<u64>,
    act_exec_latency: Histogram<u64>,
    worker_registered: Counter<u64>,
    num_pollers: MemoryGaugeU64,
    task_slots_available: MemoryGaugeU64,
    sticky_cache_hit: Counter<u64>,
    sticky_cache_miss: Counter<u64>,
    sticky_cache_size: MemoryGaugeU64,
    sticky_cache_evictions: Counter<u64>,
}

impl MetricsContext {
    pub(crate) fn no_op() -> Self {
        Self {
            kvs: Default::default(),
            instruments: Arc::new(Instruments::new_explicit(TemporalMeter::new(
                &NoopMeterProvider::new().meter("fakemeter"),
                "fakemetrics",
                Arc::new(vec![]),
            ))),
        }
    }

    pub(crate) fn top_level(namespace: String, telemetry: &TelemetryInstance) -> Self {
        let no_op_meter: Meter;
        let meter = if let Some(meter) = telemetry.get_metric_meter() {
            meter
        } else {
            no_op_meter = NoopMeterProvider::default().meter("no_op");
            TemporalMeter::new(&no_op_meter, "fakemetrics", Arc::new(vec![]))
        };

        let mut kvs = (*meter.kvs).clone();
        kvs.push(KeyValue::new(KEY_NAMESPACE, namespace));
        Self {
            kvs: Arc::new(kvs),
            instruments: Arc::new(Instruments::new_explicit(meter)),
        }
    }

    pub(crate) fn with_task_q(mut self, tq: String) -> Self {
        Arc::make_mut(&mut self.kvs).push(task_queue(tq));
        self
    }

    /// Extend an existing metrics context with new attributes
    pub(crate) fn with_new_attrs(&self, new_kvs: impl IntoIterator<Item = KeyValue>) -> Self {
        let mut kvs = self.kvs.clone();
        Arc::make_mut(&mut kvs).extend(new_kvs);
        Self {
            kvs,
            instruments: self.instruments.clone(),
        }
    }

    /// A workflow task queue poll succeeded
    pub(crate) fn wf_tq_poll_ok(&self) {
        self.instruments
            .wf_task_queue_poll_succeed_counter
            .add(1, &self.kvs);
    }

    /// A workflow task queue poll timed out / had empty response
    pub(crate) fn wf_tq_poll_empty(&self) {
        self.instruments
            .wf_task_queue_poll_empty_counter
            .add(1, &self.kvs);
    }

    /// A workflow task execution failed
    pub(crate) fn wf_task_failed(&self) {
        self.instruments
            .wf_task_execution_failure_counter
            .add(1, &self.kvs);
    }

    /// A workflow completed successfully
    pub(crate) fn wf_completed(&self) {
        self.instruments.wf_completed_counter.add(1, &self.kvs);
    }

    /// A workflow ended cancelled
    pub(crate) fn wf_canceled(&self) {
        self.instruments.wf_canceled_counter.add(1, &self.kvs);
    }

    /// A workflow ended failed
    pub(crate) fn wf_failed(&self) {
        self.instruments.wf_failed_counter.add(1, &self.kvs);
    }

    /// A workflow continued as new
    pub(crate) fn wf_continued_as_new(&self) {
        self.instruments.wf_cont_counter.add(1, &self.kvs);
    }

    /// Record workflow total execution time in milliseconds
    pub(crate) fn wf_e2e_latency(&self, dur: Duration) {
        self.instruments
            .wf_e2e_latency
            .record(dur.as_millis() as u64, &self.kvs);
    }

    /// Record workflow task schedule to start time in millis
    pub(crate) fn wf_task_sched_to_start_latency(&self, dur: Duration) {
        self.instruments
            .wf_task_sched_to_start_latency
            .record(dur.as_millis() as u64, &self.kvs);
    }

    /// Record workflow task execution time in milliseconds
    pub(crate) fn wf_task_latency(&self, dur: Duration) {
        self.instruments
            .wf_task_execution_latency
            .record(dur.as_millis() as u64, &self.kvs);
    }

    /// Record time it takes to catch up on replaying a WFT
    pub(crate) fn wf_task_replay_latency(&self, dur: Duration) {
        self.instruments
            .wf_task_replay_latency
            .record(dur.as_millis() as u64, &self.kvs);
    }

    /// An activity long poll timed out
    pub(crate) fn act_poll_timeout(&self) {
        self.instruments.act_poll_no_task.add(1, &self.kvs);
    }

    /// A count of activity tasks received
    pub(crate) fn act_task_received(&self) {
        self.instruments.act_task_received_counter.add(1, &self.kvs);
    }

    /// An activity execution failed
    pub(crate) fn act_execution_failed(&self) {
        self.instruments.act_execution_failed.add(1, &self.kvs);
    }

    /// Record activity task schedule to start time in millis
    pub(crate) fn act_sched_to_start_latency(&self, dur: Duration) {
        self.instruments
            .act_sched_to_start_latency
            .record(dur.as_millis() as u64, &self.kvs);
    }

    /// Record time it took to complete activity execution, from the time core generated the
    /// activity task, to the time lang responded with a completion (failure or success).
    pub(crate) fn act_execution_latency(&self, dur: Duration) {
        self.instruments
            .act_exec_latency
            .record(dur.as_millis() as u64, &self.kvs);
    }

    /// A worker was registered
    pub(crate) fn worker_registered(&self) {
        self.instruments.worker_registered.add(1, &self.kvs);
    }

    /// Record current number of available task slots. Context should have worker type set.
    pub(crate) fn available_task_slots(&self, num: usize) {
        self.instruments
            .task_slots_available
            .record(num as u64, self.kvs.clone())
    }

    /// Record current number of pollers. Context should include poller type / task queue tag.
    pub(crate) fn record_num_pollers(&self, num: usize) {
        self.instruments
            .num_pollers
            .record(num as u64, self.kvs.clone());
    }

    /// A workflow task found a cached workflow to run against
    pub(crate) fn sticky_cache_hit(&self) {
        self.instruments.sticky_cache_hit.add(1, &self.kvs);
    }

    /// A workflow task did not find a cached workflow
    pub(crate) fn sticky_cache_miss(&self) {
        self.instruments.sticky_cache_miss.add(1, &self.kvs);
    }

    /// Record current cache size (in number of wfs, not bytes)
    pub(crate) fn cache_size(&self, size: u64) {
        self.instruments
            .sticky_cache_size
            .record(size, self.kvs.clone());
    }

    /// Count a workflow being evicted from the cache
    pub(crate) fn cache_eviction(&self) {
        self.instruments.sticky_cache_evictions.add(1, &self.kvs);
    }
}

impl Instruments {
    fn new_explicit(meter: TemporalMeter) -> Self {
        Self {
            wf_completed_counter: meter.counter("workflow_completed"),
            wf_canceled_counter: meter.counter("workflow_canceled"),
            wf_failed_counter: meter.counter("workflow_failed"),
            wf_cont_counter: meter.counter("workflow_continue_as_new"),
            wf_e2e_latency: meter.histogram(WF_E2E_LATENCY_NAME),
            wf_task_queue_poll_empty_counter: meter.counter("workflow_task_queue_poll_empty"),
            wf_task_queue_poll_succeed_counter: meter.counter("workflow_task_queue_poll_succeed"),
            wf_task_execution_failure_counter: meter.counter("workflow_task_execution_failed"),
            wf_task_sched_to_start_latency: meter.histogram(WF_TASK_SCHED_TO_START_LATENCY_NAME),
            wf_task_replay_latency: meter.histogram(WF_TASK_REPLAY_LATENCY_NAME),
            wf_task_execution_latency: meter.histogram(WF_TASK_EXECUTION_LATENCY_NAME),
            act_poll_no_task: meter.counter("activity_poll_no_task"),
            act_task_received_counter: meter.counter("activity_task_received"),
            act_execution_failed: meter.counter("activity_execution_failed"),
            act_sched_to_start_latency: meter.histogram(ACT_SCHED_TO_START_LATENCY_NAME),
            act_exec_latency: meter.histogram(ACT_EXEC_LATENCY_NAME),
            // name kept as worker start for compat with old sdk / what users expect
            worker_registered: meter.counter("worker_start"),
            num_pollers: meter.gauge(NUM_POLLERS_NAME),
            task_slots_available: meter.gauge(TASK_SLOTS_AVAILABLE_NAME),
            sticky_cache_hit: meter.counter("sticky_cache_hit"),
            sticky_cache_miss: meter.counter("sticky_cache_miss"),
            sticky_cache_size: meter.gauge(STICKY_CACHE_SIZE_NAME),
            sticky_cache_evictions: meter.counter("sticky_cache_total_forced_eviction"),
        }
    }
}

const KEY_NAMESPACE: &str = "namespace";
const KEY_WF_TYPE: &str = "workflow_type";
const KEY_TASK_QUEUE: &str = "task_queue";
const KEY_ACT_TYPE: &str = "activity_type";
const KEY_POLLER_TYPE: &str = "poller_type";
const KEY_WORKER_TYPE: &str = "worker_type";
const KEY_EAGER: &str = "eager";

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
pub(crate) fn workflow_worker_type() -> KeyValue {
    KeyValue::new(KEY_WORKER_TYPE, "WorkflowWorker")
}
pub(crate) fn activity_worker_type() -> KeyValue {
    KeyValue::new(KEY_WORKER_TYPE, "ActivityWorker")
}
pub(crate) fn local_activity_worker_type() -> KeyValue {
    KeyValue::new(KEY_WORKER_TYPE, "LocalActivityWorker")
}
pub(crate) fn eager(is_eager: bool) -> KeyValue {
    KeyValue::new(KEY_EAGER, is_eager)
}

const WF_E2E_LATENCY_NAME: &str = "workflow_endtoend_latency";
const WF_TASK_SCHED_TO_START_LATENCY_NAME: &str = "workflow_task_schedule_to_start_latency";
const WF_TASK_REPLAY_LATENCY_NAME: &str = "workflow_task_replay_latency";
const WF_TASK_EXECUTION_LATENCY_NAME: &str = "workflow_task_execution_latency";
const ACT_SCHED_TO_START_LATENCY_NAME: &str = "activity_schedule_to_start_latency";
const ACT_EXEC_LATENCY_NAME: &str = "activity_execution_latency";
const NUM_POLLERS_NAME: &str = "num_pollers";
const TASK_SLOTS_AVAILABLE_NAME: &str = "worker_task_slots_available";
const STICKY_CACHE_SIZE_NAME: &str = "sticky_cache_size";

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
static TASK_SCHED_TO_START_MS_BUCKETS: &[f64] =
    &[100., 500., 1000., 5000., 10_000., 100_000., 1_000_000.];

/// Default buckets. Should never really be used as they will be meaningless for many things, but
/// broadly it's trying to represent latencies in millis.
pub(super) static DEFAULT_MS_BUCKETS: &[f64] = &[50., 100., 500., 1000., 2500., 10_000.];

/// Chooses appropriate aggregators for our metrics
#[derive(Debug, Clone, Default)]
pub struct SDKAggSelector {
    default: DefaultAggregationSelector,
}
impl AggregationSelector for SDKAggSelector {
    fn aggregation(&self, kind: InstrumentKind) -> Aggregation {
        match kind {
            InstrumentKind::Histogram => Aggregation::ExplicitBucketHistogram {
                boundaries: DEFAULT_MS_BUCKETS.to_vec(),
                record_min_max: true,
            },
            _ => self.default.aggregation(kind),
        }
    }
}

fn gauge_view(metric_name: &'static str) -> opentelemetry::metrics::Result<Box<dyn View>> {
    new_view(
        Instrument::new().name(format!("*{metric_name}")),
        opentelemetry_sdk::metrics::Stream::new().aggregation(Aggregation::LastValue),
    )
}

fn histo_view(
    metric_name: &'static str,
    buckets: &[f64],
) -> opentelemetry::metrics::Result<Box<dyn View>> {
    new_view(
        Instrument::new().name(format!("*{metric_name}")),
        opentelemetry_sdk::metrics::Stream::new().aggregation(
            Aggregation::ExplicitBucketHistogram {
                boundaries: buckets.to_vec(),
                record_min_max: true,
            },
        ),
    )
}

pub(super) fn augment_meter_provider_with_views(
    mpb: MeterProviderBuilder,
) -> opentelemetry::metrics::Result<MeterProviderBuilder> {
    // Some histograms are actually gauges, but we have to use histograms otherwise they forget
    // their value between collections since we don't use callbacks.
    Ok(mpb
        .with_view(gauge_view(STICKY_CACHE_SIZE_NAME)?)
        .with_view(gauge_view(NUM_POLLERS_NAME)?)
        .with_view(gauge_view(TASK_SLOTS_AVAILABLE_NAME)?)
        .with_view(histo_view(WF_E2E_LATENCY_NAME, WF_LATENCY_MS_BUCKETS)?)
        .with_view(histo_view(
            WF_TASK_EXECUTION_LATENCY_NAME,
            WF_TASK_MS_BUCKETS,
        )?)
        .with_view(histo_view(WF_TASK_REPLAY_LATENCY_NAME, WF_TASK_MS_BUCKETS)?)
        .with_view(histo_view(
            WF_TASK_SCHED_TO_START_LATENCY_NAME,
            TASK_SCHED_TO_START_MS_BUCKETS,
        )?)
        .with_view(histo_view(
            ACT_SCHED_TO_START_LATENCY_NAME,
            TASK_SCHED_TO_START_MS_BUCKETS,
        )?)
        .with_view(histo_view(ACT_EXEC_LATENCY_NAME, ACT_EXE_MS_BUCKETS)?))
}

/// OTel has no built-in synchronous Gauge. Histograms used to be able to serve that purpose, but
/// they broke that. Lovely. So, we need to implement one by hand.
pub(crate) struct MemoryGaugeU64 {
    values_tx: crossbeam::channel::Sender<(u64, Arc<Vec<KeyValue>>)>,
}

impl MemoryGaugeU64 {
    fn new(name: impl Into<Cow<'static, str>>, meter: &Meter) -> Self {
        let gauge = meter.u64_observable_gauge(name).init();
        let (values_tx, values_rx) = crossbeam::channel::unbounded::<(_, Arc<Vec<KeyValue>>)>();
        meter
            .register_callback(&[gauge.as_any()], move |o| {
                for (val, kvs) in values_rx.try_iter() {
                    o.observe_u64(&gauge, val, kvs.as_slice())
                }
            })
            .expect("works");
        MemoryGaugeU64 { values_tx }
    }
    fn record(&self, val: u64, kvs: Arc<Vec<KeyValue>>) {
        let _ = self.values_tx.send((val, kvs));
    }
}
