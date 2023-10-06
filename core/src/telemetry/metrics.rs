use crate::{
    abstractions::dbg_panic,
    telemetry::{
        default_resource, metric_temporality_to_selector, prometheus_server::PromServer,
        TelemetryInstance, TELEM_SERVICE_NAME,
    },
};
use opentelemetry::{
    self,
    metrics::{Meter, MeterProvider as MeterProviderT, Unit},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    metrics::{
        new_view,
        reader::{AggregationSelector, DefaultAggregationSelector},
        Aggregation, Instrument, InstrumentKind, MeterProvider, MeterProviderBuilder,
        PeriodicReader, View,
    },
    runtime, AttributeSet,
};
use parking_lot::RwLock;
use std::{collections::HashMap, fmt::Debug, net::SocketAddr, sync::Arc, time::Duration};
use temporal_sdk_core_api::telemetry::{
    metrics::{
        BufferAttributes, BufferInstrumentRef, CoreMeter, Counter, Gauge, Histogram,
        LazyBufferInstrument, MetricAttributes, MetricCallBufferer, MetricEvent, MetricKeyValue,
        MetricKind, MetricParameters, MetricUpdateVal, NewAttributes, NoOpCoreMeter,
    },
    OtelCollectorOptions, PrometheusExporterOptions,
};
use tokio::task::AbortHandle;
use tonic::metadata::MetadataMap;

/// Used to track context associated with metrics, and record/update them
///
/// Possible improvement: make generic over some type tag so that methods are only exposed if the
/// appropriate k/vs have already been set.
#[derive(Clone)]
pub(crate) struct MetricsContext {
    meter: Arc<dyn CoreMeter>,
    kvs: MetricAttributes,
    instruments: Arc<Instruments>,
}

struct Instruments {
    wf_completed_counter: Arc<dyn Counter>,
    wf_canceled_counter: Arc<dyn Counter>,
    wf_failed_counter: Arc<dyn Counter>,
    wf_cont_counter: Arc<dyn Counter>,
    wf_e2e_latency: Arc<dyn Histogram>,
    wf_task_queue_poll_empty_counter: Arc<dyn Counter>,
    wf_task_queue_poll_succeed_counter: Arc<dyn Counter>,
    wf_task_execution_failure_counter: Arc<dyn Counter>,
    wf_task_sched_to_start_latency: Arc<dyn Histogram>,
    wf_task_replay_latency: Arc<dyn Histogram>,
    wf_task_execution_latency: Arc<dyn Histogram>,
    act_poll_no_task: Arc<dyn Counter>,
    act_task_received_counter: Arc<dyn Counter>,
    act_execution_failed: Arc<dyn Counter>,
    act_sched_to_start_latency: Arc<dyn Histogram>,
    act_exec_latency: Arc<dyn Histogram>,
    worker_registered: Arc<dyn Counter>,
    num_pollers: Arc<dyn Gauge>,
    task_slots_available: Arc<dyn Gauge>,
    sticky_cache_hit: Arc<dyn Counter>,
    sticky_cache_miss: Arc<dyn Counter>,
    sticky_cache_size: Arc<dyn Gauge>,
    sticky_cache_evictions: Arc<dyn Counter>,
}

impl MetricsContext {
    pub(crate) fn no_op() -> Self {
        let meter = Arc::new(NoOpCoreMeter);
        Self {
            kvs: meter.new_attributes(Default::default()),
            instruments: Arc::new(Instruments::new(meter.as_ref())),
            meter,
        }
    }

    pub(crate) fn top_level(namespace: String, tq: String, telemetry: &TelemetryInstance) -> Self {
        if let Some(mut meter) = telemetry.get_temporal_metric_meter() {
            meter
                .default_attribs
                .attributes
                .push(MetricKeyValue::new(KEY_NAMESPACE, namespace));
            meter.default_attribs.attributes.push(task_queue(tq));
            let kvs = meter.inner.new_attributes(meter.default_attribs);
            Self {
                kvs,
                instruments: Arc::new(Instruments::new(meter.inner.as_ref())),
                meter: meter.inner,
            }
        } else {
            Self::no_op()
        }
    }

    /// Extend an existing metrics context with new attributes
    pub(crate) fn with_new_attrs(
        &self,
        new_attrs: impl IntoIterator<Item = MetricKeyValue>,
    ) -> Self {
        let kvs = self
            .meter
            .extend_attributes(self.kvs.clone(), new_attrs.into());
        Self {
            kvs,
            instruments: self.instruments.clone(),
            meter: self.meter.clone(),
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
            .record(num as u64, &self.kvs)
    }

    /// Record current number of pollers. Context should include poller type / task queue tag.
    pub(crate) fn record_num_pollers(&self, num: usize) {
        self.instruments.num_pollers.record(num as u64, &self.kvs);
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
        self.instruments.sticky_cache_size.record(size, &self.kvs);
    }

    /// Count a workflow being evicted from the cache
    pub(crate) fn cache_eviction(&self) {
        self.instruments.sticky_cache_evictions.add(1, &self.kvs);
    }
}

impl Instruments {
    fn new(meter: &dyn CoreMeter) -> Self {
        Self {
            wf_completed_counter: meter.counter(MetricParameters {
                name: "workflow_completed".into(),
                description: "Count of successfully completed workflows".into(),
                unit: "".into(),
            }),
            wf_canceled_counter: meter.counter(MetricParameters {
                name: "workflow_canceled".into(),
                description: "Count of canceled workflows".into(),
                unit: "".into(),
            }),
            wf_failed_counter: meter.counter(MetricParameters {
                name: "workflow_failed".into(),
                description: "Count of failed workflows".into(),
                unit: "".into(),
            }),
            wf_cont_counter: meter.counter(MetricParameters {
                name: "workflow_continue_as_new".into(),
                description: "Count of continued-as-new workflows".into(),
                unit: "".into(),
            }),
            wf_e2e_latency: meter.histogram(MetricParameters {
                name: WF_E2E_LATENCY_NAME.into(),
                unit: "ms".into(),
                description: "Histogram of total workflow execution latencies".into(),
            }),
            wf_task_queue_poll_empty_counter: meter.counter(MetricParameters {
                name: "workflow_task_queue_poll_empty".into(),
                description: "Count of workflow task queue poll timeouts (no new task)".into(),
                unit: "".into(),
            }),
            wf_task_queue_poll_succeed_counter: meter.counter(MetricParameters {
                name: "workflow_task_queue_poll_succeed".into(),
                description: "Count of workflow task queue poll successes".into(),
                unit: "".into(),
            }),
            wf_task_execution_failure_counter: meter.counter(MetricParameters {
                name: "workflow_task_execution_failed".into(),
                description: "Count of workflow task execution failures".into(),
                unit: "".into(),
            }),
            wf_task_sched_to_start_latency: meter.histogram(MetricParameters {
                name: WF_TASK_SCHED_TO_START_LATENCY_NAME.into(),
                unit: "ms".into(),
                description: "Histogram of workflow task schedule-to-start latencies".into(),
            }),
            wf_task_replay_latency: meter.histogram(MetricParameters {
                name: WF_TASK_REPLAY_LATENCY_NAME.into(),
                unit: "ms".into(),
                description: "Histogram of workflow task replay latencies".into(),
            }),
            wf_task_execution_latency: meter.histogram(MetricParameters {
                name: WF_TASK_EXECUTION_LATENCY_NAME.into(),
                unit: "ms".into(),
                description: "Histogram of workflow task execution (not replay) latencies".into(),
            }),
            act_poll_no_task: meter.counter(MetricParameters {
                name: "activity_poll_no_task".into(),
                description: "Count of activity task queue poll timeouts (no new task)".into(),
                unit: "".into(),
            }),
            act_task_received_counter: meter.counter(MetricParameters {
                name: "activity_task_received".into(),
                description: "Count of activity task queue poll successes".into(),
                unit: "".into(),
            }),
            act_execution_failed: meter.counter(MetricParameters {
                name: "activity_execution_failed".into(),
                description: "Count of activity task execution failures".into(),
                unit: "".into(),
            }),
            act_sched_to_start_latency: meter.histogram(MetricParameters {
                name: ACT_SCHED_TO_START_LATENCY_NAME.into(),
                unit: "ms".into(),
                description: "Histogram of activity schedule-to-start latencies".into(),
            }),
            act_exec_latency: meter.histogram(MetricParameters {
                name: ACT_EXEC_LATENCY_NAME.into(),
                unit: "ms".into(),
                description: "Histogram of activity execution latencies".into(),
            }),
            // name kept as worker start for compat with old sdk / what users expect
            worker_registered: meter.counter(MetricParameters {
                name: "worker_start".into(),
                description: "Count of the number of initialized workers".into(),
                unit: "".into(),
            }),
            num_pollers: meter.gauge(MetricParameters {
                name: NUM_POLLERS_NAME.into(),
                description: "Current number of active pollers per queue type".into(),
                unit: "".into(),
            }),
            task_slots_available: meter.gauge(MetricParameters {
                name: TASK_SLOTS_AVAILABLE_NAME.into(),
                description: "Current number of available slots per task type".into(),
                unit: "".into(),
            }),
            sticky_cache_hit: meter.counter(MetricParameters {
                name: "sticky_cache_hit".into(),
                description: "Count of times the workflow cache was used for a new workflow task"
                    .into(),
                unit: "".into(),
            }),
            sticky_cache_miss: meter.counter(MetricParameters {
                name: "sticky_cache_miss".into(),
                description:
                    "Count of times the workflow cache was missing a workflow for a sticky task"
                        .into(),
                unit: "".into(),
            }),
            sticky_cache_size: meter.gauge(MetricParameters {
                name: STICKY_CACHE_SIZE_NAME.into(),
                description: "Current number of cached workflows".into(),
                unit: "".into(),
            }),
            sticky_cache_evictions: meter.counter(MetricParameters {
                name: "sticky_cache_total_forced_eviction".into(),
                description: "Count of evictions of cached workflows".into(),
                unit: "".into(),
            }),
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

pub(crate) fn workflow_poller() -> MetricKeyValue {
    MetricKeyValue::new(KEY_POLLER_TYPE, "workflow_task")
}
pub(crate) fn workflow_sticky_poller() -> MetricKeyValue {
    MetricKeyValue::new(KEY_POLLER_TYPE, "sticky_workflow_task")
}
pub(crate) fn activity_poller() -> MetricKeyValue {
    MetricKeyValue::new(KEY_POLLER_TYPE, "activity_task")
}
pub(crate) fn task_queue(tq: String) -> MetricKeyValue {
    MetricKeyValue::new(KEY_TASK_QUEUE, tq)
}
pub(crate) fn activity_type(ty: String) -> MetricKeyValue {
    MetricKeyValue::new(KEY_ACT_TYPE, ty)
}
pub(crate) fn workflow_type(ty: String) -> MetricKeyValue {
    MetricKeyValue::new(KEY_WF_TYPE, ty)
}
pub(crate) fn workflow_worker_type() -> MetricKeyValue {
    MetricKeyValue::new(KEY_WORKER_TYPE, "WorkflowWorker")
}
pub(crate) fn activity_worker_type() -> MetricKeyValue {
    MetricKeyValue::new(KEY_WORKER_TYPE, "ActivityWorker")
}
pub(crate) fn local_activity_worker_type() -> MetricKeyValue {
    MetricKeyValue::new(KEY_WORKER_TYPE, "LocalActivityWorker")
}
pub(crate) fn eager(is_eager: bool) -> MetricKeyValue {
    MetricKeyValue::new(KEY_EAGER, is_eager)
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

/// Returns the default histogram buckets that lang should use for a given metric name if they
/// have not been overridden by the user.
///
/// The name must *not* be prefixed with `temporal_`
pub fn default_buckets_for(histo_name: &str) -> &'static [f64] {
    match histo_name {
        WF_E2E_LATENCY_NAME => WF_LATENCY_MS_BUCKETS,
        WF_TASK_EXECUTION_LATENCY_NAME | WF_TASK_REPLAY_LATENCY_NAME => WF_TASK_MS_BUCKETS,
        WF_TASK_SCHED_TO_START_LATENCY_NAME | ACT_SCHED_TO_START_LATENCY_NAME => {
            TASK_SCHED_TO_START_MS_BUCKETS
        }
        ACT_EXEC_LATENCY_NAME => ACT_EXE_MS_BUCKETS,
        _ => DEFAULT_MS_BUCKETS,
    }
}

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

pub(super) fn augment_meter_provider_with_defaults(
    mpb: MeterProviderBuilder,
    global_tags: &HashMap<String, String>,
) -> opentelemetry::metrics::Result<MeterProviderBuilder> {
    // Some histograms are actually gauges, but we have to use histograms otherwise they forget
    // their value between collections since we don't use callbacks.
    Ok(mpb
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
        .with_view(histo_view(ACT_EXEC_LATENCY_NAME, ACT_EXE_MS_BUCKETS)?)
        .with_resource(default_resource(global_tags)))
}

/// OTel has no built-in synchronous Gauge. Histograms used to be able to serve that purpose, but
/// they broke that. Lovely. So, we need to implement one by hand.
pub(crate) struct MemoryGaugeU64 {
    labels_to_values: Arc<RwLock<HashMap<AttributeSet, u64>>>,
}

impl MemoryGaugeU64 {
    fn new(params: MetricParameters, meter: &Meter) -> Self {
        let gauge = meter
            .u64_observable_gauge(params.name)
            .with_unit(Unit::new(params.unit))
            .with_description(params.description)
            .init();
        let map = Arc::new(RwLock::new(HashMap::<AttributeSet, u64>::new()));
        let map_c = map.clone();
        meter
            .register_callback(&[gauge.as_any()], move |o| {
                // This whole thing is... extra stupid.
                // See https://github.com/open-telemetry/opentelemetry-rust/issues/1181
                // The performance is likely bad here, but, given this is only called when metrics
                // are exported it should be livable for now.
                let map_rlock = map_c.read();
                for (kvs, val) in map_rlock.iter() {
                    let kvs: Vec<_> = kvs
                        .iter()
                        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                        .collect();
                    o.observe_u64(&gauge, *val, kvs.as_slice())
                }
            })
            .expect("instrument must exist we just created it");
        MemoryGaugeU64 {
            labels_to_values: map,
        }
    }
    fn record(&self, val: u64, kvs: &[KeyValue]) {
        self.labels_to_values
            .write()
            .insert(AttributeSet::from(kvs), val);
    }
}

/// Create an OTel meter that can be used as a [CoreMeter] to export metrics over OTLP.
pub fn build_otlp_metric_exporter(
    opts: OtelCollectorOptions,
) -> Result<CoreOtelMeter, anyhow::Error> {
    let exporter = opentelemetry_otlp::MetricsExporter::new(
        opentelemetry_otlp::TonicExporterBuilder::default()
            .with_endpoint(opts.url.to_string())
            .with_metadata(MetadataMap::from_headers((&opts.headers).try_into()?)),
        Box::new(metric_temporality_to_selector(opts.metric_temporality)),
        Box::<SDKAggSelector>::default(),
    )?;
    let reader = PeriodicReader::builder(exporter, runtime::Tokio)
        .with_interval(opts.metric_periodicity)
        .build();
    let mp = augment_meter_provider_with_defaults(
        MeterProvider::builder().with_reader(reader),
        &opts.global_tags,
    )?
    .build();
    Ok::<_, anyhow::Error>(CoreOtelMeter(mp.meter(TELEM_SERVICE_NAME)))
}

pub struct StartedPromServer {
    pub meter: Arc<CoreOtelMeter>,
    pub bound_addr: SocketAddr,
    pub abort_handle: AbortHandle,
}

/// Builds and runs a prometheus endpoint which can be scraped by prom instances for metrics export.
/// Returns the meter that can be used as a [CoreMeter].
pub fn start_prometheus_metric_exporter(
    opts: PrometheusExporterOptions,
) -> Result<StartedPromServer, anyhow::Error> {
    let (srv, exporter) = PromServer::new(&opts, SDKAggSelector::default())?;
    let meter_provider = augment_meter_provider_with_defaults(
        MeterProvider::builder().with_reader(exporter),
        &opts.global_tags,
    )?
    .build();
    let bound_addr = srv.bound_addr();
    let handle = tokio::spawn(async move { srv.run().await });
    Ok(StartedPromServer {
        meter: Arc::new(CoreOtelMeter(meter_provider.meter(TELEM_SERVICE_NAME))),
        bound_addr,
        abort_handle: handle.abort_handle(),
    })
}

/// Buffers [MetricEvent]s for periodic consumption by lang
#[derive(Debug)]
pub struct MetricsCallBuffer<I>
where
    I: BufferInstrumentRef,
{
    calls_rx: crossbeam::channel::Receiver<MetricEvent<I>>,
    calls_tx: LogErrOnFullSender<MetricEvent<I>>,
}
#[derive(Clone, Debug)]
struct LogErrOnFullSender<I>(crossbeam::channel::Sender<I>);
impl<I> LogErrOnFullSender<I> {
    fn send(&self, v: I) {
        if let Err(crossbeam::channel::TrySendError::Full(_)) = self.0.try_send(v) {
            error!(
                "Core's metrics buffer is full! Dropping call to record metrics. \
                 Make sure you drain the metric buffer often!"
            );
        }
    }
}

impl<I> MetricsCallBuffer<I>
where
    I: Clone + BufferInstrumentRef,
{
    /// Create a new buffer with the given capacity
    pub fn new(buffer_size: usize) -> Self {
        let (calls_tx, calls_rx) = crossbeam::channel::bounded(buffer_size);
        MetricsCallBuffer {
            calls_rx,
            calls_tx: LogErrOnFullSender(calls_tx),
        }
    }
    fn new_instrument(&self, params: MetricParameters, kind: MetricKind) -> BufferInstrument<I> {
        let hole = LazyBufferInstrument::hole();
        self.calls_tx.send(MetricEvent::Create {
            params,
            kind,
            populate_into: hole.clone(),
        });
        BufferInstrument {
            kind,
            instrument_ref: hole,
            tx: self.calls_tx.clone(),
        }
    }
}

impl<I> CoreMeter for MetricsCallBuffer<I>
where
    I: BufferInstrumentRef + Debug + Send + Sync + Clone + 'static,
{
    fn new_attributes(&self, opts: NewAttributes) -> MetricAttributes {
        let ba = BufferAttributes::hole();
        self.calls_tx.send(MetricEvent::CreateAttributes {
            populate_into: ba.clone(),
            append_from: None,
            attributes: opts.attributes,
        });
        MetricAttributes::Buffer(ba)
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        if let MetricAttributes::Buffer(ol) = existing {
            let ba = BufferAttributes::hole();
            self.calls_tx.send(MetricEvent::CreateAttributes {
                populate_into: ba.clone(),
                append_from: Some(ol),
                attributes: attribs.attributes,
            });
            MetricAttributes::Buffer(ba)
        } else {
            dbg_panic!("Must use buffer attributes with a buffer metric implementation");
            existing
        }
    }

    fn counter(&self, params: MetricParameters) -> Arc<dyn Counter> {
        Arc::new(self.new_instrument(params, MetricKind::Counter))
    }

    fn histogram(&self, params: MetricParameters) -> Arc<dyn Histogram> {
        Arc::new(self.new_instrument(params, MetricKind::Histogram))
    }

    fn gauge(&self, params: MetricParameters) -> Arc<dyn Gauge> {
        Arc::new(self.new_instrument(params, MetricKind::Gauge))
    }
}
impl<I> MetricCallBufferer<I> for MetricsCallBuffer<I>
where
    I: Send + Sync + BufferInstrumentRef,
{
    fn retrieve(&self) -> Vec<MetricEvent<I>> {
        self.calls_rx.try_iter().collect()
    }
}

struct BufferInstrument<I: BufferInstrumentRef> {
    kind: MetricKind,
    instrument_ref: LazyBufferInstrument<I>,
    tx: LogErrOnFullSender<MetricEvent<I>>,
}
impl<I> BufferInstrument<I>
where
    I: Clone + BufferInstrumentRef,
{
    fn send(&self, value: u64, attributes: &MetricAttributes) {
        let attributes = match attributes {
            MetricAttributes::Buffer(l) => l.clone(),
            _ => panic!("MetricsCallBuffer only works with MetricAttributes::Lang"),
        };
        self.tx.send(MetricEvent::Update {
            instrument: self.instrument_ref.clone(),
            update: match self.kind {
                MetricKind::Counter => MetricUpdateVal::Delta(value),
                MetricKind::Gauge | MetricKind::Histogram => MetricUpdateVal::Value(value),
            },
            attributes: attributes.clone(),
        });
    }
}
impl<I> Counter for BufferInstrument<I>
where
    I: BufferInstrumentRef + Send + Sync + Clone,
{
    fn add(&self, value: u64, attributes: &MetricAttributes) {
        self.send(value, attributes)
    }
}
impl<I> Gauge for BufferInstrument<I>
where
    I: BufferInstrumentRef + Send + Sync + Clone,
{
    fn record(&self, value: u64, attributes: &MetricAttributes) {
        self.send(value, attributes)
    }
}
impl<I> Histogram for BufferInstrument<I>
where
    I: BufferInstrumentRef + Send + Sync + Clone,
{
    fn record(&self, value: u64, attributes: &MetricAttributes) {
        self.send(value, attributes)
    }
}

#[derive(Debug)]
pub struct CoreOtelMeter(Meter);
impl CoreMeter for CoreOtelMeter {
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        MetricAttributes::OTel {
            kvs: Arc::new(attribs.attributes.into_iter().map(KeyValue::from).collect()),
        }
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        if let MetricAttributes::OTel { mut kvs } = existing {
            Arc::make_mut(&mut kvs).extend(attribs.attributes.into_iter().map(Into::into));
            MetricAttributes::OTel { kvs }
        } else {
            dbg_panic!("Must use OTel attributes with an OTel metric implementation");
            existing
        }
    }

    fn counter(&self, params: MetricParameters) -> Arc<dyn Counter> {
        Arc::new(
            self.0
                .u64_counter(params.name)
                .with_unit(Unit::new(params.unit))
                .with_description(params.description)
                .init(),
        )
    }

    fn histogram(&self, params: MetricParameters) -> Arc<dyn Histogram> {
        Arc::new(
            self.0
                .u64_histogram(params.name)
                .with_unit(Unit::new(params.unit))
                .with_description(params.description)
                .init(),
        )
    }

    fn gauge(&self, params: MetricParameters) -> Arc<dyn Gauge> {
        Arc::new(MemoryGaugeU64::new(params, &self.0))
    }
}

impl Gauge for MemoryGaugeU64 {
    fn record(&self, value: u64, attributes: &MetricAttributes) {
        if let MetricAttributes::OTel { kvs } = attributes {
            self.record(value, kvs);
        } else {
            dbg_panic!("Must use OTel attributes with an OTel metric implementation");
        }
    }
}

#[derive(Debug, derive_more::Constructor)]
pub(crate) struct PrefixedMetricsMeter<CM> {
    prefix: String,
    meter: CM,
}
impl<CM: CoreMeter> CoreMeter for PrefixedMetricsMeter<CM> {
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        self.meter.new_attributes(attribs)
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        self.meter.extend_attributes(existing, attribs)
    }

    fn counter(&self, mut params: MetricParameters) -> Arc<dyn Counter> {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.counter(params)
    }

    fn histogram(&self, mut params: MetricParameters) -> Arc<dyn Histogram> {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.histogram(params)
    }

    fn gauge(&self, mut params: MetricParameters) -> Arc<dyn Gauge> {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.gauge(params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::any::Any;
    use temporal_sdk_core_api::telemetry::{
        metrics::{BufferInstrumentRef, CustomMetricAttributes},
        METRIC_PREFIX,
    };
    use tracing::subscriber::NoSubscriber;

    #[derive(Debug)]
    struct DummyCustomAttrs(usize);
    impl CustomMetricAttributes for DummyCustomAttrs {
        fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
            self as Arc<dyn Any + Send + Sync>
        }
    }
    impl DummyCustomAttrs {
        fn as_id(ba: &BufferAttributes) -> usize {
            let as_dum = ba
                .get()
                .clone()
                .as_any()
                .downcast::<DummyCustomAttrs>()
                .unwrap();
            as_dum.0
        }
    }

    #[derive(Debug, Clone)]
    struct DummyInstrumentRef(usize);
    impl BufferInstrumentRef for DummyInstrumentRef {}

    #[test]
    fn test_buffered_core_context() {
        let no_op_subscriber = Arc::new(NoSubscriber::new());
        let call_buffer = Arc::new(MetricsCallBuffer::new(100));
        let telem_instance = TelemetryInstance::new(
            Some(no_op_subscriber),
            None,
            METRIC_PREFIX.to_string(),
            Some(call_buffer.clone()),
            true,
        );
        let mc = MetricsContext::top_level("foo".to_string(), "q".to_string(), &telem_instance);
        mc.cache_eviction();
        let events = call_buffer.retrieve();
        let a1 = assert_matches!(
            &events[0],
            MetricEvent::CreateAttributes {
                populate_into,
                append_from: None,
                attributes,
            }
            if attributes[0].key == "service_name" &&
               attributes[1].key == "namespace" &&
               attributes[2].key == "task_queue"
            => populate_into
        );
        a1.set(Arc::new(DummyCustomAttrs(1))).unwrap();
        // Verify all metrics are created. This number will need to get updated any time a metric
        // is added.
        let num_metrics = 23;
        #[allow(clippy::needless_range_loop)] // Sorry clippy, this reads easier.
        for metric_num in 1..=num_metrics {
            let hole = assert_matches!(&events[metric_num],
                MetricEvent::Create { populate_into, .. }
                => populate_into
            );
            hole.set(Arc::new(DummyInstrumentRef(metric_num))).unwrap();
        }
        assert_matches!(
            &events[num_metrics + 1], // +1 for attrib creation (at start), then this update
            MetricEvent::Update {
                instrument,
                attributes,
                update: MetricUpdateVal::Delta(1)
            }
            if DummyCustomAttrs::as_id(attributes) == 1 && instrument.get().0 == num_metrics
        );
        // Verify creating a new context with new attributes merges them properly
        let mc2 = mc.with_new_attrs([MetricKeyValue::new("gotta", "go fast")]);
        mc2.wf_task_latency(Duration::from_secs(1));
        let events = call_buffer.retrieve();
        let a2 = assert_matches!(
            &events[0],
            MetricEvent::CreateAttributes {
                populate_into,
                append_from: Some(eh),
                attributes
            }
            if attributes[0].key == "gotta" && DummyCustomAttrs::as_id(eh) == 1
            => populate_into
        );
        a2.set(Arc::new(DummyCustomAttrs(2))).unwrap();
        assert_matches!(
            &events[1],
            MetricEvent::Update {
                instrument,
                attributes,
                update: MetricUpdateVal::Value(1000) // milliseconds
            }
            if DummyCustomAttrs::as_id(attributes) == 2 && instrument.get().0 == 11
        );
    }

    #[test]
    fn metric_buffer() {
        let call_buffer = MetricsCallBuffer::new(10);
        let ctr = call_buffer.counter(MetricParameters {
            name: "ctr".into(),
            description: "a counter".into(),
            unit: "grognaks".into(),
        });
        let histo = call_buffer.histogram(MetricParameters {
            name: "histo".into(),
            description: "a histogram".into(),
            unit: "flubarbs".into(),
        });
        let gauge = call_buffer.gauge(MetricParameters {
            name: "gauge".into(),
            description: "a counter".into(),
            unit: "bleezles".into(),
        });
        let attrs_1 = call_buffer.new_attributes(NewAttributes {
            attributes: vec![MetricKeyValue::new("hi", "yo")],
        });
        let attrs_2 = call_buffer.new_attributes(NewAttributes {
            attributes: vec![MetricKeyValue::new("run", "fast")],
        });
        ctr.add(1, &attrs_1);
        histo.record(2, &attrs_1);
        gauge.record(3, &attrs_2);

        let mut calls = call_buffer.retrieve();
        calls.reverse();
        let ctr_1 = assert_matches!(
            calls.pop(),
            Some(MetricEvent::Create {
                params,
                populate_into,
                kind: MetricKind::Counter
            })
            if params.name == "ctr"
            => populate_into
        );
        ctr_1.set(Arc::new(DummyInstrumentRef(1))).unwrap();
        let hist_2 = assert_matches!(
            calls.pop(),
            Some(MetricEvent::Create {
                params,
                populate_into,
                kind: MetricKind::Histogram
            })
            if params.name == "histo"
            => populate_into
        );
        hist_2.set(Arc::new(DummyInstrumentRef(2))).unwrap();
        let gauge_3 = assert_matches!(
            calls.pop(),
            Some(MetricEvent::Create {
                params,
                populate_into,
                kind: MetricKind::Gauge
            })
            if params.name == "gauge"
            => populate_into
        );
        gauge_3.set(Arc::new(DummyInstrumentRef(3))).unwrap();
        let a1 = assert_matches!(
            calls.pop(),
            Some(MetricEvent::CreateAttributes {
                populate_into,
                append_from: None,
                attributes
            })
            if attributes[0].key == "hi"
            => populate_into
        );
        a1.set(Arc::new(DummyCustomAttrs(1))).unwrap();
        let a2 = assert_matches!(
            calls.pop(),
            Some(MetricEvent::CreateAttributes {
                populate_into,
                append_from: None,
                attributes
            })
            if attributes[0].key == "run"
            => populate_into
        );
        a2.set(Arc::new(DummyCustomAttrs(2))).unwrap();
        assert_matches!(
            calls.pop(),
            Some(MetricEvent::Update{
                instrument,
                attributes,
                update: MetricUpdateVal::Delta(1)
            })
            if DummyCustomAttrs::as_id(&attributes) == 1 && instrument.get().0 == 1
        );
        assert_matches!(
            calls.pop(),
            Some(MetricEvent::Update{
                instrument,
                attributes,
                update: MetricUpdateVal::Value(2)
            })
            if DummyCustomAttrs::as_id(&attributes) == 1 && instrument.get().0 == 2
        );
        assert_matches!(
            calls.pop(),
            Some(MetricEvent::Update{
                instrument,
                attributes,
                update: MetricUpdateVal::Value(3)
            })
            if DummyCustomAttrs::as_id(&attributes) == 2&& instrument.get().0 == 3
        );
    }
}
