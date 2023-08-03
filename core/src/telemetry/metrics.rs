use crate::telemetry::{
    default_resource, metric_temporality_to_selector, prometheus_server::PromServer,
    TelemetryInstance, TELEM_SERVICE_NAME,
};
use opentelemetry::{
    self,
    metrics::{Meter, MeterProvider as MeterProviderT},
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
use std::{
    borrow::Cow,
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use temporal_sdk_core_api::telemetry::{
    metrics::{
        CoreMeter, Counter, Gauge, Histogram, LangMetricAttributes, MetricAttributes,
        MetricCallBufferer, MetricEvent, MetricKeyValue, MetricKind, MetricUpdateVal,
        MetricsAttributesOptions, NoOpCoreMeter,
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
        if let Some(meter) = telemetry.get_metric_meter() {
            let kvs = meter.new_attributes(MetricsAttributesOptions::new(vec![
                MetricKeyValue::new(KEY_NAMESPACE, namespace),
                task_queue(tq),
            ]));
            Self {
                kvs,
                instruments: Arc::new(Instruments::new(meter.as_ref())),
                meter,
            }
        } else {
            Self::no_op()
        }
    }

    /// Extend an existing metrics context with new attributes
    pub(crate) fn with_new_attrs(&self, new_kvs: impl IntoIterator<Item = MetricKeyValue>) -> Self {
        let kvs = self.kvs.with_new_attrs(new_kvs);
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
        .with_resource(default_resource(&global_tags)))
}

/// OTel has no built-in synchronous Gauge. Histograms used to be able to serve that purpose, but
/// they broke that. Lovely. So, we need to implement one by hand.
pub(crate) struct MemoryGaugeU64 {
    labels_to_values: Arc<RwLock<HashMap<AttributeSet, u64>>>,
}

impl MemoryGaugeU64 {
    fn new(name: impl Into<Cow<'static, str>>, meter: &Meter) -> Self {
        let gauge = meter.u64_observable_gauge(name).init();
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
    fn record(&self, val: u64, kvs: Arc<Vec<KeyValue>>) {
        self.labels_to_values
            .write()
            .insert(AttributeSet::from(kvs.as_slice()), val);
    }
}

/// Create an OTel meter that can be used as a [CoreMeter] to export metrics over OTLP.
pub fn build_otlp_metric_exporter(opts: OtelCollectorOptions) -> Result<Arc<Meter>, anyhow::Error> {
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
    Ok::<_, anyhow::Error>(Arc::new(mp.meter(TELEM_SERVICE_NAME)))
}

pub struct StartedPromServer {
    pub meter: Arc<Meter>,
    pub bound_addr: SocketAddr,
    pub abort_handle: AbortHandle,
}

/// Builds and runs a prometheus endpoint which can be scraped by prom instances for metrics export.
/// Returns the meter that can be used as a [CoreMeter].
pub fn start_prometheus_metric_exporter(
    opts: PrometheusExporterOptions,
) -> Result<StartedPromServer, anyhow::Error> {
    let (srv, exporter) = PromServer::new(opts.socket_addr, SDKAggSelector::default())?;
    let meter_provider = augment_meter_provider_with_defaults(
        MeterProvider::builder().with_reader(exporter),
        &opts.global_tags,
    )?
    .build();
    let bound_addr = srv.bound_addr();
    let handle = tokio::spawn(async move { srv.run().await });
    Ok(StartedPromServer {
        meter: Arc::new(meter_provider.meter(TELEM_SERVICE_NAME)),
        bound_addr,
        abort_handle: handle.abort_handle(),
    })
}

/// Buffers [MetricEvent]s for periodic consumption by lang
#[derive(Debug)]
pub struct MetricsCallBuffer {
    instrument_ids: AtomicU64,
    attribute_ids: AtomicU64,
    calls_rx: crossbeam::channel::Receiver<MetricEvent>,
    calls_tx: crossbeam::channel::Sender<MetricEvent>,
}
impl MetricsCallBuffer {
    /// Create a new buffer with the given capacity
    pub fn new(buffer_size: usize) -> Self {
        let (calls_tx, calls_rx) = crossbeam::channel::bounded(buffer_size);
        MetricsCallBuffer {
            instrument_ids: AtomicU64::new(0),
            attribute_ids: AtomicU64::new(0),
            calls_rx,
            calls_tx,
        }
    }
    fn new_instrument(&self, name: &str, kind: MetricKind) -> BufferInstrument {
        let id = self.instrument_ids.fetch_add(1, Ordering::AcqRel);
        let _ = self.calls_tx.send(MetricEvent::Create {
            name: name.to_string(),
            id,
            kind,
        });
        BufferInstrument {
            kind,
            id,
            tx: self.calls_tx.clone(),
        }
    }
}
impl CoreMeter for MetricsCallBuffer {
    fn new_attributes(&self, opts: MetricsAttributesOptions) -> MetricAttributes {
        let id = self.attribute_ids.fetch_add(1, Ordering::AcqRel);
        let _ = self.calls_tx.send(MetricEvent::CreateAttributes {
            id,
            attributes: opts.attributes,
        });
        MetricAttributes::Lang(LangMetricAttributes {
            id,
            new_attributes: vec![],
        })
    }

    fn counter(&self, name: &str) -> Arc<dyn Counter> {
        Arc::new(self.new_instrument(name, MetricKind::Counter))
    }

    fn histogram(&self, name: &str) -> Arc<dyn Histogram> {
        Arc::new(self.new_instrument(name, MetricKind::Histogram))
    }

    fn gauge(&self, name: &str) -> Arc<dyn Gauge> {
        Arc::new(self.new_instrument(name, MetricKind::Gauge))
    }
}
impl MetricCallBufferer for MetricsCallBuffer {
    fn retrieve(&mut self) -> Vec<MetricEvent> {
        self.calls_rx.try_iter().collect()
    }
}

struct BufferInstrument {
    kind: MetricKind,
    id: u64,
    tx: crossbeam::channel::Sender<MetricEvent>,
}
impl BufferInstrument {
    fn send(&self, value: u64, attributes: &MetricAttributes) {
        let attributes = match attributes {
            MetricAttributes::Lang(l) => l.clone(),
            _ => panic!("MetricsCallBuffer only works with MetricAttributes::Lang"),
        };
        let _ = self.tx.send(MetricEvent::Update {
            id: self.id,
            update: match self.kind {
                MetricKind::Counter => MetricUpdateVal::Delta(value),
                MetricKind::Gauge | MetricKind::Histogram => MetricUpdateVal::Value(value),
            },
            attributes: attributes.clone(),
        });
    }
}
impl Counter for BufferInstrument {
    fn add(&self, value: u64, attributes: &MetricAttributes) {
        self.send(value, attributes)
    }
}
impl Gauge for BufferInstrument {
    fn record(&self, value: u64, attributes: &MetricAttributes) {
        self.send(value, attributes)
    }
}
impl Histogram for BufferInstrument {
    fn record(&self, value: u64, attributes: &MetricAttributes) {
        self.send(value, attributes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metric_buffer() {
        let mut call_buffer = MetricsCallBuffer::new(10);
        let ctr = call_buffer.counter("ctr");
        let histo = call_buffer.histogram("histo");
        let gauge = call_buffer.gauge("gauge");
        let attrs_1 = call_buffer.new_attributes(MetricsAttributesOptions {
            attributes: vec![MetricKeyValue::new("hi", "yo")],
        });
        let attrs_2 = call_buffer.new_attributes(MetricsAttributesOptions {
            attributes: vec![MetricKeyValue::new("run", "fast")],
        });
        ctr.add(1, &attrs_1);
        histo.record(2, &attrs_1);
        gauge.record(3, &attrs_2);

        let calls = call_buffer.retrieve();
        dbg!(calls);
    }
}
