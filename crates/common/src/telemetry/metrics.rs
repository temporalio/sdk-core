use crate::{dbg_panic, telemetry::TaskQueueLabelStrategy};
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    fmt::{Debug, Display},
    ops::Deref,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

#[cfg(feature = "core-based-sdk")]
pub mod core;

/// The string name (which may be prefixed) for this metric
pub const WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME: &str = "workflow_endtoend_latency";
/// The string name (which may be prefixed) for this metric
pub const WORKFLOW_TASK_SCHED_TO_START_LATENCY_HISTOGRAM_NAME: &str =
    "workflow_task_schedule_to_start_latency";
/// The string name (which may be prefixed) for this metric
pub const WORKFLOW_TASK_REPLAY_LATENCY_HISTOGRAM_NAME: &str = "workflow_task_replay_latency";
/// The string name (which may be prefixed) for this metric
pub const WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME: &str = "workflow_task_execution_latency";
/// The string name (which may be prefixed) for this metric
pub const ACTIVITY_SCHED_TO_START_LATENCY_HISTOGRAM_NAME: &str =
    "activity_schedule_to_start_latency";
/// The string name (which may be prefixed) for this metric
pub const ACTIVITY_EXEC_LATENCY_HISTOGRAM_NAME: &str = "activity_execution_latency";

/// Helps define buckets once in terms of millis, but also generates a seconds version
macro_rules! define_latency_buckets {
    ($(($metric_name:pat, $name:ident, $sec_name:ident, [$($bucket:expr),*])),*) => {
        $(
            pub(super) static $name: &[f64] = &[$($bucket,)*];
            pub(super) static $sec_name: &[f64] = &[$( $bucket / 1000.0, )*];
        )*

        /// Returns the default histogram buckets that lang should use for a given metric name if
        /// they have not been overridden by the user. If `use_seconds` is true, returns buckets
        /// in terms of seconds rather than milliseconds.
        ///
        /// The name must *not* be prefixed with `temporal_`
        pub fn default_buckets_for(histo_name: &str, use_seconds: bool) -> &'static [f64] {
            match histo_name {
                $(
                    $metric_name => { if use_seconds { &$sec_name } else { &$name } },
                )*
            }
        }
    };
}

define_latency_buckets!(
    (
        WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME,
        WF_LATENCY_MS_BUCKETS,
        WF_LATENCY_S_BUCKETS,
        [
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
            8.64e7       // 24 hrs
        ]
    ),
    (
        WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME
            | WORKFLOW_TASK_REPLAY_LATENCY_HISTOGRAM_NAME,
        WF_TASK_MS_BUCKETS,
        WF_TASK_S_BUCKETS,
        [1., 10., 20., 50., 100., 200., 500., 1000.]
    ),
    (
        ACTIVITY_EXEC_LATENCY_HISTOGRAM_NAME,
        ACT_EXE_MS_BUCKETS,
        ACT_EXE_S_BUCKETS,
        [50., 100., 500., 1000., 5000., 10_000., 60_000.]
    ),
    (
        WORKFLOW_TASK_SCHED_TO_START_LATENCY_HISTOGRAM_NAME
            | ACTIVITY_SCHED_TO_START_LATENCY_HISTOGRAM_NAME,
        TASK_SCHED_TO_START_MS_BUCKETS,
        TASK_SCHED_TO_START_S_BUCKETS,
        [100., 500., 1000., 5000., 10_000., 100_000., 1_000_000.]
    ),
    (
        _,
        DEFAULT_MS_BUCKETS,
        DEFAULT_S_BUCKETS,
        [50., 100., 500., 1000., 2500., 10_000.]
    )
);

/// Implementors of this trait are expected to be defined in each language's bridge.
/// The implementor is responsible for the allocation/instantiation of new metric meters which
/// Core has requested.
pub trait CoreMeter: Send + Sync + Debug {
    /// Given some k/v pairs, create a return a new instantiated instance of metric attributes.
    /// Only [MetricAttributes] created by this meter can be used when calling record on instruments
    /// created by this meter.
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes;
    /// Extend some existing attributes with new values. Implementations should create new instances
    /// when doing so, rather than mutating whatever is backing the passed in `existing` attributes.
    /// Ideally that new instance retains a ref to the extended old attribute, promoting re-use.
    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes;
    fn counter(&self, params: MetricParameters) -> Counter;

    /// Create a counter with in-memory tracking for worker heartbeating reporting
    fn counter_with_in_memory(
        &self,
        params: MetricParameters,
        in_memory_counter: HeartbeatMetricType,
    ) -> Counter {
        let primary_counter = self.counter(params);

        Counter::new_with_in_memory(primary_counter.primary.metric.clone(), in_memory_counter)
    }

    fn histogram(&self, params: MetricParameters) -> Histogram;
    fn histogram_f64(&self, params: MetricParameters) -> HistogramF64;
    /// Create a histogram which records Durations. Implementations should choose to emit in
    /// either milliseconds or seconds depending on how they have been configured.
    /// [MetricParameters::unit] should be overwritten by implementations to be `ms` or `s`
    /// accordingly.
    fn histogram_duration(&self, params: MetricParameters) -> HistogramDuration;

    /// Create a histogram duration with in-memory tracking for worker heartbeating reporting
    fn histogram_duration_with_in_memory(
        &self,
        params: MetricParameters,
        in_memory_hist: HeartbeatMetricType,
    ) -> HistogramDuration {
        let primary_hist = self.histogram_duration(params);

        HistogramDuration::new_with_in_memory(primary_hist.primary.metric.clone(), in_memory_hist)
    }
    fn gauge(&self, params: MetricParameters) -> Gauge;

    /// Create a gauge with in-memory tracking for worker heartbeating reporting
    fn gauge_with_in_memory(
        &self,
        params: MetricParameters,
        in_memory_metrics: HeartbeatMetricType,
    ) -> Gauge {
        let primary_gauge = self.gauge(params.clone());
        Gauge::new_with_in_memory(primary_gauge.primary.metric.clone(), in_memory_metrics)
    }

    fn gauge_f64(&self, params: MetricParameters) -> GaugeF64;
}

/// Provides a generic way to record metrics in memory.
/// This can be done either with individual metrics or more fine-grained metrics
/// that vary by a set of labels for the same metric.
#[derive(Clone, Debug)]
pub enum HeartbeatMetricType {
    Individual(Arc<AtomicU64>),
    WithLabel {
        label_key: String,
        metrics: HashMap<String, Arc<AtomicU64>>,
    },
}

impl HeartbeatMetricType {
    fn record_counter(&self, delta: u64) {
        match self {
            HeartbeatMetricType::Individual(metric) => {
                metric.fetch_add(delta, Ordering::Relaxed);
            }
            HeartbeatMetricType::WithLabel { .. } => {
                dbg_panic!("Counter does not support in-memory metric with labels");
            }
        }
    }

    fn record_histogram_observation(&self) {
        match self {
            HeartbeatMetricType::Individual(metric) => {
                metric.fetch_add(1, Ordering::Relaxed);
            }
            HeartbeatMetricType::WithLabel { .. } => {
                dbg_panic!("Histogram does not support in-memory metric with labels");
            }
        }
    }

    fn record_gauge(&self, value: u64, attributes: &MetricAttributes) {
        match self {
            HeartbeatMetricType::Individual(metric) => {
                metric.store(value, Ordering::Relaxed);
            }
            HeartbeatMetricType::WithLabel { label_key, metrics } => {
                if let Some(metric) = label_value_from_attributes(attributes, label_key.as_str())
                    .and_then(|label_value| metrics.get(label_value.as_str()))
                {
                    metric.store(value, Ordering::Relaxed)
                }
            }
        }
    }
}

fn label_value_from_attributes(attributes: &MetricAttributes, key: &str) -> Option<String> {
    match attributes {
        MetricAttributes::Prometheus { labels } => labels.as_prom_labels().get(key).cloned(),
        #[cfg(feature = "otel")]
        MetricAttributes::OTel { kvs } => kvs
            .iter()
            .find(|kv| kv.key.as_str() == key)
            .map(|kv| kv.value.to_string()),
        MetricAttributes::NoOp(labels) => labels.get(key).cloned(),
        _ => None,
    }
}

#[derive(Debug, Clone, bon::Builder)]
pub struct MetricParameters {
    /// The name for the new metric/instrument
    #[builder(into)]
    pub name: Cow<'static, str>,
    /// A description that will appear in metadata if the backend supports it
    #[builder(into, default = Cow::Borrowed(""))]
    pub description: Cow<'static, str>,
    /// Unit information that will appear in metadata if the backend supports it
    #[builder(into, default = Cow::Borrowed(""))]
    pub unit: Cow<'static, str>,
}
impl From<&'static str> for MetricParameters {
    fn from(value: &'static str) -> Self {
        Self {
            name: value.into(),
            description: Default::default(),
            unit: Default::default(),
        }
    }
}

/// Wraps a [CoreMeter] to enable the attaching of default labels to metrics. Cloning is cheap.
#[derive(Clone, Debug)]
pub struct TemporalMeter {
    inner: Arc<dyn CoreMeter>,
    default_attribs: MetricAttributes,
    task_queue_label_strategy: TaskQueueLabelStrategy,
}

impl TemporalMeter {
    /// Create a new TemporalMeter.
    pub fn new(
        meter: Arc<dyn CoreMeter>,
        default_attribs: NewAttributes,
        task_queue_label_strategy: TaskQueueLabelStrategy,
    ) -> Self {
        Self {
            default_attribs: meter.new_attributes(default_attribs),
            inner: meter,
            task_queue_label_strategy,
        }
    }

    /// Creates a TemporalMeter that records nothing
    pub fn no_op() -> Self {
        Self {
            inner: Arc::new(NoOpCoreMeter),
            default_attribs: MetricAttributes::NoOp(Arc::new(Default::default())),
            task_queue_label_strategy: TaskQueueLabelStrategy::UseNormal,
        }
    }

    /// Returns the default attributes this meter uses.
    pub fn get_default_attributes(&self) -> &MetricAttributes {
        &self.default_attribs
    }

    /// Returns the Task Queue labeling strategy this meter uses.
    pub fn get_task_queue_label_strategy(&self) -> &TaskQueueLabelStrategy {
        &self.task_queue_label_strategy
    }

    /// Add some new attributes to the default set already in this meter.
    pub fn merge_attributes(&mut self, new_attribs: NewAttributes) {
        self.default_attribs = self.extend_attributes(self.default_attribs.clone(), new_attribs);
    }
}

impl Deref for TemporalMeter {
    type Target = dyn CoreMeter;
    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl CoreMeter for Arc<dyn CoreMeter> {
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        self.as_ref().new_attributes(attribs)
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        self.as_ref().extend_attributes(existing, attribs)
    }

    fn counter(&self, params: MetricParameters) -> Counter {
        self.as_ref().counter(params)
    }
    fn histogram(&self, params: MetricParameters) -> Histogram {
        self.as_ref().histogram(params)
    }

    fn histogram_f64(&self, params: MetricParameters) -> HistogramF64 {
        self.as_ref().histogram_f64(params)
    }

    fn histogram_duration(&self, params: MetricParameters) -> HistogramDuration {
        self.as_ref().histogram_duration(params)
    }

    fn gauge(&self, params: MetricParameters) -> Gauge {
        self.as_ref().gauge(params)
    }

    fn gauge_f64(&self, params: MetricParameters) -> GaugeF64 {
        self.as_ref().gauge_f64(params)
    }
}

/// Attributes which are provided every time a call to record a specific metric is made.
/// Implementors must be very cheap to clone, as these attributes will be re-used frequently.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum MetricAttributes {
    #[cfg(feature = "otel")]
    OTel {
        kvs: Arc<Vec<opentelemetry::KeyValue>>,
    },
    Prometheus {
        labels: Arc<OrderedPromLabelSet>,
    },
    #[cfg(feature = "core-based-sdk")]
    Buffer(core::BufferAttributes),
    #[cfg(feature = "core-based-sdk")]
    Dynamic(Arc<dyn core::CustomMetricAttributes>),
    NoOp(Arc<HashMap<String, String>>),
    Empty,
}

/// Options that are attached to metrics on a per-call basis
#[derive(Clone, Debug, Default, derive_more::Constructor)]
pub struct NewAttributes {
    pub attributes: Vec<MetricKeyValue>,
}
impl NewAttributes {
    pub fn extend(&mut self, new_kvs: impl IntoIterator<Item = MetricKeyValue>) {
        self.attributes.extend(new_kvs)
    }
}
impl<I> From<I> for NewAttributes
where
    I: IntoIterator<Item = MetricKeyValue>,
{
    fn from(value: I) -> Self {
        Self {
            attributes: value.into_iter().collect(),
        }
    }
}

impl From<NewAttributes> for HashMap<String, String> {
    fn from(value: NewAttributes) -> Self {
        value
            .attributes
            .into_iter()
            .map(|kv| (kv.key, kv.value.to_string()))
            .collect()
    }
}

/// A K/V pair that can be used to label a specific recording of a metric
#[derive(Clone, Debug, PartialEq)]
pub struct MetricKeyValue {
    pub key: String,
    pub value: MetricValue,
}
impl MetricKeyValue {
    pub fn new(key: impl Into<String>, value: impl Into<MetricValue>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}

/// Values metric labels may assume
#[derive(Clone, Debug, PartialEq, derive_more::From)]
pub enum MetricValue {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    // can add array if needed
}
impl From<&'static str> for MetricValue {
    fn from(value: &'static str) -> Self {
        MetricValue::String(value.to_string())
    }
}
impl Display for MetricValue {
    fn fmt(&self, f1: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetricValue::String(s) => write!(f1, "{s}"),
            MetricValue::Int(i) => write!(f1, "{i}"),
            MetricValue::Float(f) => write!(f1, "{f}"),
            MetricValue::Bool(b) => write!(f1, "{b}"),
        }
    }
}

pub trait MetricAttributable<Base> {
    /// Replace any existing attributes on this metric with new ones, and return a new copy
    /// of the metric, or a base version, which can be used to record values.
    ///
    /// Note that this operation is relatively expensive compared to simply recording a value
    /// without any additional attributes, so users should prefer to save the metric instance
    /// after calling this, and use the value-only methods afterward.
    ///
    /// This operation may fail if the underlying metrics implementation disallows the registration
    /// of a new metric, or encounters any other issue.
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Base, Box<dyn std::error::Error>>;
}

#[derive(Clone)]
pub struct LazyBoundMetric<T, B> {
    metric: T,
    attributes: MetricAttributes,
    bound_cache: OnceLock<B>,
}
impl<T, B> LazyBoundMetric<T, B> {
    pub fn update_attributes(&mut self, new_attributes: MetricAttributes) {
        self.attributes = new_attributes;
        self.bound_cache = OnceLock::new();
    }
}

pub trait CounterBase: Send + Sync {
    fn adds(&self, value: u64);
}

pub type CounterImpl = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn CounterBase>> + Send + Sync>,
    Arc<dyn CounterBase>,
>;

#[derive(Clone)]
pub struct Counter {
    primary: CounterImpl,
    in_memory: Option<HeartbeatMetricType>,
}
impl Counter {
    pub fn new(inner: Arc<dyn MetricAttributable<Box<dyn CounterBase>> + Send + Sync>) -> Self {
        Self {
            primary: LazyBoundMetric {
                metric: inner,
                attributes: MetricAttributes::Empty,
                bound_cache: OnceLock::new(),
            },
            in_memory: None,
        }
    }

    pub fn new_with_in_memory(
        primary: Arc<dyn MetricAttributable<Box<dyn CounterBase>> + Send + Sync>,
        in_memory: HeartbeatMetricType,
    ) -> Self {
        Self {
            primary: LazyBoundMetric {
                metric: primary,
                attributes: MetricAttributes::Empty,
                bound_cache: OnceLock::new(),
            },
            in_memory: Some(in_memory),
        }
    }

    pub fn add(&self, value: u64, attributes: &MetricAttributes) {
        match self.primary.metric.with_attributes(attributes) {
            Ok(base) => base.adds(value),
            Err(e) => {
                dbg_panic!("Failed to initialize primary metric, will drop values: {e:?}");
            }
        }

        if let Some(ref in_mem) = self.in_memory {
            in_mem.record_counter(value);
        }
    }

    pub fn update_attributes(&mut self, new_attributes: MetricAttributes) {
        self.primary.update_attributes(new_attributes.clone());
    }
}
impl CounterBase for Counter {
    fn adds(&self, value: u64) {
        // TODO: Replace all of these with below when stable
        //   https://doc.rust-lang.org/std/sync/struct.OnceLock.html#method.get_or_try_init
        let bound = self.primary.bound_cache.get_or_init(|| {
            self.primary
                .metric
                .with_attributes(&self.primary.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize primary metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn CounterBase>
                })
        });
        bound.adds(value);

        if let Some(ref in_mem) = self.in_memory {
            in_mem.record_counter(value);
        }
    }
}
impl MetricAttributable<Counter> for Counter {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Counter, Box<dyn std::error::Error>> {
        let primary = LazyBoundMetric {
            metric: self.primary.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        };

        Ok(Counter {
            primary,
            in_memory: self.in_memory.clone(),
        })
    }
}

pub trait HistogramBase: Send + Sync {
    fn records(&self, value: u64);
}
pub type Histogram = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn HistogramBase>> + Send + Sync>,
    Arc<dyn HistogramBase>,
>;
impl Histogram {
    pub fn new(inner: Arc<dyn MetricAttributable<Box<dyn HistogramBase>> + Send + Sync>) -> Self {
        Self {
            metric: inner,
            attributes: MetricAttributes::Empty,
            bound_cache: OnceLock::new(),
        }
    }
    pub fn record(&self, value: u64, attributes: &MetricAttributes) {
        match self.metric.with_attributes(attributes) {
            Ok(base) => {
                base.records(value);
            }
            Err(e) => {
                dbg_panic!("Failed to initialize metric, will drop values: {e:?}",);
            }
        }
    }
}
impl HistogramBase for Histogram {
    fn records(&self, value: u64) {
        let bound = self.bound_cache.get_or_init(|| {
            self.metric
                .with_attributes(&self.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn HistogramBase>
                })
        });
        bound.records(value);
    }
}
impl MetricAttributable<Histogram> for Histogram {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Histogram, Box<dyn std::error::Error>> {
        Ok(Self {
            metric: self.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        })
    }
}

pub trait HistogramF64Base: Send + Sync {
    fn records(&self, value: f64);
}
pub type HistogramF64 = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn HistogramF64Base>> + Send + Sync>,
    Arc<dyn HistogramF64Base>,
>;
impl HistogramF64 {
    pub fn new(
        inner: Arc<dyn MetricAttributable<Box<dyn HistogramF64Base>> + Send + Sync>,
    ) -> Self {
        Self {
            metric: inner,
            attributes: MetricAttributes::Empty,
            bound_cache: OnceLock::new(),
        }
    }
    pub fn record(&self, value: f64, attributes: &MetricAttributes) {
        match self.metric.with_attributes(attributes) {
            Ok(base) => {
                base.records(value);
            }
            Err(e) => {
                dbg_panic!("Failed to initialize metric, will drop values: {e:?}",);
            }
        }
    }
}
impl HistogramF64Base for HistogramF64 {
    fn records(&self, value: f64) {
        let bound = self.bound_cache.get_or_init(|| {
            self.metric
                .with_attributes(&self.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn HistogramF64Base>
                })
        });
        bound.records(value);
    }
}
impl MetricAttributable<HistogramF64> for HistogramF64 {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<HistogramF64, Box<dyn std::error::Error>> {
        Ok(Self {
            metric: self.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        })
    }
}

pub trait HistogramDurationBase: Send + Sync {
    fn records(&self, value: Duration);
}

pub type HistogramDurationImpl = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn HistogramDurationBase>> + Send + Sync>,
    Arc<dyn HistogramDurationBase>,
>;

#[derive(Clone)]
pub struct HistogramDuration {
    primary: HistogramDurationImpl,
    in_memory: Option<HeartbeatMetricType>,
}
impl HistogramDuration {
    pub fn new(
        inner: Arc<dyn MetricAttributable<Box<dyn HistogramDurationBase>> + Send + Sync>,
    ) -> Self {
        Self {
            primary: LazyBoundMetric {
                metric: inner,
                attributes: MetricAttributes::Empty,
                bound_cache: OnceLock::new(),
            },
            in_memory: None,
        }
    }
    pub fn new_with_in_memory(
        primary: Arc<dyn MetricAttributable<Box<dyn HistogramDurationBase>> + Send + Sync>,
        in_memory: HeartbeatMetricType,
    ) -> Self {
        Self {
            primary: LazyBoundMetric {
                metric: primary,
                attributes: MetricAttributes::Empty,
                bound_cache: OnceLock::new(),
            },
            in_memory: Some(in_memory),
        }
    }
    pub fn record(&self, value: Duration, attributes: &MetricAttributes) {
        match self.primary.metric.with_attributes(attributes) {
            Ok(base) => {
                base.records(value);
            }
            Err(e) => {
                dbg_panic!("Failed to initialize metric, will drop values: {e:?}",);
            }
        }

        if let Some(ref in_mem) = self.in_memory {
            in_mem.record_histogram_observation();
        }
    }

    pub fn update_attributes(&mut self, new_attributes: MetricAttributes) {
        self.primary.update_attributes(new_attributes.clone());
    }
}
impl HistogramDurationBase for HistogramDuration {
    fn records(&self, value: Duration) {
        let bound = self.primary.bound_cache.get_or_init(|| {
            self.primary
                .metric
                .with_attributes(&self.primary.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn HistogramDurationBase>
                })
        });
        bound.records(value);

        if let Some(ref in_mem) = self.in_memory {
            in_mem.record_histogram_observation();
        }
    }
}
impl MetricAttributable<HistogramDuration> for HistogramDuration {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<HistogramDuration, Box<dyn std::error::Error>> {
        let primary = LazyBoundMetric {
            metric: self.primary.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        };

        Ok(HistogramDuration {
            primary,
            in_memory: self.in_memory.clone(),
        })
    }
}

pub trait GaugeBase: Send + Sync {
    fn records(&self, value: u64);
}

pub type GaugeImpl = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn GaugeBase>> + Send + Sync>,
    Arc<dyn GaugeBase>,
>;

#[derive(Clone)]
pub struct Gauge {
    primary: GaugeImpl,
    in_memory: Option<HeartbeatMetricType>,
}
impl Gauge {
    pub fn new(inner: Arc<dyn MetricAttributable<Box<dyn GaugeBase>> + Send + Sync>) -> Self {
        Self {
            primary: LazyBoundMetric {
                metric: inner,
                attributes: MetricAttributes::Empty,
                bound_cache: OnceLock::new(),
            },
            in_memory: None,
        }
    }

    pub fn new_with_in_memory(
        primary: Arc<dyn MetricAttributable<Box<dyn GaugeBase>> + Send + Sync>,
        in_memory: HeartbeatMetricType,
    ) -> Self {
        Self {
            primary: LazyBoundMetric {
                metric: primary,
                attributes: MetricAttributes::Empty,
                bound_cache: OnceLock::new(),
            },
            in_memory: Some(in_memory),
        }
    }

    pub fn record(&self, value: u64, attributes: &MetricAttributes) {
        match self.primary.metric.with_attributes(attributes) {
            Ok(base) => base.records(value),
            Err(e) => {
                dbg_panic!("Failed to initialize primary metric, will drop values: {e:?}");
            }
        }

        if let Some(ref in_mem) = self.in_memory {
            in_mem.record_gauge(value, attributes);
        }
    }

    pub fn update_attributes(&mut self, new_attributes: MetricAttributes) {
        self.primary.update_attributes(new_attributes.clone());
    }
}
impl GaugeBase for Gauge {
    fn records(&self, value: u64) {
        let bound = self.primary.bound_cache.get_or_init(|| {
            self.primary
                .metric
                .with_attributes(&self.primary.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize primary metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn GaugeBase>
                })
        });
        bound.records(value);

        if let Some(ref in_mem) = self.in_memory {
            in_mem.record_gauge(value, &self.primary.attributes);
        }
    }
}
impl MetricAttributable<Gauge> for Gauge {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Gauge, Box<dyn std::error::Error>> {
        let primary = LazyBoundMetric {
            metric: self.primary.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        };

        Ok(Gauge {
            primary,
            in_memory: self.in_memory.clone(),
        })
    }
}

pub trait GaugeF64Base: Send + Sync {
    fn records(&self, value: f64);
}
pub type GaugeF64 = LazyBoundMetric<
    Arc<dyn MetricAttributable<Box<dyn GaugeF64Base>> + Send + Sync>,
    Arc<dyn GaugeF64Base>,
>;
impl GaugeF64 {
    pub fn new(inner: Arc<dyn MetricAttributable<Box<dyn GaugeF64Base>> + Send + Sync>) -> Self {
        Self {
            metric: inner,
            attributes: MetricAttributes::Empty,
            bound_cache: OnceLock::new(),
        }
    }
    pub fn record(&self, value: f64, attributes: &MetricAttributes) {
        match self.metric.with_attributes(attributes) {
            Ok(base) => {
                base.records(value);
            }
            Err(e) => {
                dbg_panic!("Failed to initialize metric, will drop values: {e:?}",);
            }
        }
    }
}
impl GaugeF64Base for GaugeF64 {
    fn records(&self, value: f64) {
        let bound = self.bound_cache.get_or_init(|| {
            self.metric
                .with_attributes(&self.attributes)
                .map(Into::into)
                .unwrap_or_else(|e| {
                    dbg_panic!("Failed to initialize metric, will drop values: {e:?}");
                    Arc::new(NoOpInstrument) as Arc<dyn GaugeF64Base>
                })
        });
        bound.records(value);
    }
}
impl MetricAttributable<GaugeF64> for GaugeF64 {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<GaugeF64, Box<dyn std::error::Error>> {
        Ok(Self {
            metric: self.metric.clone(),
            attributes: attributes.clone(),
            bound_cache: OnceLock::new(),
        })
    }
}

#[derive(Debug)]
pub struct NoOpCoreMeter;
impl CoreMeter for NoOpCoreMeter {
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        MetricAttributes::NoOp(Arc::new(attribs.into()))
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        if let MetricAttributes::NoOp(labels) = existing {
            let mut labels = (*labels).clone();
            labels.extend::<HashMap<String, String>>(attribs.into());
            MetricAttributes::NoOp(Arc::new(labels))
        } else {
            dbg_panic!("Must use NoOp attributes with a NoOp metric implementation");
            existing
        }
    }

    fn counter(&self, _: MetricParameters) -> Counter {
        Counter::new(Arc::new(NoOpInstrument))
    }

    fn histogram(&self, _: MetricParameters) -> Histogram {
        Histogram::new(Arc::new(NoOpInstrument))
    }

    fn histogram_f64(&self, _: MetricParameters) -> HistogramF64 {
        HistogramF64::new(Arc::new(NoOpInstrument))
    }

    fn histogram_duration(&self, _: MetricParameters) -> HistogramDuration {
        HistogramDuration::new(Arc::new(NoOpInstrument))
    }

    fn gauge(&self, _: MetricParameters) -> Gauge {
        Gauge::new(Arc::new(NoOpInstrument))
    }

    fn gauge_f64(&self, _: MetricParameters) -> GaugeF64 {
        GaugeF64::new(Arc::new(NoOpInstrument))
    }
}

macro_rules! impl_metric_attributable {
    ($base_trait:ident, $rt:ty, $init:expr) => {
        impl MetricAttributable<Box<dyn $base_trait>> for $rt {
            fn with_attributes(
                &self,
                _: &MetricAttributes,
            ) -> Result<Box<dyn $base_trait>, Box<dyn std::error::Error>> {
                Ok(Box::new($init))
            }
        }
    };
}

pub struct NoOpInstrument;
macro_rules! impl_no_op {
    ($base_trait:ident, $value_type:ty) => {
        impl_metric_attributable!($base_trait, NoOpInstrument, NoOpInstrument);
        impl $base_trait for NoOpInstrument {
            fn records(&self, _: $value_type) {}
        }
    };
    ($base_trait:ident) => {
        impl_metric_attributable!($base_trait, NoOpInstrument, NoOpInstrument);
        impl $base_trait for NoOpInstrument {
            fn adds(&self, _: u64) {}
        }
    };
}
impl_no_op!(CounterBase);
impl_no_op!(HistogramBase, u64);
impl_no_op!(HistogramF64Base, f64);
impl_no_op!(HistogramDurationBase, Duration);
impl_no_op!(GaugeBase, u64);
impl_no_op!(GaugeF64Base, f64);

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    #[test]
    fn in_memory_attributes_provide_label_values() {
        let meter = NoOpCoreMeter;
        let base_attrs = meter.new_attributes(NewAttributes::default());
        let attrs = meter.extend_attributes(
            base_attrs,
            NewAttributes::from(vec![MetricKeyValue::new("poller_type", "workflow_task")]),
        );

        let value = Arc::new(AtomicU64::new(0));
        let mut metrics = HashMap::new();
        metrics.insert("workflow_task".to_string(), value.clone());
        let heartbeat_metric = HeartbeatMetricType::WithLabel {
            label_key: "poller_type".to_string(),
            metrics,
        };

        heartbeat_metric.record_gauge(3, &attrs);

        assert_eq!(value.load(Ordering::Relaxed), 3);
        assert_eq!(
            label_value_from_attributes(&attrs, "poller_type").as_deref(),
            Some("workflow_task")
        );
    }
}

#[cfg(feature = "otel")]
mod otel {
    use super::*;
    use opentelemetry::{KeyValue, metrics};

    #[derive(Clone)]
    struct InstrumentWithAttributes<I> {
        inner: I,
        attributes: MetricAttributes,
    }

    impl From<MetricKeyValue> for KeyValue {
        fn from(kv: MetricKeyValue) -> Self {
            KeyValue::new(kv.key, kv.value)
        }
    }

    impl From<MetricValue> for opentelemetry::Value {
        fn from(mv: MetricValue) -> Self {
            match mv {
                MetricValue::String(s) => opentelemetry::Value::String(s.into()),
                MetricValue::Int(i) => opentelemetry::Value::I64(i),
                MetricValue::Float(f) => opentelemetry::Value::F64(f),
                MetricValue::Bool(b) => opentelemetry::Value::Bool(b),
            }
        }
    }

    impl MetricAttributable<Box<dyn CounterBase>> for metrics::Counter<u64> {
        fn with_attributes(
            &self,
            attributes: &MetricAttributes,
        ) -> Result<Box<dyn CounterBase>, Box<dyn std::error::Error>> {
            Ok(Box::new(InstrumentWithAttributes {
                inner: self.clone(),
                attributes: attributes.clone(),
            }))
        }
    }

    impl CounterBase for InstrumentWithAttributes<metrics::Counter<u64>> {
        fn adds(&self, value: u64) {
            if let MetricAttributes::OTel { kvs } = &self.attributes {
                self.inner.add(value, kvs);
            } else {
                dbg_panic!("Must use OTel attributes with an OTel metric implementation");
            }
        }
    }

    impl MetricAttributable<Box<dyn GaugeBase>> for metrics::Gauge<u64> {
        fn with_attributes(
            &self,
            attributes: &MetricAttributes,
        ) -> Result<Box<dyn GaugeBase>, Box<dyn std::error::Error>> {
            Ok(Box::new(InstrumentWithAttributes {
                inner: self.clone(),
                attributes: attributes.clone(),
            }))
        }
    }

    impl GaugeBase for InstrumentWithAttributes<metrics::Gauge<u64>> {
        fn records(&self, value: u64) {
            if let MetricAttributes::OTel { kvs } = &self.attributes {
                self.inner.record(value, kvs);
            } else {
                dbg_panic!("Must use OTel attributes with an OTel metric implementation");
            }
        }
    }

    impl MetricAttributable<Box<dyn GaugeF64Base>> for metrics::Gauge<f64> {
        fn with_attributes(
            &self,
            attributes: &MetricAttributes,
        ) -> Result<Box<dyn GaugeF64Base>, Box<dyn std::error::Error>> {
            Ok(Box::new(InstrumentWithAttributes {
                inner: self.clone(),
                attributes: attributes.clone(),
            }))
        }
    }

    impl GaugeF64Base for InstrumentWithAttributes<metrics::Gauge<f64>> {
        fn records(&self, value: f64) {
            if let MetricAttributes::OTel { kvs } = &self.attributes {
                self.inner.record(value, kvs);
            } else {
                dbg_panic!("Must use OTel attributes with an OTel metric implementation");
            }
        }
    }

    impl MetricAttributable<Box<dyn HistogramBase>> for metrics::Histogram<u64> {
        fn with_attributes(
            &self,
            attributes: &MetricAttributes,
        ) -> Result<Box<dyn HistogramBase>, Box<dyn std::error::Error>> {
            Ok(Box::new(InstrumentWithAttributes {
                inner: self.clone(),
                attributes: attributes.clone(),
            }))
        }
    }

    impl HistogramBase for InstrumentWithAttributes<metrics::Histogram<u64>> {
        fn records(&self, value: u64) {
            if let MetricAttributes::OTel { kvs } = &self.attributes {
                self.inner.record(value, kvs);
            } else {
                dbg_panic!("Must use OTel attributes with an OTel metric implementation");
            }
        }
    }

    impl MetricAttributable<Box<dyn HistogramF64Base>> for metrics::Histogram<f64> {
        fn with_attributes(
            &self,
            attributes: &MetricAttributes,
        ) -> Result<Box<dyn HistogramF64Base>, Box<dyn std::error::Error>> {
            Ok(Box::new(InstrumentWithAttributes {
                inner: self.clone(),
                attributes: attributes.clone(),
            }))
        }
    }

    impl HistogramF64Base for InstrumentWithAttributes<metrics::Histogram<f64>> {
        fn records(&self, value: f64) {
            if let MetricAttributes::OTel { kvs } = &self.attributes {
                self.inner.record(value, kvs);
            } else {
                dbg_panic!("Must use OTel attributes with an OTel metric implementation");
            }
        }
    }
}

/// Maintains a mapping of metric labels->values with a defined ordering, used for Prometheus labels
#[derive(Debug, Clone, PartialEq, Default)]
pub struct OrderedPromLabelSet {
    attributes: BTreeMap<String, MetricValue>,
}

impl OrderedPromLabelSet {
    pub const fn new() -> Self {
        Self {
            attributes: BTreeMap::new(),
        }
    }
    pub fn keys_ordered(&self) -> impl Iterator<Item = &str> {
        self.attributes.keys().map(|s| s.as_str())
    }
    pub fn as_prom_labels(&self) -> HashMap<&str, String> {
        let mut labels = HashMap::new();
        for (k, v) in self.attributes.iter() {
            labels.insert(k.as_str(), v.to_string());
        }
        labels
    }
    pub fn add_kv(&mut self, kv: MetricKeyValue) {
        // Replace '-' with '_' per Prom naming requirements
        self.attributes.insert(kv.key.replace('-', "_"), kv.value);
    }
}

impl From<NewAttributes> for OrderedPromLabelSet {
    fn from(n: NewAttributes) -> Self {
        let mut me = Self::default();
        for kv in n.attributes {
            me.add_kv(kv);
        }
        me
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

    fn counter(&self, mut params: MetricParameters) -> Counter {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.counter(params)
    }

    fn histogram(&self, mut params: MetricParameters) -> Histogram {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.histogram(params)
    }

    fn histogram_f64(&self, mut params: MetricParameters) -> HistogramF64 {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.histogram_f64(params)
    }

    fn histogram_duration(&self, mut params: MetricParameters) -> HistogramDuration {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.histogram_duration(params)
    }

    fn gauge(&self, mut params: MetricParameters) -> Gauge {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.gauge(params)
    }

    fn gauge_f64(&self, mut params: MetricParameters) -> GaugeF64 {
        params.name = (self.prefix.clone() + &*params.name).into();
        self.meter.gauge_f64(params)
    }
}
