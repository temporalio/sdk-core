use crate::abstractions::dbg_panic;
use anyhow::anyhow;
use parking_lot::RwLock;
use prometheus::{
    GaugeVec, HistogramVec, IntCounterVec, IntGaugeVec, Opts,
    core::{Collector, Desc, GenericCounter},
    proto::{LabelPair, MetricFamily},
};
use std::{
    collections::{BTreeMap, HashMap, HashSet, btree_map, hash_map},
    fmt::{Debug, Formatter},
    sync::Arc,
    time::Duration,
};
use temporal_sdk_core_api::telemetry::metrics::{
    CoreMeter, Counter, CounterBase, Gauge, GaugeBase, GaugeF64, GaugeF64Base, Histogram,
    HistogramBase, HistogramDuration, HistogramDurationBase, HistogramF64, HistogramF64Base,
    LabelSet, MetricAttributable, MetricAttributes, MetricParameters, NewAttributes,
};

/// Represents the schema of labels for a metric (the set of label names, not their values)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct LabelSchema {
    label_names: Vec<String>,
}

impl LabelSchema {
    fn from_label_set(labels: &LabelSet) -> Self {
        let mut label_names: Vec<String> = labels
            .iter()
            .filter(|(_, v)| !v.is_empty()) // TODO: Shouldn't be necessary (is right now for some)
            .map(|(k, _)| k.to_string())
            .collect();
        label_names.sort();
        Self { label_names }
    }

    fn label_names_ref(&self) -> Vec<&str> {
        self.label_names.iter().map(|s| s.as_str()).collect()
    }
}

#[derive(derive_more::From, Debug, Clone)]
enum PromCollector {
    Histo(HistogramVec),
    Counter(IntCounterVec),
    Gauge(IntGaugeVec),
    GaugeF64(GaugeVec),
}

impl TryFrom<PromCollector> for HistogramVec {
    type Error = ();
    fn try_from(value: PromCollector) -> Result<Self, Self::Error> {
        match value {
            PromCollector::Histo(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl TryFrom<PromCollector> for IntCounterVec {
    type Error = ();
    fn try_from(value: PromCollector) -> Result<Self, Self::Error> {
        match value {
            PromCollector::Counter(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl TryFrom<PromCollector> for IntGaugeVec {
    type Error = ();
    fn try_from(value: PromCollector) -> Result<Self, Self::Error> {
        match value {
            PromCollector::Gauge(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl TryFrom<PromCollector> for GaugeVec {
    type Error = ();
    fn try_from(value: PromCollector) -> Result<Self, Self::Error> {
        match value {
            PromCollector::GaugeF64(v) => Ok(v),
            _ => Err(()),
        }
    }
}

impl Collector for PromCollector {
    fn desc(&self) -> Vec<&Desc> {
        match self {
            PromCollector::Histo(v) => v.desc(),
            PromCollector::Counter(v) => v.desc(),
            PromCollector::Gauge(v) => v.desc(),
            PromCollector::GaugeF64(v) => v.desc(),
        }
    }

    fn collect(&self) -> Vec<MetricFamily> {
        match self {
            PromCollector::Histo(v) => v.collect(),
            PromCollector::Counter(v) => v.collect(),
            PromCollector::Gauge(v) => v.collect(),
            PromCollector::GaugeF64(v) => v.collect(),
        }
    }
}

/// Replaces Prometheus's default registry with a custom one that allows us to register metrics that
/// have different label sets for the same name.
#[derive(Clone)]
pub(super) struct Registry {
    collectors_by_id: Arc<RwLock<HashMap<u64, PromCollector>>>,
    global_tags: BTreeMap<String, String>,
}

// A lot of this implementation code is lifted from the prometheus crate itself, and as such is a
// derivative work of the following:
// Copyright 2014 The Prometheus Authors
// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
impl Registry {
    pub(super) fn new(global_tags: HashMap<String, String>) -> Self {
        Self {
            collectors_by_id: Arc::new(RwLock::new(HashMap::new())),
            global_tags: BTreeMap::from_iter(global_tags),
        }
    }

    fn register<T: Into<PromCollector>>(&self, c: T) -> Option<PromCollector> {
        let mut desc_id_set = HashSet::new();
        let mut collector_id: u64 = 0;
        let c = c.into();

        for desc in c.desc() {
            // If it is not a duplicate desc in this collector, add it to
            // the collector_id. Here we assume that collectors (ie: metric vecs / histograms etc)
            // should internally not repeat the same descriptor -- even though we allow entirely
            // separate metrics with overlapping labels to be registered generally.
            if desc_id_set.insert(desc.id) {
                // Add the id and the dim hash, which includes both static and variable labels
                collector_id = collector_id
                    .wrapping_add(desc.id)
                    .wrapping_add(desc.dim_hash);
            } else {
                dbg_panic!(
                    "Prometheus metric has duplicate descriptors, values may not be recorded on \
                    this metric. This is an SDK bug. Details: {:?}",
                    c.desc(),
                );
                return None;
            }
        }
        match self.collectors_by_id.write().entry(collector_id) {
            hash_map::Entry::Vacant(vc) => {
                vc.insert(c);
                None
            }
            hash_map::Entry::Occupied(o) => Some(o.get().clone()),
        }
    }

    pub(super) fn gather(&self) -> Vec<MetricFamily> {
        let mut mf_by_name = BTreeMap::new();

        for c in self.collectors_by_id.read().values() {
            let mfs = c.collect();
            for mut mf in mfs {
                if mf.get_metric().is_empty() {
                    continue;
                }

                let name = mf.name().to_owned();
                match mf_by_name.entry(name) {
                    btree_map::Entry::Vacant(entry) => {
                        entry.insert(mf);
                    }
                    btree_map::Entry::Occupied(mut entry) => {
                        let existent_mf = entry.get_mut();
                        let existent_metrics = existent_mf.mut_metric();
                        for metric in mf.take_metric().into_iter() {
                            existent_metrics.push(metric);
                        }
                    }
                }
            }
        }

        // Now that MetricFamilies are all set, sort their Metrics
        // lexicographically by their label values.
        for mf in mf_by_name.values_mut() {
            mf.mut_metric().sort_by(|m1, m2| {
                let lps1 = m1.get_label();
                let lps2 = m2.get_label();

                if lps1.len() != lps2.len() {
                    return lps1.len().cmp(&lps2.len());
                }

                for (lp1, lp2) in lps1.iter().zip(lps2.iter()) {
                    if lp1.value() != lp2.value() {
                        return lp1.value().cmp(lp2.value());
                    }
                }

                // We should never arrive here. Multiple metrics with the same
                // label set in the same scrape will lead to undefined ingestion
                // behavior. However, as above, we have to provide stable sorting
                // here, even for inconsistent metrics. So sort equal metrics
                // by their timestamp, with missing timestamps (implying "now")
                // coming last.
                m1.timestamp_ms().cmp(&m2.timestamp_ms())
            });
        }

        mf_by_name
            .into_values()
            .map(|mut m| {
                if self.global_tags.is_empty() {
                    return m;
                }
                // Add global labels
                let pairs: Vec<LabelPair> = self
                    .global_tags
                    .iter()
                    .map(|(k, v)| {
                        let mut label = LabelPair::default();
                        label.set_name(k.to_string());
                        label.set_value(v.to_string());
                        label
                    })
                    .collect();

                for metric in m.mut_metric().iter_mut() {
                    let mut labels: Vec<_> = metric.take_label();
                    labels.append(&mut pairs.clone());
                    metric.set_label(labels);
                }
                m
            })
            .collect()
    }
}

impl Debug for Registry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Registry({} collectors)",
            self.collectors_by_id.read().keys().len()
        )
    }
}

#[derive(Debug)]
struct PromMetric<T> {
    metric_name: String,
    metric_description: String,
    registry: Registry,
    /// Map from label schema to the corresponding Prometheus vector metric
    vectors: RwLock<HashMap<LabelSchema, T>>,
    /// Bucket configuration for histograms (None for other metric types)
    histogram_buckets: Option<Vec<f64>>,
}

impl<T> PromMetric<T>
where
    T: Clone + Into<PromCollector> + TryFrom<PromCollector> + 'static,
{
    fn new(metric_name: String, metric_description: String, registry: Registry) -> Self {
        Self {
            metric_name,
            metric_description,
            registry,
            vectors: RwLock::new(HashMap::new()),
            histogram_buckets: None,
        }
    }

    /// Generic double-checked locking pattern for vector creation
    fn get_or_create_vector<F>(&self, labels: &LabelSet, create_fn: F) -> anyhow::Result<T>
    where
        F: FnOnce(&str, &str, &[&str]) -> T,
    {
        let schema = LabelSchema::from_label_set(labels);

        // Fast path: try to get existing vector
        {
            let vectors = self.vectors.read();
            if let Some(vector) = vectors.get(&schema) {
                return Ok(vector.clone());
            }
        }

        // Slow path: create new vector for this label schema
        let mut vectors = self.vectors.write();
        // Double-check pattern in case another thread created it
        if let Some(vector) = vectors.get(&schema) {
            return Ok(vector.clone());
        }

        let label_names = schema.label_names_ref();
        let description = if self.metric_description.is_empty() {
            &self.metric_name
        } else {
            &self.metric_description
        };

        let vector = create_fn(&self.metric_name, description, &label_names);

        let maybe_exists = self.registry.register(vector.clone());
        let vector = if let Some(m) = maybe_exists {
            T::try_from(m).map_err(|_| {
                anyhow!(
                    "Tried to register a metric that already exists as a different type: {:?}",
                    self.metric_name
                )
            })?
        } else {
            vector
        };

        vectors.insert(schema, vector.clone());
        Ok(vector)
    }
}

impl PromMetric<HistogramVec> {
    fn new_with_buckets(
        metric_name: String,
        metric_description: String,
        registry: Registry,
        buckets: Vec<f64>,
    ) -> Self {
        Self {
            metric_name,
            metric_description,
            registry,
            vectors: RwLock::new(HashMap::new()),
            histogram_buckets: Some(buckets),
        }
    }

    /// Specialized get_or_create_vector for histograms that handles custom buckets
    fn get_or_create_vector_with_buckets(&self, labels: &LabelSet) -> anyhow::Result<HistogramVec> {
        self.get_or_create_vector(labels, |name, desc, label_names| {
            let mut opts = prometheus::HistogramOpts::new(name, desc);
            if let Some(buckets) = &self.histogram_buckets {
                opts = opts.buckets(buckets.clone());
            }
            HistogramVec::new(opts, label_names).unwrap()
        })
    }
}

impl<T> PromMetric<T>
where
    T: Clone + Collector + 'static,
{
    fn extract_prometheus_labels<'a>(
        &self,
        attributes: &'a MetricAttributes,
    ) -> anyhow::Result<(&'a LabelSet, HashMap<&'a str, &'a str>)> {
        if matches!(attributes, MetricAttributes::Empty) {
            return Ok((LabelSet::empty(), HashMap::new()));
        }
        if let MetricAttributes::Prometheus { labels } = attributes {
            let prom_labels = labels.to_prometheus_labels_filtered();
            Ok((labels, prom_labels))
        } else {
            dbg_panic!(
                "Must use Prometheus attributes with a Prometheus metric implementation. Got: {:?}",
                attributes
            );
            Err(anyhow!(
                "Must use Prometheus attributes with a Prometheus metric implementation. Got: {:?}",
                attributes
            ))
        }
    }

    fn label_mismatch_err(
        &self,
        attributes: &MetricAttributes,
        prom_labels: &HashMap<&str, &str>,
    ) -> anyhow::Error {
        dbg_panic!(
            "Mismatch between expected # of prometheus labels and provided. \
            This is an SDK bug. Attributes: {:?} / Labels: {:?}",
            attributes,
            prom_labels
        );
        anyhow!(
            "Mismatch between expected # of prometheus labels and provided. \
            This is an SDK bug. Attributes: {:?} / Labels: {:?}",
            attributes,
            prom_labels
        )
    }
}

struct CorePromCounter(GenericCounter<prometheus::core::AtomicU64>);
impl CounterBase for CorePromCounter {
    fn adds(&self, value: u64) {
        self.0.inc_by(value);
    }
}
impl MetricAttributable<Box<dyn CounterBase>> for PromMetric<IntCounterVec> {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Box<dyn CounterBase>, Box<dyn std::error::Error>> {
        let (labels, prom_labels) = self.extract_prometheus_labels(attributes)?;
        let vector = self.get_or_create_vector(labels, |name, desc, label_names| {
            let opts = Opts::new(name, desc);
            IntCounterVec::new(opts, label_names).unwrap()
        })?;
        if let Ok(c) = vector.get_metric_with(&prom_labels) {
            Ok(Box::new(CorePromCounter(c)))
        } else {
            Err(self.label_mismatch_err(attributes, &prom_labels).into())
        }
    }
}

struct CorePromIntGauge(prometheus::IntGauge);
impl GaugeBase for CorePromIntGauge {
    fn records(&self, value: u64) {
        self.0.set(value as i64);
    }
}
impl MetricAttributable<Box<dyn GaugeBase>> for PromMetric<IntGaugeVec> {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Box<dyn GaugeBase>, Box<dyn std::error::Error>> {
        let (labels, prom_labels) = self.extract_prometheus_labels(attributes)?;
        let vector = self.get_or_create_vector(labels, |name, desc, label_names| {
            let opts = Opts::new(name, desc);
            IntGaugeVec::new(opts, label_names).unwrap()
        })?;
        if let Ok(g) = vector.get_metric_with(&prom_labels) {
            Ok(Box::new(CorePromIntGauge(g)))
        } else {
            Err(self.label_mismatch_err(attributes, &prom_labels).into())
        }
    }
}

struct CorePromGauge(prometheus::Gauge);
impl GaugeF64Base for CorePromGauge {
    fn records(&self, value: f64) {
        self.0.set(value);
    }
}
impl MetricAttributable<Box<dyn GaugeF64Base>> for PromMetric<GaugeVec> {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Box<dyn GaugeF64Base>, Box<dyn std::error::Error>> {
        let (labels, prom_labels) = self.extract_prometheus_labels(attributes)?;
        let vector = self.get_or_create_vector(labels, |name, desc, label_names| {
            let opts = Opts::new(name, desc);
            GaugeVec::new(opts, label_names).unwrap()
        })?;
        if let Ok(g) = vector.get_metric_with(&prom_labels) {
            Ok(Box::new(CorePromGauge(g)))
        } else {
            Err(self.label_mismatch_err(attributes, &prom_labels).into())
        }
    }
}

#[derive(Clone)]
struct CorePromHistogram(prometheus::Histogram);
impl HistogramBase for CorePromHistogram {
    fn records(&self, value: u64) {
        self.0.observe(value as f64);
    }
}
impl HistogramF64Base for CorePromHistogram {
    fn records(&self, value: f64) {
        self.0.observe(value);
    }
}

#[derive(Debug)]
struct PromHistogramU64(PromMetric<HistogramVec>);
impl MetricAttributable<Box<dyn HistogramBase>> for PromHistogramU64 {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Box<dyn HistogramBase>, Box<dyn std::error::Error>> {
        let (labels, prom_labels) = self.0.extract_prometheus_labels(attributes)?;
        let vector = self.0.get_or_create_vector_with_buckets(labels)?;
        if let Ok(h) = vector.get_metric_with(&prom_labels) {
            Ok(Box::new(CorePromHistogram(h)))
        } else {
            Err(self.0.label_mismatch_err(attributes, &prom_labels).into())
        }
    }
}

#[derive(Debug)]
struct PromHistogramF64(PromMetric<HistogramVec>);
impl MetricAttributable<Box<dyn HistogramF64Base>> for PromHistogramF64 {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Box<dyn HistogramF64Base>, Box<dyn std::error::Error>> {
        let (labels, prom_labels) = self.0.extract_prometheus_labels(attributes)?;
        let vector = self.0.get_or_create_vector_with_buckets(labels)?;
        if let Ok(h) = vector.get_metric_with(&prom_labels) {
            Ok(Box::new(CorePromHistogram(h)))
        } else {
            Err(self.0.label_mismatch_err(attributes, &prom_labels).into())
        }
    }
}

/// A CoreMeter implementation backed by Prometheus metrics with dynamic label management
#[derive(Debug)]
pub struct CorePrometheusMeter {
    registry: Registry,
    use_seconds_for_durations: bool,
    unit_suffix: bool,
    bucket_overrides: temporal_sdk_core_api::telemetry::HistogramBucketOverrides,
}

impl CorePrometheusMeter {
    pub(super) fn new(
        registry: Registry,
        use_seconds_for_durations: bool,
        unit_suffix: bool,
        bucket_overrides: temporal_sdk_core_api::telemetry::HistogramBucketOverrides,
    ) -> Self {
        Self {
            registry,
            use_seconds_for_durations,
            unit_suffix,
            bucket_overrides,
        }
    }

    fn create_u64_hist(&self, params: &MetricParameters) -> PromHistogramU64 {
        let base_name = params.name.to_string();
        let actual_metric_name = self.get_histogram_metric_name(&base_name, &params.unit);
        let buckets = self.get_buckets_for_metric(&base_name);
        PromHistogramU64(PromMetric::new_with_buckets(
            actual_metric_name,
            params.description.to_string(),
            self.registry.clone(),
            buckets,
        ))
    }

    fn create_f64_hist(&self, params: &MetricParameters) -> PromHistogramF64 {
        let base_name = params.name.to_string();
        let actual_metric_name = self.get_histogram_metric_name(&base_name, &params.unit);
        let buckets = self.get_buckets_for_metric(&base_name);
        PromHistogramF64(PromMetric::new_with_buckets(
            actual_metric_name,
            params.description.to_string(),
            self.registry.clone(),
            buckets,
        ))
    }
}

impl CoreMeter for CorePrometheusMeter {
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        MetricAttributes::Prometheus {
            labels: Arc::new(LabelSet::from(attribs.attributes)),
        }
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        if let MetricAttributes::Prometheus {
            labels: existing_labels,
        } = existing
        {
            // Merge existing labels with new ones
            let mut all_labels = Vec::new();

            // Add existing labels
            for (k, v) in existing_labels.iter() {
                all_labels.push((k.to_string(), v.to_string()));
            }

            // Add new labels (potentially overriding existing ones)
            let new_label_set: LabelSet = attribs.attributes.into();
            for (k, v) in new_label_set.iter() {
                // Remove any existing entry with the same key
                all_labels.retain(|(existing_k, _)| existing_k != k);
                all_labels.push((k.to_string(), v.to_string()));
            }

            MetricAttributes::Prometheus {
                labels: Arc::new(LabelSet::new(all_labels)),
            }
        } else {
            dbg_panic!("Must use Prometheus attributes with a Prometheus metric implementation");
            self.new_attributes(attribs)
        }
    }

    fn counter(&self, params: MetricParameters) -> Counter {
        let metric_name = params.name.to_string();
        Counter::new(Arc::new(PromMetric::<IntCounterVec>::new(
            metric_name,
            params.description.to_string(),
            self.registry.clone(),
        )))
    }

    fn histogram(&self, params: MetricParameters) -> Histogram {
        let hist = self.create_u64_hist(&params);
        Histogram::new(Arc::new(hist))
    }

    fn histogram_f64(&self, params: MetricParameters) -> HistogramF64 {
        let hist = self.create_f64_hist(&params);
        HistogramF64::new(Arc::new(hist))
    }

    fn histogram_duration(&self, mut params: MetricParameters) -> HistogramDuration {
        HistogramDuration::new(Arc::new(if self.use_seconds_for_durations {
            params.unit = "seconds".into();
            DurationHistogram::Seconds(self.create_f64_hist(&params))
        } else {
            params.unit = "milliseconds".into();
            DurationHistogram::Milliseconds(self.create_u64_hist(&params))
        }))
    }

    fn gauge(&self, params: MetricParameters) -> Gauge {
        let metric_name = params.name.to_string();
        Gauge::new(Arc::new(PromMetric::<IntGaugeVec>::new(
            metric_name,
            params.description.to_string(),
            self.registry.clone(),
        )))
    }

    fn gauge_f64(&self, params: MetricParameters) -> GaugeF64 {
        let metric_name = params.name.to_string();
        GaugeF64::new(Arc::new(PromMetric::<GaugeVec>::new(
            metric_name,
            params.description.to_string(),
            self.registry.clone(),
        )))
    }
}

impl CorePrometheusMeter {
    /// Get bucket configuration for a histogram metric
    fn get_buckets_for_metric(&self, metric_name: &str) -> Vec<f64> {
        // Check for specific override first
        for (name_pattern, buckets) in &self.bucket_overrides.overrides {
            if metric_name.contains(name_pattern) {
                return buckets.clone();
            }
        }

        // Strip the temporal prefix if present to match against base metric names
        let base_metric_name = metric_name.strip_prefix("temporal_").unwrap_or(metric_name);

        // Use default buckets for temporal metrics
        crate::telemetry::default_buckets_for(base_metric_name, self.use_seconds_for_durations)
            .to_vec()
    }

    /// Get the appropriate metric name with unit suffix if configured
    fn get_histogram_metric_name(&self, base_name: &str, unit: &str) -> String {
        if self.unit_suffix && !unit.is_empty() {
            format!("{}_{}", base_name, unit)
        } else {
            base_name.to_string()
        }
    }
}

enum DurationHistogram {
    Milliseconds(PromHistogramU64),
    Seconds(PromHistogramF64),
}

enum DurationHistogramBase {
    Millis(Box<dyn HistogramBase>),
    Secs(Box<dyn HistogramF64Base>),
}

impl HistogramDurationBase for DurationHistogramBase {
    fn records(&self, value: Duration) {
        match self {
            DurationHistogramBase::Millis(h) => h.records(value.as_millis() as u64),
            DurationHistogramBase::Secs(h) => h.records(value.as_secs_f64()),
        }
    }
}
impl MetricAttributable<Box<dyn HistogramDurationBase>> for DurationHistogram {
    fn with_attributes(
        &self,
        attributes: &MetricAttributes,
    ) -> Result<Box<dyn HistogramDurationBase>, Box<dyn std::error::Error>> {
        Ok(match self {
            DurationHistogram::Milliseconds(h) => Box::new(DurationHistogramBase::Millis(
                h.with_attributes(attributes)?,
            )),
            DurationHistogram::Seconds(h) => {
                Box::new(DurationHistogramBase::Secs(h.with_attributes(attributes)?))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::telemetry::{TelemetryInstance, metrics::MetricsContext};
    use prometheus::{Encoder, TextEncoder};
    use temporal_sdk_core_api::telemetry::{
        METRIC_PREFIX,
        metrics::{MetricKeyValue, NewAttributes},
    };

    #[test]
    fn test_prometheus_meter_dynamic_labels() {
        let registry = Registry::new(HashMap::from([("global".to_string(), "value".to_string())]));
        let meter = CorePrometheusMeter::new(
            registry.clone(),
            false,
            false,
            temporal_sdk_core_api::telemetry::HistogramBucketOverrides::default(),
        );

        let counter = meter.counter(MetricParameters {
            name: "test_counter".into(),
            description: "A test counter metric".into(),
            unit: "".into(),
        });

        let attrs1 = meter.new_attributes(NewAttributes::new(vec![
            MetricKeyValue::new("service", "service1"),
            MetricKeyValue::new("method", "get"),
        ]));
        counter.add(5, &attrs1);

        let attrs2 = meter.new_attributes(NewAttributes::new(vec![
            MetricKeyValue::new("service", "service2"),
            MetricKeyValue::new("method", "post"),
        ]));
        counter.add(3, &attrs2);

        let output = output_string(&registry);

        // Both label combinations should be present
        assert!(
            output.contains("test_counter{method=\"get\",service=\"service1\",global=\"value\"} 5")
        );
        assert!(
            output
                .contains("test_counter{method=\"post\",service=\"service2\",global=\"value\"} 3")
        );
    }

    #[test]
    fn test_extend_attributes() {
        let registry = Registry::new(HashMap::new());
        let meter = CorePrometheusMeter::new(
            registry.clone(),
            false,
            false,
            temporal_sdk_core_api::telemetry::HistogramBucketOverrides::default(),
        );

        let base_attrs = meter.new_attributes(NewAttributes::new(vec![
            MetricKeyValue::new("service", "my_service"),
            MetricKeyValue::new("version", "1.0"),
        ]));

        let extended_attrs = meter.extend_attributes(
            base_attrs,
            NewAttributes::new(vec![
                MetricKeyValue::new("method", "GET"),
                MetricKeyValue::new("version", "2.0"), // This should override
            ]),
        );

        let counter = meter.counter(MetricParameters {
            name: "test_extended".into(),
            description: "Test extended attributes".into(),
            unit: "".into(),
        });
        counter.add(1, &extended_attrs);

        let output = output_string(&registry);

        assert!(output.contains("service=\"my_service\""));
        assert!(output.contains("method=\"GET\""));
        assert!(output.contains("version=\"2.0\""));
        assert!(!output.contains("version=\"1.0\""));
    }

    #[test]
    fn test_workflow_e2e_latency_buckets() {
        let registry = Registry::new(HashMap::new());

        let meter_ms = CorePrometheusMeter::new(
            registry.clone(),
            false,
            false,
            temporal_sdk_core_api::telemetry::HistogramBucketOverrides::default(),
        );

        let histogram_ms = meter_ms.histogram_duration(MetricParameters {
            name: format!(
                "temporal_{}",
                crate::telemetry::WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME
            )
            .into(),
            description: "Test workflow e2e latency".into(),
            unit: "duration".into(),
        });
        let attrs = meter_ms.new_attributes(NewAttributes::new(vec![]));
        histogram_ms.record(Duration::from_millis(100), &attrs);

        let output = output_string(&registry);

        println!("Milliseconds histogram output:\n{}", output);

        assert!(
            output.contains("le=\"100\""),
            "Missing le=\"100\" bucket in milliseconds output"
        );

        // Test seconds configuration
        let registry_s = Registry::new(HashMap::new());
        let meter_s = CorePrometheusMeter::new(
            registry_s.clone(),
            true,
            false,
            temporal_sdk_core_api::telemetry::HistogramBucketOverrides::default(),
        );

        let histogram_s = meter_s.histogram_duration(MetricParameters {
            name: format!(
                "temporal_{}",
                crate::telemetry::WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME
            )
            .into(),
            description: "Test workflow e2e latency".into(),
            unit: "duration".into(),
        });
        let attrs_s = meter_s.new_attributes(NewAttributes::new(vec![]));
        histogram_s.record(Duration::from_millis(100), &attrs_s);

        let output_s = output_string(&registry_s);

        println!("Seconds histogram output:\n{}", output_s);

        assert!(
            output_s.contains("le=\"0.1\""),
            "Missing le=\"0.1\" bucket in seconds output"
        );
    }

    #[test]
    fn can_record_with_no_labels() {
        let registry = Registry::new(HashMap::new());
        let meter = CorePrometheusMeter::new(
            registry.clone(),
            false,
            false,
            temporal_sdk_core_api::telemetry::HistogramBucketOverrides::default(),
        );
        let counter = meter.counter(MetricParameters {
            name: "no_labels".into(),
            description: "No labels".into(),
            unit: "".into(),
        });
        counter.adds(1);

        let output = output_string(&registry);

        assert!(output.contains("no_labels 1"));
    }

    #[test]
    fn works_with_recreated_metrics_context() {
        let registry = Registry::new(HashMap::new());
        let meter = CorePrometheusMeter::new(
            registry.clone(),
            false,
            false,
            temporal_sdk_core_api::telemetry::HistogramBucketOverrides::default(),
        );
        let telem_instance = TelemetryInstance::new(
            None,
            None,
            METRIC_PREFIX.to_string(),
            Some(Arc::new(meter)),
            true,
        );
        let mc = MetricsContext::top_level("foo".to_string(), "q".to_string(), &telem_instance);
        mc.worker_registered();
        drop(mc);

        let mc = MetricsContext::top_level("foo".to_string(), "q".to_string(), &telem_instance);
        mc.worker_registered();

        let mc = MetricsContext::top_level("foo".to_string(), "q2".to_string(), &telem_instance);
        mc.worker_registered();

        let output = output_string(&registry);
        assert!(output.contains("temporal_worker_start{namespace=\"foo\",service_name=\"temporal-core-sdk\",task_queue=\"q\"} 2"));
        assert!(output.contains("temporal_worker_start{namespace=\"foo\",service_name=\"temporal-core-sdk\",task_queue=\"q2\"} 1"));
    }

    fn output_string(registry: &Registry) -> String {
        let metric_families = registry.gather();
        let encoder = TextEncoder::new();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}
