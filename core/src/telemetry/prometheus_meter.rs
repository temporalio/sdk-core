use crate::abstractions::dbg_panic;
use parking_lot::RwLock;
use prometheus::{CounterVec, GaugeVec, HistogramVec, Opts, Registry};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use temporal_sdk_core_api::telemetry::metrics::{
    CoreMeter, Counter, Gauge, GaugeF64, Histogram, HistogramDuration, HistogramF64, LabelSet,
    MetricAttributes, MetricParameters, NewAttributes,
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
            .filter(|(_, v)| !v.is_empty()) // Only include non-empty labels
            .map(|(k, _)| k.to_string())
            .collect();
        label_names.sort(); // Ensure consistent ordering
        Self { label_names }
    }

    fn label_names_ref(&self) -> Vec<&str> {
        self.label_names.iter().map(|s| s.as_str()).collect()
    }
}

/// A generic Prometheus metric wrapper that manages different vector instances for different label
/// schemas
#[derive(Debug)]
struct PromMetric<T> {
    metric_name: String,
    metric_description: String,
    registry: Registry,
    // Map from label schema to the corresponding Prometheus vector metric
    vectors: RwLock<HashMap<LabelSchema, T>>,
    // Bucket configuration for histograms (None for other metric types)
    histogram_buckets: Option<Vec<f64>>,
    registered: AtomicBool,
}

impl<T> PromMetric<T>
where
    T: Clone + prometheus::core::Collector + 'static,
{
    fn new(metric_name: String, metric_description: String, registry: Registry) -> Self {
        Self {
            metric_name,
            metric_description,
            registry,
            vectors: RwLock::new(HashMap::new()),
            histogram_buckets: None,
            registered: AtomicBool::new(false),
        }
    }

    /// Generic double-checked locking pattern for vector creation
    fn get_or_create_vector<F>(&self, labels: &LabelSet, create_fn: F) -> T
    where
        F: FnOnce(&str, &str, &[&str]) -> T,
    {
        let schema = LabelSchema::from_label_set(labels);

        // Fast path: try to get existing vector
        {
            let vectors = self.vectors.read();
            if let Some(vector) = vectors.get(&schema) {
                return vector.clone();
            }
        }

        // Slow path: create new vector for this label schema
        let mut vectors = self.vectors.write();
        // Double-check pattern in case another thread created it
        if let Some(vector) = vectors.get(&schema) {
            return vector.clone();
        }

        let label_names = schema.label_names_ref();
        let description = if self.metric_description.is_empty() {
            &self.metric_name
        } else {
            &self.metric_description
        };

        let vector = create_fn(&self.metric_name, description, &label_names);

        if !self.registered.load(Ordering::Relaxed) {
            self.registry
                .register(Box::new(vector.clone()))
                .expect("registration works");
            self.registered.store(true, Ordering::Relaxed);
        }

        vectors.insert(schema, vector.clone());
        vector
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
            registered: AtomicBool::new(false),
        }
    }

    /// Specialized get_or_create_vector for histograms that handles custom buckets
    fn get_or_create_vector_with_buckets(&self, labels: &LabelSet) -> HistogramVec {
        self.get_or_create_vector(labels, |name, desc, label_names| {
            let mut opts = prometheus::HistogramOpts::new(name, desc);
            if let Some(buckets) = &self.histogram_buckets {
                opts = opts.buckets(buckets.clone());
            }
            HistogramVec::new(opts, label_names).unwrap()
        })
    }
}

// Trait implementations for specific metric types
impl Counter for PromMetric<CounterVec> {
    fn add(&self, value: u64, attributes: &MetricAttributes) {
        if let MetricAttributes::Prometheus { labels } = attributes {
            let vector = self.get_or_create_vector(labels, |name, desc, label_names| {
                let opts = Opts::new(name, desc);
                CounterVec::new(opts, label_names).unwrap()
            });
            let prom_labels = labels.to_prometheus_labels_filtered();
            let counter = vector.with(&prom_labels);
            counter.inc_by(value as f64);
        } else {
            debug_assert!(
                false,
                "Must use Prometheus attributes with a Prometheus metric implementation. Got: {:?}",
                attributes
            );
        }
    }
}

impl Gauge for PromMetric<GaugeVec> {
    fn record(&self, value: u64, attributes: &MetricAttributes) {
        if let MetricAttributes::Prometheus { labels } = attributes {
            let vector = self.get_or_create_vector(labels, |name, desc, label_names| {
                let opts = Opts::new(name, desc);
                GaugeVec::new(opts, label_names).unwrap()
            });
            let prom_labels = labels.to_prometheus_labels_filtered();
            let gauge = vector.with(&prom_labels);
            gauge.set(value as f64);
        } else {
            debug_assert!(
                false,
                "Must use Prometheus attributes with a Prometheus metric implementation. Got: {:?}",
                attributes
            );
        }
    }
}

impl GaugeF64 for PromMetric<GaugeVec> {
    fn record(&self, value: f64, attributes: &MetricAttributes) {
        if let MetricAttributes::Prometheus { labels } = attributes {
            let vector = self.get_or_create_vector(labels, |name, desc, label_names| {
                let opts = Opts::new(name, desc);
                GaugeVec::new(opts, label_names).unwrap()
            });
            let prom_labels = labels.to_prometheus_labels_filtered();
            let gauge = vector.with(&prom_labels);
            gauge.set(value);
        } else {
            debug_assert!(
                false,
                "Must use Prometheus attributes with a Prometheus metric implementation. Got: {:?}",
                attributes
            );
        }
    }
}

// Trait implementations for Histogram
impl Histogram for PromMetric<HistogramVec> {
    fn record(&self, value: u64, attributes: &MetricAttributes) {
        if let MetricAttributes::Prometheus { labels } = attributes {
            let vector = self.get_or_create_vector_with_buckets(labels);
            let prom_labels = labels.to_prometheus_labels_filtered();
            let histogram = vector.with(&prom_labels);
            histogram.observe(value as f64);
        } else {
            debug_assert!(
                false,
                "Must use Prometheus attributes with a Prometheus metric implementation. Got: {:?}",
                attributes
            );
        }
    }
}

impl HistogramF64 for PromMetric<HistogramVec> {
    fn record(&self, value: f64, attributes: &MetricAttributes) {
        if let MetricAttributes::Prometheus { labels } = attributes {
            let vector = self.get_or_create_vector_with_buckets(labels);
            let prom_labels = labels.to_prometheus_labels_filtered();
            let histogram = vector.with(&prom_labels);
            histogram.observe(value);
        } else {
            debug_assert!(
                false,
                "Must use Prometheus attributes with a Prometheus metric implementation. Got: {:?}",
                attributes
            );
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
    // Cache to avoid registering the same metric multiple times
    counters: RwLock<HashMap<String, Arc<PromMetric<CounterVec>>>>,
    histograms: RwLock<HashMap<String, Arc<PromMetric<HistogramVec>>>>,
    gauges: RwLock<HashMap<String, Arc<PromMetric<GaugeVec>>>>,
}

impl CorePrometheusMeter {
    pub fn new(
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
            counters: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
        }
    }

    /// Generic helper to get or create any metric with double-checked locking pattern
    fn get_or_create_metric<T, F>(
        &self,
        cache: &RwLock<HashMap<String, Arc<PromMetric<T>>>>,
        metric_name: String,
        create_fn: F,
    ) -> Arc<PromMetric<T>>
    where
        F: FnOnce() -> PromMetric<T>,
    {
        // Fast path: try to get existing metric
        {
            let cache_read = cache.read();
            if let Some(metric) = cache_read.get(&metric_name) {
                return metric.clone();
            }
        }

        // Slow path: create new metric
        let mut cache_write = cache.write();
        // Double-check pattern in case another thread created it
        if let Some(metric) = cache_write.get(&metric_name) {
            return metric.clone();
        }

        let metric = Arc::new(create_fn());
        cache_write.insert(metric_name, metric.clone());
        metric
    }
}

impl CoreMeter for CorePrometheusMeter {
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        MetricAttributes::Prometheus {
            labels: LabelSet::from(attribs.attributes),
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
                labels: LabelSet::new(all_labels),
            }
        } else {
            dbg_panic!("Must use Prometheus attributes with a Prometheus metric implementation");
            self.new_attributes(attribs)
        }
    }

    fn counter(&self, params: MetricParameters) -> Arc<dyn Counter> {
        self.get_or_create_counter(params)
    }

    fn histogram(&self, params: MetricParameters) -> Arc<dyn Histogram> {
        let base_name = params.name.to_string();
        let actual_metric_name = self.get_histogram_metric_name(&base_name, &params.unit);
        let buckets = self.get_buckets_for_metric(&base_name);

        self.get_or_create_histogram(params, actual_metric_name, buckets)
    }

    fn histogram_f64(&self, params: MetricParameters) -> Arc<dyn HistogramF64> {
        let base_name = params.name.to_string();
        let actual_metric_name = self.get_histogram_metric_name(&base_name, &params.unit);
        let buckets = self.get_buckets_for_metric(&base_name);

        self.get_or_create_histogram(params, actual_metric_name, buckets)
    }

    fn histogram_duration(&self, mut params: MetricParameters) -> Arc<dyn HistogramDuration> {
        Arc::new(if self.use_seconds_for_durations {
            params.unit = "seconds".into();
            DurationHistogram::Seconds(self.histogram_f64(params))
        } else {
            params.unit = "milliseconds".into();
            DurationHistogram::Milliseconds(self.histogram(params))
        })
    }

    fn gauge(&self, params: MetricParameters) -> Arc<dyn Gauge> {
        self.get_or_create_gauge(params)
    }

    fn gauge_f64(&self, params: MetricParameters) -> Arc<dyn GaugeF64> {
        self.get_or_create_gauge(params)
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

    /// Get or create a counter metric
    fn get_or_create_counter(&self, params: MetricParameters) -> Arc<PromMetric<CounterVec>> {
        let metric_name = params.name.to_string();
        self.get_or_create_metric(&self.counters, metric_name.clone(), || {
            PromMetric::new(
                metric_name,
                params.description.to_string(),
                self.registry.clone(),
            )
        })
    }

    /// Get or create a gauge metric
    fn get_or_create_gauge(&self, params: MetricParameters) -> Arc<PromMetric<GaugeVec>> {
        let metric_name = params.name.to_string();
        self.get_or_create_metric(&self.gauges, metric_name.clone(), || {
            PromMetric::new(
                metric_name,
                params.description.to_string(),
                self.registry.clone(),
            )
        })
    }

    /// Get or create a histogram metric
    fn get_or_create_histogram(
        &self,
        params: MetricParameters,
        actual_metric_name: String,
        buckets: Vec<f64>,
    ) -> Arc<PromMetric<HistogramVec>> {
        self.get_or_create_metric(&self.histograms, actual_metric_name.clone(), || {
            PromMetric::new_with_buckets(
                actual_metric_name,
                params.description.to_string(),
                self.registry.clone(),
                buckets,
            )
        })
    }
}

/// A histogram being used to record durations.
#[derive(Clone)]
enum DurationHistogram {
    Milliseconds(Arc<dyn Histogram>),
    Seconds(Arc<dyn HistogramF64>),
}

impl HistogramDuration for DurationHistogram {
    fn record(&self, value: Duration, attributes: &MetricAttributes) {
        match self {
            DurationHistogram::Milliseconds(h) => h.record(value.as_millis() as u64, attributes),
            DurationHistogram::Seconds(h) => h.record(value.as_secs_f64(), attributes),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus::{Encoder, TextEncoder};
    use temporal_sdk_core_api::telemetry::metrics::{MetricKeyValue, NewAttributes};

    #[test]
    fn test_prometheus_meter_dynamic_labels() {
        let registry = Registry::new();
        let meter = CorePrometheusMeter::new(
            registry.clone(),
            false,
            false,
            temporal_sdk_core_api::telemetry::HistogramBucketOverrides::default(),
        );

        // Test counter with different label sets
        let counter = meter.counter(MetricParameters {
            name: "test_counter".into(),
            description: "A test counter metric".into(),
            unit: "".into(),
        });

        // First label set
        let attrs1 = meter.new_attributes(NewAttributes::new(vec![
            MetricKeyValue::new("service", "service1"),
            MetricKeyValue::new("method", "get"),
        ]));
        counter.add(5, &attrs1);

        // Second label set with different labels
        let attrs2 = meter.new_attributes(NewAttributes::new(vec![
            MetricKeyValue::new("service", "service2"),
            MetricKeyValue::new("method", "post"),
        ]));
        counter.add(3, &attrs2);

        // Verify metrics are recorded correctly
        let metric_families = registry.gather();
        let encoder = TextEncoder::new();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).unwrap();
        let output = String::from_utf8(buffer).unwrap();

        println!("Prometheus output:\n{}", output);

        assert!(output.contains("test_counter"));
        // Both label combinations should be present
        assert!(output.contains("service=\"service1\""));
        assert!(output.contains("service=\"service2\""));
        assert!(output.contains("method=\"get\""));
        assert!(output.contains("method=\"post\""));
    }

    #[test]
    fn test_label_schema_efficiency() {
        // Test that LabelSchema provides efficient comparison and hashing
        let labels1 = LabelSet::new(vec![
            ("b".to_string(), "2".to_string()),
            ("a".to_string(), "1".to_string()),
        ]);
        let labels2 = LabelSet::new(vec![
            ("a".to_string(), "1".to_string()),
            ("b".to_string(), "2".to_string()),
        ]);

        let schema1 = LabelSchema::from_label_set(&labels1);
        let schema2 = LabelSchema::from_label_set(&labels2);

        // Should be equal despite different insertion order
        assert_eq!(schema1, schema2);

        // Should be efficient for HashMap usage
        let mut map = HashMap::new();
        map.insert(schema1.clone(), "value1");
        assert_eq!(map.get(&schema2), Some(&"value1"));
    }

    #[test]
    fn test_histogram_buckets() {
        let registry = Registry::new();
        let meter = CorePrometheusMeter::new(
            registry.clone(),
            false, // use_seconds_for_durations = false (milliseconds)
            true,  // unit_suffix = true
            temporal_sdk_core_api::telemetry::HistogramBucketOverrides::default(),
        );

        // Test workflow e2e latency histogram - should get default buckets
        let histogram = meter.histogram_duration(MetricParameters {
            name: crate::telemetry::WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME.into(),
            description: "Test workflow e2e latency".into(),
            unit: "duration".into(),
        });

        // Record a value to ensure the histogram is created
        let attrs = meter.new_attributes(NewAttributes::new(vec![]));
        histogram.record(Duration::from_millis(100), &attrs);

        // Check the prometheus output
        let metric_families = registry.gather();
        let encoder = prometheus::TextEncoder::new();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).unwrap();
        let output = String::from_utf8(buffer).unwrap();

        println!("Histogram output:\n{}", output);

        // Should contain the bucket le="100" for milliseconds
        assert!(output.contains("le=\"100\""));
    }

    #[test]
    fn test_extend_attributes() {
        let registry = Registry::new();
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

        // Test that the extended attributes work correctly
        let counter = meter.counter(MetricParameters {
            name: "test_extended".into(),
            description: "Test extended attributes".into(),
            unit: "".into(),
        });
        counter.add(1, &extended_attrs);

        let metric_families = registry.gather();
        let encoder = TextEncoder::new();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).unwrap();
        let output = String::from_utf8(buffer).unwrap();

        assert!(output.contains("service=\"my_service\""));
        assert!(output.contains("method=\"GET\""));
        assert!(output.contains("version=\"2.0\"")); // Should have new value
        assert!(!output.contains("version=\"1.0\"")); // Should not have old value
    }

    #[test]
    fn test_workflow_e2e_latency_buckets() {
        // Test that default buckets are correctly applied to workflow E2E latency histogram
        let registry = Registry::new();

        // Test milliseconds configuration
        let meter_ms = CorePrometheusMeter::new(
            registry.clone(),
            false, // use_seconds_for_durations = false (milliseconds)
            false, // unit_suffix = false
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

        // Record a value to ensure the histogram is created
        let attrs = meter_ms.new_attributes(NewAttributes::new(vec![]));
        histogram_ms.record(Duration::from_millis(100), &attrs);

        // Check the prometheus output for milliseconds
        let metric_families = registry.gather();
        let encoder = prometheus::TextEncoder::new();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).unwrap();
        let output = String::from_utf8(buffer).unwrap();

        println!("Milliseconds histogram output:\n{}", output);

        // Should contain the bucket le="100" for milliseconds
        assert!(
            output.contains("le=\"100\""),
            "Missing le=\"100\" bucket in milliseconds output"
        );

        // Test seconds configuration
        let registry_s = Registry::new();
        let meter_s = CorePrometheusMeter::new(
            registry_s.clone(),
            true,  // use_seconds_for_durations = true (seconds)
            false, // unit_suffix = false
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

        // Record a value to ensure the histogram is created
        let attrs_s = meter_s.new_attributes(NewAttributes::new(vec![]));
        histogram_s.record(Duration::from_millis(100), &attrs_s);

        // Check the prometheus output for seconds
        let metric_families_s = registry_s.gather();
        let mut buffer_s = vec![];
        encoder.encode(&metric_families_s, &mut buffer_s).unwrap();
        let output_s = String::from_utf8(buffer_s).unwrap();

        println!("Seconds histogram output:\n{}", output_s);

        // Should contain the bucket le="0.1" for seconds (100ms = 0.1s)
        assert!(
            output_s.contains("le=\"0.1\""),
            "Missing le=\"0.1\" bucket in seconds output"
        );
    }
}
