use crate::abstractions::dbg_panic;
use prometheus::{CounterVec, GaugeVec, HistogramVec, Opts, Registry};
use std::{sync::Arc, time::Duration};
use temporal_sdk_core_api::telemetry::metrics::{
    CoreMeter, Counter, Gauge, GaugeF64, Histogram, HistogramDuration, HistogramF64,
    MetricAttributes, MetricParameters, MetricValue, NewAttributes,
};

/// A CoreMeter implementation backed by Prometheus metrics
#[derive(Debug)]
pub struct CorePrometheusMeter {
    registry: Registry,
    use_seconds_for_durations: bool,
}

impl CorePrometheusMeter {
    pub fn new(registry: Registry, use_seconds_for_durations: bool) -> Self {
        Self {
            registry,
            use_seconds_for_durations,
        }
    }
}

impl CoreMeter for CorePrometheusMeter {
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        MetricAttributes::Prometheus {
            labels: attribs
                .attributes
                .into_iter()
                .map(|kv| {
                    let value = match kv.value {
                        MetricValue::String(s) => s,
                        MetricValue::Int(i) => i.to_string(),
                        MetricValue::Float(f) => f.to_string(),
                        MetricValue::Bool(b) => b.to_string(),
                    };
                    (kv.key, value)
                })
                .collect(),
        }
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        if let MetricAttributes::Prometheus { mut labels } = existing {
            for kv in attribs.attributes {
                let value = match kv.value {
                    MetricValue::String(s) => s,
                    MetricValue::Int(i) => i.to_string(),
                    MetricValue::Float(f) => f.to_string(),
                    MetricValue::Bool(b) => b.to_string(),
                };
                labels.insert(kv.key, value);
            }
            MetricAttributes::Prometheus { labels }
        } else {
            dbg_panic!("Must use Prometheus attributes with a Prometheus metric implementation");
            self.new_attributes(attribs)
        }
    }

    fn counter(&self, params: MetricParameters) -> Arc<dyn Counter> {
        let opts = Opts::new(params.name.clone(), params.description.clone());
        // TODO: These fixed sets of label names cannot be here
        let label_names = &[
            "service_name",
            "namespace",
            "task_queue",
            "poller_type",
            "worker_type",
            "activity_type",
            "workflow_type",
            "eager",
            "failure_reason",
        ];
        let counter = CounterVec::new(opts, label_names).unwrap();
        self.registry.register(Box::new(counter.clone())).unwrap();
        Arc::new(counter)
    }

    fn histogram(&self, params: MetricParameters) -> Arc<dyn Histogram> {
        let opts = prometheus::HistogramOpts::new(params.name.clone(), params.description.clone());
        let label_names = &[
            "service_name",
            "namespace",
            "task_queue",
            "poller_type",
            "worker_type",
            "activity_type",
            "workflow_type",
            "eager",
            "failure_reason",
        ];
        let histogram = HistogramVec::new(opts, label_names).unwrap();
        self.registry.register(Box::new(histogram.clone())).unwrap();
        Arc::new(histogram)
    }

    fn histogram_f64(&self, params: MetricParameters) -> Arc<dyn HistogramF64> {
        let opts = prometheus::HistogramOpts::new(params.name.clone(), params.description.clone());
        let label_names = &[
            "service_name",
            "namespace",
            "task_queue",
            "poller_type",
            "worker_type",
            "activity_type",
            "workflow_type",
            "eager",
            "failure_reason",
        ];
        let histogram = HistogramVec::new(opts, label_names).unwrap();
        self.registry.register(Box::new(histogram.clone())).unwrap();
        Arc::new(histogram)
    }

    fn histogram_duration(&self, mut params: MetricParameters) -> Arc<dyn HistogramDuration> {
        Arc::new(if self.use_seconds_for_durations {
            params.unit = "s".into();
            DurationHistogram::Seconds(self.histogram_f64(params))
        } else {
            params.unit = "ms".into();
            DurationHistogram::Milliseconds(self.histogram(params))
        })
    }

    fn gauge(&self, params: MetricParameters) -> Arc<dyn Gauge> {
        let opts = Opts::new(params.name.clone(), params.description.clone());
        let label_names = &[
            "service_name",
            "namespace",
            "task_queue",
            "poller_type",
            "worker_type",
            "activity_type",
            "workflow_type",
            "eager",
            "failure_reason",
        ];
        let gauge = GaugeVec::new(opts, label_names).unwrap();
        self.registry.register(Box::new(gauge.clone())).unwrap();
        Arc::new(gauge)
    }

    fn gauge_f64(&self, params: MetricParameters) -> Arc<dyn GaugeF64> {
        let opts = Opts::new(params.name.clone(), params.description.clone());
        let label_names = &[
            "service_name",
            "namespace",
            "task_queue",
            "poller_type",
            "worker_type",
            "activity_type",
            "workflow_type",
            "eager",
            "failure_reason",
        ];
        let gauge = GaugeVec::new(opts, label_names).unwrap();
        self.registry.register(Box::new(gauge.clone())).unwrap();
        Arc::new(gauge)
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
    fn test_prometheus_meter_basic_functionality() {
        let registry = Registry::new();
        let meter = CorePrometheusMeter::new(registry.clone(), false);

        // Test counter
        let counter = meter.counter(MetricParameters {
            name: "test_counter".into(),
            description: "A test counter metric".into(),
            unit: "".into(),
        });
        let attrs = meter.new_attributes(NewAttributes::new(vec![MetricKeyValue::new(
            "service_name",
            "test_service",
        )]));
        counter.add(5, &attrs);

        // Test histogram
        let histogram = meter.histogram(MetricParameters {
            name: "test_histogram".into(),
            description: "A test histogram metric".into(),
            unit: "".into(),
        });
        histogram.record(100, &attrs);

        // Test gauge
        let gauge = meter.gauge(MetricParameters {
            name: "test_gauge".into(),
            description: "A test gauge metric".into(),
            unit: "".into(),
        });
        gauge.record(42, &attrs);

        // Verify metrics are registered and have values
        let metric_families = registry.gather();
        assert_eq!(metric_families.len(), 3);

        // Check that we can encode the metrics
        let encoder = TextEncoder::new();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).unwrap();
        let output = String::from_utf8(buffer).unwrap();

        assert!(output.contains("test_counter"));
        assert!(output.contains("test_histogram"));
        assert!(output.contains("test_gauge"));
    }

    #[test]
    fn test_prometheus_meter_duration_histogram() {
        let registry = Registry::new();
        let meter = CorePrometheusMeter::new(registry.clone(), true); // Use seconds

        let duration_histogram = meter.histogram_duration(MetricParameters {
            name: "test_duration".into(),
            description: "A test duration histogram metric".into(),
            unit: "".into(),
        });
        let attrs = meter.new_attributes(NewAttributes::new(vec![MetricKeyValue::new(
            "service_name",
            "test_service",
        )]));

        duration_histogram.record(Duration::from_millis(1500), &attrs);

        let metric_families = registry.gather();
        let encoder = TextEncoder::new();
        let mut buffer = vec![];
        encoder.encode(&metric_families, &mut buffer).unwrap();
        let output = String::from_utf8(buffer).unwrap();

        assert!(output.contains("test_duration"));
        // Should record as 1.5 seconds
        assert!(output.contains("1.5"));
    }
}
