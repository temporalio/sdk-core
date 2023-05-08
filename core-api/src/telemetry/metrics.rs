use std::{fmt::Debug, sync::Arc};

/// Implementors of this trait are expected to be defined in each language's bridge.
/// The implementor is responsible for the allocation/instantiation of new metric meters which
/// Core has requested.
pub trait CoreMeter: Send + Sync + Debug {
    fn new_attributes(&self, attribs: MetricsAttributesOptions) -> MetricAttributes;
    fn counter(&self, name: &str) -> Arc<dyn Counter>;
    fn histogram(&self, name: &str) -> Arc<dyn Histogram>;
    fn gauge(&self, name: &str) -> Arc<dyn Gauge>;
}

#[derive(Debug)]
pub struct NoOpCoreMeter;
impl CoreMeter for NoOpCoreMeter {
    fn new_attributes(&self, _: MetricsAttributesOptions) -> MetricAttributes {
        MetricAttributes::Lang {
            id: 0,
            new_attributes: vec![],
        }
    }

    fn counter(&self, _: &str) -> Arc<dyn Counter> {
        Arc::new(NoOpInstrument)
    }

    fn histogram(&self, _: &str) -> Arc<dyn Histogram> {
        Arc::new(NoOpInstrument)
    }

    fn gauge(&self, _: &str) -> Arc<dyn Gauge> {
        Arc::new(NoOpInstrument)
    }
}

#[derive(Debug)]
pub struct PrefixedMetricsMeter<CM>(&'static str, CM);
impl<CM> PrefixedMetricsMeter<CM> {
    pub fn new(prefix: &'static str, core_meter: CM) -> Self {
        PrefixedMetricsMeter(prefix, core_meter)
    }
}
impl<CM: CoreMeter> CoreMeter for PrefixedMetricsMeter<CM> {
    fn new_attributes(&self, attribs: MetricsAttributesOptions) -> MetricAttributes {
        self.1.new_attributes(attribs)
    }

    fn counter(&self, name: &str) -> Arc<dyn Counter> {
        self.1.counter(&(self.0.to_string() + name))
    }

    fn histogram(&self, name: &str) -> Arc<dyn Histogram> {
        self.1.histogram(&(self.0.to_string() + name))
    }

    fn gauge(&self, name: &str) -> Arc<dyn Gauge> {
        self.1.gauge(&(self.0.to_string() + name))
    }
}

impl CoreMeter for Arc<dyn CoreMeter> {
    fn new_attributes(&self, attribs: MetricsAttributesOptions) -> MetricAttributes {
        self.as_ref().new_attributes(attribs)
    }

    fn counter(&self, name: &str) -> Arc<dyn Counter> {
        self.as_ref().counter(name)
    }

    fn histogram(&self, name: &str) -> Arc<dyn Histogram> {
        self.as_ref().histogram(name)
    }

    fn gauge(&self, name: &str) -> Arc<dyn Gauge> {
        self.as_ref().gauge(name)
    }
}

/// Attributes which are provided every time a call to record a specific metric is made.
/// Implementors must be very cheap to clone, as these attributes will be re-used frequently.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum MetricAttributes {
    #[cfg(feature = "otel_impls")]
    OTel {
        kvs: Arc<Vec<opentelemetry::KeyValue>>,
        ctx: opentelemetry::Context,
    },
    Lang {
        /// An opaque reference to attributes stored in lang memory
        id: u64,
        /// If populated, these key values should also be used in addition to the referred-to
        /// existing attributes when recording
        new_attributes: Vec<MetricKeyValue>,
    },
}

impl MetricAttributes {
    /// Extend existing metrics attributes with new k/vs, returning a new instance
    pub fn with_new_attrs(&self, new_kvs: impl IntoIterator<Item = MetricKeyValue>) -> Self {
        let mut me = self.clone();
        me.add_new_attrs(new_kvs);
        me
    }

    /// Mutate self to add new kvs
    pub fn add_new_attrs(&mut self, new_kvs: impl IntoIterator<Item = MetricKeyValue>) {
        match self {
            #[cfg(feature = "otel_impls")]
            MetricAttributes::OTel { ref mut kvs, .. } => {
                Arc::make_mut(kvs).extend(new_kvs.into_iter().map(Into::into));
            }
            MetricAttributes::Lang {
                ref mut new_attributes,
                ..
            } => {
                new_attributes.extend(new_kvs.into_iter());
            }
        }
    }
}

/// Options that are attached to metrics on a per-call basis
#[derive(Clone, Default, derive_more::Constructor)]
pub struct MetricsAttributesOptions {
    attributes: Vec<MetricKeyValue>,
}
impl MetricsAttributesOptions {
    pub fn extend(&mut self, new_kvs: impl IntoIterator<Item = MetricKeyValue>) {
        self.attributes.extend(new_kvs.into_iter())
    }
}

/// A K/V pair that can be used to label a specific recording of a metric
#[derive(Clone, Debug)]
pub struct MetricKeyValue {
    key: String,
    value: MetricValue,
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
#[derive(Clone, Debug, derive_more::From)]
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

/// Histogram buckets
#[derive(Clone, Debug)]
pub struct Buckets {
    buckets: Vec<f64>,
}

pub trait Counter: Send + Sync {
    fn add(&self, value: u64, attributes: &MetricAttributes);
}

pub trait Histogram: Send + Sync {
    // When referring to durations, this value is in millis
    fn record(&self, value: u64, attributes: &MetricAttributes);
}

pub trait Gauge: Send + Sync {
    // When referring to durations, this value is in millis
    fn record(&self, value: u64, attributes: &MetricAttributes);
}

pub struct NoOpInstrument;
impl Counter for NoOpInstrument {
    fn add(&self, _: u64, _: &MetricAttributes) {}
}
impl Histogram for NoOpInstrument {
    fn record(&self, _: u64, _: &MetricAttributes) {}
}
impl Gauge for NoOpInstrument {
    fn record(&self, _: u64, _: &MetricAttributes) {}
}

#[cfg(feature = "otel_impls")]
mod otel_impls {
    use super::*;
    use opentelemetry::{metrics, metrics::Meter, KeyValue};

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

    impl CoreMeter for Meter {
        fn new_attributes(&self, attribs: MetricsAttributesOptions) -> MetricAttributes {
            MetricAttributes::OTel {
                kvs: Arc::new(attribs.attributes.into_iter().map(KeyValue::from).collect()),
                // TODO: Pass in? Hard to make work with trait
                ctx: opentelemetry::Context::current(),
            }
        }

        fn counter(&self, name: &str) -> Arc<dyn Counter> {
            Arc::new(self.u64_counter(name).init())
        }

        fn histogram(&self, name: &str) -> Arc<dyn Histogram> {
            Arc::new(self.u64_histogram(name).init())
        }

        fn gauge(&self, name: &str) -> Arc<dyn Gauge> {
            Arc::new(self.u64_histogram(name).init())
        }
    }

    // TODO: Dbg panic
    impl Counter for metrics::Counter<u64> {
        fn add(&self, value: u64, attributes: &MetricAttributes) {
            if let MetricAttributes::OTel { ctx, kvs } = attributes {
                self.add(ctx, value, kvs);
            }
        }
    }

    impl Histogram for metrics::Histogram<u64> {
        fn record(&self, value: u64, attributes: &MetricAttributes) {
            if let MetricAttributes::OTel { ctx, kvs } = attributes {
                self.record(ctx, value, kvs);
            }
        }
    }

    impl Gauge for metrics::Histogram<u64> {
        fn record(&self, value: u64, attributes: &MetricAttributes) {
            if let MetricAttributes::OTel { ctx, kvs } = attributes {
                self.record(ctx, value, kvs);
            }
        }
    }
}
