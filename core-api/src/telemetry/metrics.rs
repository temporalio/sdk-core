use std::{collections::HashSet, fmt::Debug, sync::Arc};

/// Implementors of this trait are expected to be defined in each language's bridge.
/// The implementor is responsible for the allocation/instantiation of new metric meters which
/// Core has requested.
pub trait CoreMeter: Send + Sync + Debug {
    fn new_attributes(&self, attribs: MetricsAttributesOptions) -> MetricAttributes;
    // TODO: Return result? One possible error: Name collisions.
    fn counter(&self, name: &str) -> Arc<dyn Counter>;
    fn histogram(&self, name: &str) -> Arc<dyn Histogram>;
    fn gauge(&self, name: &str) -> Arc<dyn Gauge>;
}

/// Wraps a [CoreMeter] to enable the attaching of default labels to metrics. Cloning is cheap.
#[derive(derive_more::Constructor, Clone, Debug)]
pub struct TemporalMeter {
    pub inner: Arc<dyn CoreMeter>,
    pub default_attribs: MetricsAttributesOptions,
}

#[derive(Debug, Clone)]
pub enum MetricEvent {
    Create {
        name: String,
        id: u64,
        kind: MetricKind,
    },
    CreateAttributes {
        id: u64,
        attributes: Vec<MetricKeyValue>,
    },
    Update {
        id: u64,
        attributes: LangMetricAttributes,
        update: MetricUpdateVal,
    },
}
#[derive(Debug, Clone, Copy)]
pub enum MetricKind {
    Counter,
    Gauge,
    Histogram,
}
#[derive(Debug, Clone, Copy)]
pub enum MetricUpdateVal {
    // Currently all deltas are natural numbers
    Delta(u64),
    // Currently all values are natural numbers
    Value(u64),
}

pub trait MetricCallBufferer: Send + Sync {
    fn retrieve(&mut self) -> Vec<MetricEvent>;
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
    },
    Lang(LangMetricAttributes),
}
#[derive(Clone, Debug)]
pub struct LangMetricAttributes {
    /// A set of references to attributes stored in lang memory. All referenced attributes should
    /// be attached to the metric when recording.
    pub ids: HashSet<u64>,
    /// If populated, these key values should also be used in addition to the referred-to
    /// existing attributes when recording
    pub new_attributes: Vec<MetricKeyValue>,
}

impl MetricAttributes {
    /// Extend existing metrics attributes with others, returning a new instance
    pub fn merge(&self, other: MetricAttributes) -> Self {
        let mut me = self.clone();
        match (&mut me, other) {
            (MetricAttributes::OTel { ref mut kvs }, MetricAttributes::OTel { kvs: other_kvs }) => {
                Arc::make_mut(kvs).extend((*other_kvs).clone());
            }
            (MetricAttributes::Lang(ref mut l), MetricAttributes::Lang(ol)) => {
                l.ids.extend(ol.ids);
                l.new_attributes.extend(ol.new_attributes);
            }
            _ => panic!("Cannot merge metric attributes of different kinds"),
        }
        me
    }

    /// Mutate self to add new kvs
    pub fn add_new_attrs(&mut self, new_kvs: impl IntoIterator<Item = MetricKeyValue>) {
        match self {
            #[cfg(feature = "otel_impls")]
            MetricAttributes::OTel { ref mut kvs, .. } => {
                Arc::make_mut(kvs).extend(new_kvs.into_iter().map(Into::into));
            }
            MetricAttributes::Lang(ref mut attrs, ..) => {
                attrs.new_attributes.extend(new_kvs.into_iter());
            }
        }
    }
}

/// Options that are attached to metrics on a per-call basis
#[derive(Clone, Debug, Default, derive_more::Constructor)]
pub struct MetricsAttributesOptions {
    pub attributes: Vec<MetricKeyValue>,
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

#[derive(Debug)]
pub struct NoOpCoreMeter;
impl CoreMeter for NoOpCoreMeter {
    fn new_attributes(&self, _: MetricsAttributesOptions) -> MetricAttributes {
        MetricAttributes::Lang(LangMetricAttributes {
            ids: HashSet::new(),
            new_attributes: vec![],
        })
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
    use opentelemetry::{metrics, KeyValue};

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

    // TODO: Dbg panic
    impl Counter for metrics::Counter<u64> {
        fn add(&self, value: u64, attributes: &MetricAttributes) {
            if let MetricAttributes::OTel { kvs } = attributes {
                self.add(value, kvs);
            }
        }
    }

    impl Histogram for metrics::Histogram<u64> {
        fn record(&self, value: u64, attributes: &MetricAttributes) {
            if let MetricAttributes::OTel { kvs } = attributes {
                self.record(value, kvs);
            }
        }
    }
}
