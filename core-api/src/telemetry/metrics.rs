use std::{borrow::Cow, collections::HashSet, fmt::Debug, sync::Arc};

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
    fn counter(&self, params: MetricParameters) -> Arc<dyn Counter>;
    fn histogram(&self, params: MetricParameters) -> Arc<dyn Histogram>;
    fn gauge(&self, params: MetricParameters) -> Arc<dyn Gauge>;
}

#[derive(Debug, Clone, derive_builder::Builder)]
pub struct MetricParameters {
    /// The name for the new metric/instrument
    #[builder(setter(into))]
    pub name: Cow<'static, str>,
    /// A description that will appear in metadata if the backend supports it
    #[builder(setter(into), default = "\"\".into()")]
    pub description: Cow<'static, str>,
    /// Unit information that will appear in metadata if the backend supports it
    #[builder(setter(into), default = "\"\".into()")]
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
#[derive(derive_more::Constructor, Clone, Debug)]
pub struct TemporalMeter {
    pub inner: Arc<dyn CoreMeter>,
    pub default_attribs: NewAttributes,
}

#[derive(Debug, Clone)]
pub enum MetricEvent {
    Create {
        params: MetricParameters,
        id: u64,
        kind: MetricKind,
    },
    CreateAttributes {
        id: u64,
        attributes: Vec<MetricKeyValue>,
    },
    Update {
        id: u64,
        attributes: BufferAttributes,
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
    fn retrieve(&self) -> Vec<MetricEvent>;
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

    fn counter(&self, params: MetricParameters) -> Arc<dyn Counter> {
        self.as_ref().counter(params)
    }
    fn histogram(&self, params: MetricParameters) -> Arc<dyn Histogram> {
        self.as_ref().histogram(params)
    }
    fn gauge(&self, params: MetricParameters) -> Arc<dyn Gauge> {
        self.as_ref().gauge(params)
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
    Buffer(BufferAttributes),
    Dynamic(Arc<dyn CustomMetricAttributes>),
}

/// A reference to some attributes created lang side.
pub trait CustomMetricAttributes: Debug + Send + Sync {}

#[derive(Clone, Debug)]
pub struct BufferAttributes {
    /// A set of references to attributes stored in lang memory. All referenced attributes should
    /// be attached to the metric when recording.
    pub ids: HashSet<u64>,
    /// If populated, these key values should also be used in addition to the referred-to
    /// existing attributes when recording
    pub new_attributes: Vec<MetricKeyValue>,
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

/// A K/V pair that can be used to label a specific recording of a metric
#[derive(Clone, Debug)]
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
    fn new_attributes(&self, _: NewAttributes) -> MetricAttributes {
        MetricAttributes::Buffer(BufferAttributes {
            ids: HashSet::new(),
            new_attributes: vec![],
        })
    }

    fn extend_attributes(&self, existing: MetricAttributes, _: NewAttributes) -> MetricAttributes {
        existing
    }

    fn counter(&self, _: MetricParameters) -> Arc<dyn Counter> {
        Arc::new(NoOpInstrument)
    }

    fn histogram(&self, _: MetricParameters) -> Arc<dyn Histogram> {
        Arc::new(NoOpInstrument)
    }

    fn gauge(&self, _: MetricParameters) -> Arc<dyn Gauge> {
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

    impl Counter for metrics::Counter<u64> {
        fn add(&self, value: u64, attributes: &MetricAttributes) {
            if let MetricAttributes::OTel { kvs } = attributes {
                self.add(value, kvs);
            } else {
                debug_assert!(
                    false,
                    "Must use OTel attributes with an OTel metric implementation"
                );
            }
        }
    }

    impl Histogram for metrics::Histogram<u64> {
        fn record(&self, value: u64, attributes: &MetricAttributes) {
            if let MetricAttributes::OTel { kvs } = attributes {
                self.record(value, kvs);
            } else {
                debug_assert!(
                    false,
                    "Must use OTel attributes with an OTel metric implementation"
                );
            }
        }
    }
}
