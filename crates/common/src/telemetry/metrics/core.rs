//! Metrics related code only needed by core and core-based SDKs

use crate::telemetry::metrics::{MetricKeyValue, MetricParameters};
use std::{
    any::Any,
    fmt::Debug,
    sync::{Arc, OnceLock},
    time::Duration,
};

/// A reference to some attributes created lang side.
pub trait CustomMetricAttributes: Debug + Send + Sync {
    /// Must be implemented to work around existing type system restrictions, see
    /// [here](https://internals.rust-lang.org/t/downcast-not-from-any-but-from-any-trait/16736/12)
    fn as_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}

#[derive(Debug, Clone)]
pub enum MetricEvent<I: BufferInstrumentRef> {
    Create {
        params: MetricParameters,
        /// Once you receive this event, call `set` on this with the initialized instrument
        /// reference
        populate_into: LazyBufferInstrument<I>,
        kind: MetricKind,
    },
    CreateAttributes {
        /// Once you receive this event, call `set` on this with the initialized attributes
        populate_into: BufferAttributes,
        /// If not `None`, use these already-initialized attributes as the base (extended with
        /// `attributes`) for the ones you are about to initialize.
        append_from: Option<BufferAttributes>,
        attributes: Vec<MetricKeyValue>,
    },
    Update {
        instrument: LazyBufferInstrument<I>,
        attributes: BufferAttributes,
        update: MetricUpdateVal,
    },
}
#[derive(Debug, Clone, Copy)]
pub enum MetricKind {
    Counter,
    Gauge,
    GaugeF64,
    Histogram,
    HistogramF64,
    HistogramDuration,
}
#[derive(Debug, Clone, Copy)]
pub enum MetricUpdateVal {
    Delta(u64),
    DeltaF64(f64),
    Value(u64),
    ValueF64(f64),
    Duration(Duration),
}

pub trait MetricCallBufferer<I: BufferInstrumentRef>: Send + Sync {
    fn retrieve(&self) -> Vec<MetricEvent<I>>;
}

/// A lazy reference to some metrics buffer attributes
pub type BufferAttributes = LazyRef<Arc<dyn CustomMetricAttributes + 'static>>;

/// Types lang uses to contain references to its lang-side defined instrument references must
/// implement this marker trait
pub trait BufferInstrumentRef {}
/// A lazy reference to a metrics buffer instrument
pub type LazyBufferInstrument<T> = LazyRef<Arc<T>>;

#[derive(Debug, Clone)]
pub struct LazyRef<T> {
    to_be_initted: Arc<OnceLock<T>>,
}
impl<T> LazyRef<T> {
    pub fn hole() -> Self {
        Self {
            to_be_initted: Arc::new(OnceLock::new()),
        }
    }

    /// Get the reference you previously initialized
    ///
    /// # Panics
    /// If `set` has not already been called. You must set the reference before using it.
    pub fn get(&self) -> &T {
        self.to_be_initted
            .get()
            .expect("You must initialize the reference before using it")
    }

    /// Assigns a value to fill this reference.
    /// Returns according to semantics of [OnceLock].
    pub fn set(&self, val: T) -> Result<(), T> {
        self.to_be_initted.set(val)
    }
}
