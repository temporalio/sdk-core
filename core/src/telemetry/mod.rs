//! This module helps with the initialization and management of telemetry. IE: Metrics and tracing.
//! Logs from core are all traces, which may be exported to the console, in memory, or externally.

mod log_export;
pub(crate) mod metrics;
mod prometheus_server;

pub use metrics::{
    build_otlp_metric_exporter, default_buckets_for, start_prometheus_metric_exporter,
    MetricsCallBuffer,
};

use crate::telemetry::{
    log_export::{CoreLogExportLayer, CoreLogsOut},
    metrics::PrefixedMetricsMeter,
};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use opentelemetry::{sdk::Resource, KeyValue};
use opentelemetry_sdk::metrics::{data::Temporality, reader::TemporalitySelector, InstrumentKind};
use parking_lot::Mutex;
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    env,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use temporal_sdk_core_api::telemetry::{
    metrics::{CoreMeter, MetricKeyValue, MetricsAttributesOptions, TemporalMeter},
    CoreLog, CoreTelemetry, Logger, MetricTemporality, TelemetryOptions,
};
use tracing::{Level, Subscriber};
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Layer};

const TELEM_SERVICE_NAME: &str = "temporal-core-sdk";

/// Help you construct an [EnvFilter] compatible filter string which will forward all core module
/// traces at `core_level` and all others (from 3rd party modules, etc) at `other_level`.
pub fn construct_filter_string(core_level: Level, other_level: Level) -> String {
    format!(
        "{other_level},temporal_sdk_core={core_level},temporal_client={core_level},temporal_sdk={core_level}"
    )
}

/// Holds initialized tracing/metrics exporters, etc
pub struct TelemetryInstance {
    metric_prefix: &'static str,
    logs_out: Option<Mutex<CoreLogsOut>>,
    metrics: Option<Arc<dyn CoreMeter + 'static>>,
    trace_subscriber: Arc<dyn Subscriber + Send + Sync>,
    attach_service_name: bool,
}

impl TelemetryInstance {
    fn new(
        trace_subscriber: Arc<dyn Subscriber + Send + Sync>,
        logs_out: Option<Mutex<CoreLogsOut>>,
        metric_prefix: &'static str,
        metrics: Option<Arc<dyn CoreMeter + 'static>>,
        attach_service_name: bool,
    ) -> Self {
        Self {
            metric_prefix,
            logs_out,
            metrics,
            trace_subscriber,
            attach_service_name,
        }
    }

    /// Returns a trace subscriber which can be used with the tracing crate, or with our own
    /// [set_trace_subscriber_for_current_thread] function.
    pub fn trace_subscriber(&self) -> Arc<dyn Subscriber + Send + Sync> {
        self.trace_subscriber.clone()
    }

    /// Returns our wrapper for metric meters, can be used to, ex: initialize clients
    pub fn get_metric_meter(&self) -> Option<TemporalMeter> {
        self.metrics.clone().map(|m| {
            let kvs = if self.attach_service_name {
                vec![MetricKeyValue::new("service_name", TELEM_SERVICE_NAME)]
            } else {
                vec![]
            };
            let attribs = MetricsAttributesOptions::new(kvs);
            TemporalMeter::new(
                Arc::new(PrefixedMetricsMeter::new(self.metric_prefix, m)) as Arc<dyn CoreMeter>,
                attribs,
            )
        })
    }
}

thread_local! {
    static SUB_GUARD: RefCell<Option<tracing::subscriber::DefaultGuard>> = RefCell::new(None);
}
/// Set the trace subscriber for the current thread. This must be done in every thread which uses
/// core stuff, otherwise traces/logs will not be collected on that thread. For example, if using
/// a multithreaded Tokio runtime, you should ensure that said runtime uses
/// [on_thread_start](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.on_thread_start)
/// or a similar mechanism to call this for each thread within the runtime.
pub fn set_trace_subscriber_for_current_thread(sub: impl Subscriber + Send + Sync + 'static) {
    SUB_GUARD.with(|sg| {
        if sg.borrow().is_none() {
            let g = tracing::subscriber::set_default(sub);
            *sg.borrow_mut() = Some(g);
        }
    })
}

/// Undoes [set_trace_subscriber_for_current_thread]
pub fn remove_trace_subscriber_for_current_thread() {
    SUB_GUARD.with(|sg| sg.take());
}

fn metric_prefix(opts: &TelemetryOptions) -> &'static str {
    if opts.no_temporal_prefix_for_metrics {
        ""
    } else {
        "temporal_"
    }
}

impl CoreTelemetry for TelemetryInstance {
    fn fetch_buffered_logs(&self) -> Vec<CoreLog> {
        if let Some(logs_out) = self.logs_out.as_ref() {
            logs_out.lock().pop_iter().collect()
        } else {
            vec![]
        }
    }
}

/// Initialize tracing subscribers/output and logging export, returning a [TelemetryInstance]
/// which can be used to register default / global tracing subscribers.
///
/// You should only call this once per unique [TelemetryOptions]
///
/// See [TelemetryOptions] docs for more on configuration.
pub fn telemetry_init(opts: TelemetryOptions) -> Result<TelemetryInstance, anyhow::Error> {
    // This is a bit odd, but functional. It's desirable to create a separate tokio runtime for
    // metrics handling, since tests typically use a single-threaded runtime and initializing
    // pipeline requires us to know if the runtime is single or multithreaded, we will crash
    // in one case or the other. There does not seem to be a way to tell from the current runtime
    // handle if it is single or multithreaded. Additionally, we can isolate metrics work this
    // way which is nice.
    // Parts of telem dat ====
    let mut logs_out = None;
    let metric_prefix = metric_prefix(&opts);
    // =======================

    // Tracing subscriber layers =========
    let mut console_pretty_layer = None;
    let mut console_compact_layer = None;
    let mut forward_layer = None;
    // ===================================

    if let Some(ref logger) = opts.logging {
        match logger {
            Logger::Console { filter } => {
                // This is silly dupe but can't be avoided without boxing.
                if env::var("TEMPORAL_CORE_PRETTY_LOGS").is_ok() {
                    console_pretty_layer = Some(
                        tracing_subscriber::fmt::layer()
                            .with_target(false)
                            .event_format(
                                tracing_subscriber::fmt::format()
                                    .pretty()
                                    .with_source_location(false),
                            )
                            .with_filter(EnvFilter::new(filter)),
                    )
                } else {
                    console_compact_layer = Some(
                        tracing_subscriber::fmt::layer()
                            .with_target(false)
                            .event_format(
                                tracing_subscriber::fmt::format()
                                    .compact()
                                    .with_source_location(false),
                            )
                            .with_filter(EnvFilter::new(filter)),
                    )
                }
            }
            Logger::Forward { filter } => {
                let (export_layer, lo) = CoreLogExportLayer::new();
                logs_out = Some(Mutex::new(lo));
                forward_layer = Some(export_layer.with_filter(EnvFilter::new(filter)));
            }
        };
    };

    let reg = tracing_subscriber::registry()
        .with(console_pretty_layer)
        .with(console_compact_layer)
        .with(forward_layer);

    #[cfg(feature = "tokio-console")]
    let reg = reg.with(console_subscriber::spawn());

    Ok(TelemetryInstance::new(
        Arc::new(reg),
        logs_out,
        metric_prefix,
        opts.metrics,
        opts.attach_service_name,
    ))
}

/// Initialize telemetry/tracing globally. Useful for testing. Only takes affect when called
/// the first time. Subsequent calls are ignored.
pub fn telemetry_init_global(opts: TelemetryOptions) -> Result<(), anyhow::Error> {
    static INITTED: AtomicBool = AtomicBool::new(false);
    if INITTED
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
    {
        let ti = telemetry_init(opts)?;
        tracing::subscriber::set_global_default(ti.trace_subscriber())?;
    }
    Ok(())
}

fn default_resource_kvs() -> &'static [KeyValue] {
    static INSTANCE: OnceCell<[KeyValue; 1]> = OnceCell::new();
    INSTANCE.get_or_init(|| [KeyValue::new("service.name", TELEM_SERVICE_NAME)])
}

fn default_resource(override_values: &HashMap<String, String>) -> Resource {
    let override_kvs = override_values
        .iter()
        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()));
    Resource::new(default_resource_kvs().iter().cloned()).merge(&Resource::new(override_kvs))
}

#[derive(Clone)]
struct ConstantTemporality(Temporality);
impl TemporalitySelector for ConstantTemporality {
    fn temporality(&self, _: InstrumentKind) -> Temporality {
        self.0
    }
}
fn metric_temporality_to_selector(
    t: MetricTemporality,
) -> impl TemporalitySelector + Send + Sync + Clone {
    match t {
        MetricTemporality::Cumulative => ConstantTemporality(Temporality::Cumulative),
        MetricTemporality::Delta => ConstantTemporality(Temporality::Delta),
    }
}

#[cfg(test)]
pub mod test_initters {
    use super::*;
    use temporal_sdk_core_api::telemetry::TelemetryOptionsBuilder;

    #[allow(dead_code)] // Not always used, called to enable for debugging when needed
    pub fn test_telem_console() {
        telemetry_init_global(
            TelemetryOptionsBuilder::default()
                .logging(Logger::Console {
                    filter: construct_filter_string(Level::DEBUG, Level::WARN),
                })
                .build()
                .unwrap(),
        )
        .unwrap();
    }
}
#[cfg(test)]
pub use test_initters::*;

/// A trait for using [Display] on the contents of vecs, etc, which don't implement it.
///
/// Dislike this, but, there doesn't seem to be a great alternative. Calling itertools format
/// inline in an `event!` macro can panic because it gets evaluated twice somehow.
pub(crate) trait VecDisplayer {
    fn display(&self) -> String;
}

impl<T> VecDisplayer for Vec<T>
where
    T: std::fmt::Display,
{
    fn display(&self) -> String {
        format!("[{}]", self.iter().format(","))
    }
}

impl<T> VecDisplayer for VecDeque<T>
where
    T: std::fmt::Display,
{
    fn display(&self) -> String {
        format!("[{}]", self.iter().format(","))
    }
}
