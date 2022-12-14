//! This module helps with the initialization and management of telemetry. IE: Metrics and tracing.
//! Logs from core are all traces, which may be exported to the console, in memory, or externally.

mod log_export;
pub(crate) mod metrics;
mod prometheus_server;

use crate::telemetry::{
    log_export::{CoreLogExportLayer, CoreLogsOut},
    metrics::SDKAggSelector,
    prometheus_server::PromServer,
};
use crossbeam::channel::Receiver;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use opentelemetry::{
    metrics::{Meter, MeterProvider},
    runtime,
    sdk::{
        export::metrics::aggregation::{self, Temporality, TemporalitySelector},
        trace::Config,
        Resource,
    },
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use parking_lot::Mutex;
use std::{
    cell::RefCell,
    collections::VecDeque,
    convert::TryInto,
    env,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use temporal_sdk_core_api::telemetry::{
    CoreLog, CoreTelemetry, Logger, MetricTemporality, MetricsExporter, OtelCollectorOptions,
    TelemetryOptions, TraceExporter,
};
use tonic::metadata::MetadataMap;
use tracing::{Level, Subscriber};
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Layer};

const TELEM_SERVICE_NAME: &str = "temporal-core-sdk";

/// Help you construct an [EnvFilter] compatible filter string which will forward all core module
/// traces at `core_level` and all others (from 3rd party modules, etc) at `other_levl.
pub fn construct_filter_string(core_level: Level, other_level: Level) -> String {
    format!(
        "{o},temporal_sdk_core={l},temporal_client={l},temporal_sdk={l}",
        o = other_level,
        l = core_level
    )
}

/// Holds initialized tracing/metrics exporters, etc
pub struct TelemetryInstance {
    metric_prefix: &'static str,
    logs_out: Option<Mutex<CoreLogsOut>>,
    metrics: Option<(Box<dyn MeterProvider + Send + Sync + 'static>, Meter)>,
    trace_subscriber: Arc<dyn Subscriber + Send + Sync>,
    _keepalive_rx: Receiver<()>,
}

impl TelemetryInstance {
    fn new(
        trace_subscriber: Arc<dyn Subscriber + Send + Sync>,
        logs_out: Option<Mutex<CoreLogsOut>>,
        metric_prefix: &'static str,
        mut meter_provider: Option<Box<dyn MeterProvider + Send + Sync + 'static>>,
        keepalive_rx: Receiver<()>,
    ) -> Self {
        let metrics = meter_provider.take().map(|mp| {
            let meter = mp.meter(TELEM_SERVICE_NAME);
            (mp, meter)
        });
        Self {
            metric_prefix,
            logs_out,
            metrics,
            trace_subscriber,
            _keepalive_rx: keepalive_rx,
        }
    }

    /// Returns a trace subscriber which can be used with the tracing crate, or with our own
    /// [set_trace_subscriber_for_current_thread] function.
    pub fn trace_subscriber(&self) -> Arc<dyn Subscriber + Send + Sync> {
        self.trace_subscriber.clone()
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

    fn get_metric_meter(&self) -> Option<&Meter> {
        self.metrics.as_ref().map(|(_, m)| m)
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
    let (tx, rx) = crossbeam::channel::bounded(0);
    let (keepalive_tx, keepalive_rx) = crossbeam::channel::bounded(0);
    let jh = std::thread::spawn(move || -> Result<(), anyhow::Error> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("telemetry")
            .worker_threads(2)
            .enable_all()
            .build()?;
        // Parts of telem dat ====
        let mut logs_out = None;
        let metric_prefix = metric_prefix(&opts);
        // =======================

        // Tracing subscriber layers =========
        let mut console_pretty_layer = None;
        let mut console_compact_layer = None;
        let mut forward_layer = None;
        let mut export_layer = None;
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

        let meter_provider = if let Some(ref metrics) = opts.metrics {
            let aggregator = SDKAggSelector { metric_prefix };
            match metrics {
                MetricsExporter::Prometheus(addr) => {
                    let srv = PromServer::new(
                        *addr,
                        aggregator,
                        metric_temporality_to_selector(opts.metric_temporality),
                    )?;
                    let mp = srv.exporter.meter_provider()?;
                    runtime.spawn(async move { srv.run().await });
                    Some(Box::new(mp) as Box<dyn MeterProvider + Send + Sync>)
                }
                MetricsExporter::Otel(OtelCollectorOptions {
                    url,
                    headers,
                    metric_periodicity,
                }) => runtime.block_on(async {
                    let metrics = opentelemetry_otlp::new_pipeline()
                        .metrics(
                            aggregator,
                            metric_temporality_to_selector(opts.metric_temporality),
                            runtime::Tokio,
                        )
                        .with_period(metric_periodicity.unwrap_or_else(|| Duration::from_secs(1)))
                        .with_resource(default_resource())
                        .with_exporter(
                            opentelemetry_otlp::new_exporter()
                                .tonic()
                                .with_endpoint(url.to_string())
                                .with_metadata(MetadataMap::from_headers(headers.try_into()?)),
                        )
                        .build()?;
                    Ok::<_, anyhow::Error>(Some(
                        Box::new(metrics) as Box<dyn MeterProvider + Send + Sync>
                    ))
                })?,
            }
        } else {
            None
        };

        if let Some(ref tracing) = opts.tracing {
            match &tracing.exporter {
                TraceExporter::Otel(OtelCollectorOptions { url, headers, .. }) => {
                    runtime.block_on(async {
                        let tracer_cfg = Config::default().with_resource(default_resource());
                        let tracer = opentelemetry_otlp::new_pipeline()
                            .tracing()
                            .with_exporter(
                                opentelemetry_otlp::new_exporter()
                                    .tonic()
                                    .with_endpoint(url.to_string())
                                    .with_metadata(MetadataMap::from_headers(headers.try_into()?)),
                            )
                            .with_trace_config(tracer_cfg)
                            .install_batch(runtime::Tokio)?;

                        let opentelemetry = tracing_opentelemetry::layer()
                            .with_tracer(tracer)
                            .with_filter(EnvFilter::new(&tracing.filter));

                        export_layer = Some(opentelemetry);
                        Result::<(), anyhow::Error>::Ok(())
                    })?;
                }
            };
        };

        let reg = tracing_subscriber::registry()
            .with(console_pretty_layer)
            .with(console_compact_layer)
            .with(forward_layer)
            .with(export_layer);

        tx.send(TelemetryInstance::new(
            Arc::new(reg),
            logs_out,
            metric_prefix,
            meter_provider,
            keepalive_rx,
        ))
        .expect("Must be able to send telem instance out of thread");
        // Now keep the thread alive until the telemetry instance is dropped by trying to send
        // something forever
        let _ = keepalive_tx.send(());
        Ok(())
    });
    match rx.recv() {
        Ok(ti) => Ok(ti),
        Err(_) => {
            // Immediately join the thread since something went wrong in it
            jh.join().expect("Telemetry must init cleanly")?;
            // This can't happen. The rx channel can't be dropped unless the thread errored.
            unreachable!("Impossible error in telemetry init thread");
        }
    }
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
fn default_resource() -> Resource {
    Resource::new(default_resource_kvs().iter().cloned())
}

fn metric_temporality_to_selector(
    t: MetricTemporality,
) -> impl TemporalitySelector + Send + Sync + Clone {
    match t {
        MetricTemporality::Cumulative => {
            aggregation::constant_temporality_selector(Temporality::Cumulative)
        }
        MetricTemporality::Delta => aggregation::constant_temporality_selector(Temporality::Delta),
    }
}

#[cfg(test)]
pub mod test_initters {
    use super::*;
    use temporal_sdk_core_api::telemetry::{TelemetryOptionsBuilder, TraceExportConfig};

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

    #[allow(dead_code)] // Not always used, called to enable for debugging when needed
    pub fn test_telem_collector() {
        telemetry_init_global(
            TelemetryOptionsBuilder::default()
                .logging(Logger::Console {
                    filter: construct_filter_string(Level::DEBUG, Level::WARN),
                })
                .tracing(TraceExportConfig {
                    filter: construct_filter_string(Level::DEBUG, Level::WARN),
                    exporter: TraceExporter::Otel(OtelCollectorOptions {
                        url: "grpc://localhost:4317".parse().unwrap(),
                        headers: Default::default(),
                        metric_periodicity: None,
                    }),
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
