mod log_export;
pub(crate) mod metrics;
mod prometheus_server;

use crate::{
    telemetry::{
        log_export::{CoreLogExportLayer, CoreLogsOut},
        metrics::SDKAggSelector,
        prometheus_server::PromServer,
    },
    METRIC_METER,
};
use itertools::Itertools;
use once_cell::sync::OnceCell;
use opentelemetry::{
    global,
    metrics::Meter,
    runtime,
    sdk::{
        export::metrics::aggregation::{self, Temporality, TemporalitySelector},
        trace::Config,
        Resource,
    },
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use parking_lot::{const_mutex, Mutex};
use std::{collections::VecDeque, convert::TryInto, env, time::Duration};
use temporal_sdk_core_api::worker::telemetry::{
    CoreLog, CoreTelemetry, Logger, MetricTemporality, MetricsExporter, OtelCollectorOptions,
    TelemetryOptions, TraceExporter,
};
use tonic::metadata::MetadataMap;
use tracing::Level;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Layer};

const TELEM_SERVICE_NAME: &str = "temporal-core-sdk";
static GLOBAL_TELEM_DAT: OnceCell<GlobalTelemDat> = OnceCell::new();
static TELETM_MUTEX: Mutex<()> = const_mutex(());

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

/// Help you construct an [EnvFilter] compatible filter string which will forward all core module
/// traces at `core_level` and all others (from 3rd party modules, etc) at `other_levl.
pub fn construct_filter_string(core_level: Level, other_level: Level) -> String {
    format!(
        "{o},temporal_sdk_core={l},temporal_client={l},temporal_sdk={l}",
        o = other_level,
        l = core_level
    )
}

/// Things that need to not be dropped while telemetry is ongoing
#[derive(Default)]
pub struct GlobalTelemDat {
    runtime: Option<tokio::runtime::Runtime>,
    prom_srv: Option<PromServer>,
    no_temporal_prefix_for_metrics: bool,
    logs_out: Option<Mutex<CoreLogsOut>>,
}

impl GlobalTelemDat {
    fn init(&'static self) {
        if let Some(srv) = &self.prom_srv {
            self.runtime
                .as_ref()
                .expect("Telemetry runtime is initted")
                .spawn(srv.run());
        }
    }
}

impl CoreTelemetry for GlobalTelemDat {
    fn fetch_buffered_logs(&self) -> Vec<CoreLog> {
        fetch_global_buffered_logs()
    }

    fn get_metric_meter(&self) -> Option<&Meter> {
        if GLOBAL_TELEM_DAT.get().is_some() {
            return Some(&METRIC_METER);
        }
        None
    }
}

/// Initialize tracing subscribers/output and logging export. If this function is called more than
/// once, subsequent calls do nothing.
///
/// See [TelemetryOptions] docs for more on configuration.
pub fn telemetry_init(opts: &TelemetryOptions) -> Result<&'static GlobalTelemDat, anyhow::Error> {
    // Ensure we don't pointlessly spawn threads that won't do anything or call telem dat's init 2x
    let guard = TELETM_MUTEX.lock();
    if let Some(gtd) = GLOBAL_TELEM_DAT.get() {
        return Ok(gtd);
    }

    // This is a bit odd, but functional. It's desirable to create a separate tokio runtime for
    // metrics handling, since tests typically use a single-threaded runtime and initializing
    // pipeline requires us to know if the runtime is single or multithreaded, we will crash
    // in one case or the other. There does not seem to be a way to tell from the current runtime
    // handle if it is single or multithreaded. Additionally, we can isolate metrics work this
    // way which is nice.
    let opts = opts.clone();
    std::thread::spawn(move || {
        let res = GLOBAL_TELEM_DAT.get_or_try_init::<_, anyhow::Error>(move || {
            // Ensure closure captures the mutex guard
            let _ = &*guard;

            let runtime = tokio::runtime::Builder::new_multi_thread()
                .thread_name("telemetry")
                .worker_threads(2)
                .enable_all()
                .build()?;
            let mut globaldat = GlobalTelemDat {
                no_temporal_prefix_for_metrics: opts.no_temporal_prefix_for_metrics,
                ..Default::default()
            };

            let mut console_pretty_layer = None;
            let mut console_compact_layer = None;
            let mut forward_layer = None;
            let mut export_layer = None;

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
                        let (export_layer, logs_out) = CoreLogExportLayer::new();
                        globaldat.logs_out = Some(Mutex::new(logs_out));
                        forward_layer = Some(export_layer.with_filter(EnvFilter::new(filter)));
                    }
                };
            };

            if let Some(ref metrics) = opts.metrics {
                match metrics {
                    MetricsExporter::Prometheus(addr) => {
                        let srv = PromServer::new(
                            *addr,
                            metric_temporality_to_selector(opts.metric_temporality),
                        )?;
                        globaldat.prom_srv = Some(srv);
                    }
                    MetricsExporter::Otel(OtelCollectorOptions { url, headers }) => {
                        runtime.block_on(async {
                            let metrics = opentelemetry_otlp::new_pipeline()
                                .metrics(
                                    SDKAggSelector,
                                    metric_temporality_to_selector(opts.metric_temporality),
                                    runtime::Tokio,
                                )
                                .with_period(Duration::from_secs(1))
                                .with_resource(default_resource())
                                .with_exporter(
                                    // No joke exporter builder literally not cloneable for some
                                    // insane reason
                                    opentelemetry_otlp::new_exporter()
                                        .tonic()
                                        .with_endpoint(url.to_string())
                                        .with_metadata(MetadataMap::from_headers(
                                            headers.try_into()?,
                                        )),
                                )
                                .build()?;
                            global::set_meter_provider(metrics);
                            Result::<(), anyhow::Error>::Ok(())
                        })?;
                    }
                };
            };

            if let Some(ref tracing) = opts.tracing {
                match &tracing.exporter {
                    TraceExporter::Otel(OtelCollectorOptions { url, headers }) => {
                        runtime.block_on(async {
                            let tracer_cfg = Config::default().with_resource(default_resource());
                            let tracer = opentelemetry_otlp::new_pipeline()
                                .tracing()
                                .with_exporter(
                                    opentelemetry_otlp::new_exporter()
                                        .tonic()
                                        .with_endpoint(url.to_string())
                                        .with_metadata(MetadataMap::from_headers(
                                            headers.try_into()?,
                                        )),
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
            tracing::subscriber::set_global_default(reg)?;

            globaldat.runtime = Some(runtime);
            Ok(globaldat)
        })?;

        res.init();
        Result::<_, anyhow::Error>::Ok(res)
    })
    .join()
    .expect("Telemetry initialization panicked")
}

/// Returned buffered logs for export to lang from the global logging instance.
/// If [telemetry_init] has not been called, always returns an empty vec.
pub fn fetch_global_buffered_logs() -> Vec<CoreLog> {
    if let Some(logs_out) = GLOBAL_TELEM_DAT.get().and_then(|gd| gd.logs_out.as_ref()) {
        logs_out.lock().pop_iter().collect()
    } else {
        vec![]
    }
}

#[cfg(test)]
pub mod test_initters {
    use super::*;
    use temporal_sdk_core_api::worker::telemetry::{TelemetryOptionsBuilder, TraceExportConfig};

    #[allow(dead_code)] // Not always used, called to enable for debugging when needed
    pub fn test_telem_console() {
        telemetry_init(
            &TelemetryOptionsBuilder::default()
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
        telemetry_init(
            &TelemetryOptionsBuilder::default()
                .logging(Logger::Console {
                    filter: construct_filter_string(Level::DEBUG, Level::WARN),
                })
                .tracing(TraceExportConfig {
                    filter: construct_filter_string(Level::DEBUG, Level::WARN),
                    exporter: TraceExporter::Otel(OtelCollectorOptions {
                        url: "grpc://localhost:4317".parse().unwrap(),
                        headers: Default::default(),
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
