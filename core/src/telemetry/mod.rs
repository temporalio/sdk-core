pub(crate) mod metrics;
mod prometheus_server;

use crate::{
    log_export::CoreExportLogger,
    telemetry::{metrics::SDKAggSelector, prometheus_server::PromServer},
    CoreLog, METRIC_METER,
};
use itertools::Itertools;
use log::LevelFilter;
use once_cell::sync::OnceCell;
use opentelemetry::{
    global,
    metrics::Meter,
    sdk::{metrics::PushController, trace::Config, Resource},
    util::tokio_interval_stream,
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use parking_lot::{const_mutex, Mutex};
use std::{
    collections::{HashMap, VecDeque},
    convert::TryInto,
    net::SocketAddr,
    time::Duration,
};
use temporal_sdk_core_api::CoreTelemetry;
use tracing_subscriber::{filter::ParseError, layer::SubscriberExt, EnvFilter};
use url::Url;

const TELEM_SERVICE_NAME: &str = "temporal-core-sdk";
static DEFAULT_FILTER: &str = "temporal_sdk_core=INFO";
static GLOBAL_TELEM_DAT: OnceCell<GlobalTelemDat> = OnceCell::new();
static TELETM_MUTEX: Mutex<()> = const_mutex(());

fn default_resource_kvs() -> &'static [KeyValue] {
    static INSTANCE: OnceCell<[KeyValue; 1]> = OnceCell::new();
    INSTANCE.get_or_init(|| [KeyValue::new("service.name", TELEM_SERVICE_NAME)])
}
fn default_resource() -> Resource {
    Resource::new(default_resource_kvs().iter().cloned())
}

/// Options for exporting to an OpenTelemetry Collector
#[derive(Debug, Clone)]
pub struct OtelCollectorOptions {
    /// The url of the OTel collector to export telemetry and metrics to. Lang SDK should also
    /// export to this same collector.
    pub url: Url,
    /// Optional set of HTTP headers to send to the Collector, e.g for authentication.
    pub headers: HashMap<String, String>,
}

/// Control where traces are exported
#[derive(Debug, Clone)]
pub enum TraceExporter {
    /// Export traces to an OpenTelemetry Collector <https://opentelemetry.io/docs/collector/>.
    Otel(OtelCollectorOptions),
}

/// Control where metrics are exported
#[derive(Debug, Clone)]
pub enum MetricsExporter {
    /// Export metrics to an OpenTelemetry Collector <https://opentelemetry.io/docs/collector/>.
    Otel(OtelCollectorOptions),
    /// Expose metrics directly via an embedded http server bound to the provided address.
    Prometheus(SocketAddr),
}

/// Control where logs go
#[derive(Debug, Clone)]
pub enum Logger {
    /// Log directly to console.
    Console,
    /// Forward logs to Lang - collectable with `fetch_global_buffered_logs`.
    Forward(LevelFilter),
}

/// Telemetry configuration options. Construct with [TelemetryOptionsBuilder]
#[derive(Debug, Clone, derive_builder::Builder)]
#[non_exhaustive]
pub struct TelemetryOptions {
    /// A string in the [EnvFilter] format which specifies what tracing data is included in
    /// telemetry, log forwarded to lang, or console output. May be overridden by the
    /// `TEMPORAL_TRACING_FILTER` env variable.
    #[builder(default = "DEFAULT_FILTER.to_string()")]
    pub tracing_filter: String,

    /// Optional trace exporter - set as None to disable.
    #[builder(setter(into, strip_option), default)]
    pub tracing: Option<TraceExporter>,
    /// Optional logger - set as None to disable.
    #[builder(setter(into, strip_option), default)]
    pub logging: Option<Logger>,
    /// Optional metrics exporter - set as None to disable.
    #[builder(setter(into, strip_option), default)]
    pub metrics: Option<MetricsExporter>,

    /// If set true, do not prefix metrics with `temporal_`. Will be removed eventually as
    /// the prefix is consistent with other SDKs.
    #[builder(default)]
    pub no_temporal_prefix_for_metrics: bool,
}

impl TelemetryOptions {
    /// Construct an [EnvFilter] from given `tracing_filter`.
    pub fn try_get_env_filter(&self) -> Result<EnvFilter, ParseError> {
        EnvFilter::try_new(if self.tracing_filter.is_empty() {
            DEFAULT_FILTER
        } else {
            &self.tracing_filter
        })
    }
}

impl Default for TelemetryOptions {
    fn default() -> Self {
        TelemetryOptionsBuilder::default().build().unwrap()
    }
}

/// Things that need to not be dropped while telemetry is ongoing
#[derive(Default)]
pub struct GlobalTelemDat {
    metric_push_controller: Option<PushController>,
    core_export_logger: Option<CoreExportLogger>,
    runtime: Option<tokio::runtime::Runtime>,
    prom_srv: Option<PromServer>,
    no_temporal_prefix_for_metrics: bool,
}

impl GlobalTelemDat {
    fn init(&'static self) {
        if let Some(loggr) = &self.core_export_logger {
            let _ = log::set_logger(loggr);
        }
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
    // TODO: Per-layer filtering has been implemented but does not yet support
    //   env-filter. When it does, allow filtering logs/telemetry separately.

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

            if let Some(ref logger) = opts.logging {
                match logger {
                    Logger::Console => {
                        // TODO: this is duplicated below and is quite ugly, remove the duplication
                        if opts.tracing.is_none() {
                            let pretty_fmt = tracing_subscriber::fmt::format()
                                .pretty()
                                .with_source_location(false);
                            let reg = tracing_subscriber::registry()
                                .with((&opts).try_get_env_filter()?)
                                .with(
                                    tracing_subscriber::fmt::layer()
                                        .with_target(false)
                                        .event_format(pretty_fmt),
                                );
                            tracing::subscriber::set_global_default(reg)?;
                        }
                    }
                    Logger::Forward(filter) => {
                        log::set_max_level(*filter);
                        globaldat.core_export_logger = Some(CoreExportLogger::new(*filter));
                    }
                };
            };

            if let Some(ref metrics) = opts.metrics {
                match metrics {
                    MetricsExporter::Prometheus(addr) => {
                        let srv = PromServer::new(*addr)?;
                        globaldat.prom_srv = Some(srv);
                    }
                    MetricsExporter::Otel(OtelCollectorOptions { url, headers }) => {
                        runtime.block_on(async {
                            let metrics = opentelemetry_otlp::new_pipeline()
                                .metrics(|f| runtime.spawn(f), tokio_interval_stream)
                                .with_aggregator_selector(SDKAggSelector)
                                .with_period(Duration::from_secs(1))
                                .with_resource(default_resource_kvs().iter().cloned())
                                .with_exporter(
                                    // No joke exporter builder literally not cloneable for some insane
                                    // reason
                                    opentelemetry_otlp::new_exporter()
                                        .tonic()
                                        .with_endpoint(url.to_string())
                                        .with_metadata(
                                            tonic_otel::metadata::MetadataMap::from_headers(
                                                headers.try_into()?,
                                            ),
                                        ),
                                )
                                .build()?;
                            global::set_meter_provider(metrics.provider());
                            globaldat.metric_push_controller = Some(metrics);
                            Result::<(), anyhow::Error>::Ok(())
                        })?;
                    }
                };
            };

            if let Some(ref tracing) = opts.tracing {
                match tracing {
                    TraceExporter::Otel(OtelCollectorOptions { url, headers }) => {
                        runtime.block_on(async {
                            let tracer_cfg = Config::default().with_resource(default_resource());
                            let tracer = opentelemetry_otlp::new_pipeline()
                                .tracing()
                                .with_exporter(
                                    opentelemetry_otlp::new_exporter()
                                        .tonic()
                                        .with_endpoint(url.to_string())
                                        .with_metadata(
                                            tonic_otel::metadata::MetadataMap::from_headers(
                                                headers.try_into()?,
                                            ),
                                        ),
                                )
                                .with_trace_config(tracer_cfg)
                                .install_batch(opentelemetry::runtime::Tokio)?;

                            let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

                            // TODO: remove all of this duplicate code
                            if let Some(Logger::Console) = opts.logging {
                                let pretty_fmt = tracing_subscriber::fmt::format()
                                    .pretty()
                                    .with_source_location(false);
                                let reg = tracing_subscriber::registry()
                                    .with(opentelemetry)
                                    .with(opts.try_get_env_filter()?)
                                    .with(
                                        tracing_subscriber::fmt::layer()
                                            .with_target(false)
                                            .event_format(pretty_fmt),
                                    );
                                // Can't use try_init here as it will blow away our custom logger if we do
                                tracing::subscriber::set_global_default(reg)?;
                            } else {
                                let reg = tracing_subscriber::registry()
                                    .with(opentelemetry)
                                    .with(opts.try_get_env_filter()?);
                                // Can't use try_init here as it will blow away our custom logger if we do
                                tracing::subscriber::set_global_default(reg)?;
                            }
                            Result::<(), anyhow::Error>::Ok(())
                        })?;
                    }
                };
            };

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
    if let Some(loggr) = GLOBAL_TELEM_DAT
        .get()
        .and_then(|gd| gd.core_export_logger.as_ref())
    {
        loggr.drain()
    } else {
        vec![]
    }
}

#[allow(dead_code)] // Not always used, called to enable for debugging when needed
#[cfg(test)]
pub(crate) fn test_telem_console() {
    telemetry_init(&TelemetryOptions {
        tracing_filter: "temporal_sdk_core=DEBUG,temporal_sdk=DEBUG".to_string(),
        logging: Some(Logger::Console),
        tracing: None,
        metrics: None,
        no_temporal_prefix_for_metrics: false,
    })
    .unwrap();
}

#[allow(dead_code)] // Not always used, called to enable for debugging when needed
#[cfg(test)]
pub(crate) fn test_telem_collector() {
    telemetry_init(&TelemetryOptions {
        tracing_filter: "temporal_sdk_core=DEBUG,temporal_sdk=DEBUG".to_string(),
        logging: Some(Logger::Console),
        tracing: Some(TraceExporter::Otel(OtelCollectorOptions {
            url: "grpc://localhost:4317".parse().unwrap(),
            headers: Default::default(),
        })),
        metrics: None,
        no_temporal_prefix_for_metrics: false,
    })
    .unwrap();
}

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
