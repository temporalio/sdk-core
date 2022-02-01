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
use std::{collections::VecDeque, net::SocketAddr, time::Duration};
use temporal_sdk_core_api::CoreTelemetry;
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};
use url::Url;

const TELEM_SERVICE_NAME: &str = "temporal-core-sdk";
const LOG_FILTER_ENV_VAR: &str = "TEMPORAL_TRACING_FILTER";
static DEFAULT_FILTER: &str = "temporal_sdk_core=INFO";
static GLOBAL_TELEM_DAT: OnceCell<GlobalTelemDat> = OnceCell::new();
static TELETM_MUTEX: Mutex<()> = const_mutex(());

/// Telemetry configuration options. Construct with [TelemetryOptionsBuilder]
#[derive(Debug, Clone, derive_builder::Builder)]
#[non_exhaustive]
pub struct TelemetryOptions {
    /// The url of the OTel collector to export telemetry and metrics to. Lang SDK should also
    /// export to this same collector. If unset, telemetry is not exported and tracing data goes
    /// to the console instead.
    #[builder(setter(into, strip_option), default)]
    pub otel_collector_url: Option<Url>,
    /// A string in the [EnvFilter] format which specifies what tracing data is included in
    /// telemetry, log forwarded to lang, or console output. May be overridden by the
    /// `TEMPORAL_TRACING_FILTER` env variable.
    #[builder(default = "DEFAULT_FILTER.to_string()")]
    pub tracing_filter: String,
    /// Core can forward logs to lang for them to be rendered by the user's logging facility.
    /// The logs are somewhat contextually lacking, but still useful in a local test situation when
    /// running only one workflow at a time. This sets the level at which they are filtered.
    /// TRACE level will export span start/stop info as well.
    ///
    /// Default is INFO. If set to `Off`, the mechanism is disabled entirely (saves on perf).
    /// If set to anything besides `Off`, any console output directly from core is disabled.
    #[builder(setter(into), default = "LevelFilter::Info")]
    pub log_forwarding_level: LevelFilter,
    /// If set, prometheus metrics will be exposed directly via an embedded http server bound to
    /// the provided address. Useful if users would like to collect metrics with prometheus but
    /// do not want to run an OTel collector. **Note**: If this is set metrics will *not* be sent
    /// to the OTel collector if it is also set, only traces will be.
    #[builder(setter(into, strip_option), default)]
    pub prometheus_export_bind_address: Option<SocketAddr>,
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
            let mut globaldat = GlobalTelemDat::default();
            let am_forwarding_logs = opts.log_forwarding_level != LevelFilter::Off;

            if am_forwarding_logs {
                log::set_max_level(opts.log_forwarding_level);
                globaldat.core_export_logger =
                    Some(CoreExportLogger::new(opts.log_forwarding_level));
            }

            let filter_layer = EnvFilter::try_from_env(LOG_FILTER_ENV_VAR).or_else(|_| {
                let filter = if opts.tracing_filter.is_empty() {
                    DEFAULT_FILTER
                } else {
                    &opts.tracing_filter
                };
                EnvFilter::try_new(filter)
            })?;

            if let Some(addr) = opts.prometheus_export_bind_address.as_ref() {
                let srv = PromServer::new(*addr)?;
                globaldat.prom_srv = Some(srv);
            };

            if let Some(otel_url) = opts.otel_collector_url.as_ref() {
                runtime.block_on(async {
                    let tracer_cfg =
                        Config::default().with_resource(Resource::new(vec![KeyValue::new(
                            "service.name",
                            TELEM_SERVICE_NAME,
                        )]));
                    let tracer = opentelemetry_otlp::new_pipeline()
                        .tracing()
                        .with_exporter(
                            opentelemetry_otlp::new_exporter()
                                .tonic()
                                .with_endpoint(otel_url.to_string()),
                        )
                        .with_trace_config(tracer_cfg)
                        .install_batch(opentelemetry::runtime::Tokio)?;

                    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

                    if globaldat.prom_srv.is_none() {
                        let metrics = opentelemetry_otlp::new_pipeline()
                            .metrics(|f| runtime.spawn(f), tokio_interval_stream)
                            .with_aggregator_selector(SDKAggSelector)
                            .with_period(Duration::from_secs(1))
                            .with_exporter(
                                // No joke exporter builder literally not cloneable for some insane
                                // reason
                                opentelemetry_otlp::new_exporter()
                                    .tonic()
                                    .with_endpoint(otel_url.to_string()),
                            )
                            .build()?;
                        global::set_meter_provider(metrics.provider());
                        globaldat.metric_push_controller = Some(metrics);
                    }

                    let reg = tracing_subscriber::registry()
                        .with(opentelemetry)
                        .with(filter_layer);
                    // Can't use try_init here as it will blow away our custom logger if we do
                    tracing::subscriber::set_global_default(reg)?;
                    Result::<(), anyhow::Error>::Ok(())
                })?;
            } else if !am_forwarding_logs {
                let pretty_fmt = tracing_subscriber::fmt::format()
                    .pretty()
                    .with_source_location(false);
                let reg = tracing_subscriber::registry().with(filter_layer).with(
                    tracing_subscriber::fmt::layer()
                        .with_target(false)
                        .event_format(pretty_fmt),
                );
                tracing::subscriber::set_global_default(reg)?;
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
        otel_collector_url: None,
        tracing_filter: "temporal_sdk_core=DEBUG".to_string(),
        log_forwarding_level: LevelFilter::Off,
        prometheus_export_bind_address: None,
    })
    .unwrap();
}

#[allow(dead_code)] // Not always used, called to enable for debugging when needed
#[cfg(test)]
pub(crate) fn test_telem_collector() {
    telemetry_init(&TelemetryOptions {
        otel_collector_url: Some("grpc://localhost:4317".parse().unwrap()),
        tracing_filter: "temporal_sdk_core=DEBUG".to_string(),
        log_forwarding_level: LevelFilter::Off,
        prometheus_export_bind_address: None,
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
