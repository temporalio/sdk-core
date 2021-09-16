pub(crate) mod metrics;

use crate::{log_export::CoreExportLogger, telemetry::metrics::SDKAggSelector, CoreLog};
use itertools::Itertools;
use log::LevelFilter;
use once_cell::sync::OnceCell;
use opentelemetry::{
    global,
    sdk::{metrics::PushController, trace::Config, Resource},
    util::tokio_interval_stream,
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use std::{collections::VecDeque, time::Duration};
use tracing_subscriber::{layer::SubscriberExt, EnvFilter};
use url::Url;

const TELEM_SERVICE_NAME: &str = "temporal-core-sdk";
const LOG_FILTER_ENV_VAR: &str = "TEMPORAL_TRACING_FILTER";
static DEFAULT_FILTER: &str = "temporal_sdk_core=INFO";
static GLOBAL_TELEM_DAT: OnceCell<GlobalTelemDat> = OnceCell::new();

/// Telemetry configuration options
#[derive(Debug, Clone)]
pub struct TelemetryOptions {
    /// The url of the OTel collector to export telemetry and metrics to. Lang SDK should also
    /// export to this same collector. If unset, telemetry is not exported and tracing data goes
    /// to the console instead.
    pub otel_collector_url: Option<Url>,
    /// A string in the [EnvFilter] format which specifies what tracing data is included in
    /// telemetry, log forwarded to lang, or console output. May be overridden by the
    /// `TEMPORAL_TRACING_FILTER` env variable.
    pub tracing_filter: String,
    /// Core can forward logs to lang for them to be rendered by the user's logging facility.
    /// The logs are somewhat contextually lacking, but still useful in a local test situation when
    /// running only one workflow at a time. This sets the level at which they are filtered.
    /// TRACE level will export span start/stop info as well.
    ///
    /// Default is INFO. If set to `Off`, the mechanism is disabled entirely (saves on perf).
    /// If set to anything besides `Off`, any console output directly from core is disabled.
    pub log_forwarding_level: LevelFilter,
}

impl Default for TelemetryOptions {
    fn default() -> Self {
        Self {
            otel_collector_url: None,
            tracing_filter: DEFAULT_FILTER.to_string(),
            log_forwarding_level: LevelFilter::Info,
        }
    }
}

/// Things that need to not be dropped while telemetry is ongoing
#[derive(Default)]
struct GlobalTelemDat {
    metric_push_controller: Option<PushController>,
    core_export_logger: Option<CoreExportLogger>,
    runtime: Option<tokio::runtime::Runtime>,
    // TODO: Expose prometheus metrics directly when requested
    // prom_exporter: Option<PrometheusExporter>,
}

/// Initialize tracing subscribers and output. Core [crate::init] calls this, but it may be called
/// separately so that tests may choose to initialize tracing differently. If this function is
/// called more than once, subsequent calls do nothing.
///
/// See [TelemetryOptions] docs for more on configuration.
pub(crate) fn telemetry_init(opts: &TelemetryOptions) -> Result<(), anyhow::Error> {
    let opts = opts.clone();
    // TODO: This is insane. Probably move this to be per-core-instance
    std::thread::spawn(move || {
        let res = GLOBAL_TELEM_DAT
            .get_or_try_init::<_, anyhow::Error>(|| {
                let runtime = tokio::runtime::Runtime::new()?;
                let mut globaldat = GlobalTelemDat::default();
                let mut am_forwarding_logs = false;

                if opts.log_forwarding_level != LevelFilter::Off {
                    log::set_max_level(opts.log_forwarding_level);
                    globaldat.core_export_logger =
                        Some(CoreExportLogger::new(opts.log_forwarding_level));
                    am_forwarding_logs = true;
                }

                let filter_layer = EnvFilter::try_from_env(LOG_FILTER_ENV_VAR).or_else(|_| {
                    let filter = if opts.tracing_filter.is_empty() {
                        DEFAULT_FILTER
                    } else {
                        &opts.tracing_filter
                    };
                    EnvFilter::try_new(filter)
                })?;

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

                        // TODO: Expose prometheus metrics directly when requested
                        // let promexport = opentelemetry_prometheus::exporter()
                        //     .with_resource(Resource::new(vec![]))
                        //     .init();
                        // globaldat.prom_exporter = Some(promexport);
                        //
                        // tokio::spawn(async {
                        //     let encoder = TextEncoder::new();
                        //     loop {
                        //         tokio::time::sleep(Duration::from_millis(100));
                        //         if let Some(gt) = GLOBAL_TELEM_DAT.get() {
                        //             let mf = gt.prom_exporter.as_ref().unwrap().registry().gather();
                        //             let mut buffer = vec![];
                        //             encoder.encode(&mf, &mut buffer).unwrap();
                        //             dbg!(String::from_utf8(buffer));
                        //         }
                        //     }
                        // });

                        let metrics = opentelemetry_otlp::new_pipeline()
                            .metrics(|f| runtime.spawn(f), tokio_interval_stream)
                            .with_aggregator_selector(SDKAggSelector)
                            .with_period(Duration::from_secs(1))
                            .with_exporter(
                                // No joke exporter builder literally not cloneable for some insane reason
                                opentelemetry_otlp::new_exporter()
                                    .tonic()
                                    .with_endpoint(otel_url.to_string()),
                            )
                            .build()?;
                        global::set_meter_provider(metrics.provider());
                        globaldat.metric_push_controller = Some(metrics);

                        // TODO: Need https://github.com/tokio-rs/tracing/pull/1523 to land in order to filter
                        //   console output and telemetry output differently. For now we'll assume if telemetry
                        //   is on then we prefer that over outputting tracing data to the console.
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
            })
            .expect("Telem init works");

        if let Some(loggr) = &res.core_export_logger {
            // If telem init is called twice for some reason, this would error, so just ignore that.
            let _ = log::set_logger(loggr);
        }
    })
    .join()
    .expect("Aahhhhh");

    Ok(())
}

/// Returned buffered logs for export to lang from the global logging instance
pub(crate) fn fetch_global_buffered_logs() -> Vec<CoreLog> {
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
    })
    .unwrap()
}

#[allow(dead_code)] // Not always used, called to enable for debugging when needed
#[cfg(test)]
pub(crate) fn test_telem_collector() {
    telemetry_init(&TelemetryOptions {
        otel_collector_url: Some("grpc://localhost:4317".parse().unwrap()),
        tracing_filter: "temporal_sdk_core=DEBUG".to_string(),
        log_forwarding_level: LevelFilter::Off,
    })
    .unwrap()
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
