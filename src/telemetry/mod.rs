pub(crate) mod metrics;

use itertools::Itertools;
use once_cell::sync::OnceCell;
use opentelemetry::{
    global,
    sdk::{metrics::PushController, trace::Config, Resource},
    util::tokio_interval_stream,
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use std::{collections::VecDeque, time::Duration};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use url::Url;

const TELEM_SERVICE_NAME: &str = "temporal-core-sdk";
const LOG_FILTER_ENV_VAR: &str = "TEMPORAL_TRACING_FILTER";
static TRACING_INIT: OnceCell<Result<GlobalTracingDats, anyhow::Error>> = OnceCell::new();

/// Telemetry configuration options
#[derive(Debug, Clone)]
pub struct TelemetryOptions {
    /// The url of the OTel collector to export telemetry and metrics to. Lang SDK should also
    /// export to this same collector. If unset, telemetry is not exported and tracing data goes
    /// to the console instead.
    pub otel_collector_url: Option<Url>,
    /// A string in the [EnvFilter] format which specifies what tracing data is included in
    /// telemetry or console output. May be overridden by the `TEMPORAL_TRACING_FILTER` env
    /// variable.
    pub tracing_filter: String,
}

impl Default for TelemetryOptions {
    fn default() -> Self {
        Self {
            otel_collector_url: None,
            tracing_filter: "temporal_sdk_core=INFO".to_string(),
        }
    }
}

/// Things that need to not be dropped while telemetry is ongoing
#[derive(Default)]
struct GlobalTracingDats {
    metric_push_controller: Option<PushController>,
}

/// Initialize tracing subscribers and output. Core [crate::init] calls this, but it may be called
/// separately so that tests may choose to initialize tracing differently. If this function is
/// called more than once, subsequent calls do nothing.
///
/// See [TelemetryOptions] docs for more on configuration.
pub(crate) fn telemetry_init(opts: &TelemetryOptions) -> Result<(), anyhow::Error> {
    TRACING_INIT.get_or_init(|| {
        let mut globaldat = GlobalTracingDats::default();

        let filter_layer = EnvFilter::try_from_env(LOG_FILTER_ENV_VAR)
            .or_else(|_| EnvFilter::try_new(&opts.tracing_filter))?;

        if let Some(otel_url) = opts.otel_collector_url.as_ref() {
            let tracer_cfg = Config::default().with_resource(Resource::new(vec![KeyValue::new(
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

            let metrics = opentelemetry_otlp::new_pipeline()
                .metrics(tokio::spawn, tokio_interval_stream)
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
            tracing_subscriber::registry()
                .with(opentelemetry)
                .with(filter_layer)
                .try_init()?;
        } else {
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
        Ok(globaldat)
    });

    Ok(())
}

#[allow(dead_code)] // Not always used, called to enable for debugging when needed
#[cfg(test)]
pub(crate) fn test_telem() {
    telemetry_init(&TelemetryOptions {
        otel_collector_url: None,
        tracing_filter: "temporal_sdk_core=DEBUG".to_string(),
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
