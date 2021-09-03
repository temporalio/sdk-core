pub(crate) mod metrics;

use itertools::Itertools;
use once_cell::sync::OnceCell;
use opentelemetry::sdk::trace::Config;
use opentelemetry::sdk::Resource;
use opentelemetry::{global, sdk::metrics::PushController, util::tokio_interval_stream, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use std::collections::VecDeque;
use std::time::Duration;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const TELEM_SERVICE_NAME: &str = "temporal-core-sdk";
const ENABLE_OPENTELEM_ENV_VAR: &str = "TEMPORAL_ENABLE_OPENTELEMETRY";
static TRACING_INIT: OnceCell<GlobalTracingDats> = OnceCell::new();

/// Things that need to not be dropped while telemetry is ongoing
#[derive(Default)]
struct GlobalTracingDats {
    metric_push_controller: Option<PushController>,
}

/// Initialize tracing subscribers and output. Core will not call this itself, it exists here so
/// that consumers and tests have an easy way to initialize tracing.
///
/// If [ENABLE_OPENTELEM_ENV_VAR] is set, it's value will be used following the standard [EnvFilter]
/// pattern for filtering opentelem output. If it is *not* set, the standard `RUST_LOG` env var
/// is used for filtering console output.
pub fn telemetry_init() -> Result<(), anyhow::Error> {
    TRACING_INIT.get_or_init(|| {
        let mut globaldat = GlobalTracingDats::default();
        let opentelem_on = std::env::var(ENABLE_OPENTELEM_ENV_VAR).is_ok();
        let filter_env_var = if opentelem_on {
            ENABLE_OPENTELEM_ENV_VAR
        } else {
            EnvFilter::DEFAULT_ENV
        };

        let filter_layer = EnvFilter::try_from_env(filter_env_var)
            .or_else(|_| EnvFilter::try_new("info"))
            .unwrap();

        if opentelem_on {
            let tracer_cfg = Config::default().with_resource(Resource::new(vec![KeyValue::new(
                "service.name",
                TELEM_SERVICE_NAME,
            )]));
            let tracer = opentelemetry_otlp::new_pipeline()
                .tracing()
                .with_exporter(
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_endpoint("http://localhost:4317"),
                )
                .with_trace_config(tracer_cfg)
                .install_batch(opentelemetry::runtime::Tokio)
                .unwrap();

            let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

            let metrics = opentelemetry_otlp::new_pipeline()
                .metrics(tokio::spawn, |dur| tokio_interval_stream(dur))
                .with_period(Duration::from_secs(1))
                .with_exporter(
                    // No joke exporter builder literally not cloneable for some insane reason
                    opentelemetry_otlp::new_exporter()
                        .tonic()
                        .with_endpoint("http://localhost:4317"),
                )
                .build()
                .unwrap();
            global::set_meter_provider(metrics.provider());
            globaldat.metric_push_controller = Some(metrics);

            // TODO: Need https://github.com/tokio-rs/tracing/pull/1523 to land in order to filter
            //   console output and telemetry output differently. For now we'll assume if telemetry
            //   is on then we're forwarding everything to lang and it will puke out logs
            tracing_subscriber::registry()
                .with(opentelemetry)
                .with(filter_layer)
                .try_init()
                .unwrap();
        } else {
            let pretty_fmt = tracing_subscriber::fmt::format()
                .pretty()
                .with_source_location(false);
            let reg = tracing_subscriber::registry().with(filter_layer).with(
                tracing_subscriber::fmt::layer()
                    .with_target(false)
                    .event_format(pretty_fmt),
            );
            tracing::subscriber::set_global_default(reg).unwrap();
        };
        globaldat
    });
    Ok(())
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
