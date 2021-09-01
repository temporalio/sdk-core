mod lang_exporter;
pub(crate) mod metrics;

use crate::telemetry::lang_exporter::{LangSpanExporter, OTelExportStreams};
use itertools::Itertools;
use opentelemetry::sdk::trace::Config;
use opentelemetry::sdk::Resource;
use opentelemetry::{
    global, sdk::trace::TracerProvider, trace::TracerProvider as TracerProviderTrait, KeyValue,
};
use std::{collections::VecDeque, sync::Once};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const TELEM_SERVICE_NAME: &str = "temporal-core-sdk";
const ENABLE_OPENTELEM_ENV_VAR: &str = "TEMPORAL_ENABLE_OPENTELEMETRY";
static TRACING_INIT: Once = Once::new();

/// Initialize tracing subscribers and output. Core will not call this itself, it exists here so
/// that consumers and tests have an easy way to initialize tracing.
///
/// If [ENABLE_OPENTELEM_ENV_VAR] is set, it's value will be used following the standard [EnvFilter]
/// pattern for filtering opentelem output. If it is *not* set, the standard `RUST_LOG` env var
/// is used for filtering console output.
pub fn telemetry_init() -> Result<OTelExportStreams, anyhow::Error> {
    let mut retme = Err(anyhow::anyhow!("Telemetry already initialized"));
    TRACING_INIT.call_once(|| {
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
            let mut export_streams = OTelExportStreams::default();
            // Set up our custom exporters
            let (trace_export, trace_rx) = LangSpanExporter::new(10);
            export_streams.tracing = Some(trace_rx);

            let tracer_cfg = Config::default().with_resource(Resource::new(vec![KeyValue::new(
                "service.name",
                TELEM_SERVICE_NAME,
            )]));
            let tracer_provider = TracerProvider::builder()
                .with_batch_exporter(trace_export, opentelemetry::runtime::TokioCurrentThread)
                .with_config(tracer_cfg)
                .build();
            let tracer = tracer_provider.tracer("temporal-core-sdk-tracer", None);

            // TODO: Metrics pipeline

            retme = Ok(export_streams);

            global::set_tracer_provider(tracer_provider);
            let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

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
    });
    retme
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
