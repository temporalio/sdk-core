use itertools::Itertools;
use std::{collections::VecDeque, sync::Once};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const ENABLE_OPENTELEM_ENV_VAR: &str = "TEMPORAL_ENABLE_OPENTELEMETRY";
static TRACING_INIT: Once = Once::new();

/// Initialize tracing subscribers and output. Core will not call this itself, it exists here so
/// that consumers and tests have an easy way to initialize tracing.
pub fn tracing_init() {
    TRACING_INIT.call_once(|| {
        let opentelem_on = std::env::var(ENABLE_OPENTELEM_ENV_VAR).is_ok();

        let filter_layer = EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new("info"))
            .unwrap();
        let pretty_fmt = tracing_subscriber::fmt::format()
            .pretty()
            .with_source_location(false);

        if opentelem_on {
            // Not all low-down unit tests use Tokio
            #[cfg(test)]
            let tracer = opentelemetry_jaeger::new_pipeline()
                .with_service_name("coresdk")
                .install_simple()
                .unwrap();

            #[cfg(not(test))]
            let tracer = opentelemetry_jaeger::new_pipeline()
                .with_service_name("coresdk")
                .install_batch(opentelemetry::runtime::Tokio)
                .unwrap();

            let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

            tracing_subscriber::registry()
                .with(opentelemetry)
                .with(filter_layer)
                .with(
                    tracing_subscriber::fmt::layer()
                        .with_target(false)
                        .event_format(pretty_fmt),
                )
                .try_init()
                .unwrap();
        } else {
            // Because these types don't compose nicely we must repeat ourselves in these branches
            let reg = tracing_subscriber::registry().with(filter_layer).with(
                tracing_subscriber::fmt::layer()
                    .with_target(false)
                    .event_format(pretty_fmt),
            );
            tracing::subscriber::set_global_default(reg).unwrap();
        };
    });
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
