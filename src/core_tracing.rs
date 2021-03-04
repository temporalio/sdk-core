use once_cell::sync::OnceCell;
use opentelemetry_jaeger::Uninstall;
use std::collections::VecDeque;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

static TRACING_INIT: OnceCell<Uninstall> = OnceCell::new();

/// Initialize tracing subscribers and output. Core will not call this itself, it exists here so
/// that consumers and tests have an easy way to initialize tracing.
pub(crate) fn tracing_init() {
    TRACING_INIT.get_or_init(|| {
        let (tracer, uninstall) = opentelemetry_jaeger::new_pipeline()
            .with_service_name("coresdk")
            .install()
            .unwrap();
        let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
        let filter_layer = EnvFilter::try_from_default_env()
            .or_else(|_| EnvFilter::try_new("info"))
            .unwrap();
        let fmt_layer = tracing_subscriber::fmt::layer().with_target(false).pretty();

        tracing_subscriber::registry()
            .with(opentelemetry)
            .with(filter_layer)
            .with(fmt_layer)
            .try_init()
            .unwrap();
        uninstall
    });
}

// TODO: Dislike this.
pub(crate) trait VecDisplayer {
    fn display(&self) -> String;
}

impl<T> VecDisplayer for Vec<T>
where
    T: std::fmt::Display,
{
    fn display(&self) -> String {
        let mut r = "[".to_string();
        for i in self {
            r = r + &format!("{}, ", i);
        }
        r += "]";
        r
    }
}

impl<T> VecDisplayer for VecDeque<T>
where
    T: std::fmt::Display,
{
    fn display(&self) -> String {
        let mut r = "[".to_string();
        for i in self {
            r = r + &format!("{}, ", i);
        }
        r += "]";
        r
    }
}
