use once_cell::sync::OnceCell;
use opentelemetry_jaeger::Uninstall;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

static TRACING_INIT: OnceCell<Uninstall> = OnceCell::new();

pub(crate) fn tracing_init() {
    let _ = env_logger::try_init();
    // TRACING_INIT.get_or_init(|| {
    //     let (tracer, uninstall) = opentelemetry_jaeger::new_pipeline()
    //         .with_service_name("coresdk")
    //         .install()
    //         .unwrap();
    //     let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    //     tracing_subscriber::registry()
    //         .with(opentelemetry)
    //         .try_init()
    //         .unwrap();
    //     uninstall
    // });
}
