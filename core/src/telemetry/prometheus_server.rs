use crate::telemetry::metrics::{SDKAggSelector, DEFAULT_MS_BUCKETS};
use hyper::{
    header::CONTENT_TYPE,
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server,
};
use opentelemetry::{
    global,
    metrics::MetricsError,
    sdk::{export::metrics::ExportKindSelector, metrics::controllers},
};
use opentelemetry_prometheus::PrometheusExporter;
use prometheus::{Encoder, TextEncoder};
use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};

/// Exposes prometheus metrics for scraping
pub(super) struct PromServer {
    addr: SocketAddr,
    pub exporter: Arc<PrometheusExporter>,
}

impl PromServer {
    pub fn new(addr: SocketAddr) -> Result<Self, MetricsError> {
        let exporter = create_exporter()?;
        Ok(Self {
            exporter: Arc::new(exporter),
            addr,
        })
    }

    pub async fn run(&self) -> hyper::Result<()> {
        // Spin up hyper server to serve metrics for scraping. We use hyper since we already depend
        // on it via Tonic.
        let expclone = self.exporter.clone();
        let svc = make_service_fn(move |_conn| {
            let expclone = expclone.clone();
            async move { Ok::<_, Infallible>(service_fn(move |req| metrics_req(req, expclone.clone()))) }
        });
        let server = Server::bind(&self.addr).serve(svc);
        server.await
    }
}

/// Serves prometheus metrics in the expected format for scraping
async fn metrics_req(
    req: Request<Body>,
    exporter: Arc<PrometheusExporter>,
) -> Result<Response<Body>, hyper::Error> {
    let response = match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            let mut buffer = vec![];
            let encoder = TextEncoder::new();
            let metric_families = exporter.registry().gather();
            encoder.encode(&metric_families, &mut buffer).unwrap();

            Response::builder()
                .status(200)
                .header(CONTENT_TYPE, encoder.format_type())
                .body(Body::from(buffer))
                .unwrap()
        }
        _ => Response::builder()
            .status(404)
            .body(Body::empty())
            .expect("Can't fail to construct empty resp"),
    };
    Ok(response)
}

/// There is no easy way to customize the aggregation selector for the prom exporter so this
/// code is some dupe of the exporter's try_init code.
pub fn create_exporter() -> Result<PrometheusExporter, MetricsError> {
    let controller_builder = controllers::pull(
        Box::new(SDKAggSelector),
        Box::new(ExportKindSelector::Cumulative),
    )
    .with_cache_period(Duration::from_secs(0))
    .with_memory(true);
    let controller = controller_builder.build();

    global::set_meter_provider(controller.provider());

    // No choice bro https://github.com/open-telemetry/opentelemetry-rust/issues/633
    #[allow(deprecated)]
    PrometheusExporter::new(
        prometheus::Registry::new(),
        controller,
        DEFAULT_MS_BUCKETS.to_vec(),
        DEFAULT_MS_BUCKETS.to_vec(),
        // Host and port aren't actually used for anything
        "".to_string(),
        0,
    )
}
