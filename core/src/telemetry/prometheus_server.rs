use hyper::{
    header::CONTENT_TYPE,
    server::conn::AddrIncoming,
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server,
};
use opentelemetry_prometheus::PrometheusExporter;
use opentelemetry_sdk::metrics::reader::AggregationSelector;
use prometheus::{Encoder, Registry, TextEncoder};
use std::{convert::Infallible, net::SocketAddr};

/// Exposes prometheus metrics for scraping
pub(super) struct PromServer {
    bound_addr: AddrIncoming,
    registry: Registry,
}

impl PromServer {
    pub fn new(
        addr: SocketAddr,
        aggregation: impl AggregationSelector + Send + Sync + 'static,
    ) -> Result<(Self, PrometheusExporter), anyhow::Error> {
        let registry = Registry::new();
        let exporter = opentelemetry_prometheus::exporter()
            .with_aggregation_selector(aggregation)
            .without_target_info()
            .without_scope_info()
            .with_registry(registry.clone());
        let bound_addr = AddrIncoming::bind(&addr)?;
        Ok((
            Self {
                bound_addr,
                registry,
            },
            exporter.build()?,
        ))
    }

    pub async fn run(self) -> hyper::Result<()> {
        // Spin up hyper server to serve metrics for scraping. We use hyper since we already depend
        // on it via Tonic.
        let regclone = self.registry.clone();
        let svc = make_service_fn(move |_conn| {
            let regclone = regclone.clone();
            async move { Ok::<_, Infallible>(service_fn(move |req| metrics_req(req, regclone.clone()))) }
        });
        let server = Server::builder(self.bound_addr).serve(svc);
        server.await
    }

    pub fn bound_addr(&self) -> SocketAddr {
        self.bound_addr.local_addr()
    }
}

/// Serves prometheus metrics in the expected format for scraping
async fn metrics_req(
    req: Request<Body>,
    registry: Registry,
) -> Result<Response<Body>, hyper::Error> {
    let response = match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            let mut buffer = vec![];
            let encoder = TextEncoder::new();
            let metric_families = registry.gather();
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
