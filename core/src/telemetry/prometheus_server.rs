use crate::telemetry::default_resource;
use hyper::{
    header::CONTENT_TYPE,
    server::conn::AddrIncoming,
    service::{make_service_fn, service_fn},
    Body, Method, Request, Response, Server,
};
use opentelemetry::sdk::{
    export::metrics::{aggregation::TemporalitySelector, AggregatorSelector},
    metrics::{controllers, processors},
};
use opentelemetry_prometheus::{ExporterBuilder, PrometheusExporter};
use prometheus::{Encoder, TextEncoder};
use std::{collections::HashMap, convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};

/// Exposes prometheus metrics for scraping
pub(super) struct PromServer {
    bound_addr: AddrIncoming,
    pub exporter: Arc<PrometheusExporter>,
}

impl PromServer {
    pub fn new(
        addr: SocketAddr,
        aggregation: impl AggregatorSelector + Send + Sync + 'static,
        temporality: impl TemporalitySelector + Send + Sync + 'static,
        tags: &HashMap<String, String>,
    ) -> Result<Self, anyhow::Error> {
        let controller =
            controllers::basic(processors::factory(aggregation, temporality).with_memory(true))
                // Because Prom is pull-based, make this always refresh
                .with_collect_period(Duration::from_secs(0))
                .with_resource(default_resource(tags))
                .build();
        let exporter = ExporterBuilder::new(controller).try_init()?;
        let bound_addr = AddrIncoming::bind(&addr)?;
        Ok(Self {
            exporter: Arc::new(exporter),
            bound_addr,
        })
    }

    pub async fn run(self) -> hyper::Result<()> {
        // Spin up hyper server to serve metrics for scraping. We use hyper since we already depend
        // on it via Tonic.
        let expclone = self.exporter.clone();
        let svc = make_service_fn(move |_conn| {
            let expclone = expclone.clone();
            async move { Ok::<_, Infallible>(service_fn(move |req| metrics_req(req, expclone.clone()))) }
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
