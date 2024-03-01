use http_body_util::Full;
use hyper::{body::Bytes, header::CONTENT_TYPE, service::service_fn, Method, Request, Response};
use hyper_util::{
    rt::{TokioExecutor, TokioIo},
    server::conn::auto,
};
use opentelemetry_prometheus::PrometheusExporter;
use opentelemetry_sdk::metrics::reader::AggregationSelector;
use prometheus::{Encoder, Registry, TextEncoder};
use std::net::SocketAddr;
use temporal_sdk_core_api::telemetry::PrometheusExporterOptions;
use tokio::net::TcpListener;

/// Exposes prometheus metrics for scraping
pub(super) struct PromServer {
    socket_addr: SocketAddr,
    registry: Registry,
}

impl PromServer {
    pub fn new(
        opts: &PrometheusExporterOptions,
        aggregation: impl AggregationSelector + Send + Sync + 'static,
    ) -> Result<(Self, PrometheusExporter), anyhow::Error> {
        let registry = Registry::new();
        let exporter = opentelemetry_prometheus::exporter()
            .with_aggregation_selector(aggregation)
            .without_scope_info()
            .with_registry(registry.clone());
        let exporter = if !opts.counters_total_suffix {
            exporter.without_counter_suffixes()
        } else {
            exporter
        };
        let exporter = if !opts.unit_suffix {
            exporter.without_units()
        } else {
            exporter
        };
        Ok((
            Self {
                socket_addr: opts.socket_addr,
                registry,
            },
            exporter.build()?,
        ))
    }

    pub async fn run(self) -> Result<(), anyhow::Error> {
        // Spin up hyper server to serve metrics for scraping. We use hyper since we already depend
        // on it via Tonic.
        loop {
            let listener = TcpListener::bind(self.socket_addr).await?;
            let (stream, _) = listener.accept().await?;
            let io = TokioIo::new(stream);
            let regclone = self.registry.clone();
            tokio::task::spawn(async move {
                let server = auto::Builder::new(TokioExecutor::new());
                if let Err(e) = server
                    .serve_connection(
                        io,
                        service_fn(move |req| metrics_req(req, regclone.clone())),
                    )
                    .await
                {
                    warn!("Error serving metrics connection: {:?}", e);
                }
            });
        }
    }

    pub fn bound_addr(&self) -> SocketAddr {
        self.socket_addr
    }
}

/// Serves prometheus metrics in the expected format for scraping
async fn metrics_req(
    req: Request<hyper::body::Incoming>,
    registry: Registry,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let response = match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            let mut buffer = vec![];
            let encoder = TextEncoder::new();
            let metric_families = registry.gather();
            encoder.encode(&metric_families, &mut buffer).unwrap();

            Response::builder()
                .status(200)
                .header(CONTENT_TYPE, encoder.format_type())
                .body(buffer.into())
                .unwrap()
        }
        _ => Response::builder()
            .status(404)
            .body(vec![].into())
            .expect("Can't fail to construct empty resp"),
    };
    Ok(response)
}
