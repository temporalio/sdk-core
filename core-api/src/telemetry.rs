use opentelemetry::metrics::Meter;
use std::{
    collections::HashMap,
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing_core::Level;
use url::Url;

/// Each core runtime instance has a telemetry subsystem associated with it, this trait defines the
/// operations that lang might want to perform on that telemetry after it's initialized.
pub trait CoreTelemetry {
    /// Each worker buffers logs that should be shuttled over to lang so that they may be rendered
    /// with the user's desired logging library. Use this function to grab the most recent buffered
    /// logs since the last time it was called. A fixed number of such logs are retained at maximum,
    /// with the oldest being dropped when full.
    ///
    /// Returns the list of logs from oldest to newest. Returns an empty vec if the feature is not
    /// configured.
    fn fetch_buffered_logs(&self) -> Vec<CoreLog>;

    /// If metrics gathering is enabled, returns the OTel meter for core telemetry, which can be
    /// used to create metrics instruments, or passed to things that create/record metrics (ex:
    /// clients).
    fn get_metric_meter(&self) -> Option<&Meter>;
}

/// Telemetry configuration options. Construct with [TelemetryOptionsBuilder]
#[derive(Debug, Clone, derive_builder::Builder)]
#[non_exhaustive]
pub struct TelemetryOptions {
    /// Optional trace exporter - set as None to disable.
    #[builder(setter(into, strip_option), default)]
    pub tracing: Option<TraceExportConfig>,
    /// Optional logger - set as None to disable.
    #[builder(setter(into, strip_option), default)]
    pub logging: Option<Logger>,
    /// Optional metrics exporter - set as None to disable.
    #[builder(setter(into, strip_option), default)]
    pub metrics: Option<MetricsExporter>,

    /// If set true, do not prefix metrics with `temporal_`. Will be removed eventually as
    /// the prefix is consistent with other SDKs.
    #[builder(default)]
    pub no_temporal_prefix_for_metrics: bool,

    /// Specifies the aggregation temporality for metric export. Defaults to cumulative.
    #[builder(default = "MetricTemporality::Cumulative")]
    pub metric_temporality: MetricTemporality,
}

/// Options for exporting to an OpenTelemetry Collector
#[derive(Debug, Clone)]
pub struct OtelCollectorOptions {
    /// The url of the OTel collector to export telemetry and metrics to. Lang SDK should also
    /// export to this same collector.
    pub url: Url,
    /// Optional set of HTTP headers to send to the Collector, e.g for authentication.
    pub headers: HashMap<String, String>,
    /// Optionally specify how frequently metrics should be exported. Defaults to 1 second.
    pub metric_periodicity: Option<Duration>,
}

/// Configuration for the external export of traces
#[derive(Debug, Clone)]
pub struct TraceExportConfig {
    /// An [EnvFilter](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/struct.EnvFilter.html) filter string.
    pub filter: String,
    /// Where they should go
    pub exporter: TraceExporter,
}

/// Control where traces are exported.
#[derive(Debug, Clone)]
pub enum TraceExporter {
    /// Export traces to an OpenTelemetry Collector <https://opentelemetry.io/docs/collector/>.
    Otel(OtelCollectorOptions),
}

/// Control where metrics are exported
#[derive(Debug, Clone)]
pub enum MetricsExporter {
    /// Export metrics to an OpenTelemetry Collector <https://opentelemetry.io/docs/collector/>.
    Otel(OtelCollectorOptions),
    /// Expose metrics directly via an embedded http server bound to the provided address.
    Prometheus(SocketAddr),
}

/// Control where logs go
#[derive(Debug, Clone)]
pub enum Logger {
    /// Log directly to console.
    Console {
        /// An [EnvFilter](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/struct.EnvFilter.html) filter string.
        filter: String,
    },
    /// Forward logs to Lang - collectable with `fetch_global_buffered_logs`.
    Forward {
        /// An [EnvFilter](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/struct.EnvFilter.html) filter string.
        filter: String,
    },
}

/// Types of aggregation temporality for metric export.
/// See: <https://github.com/open-telemetry/opentelemetry-specification/blob/ce50e4634efcba8da445cc23523243cb893905cb/specification/metrics/datamodel.md#temporality>
#[derive(Debug, Clone, Copy)]
pub enum MetricTemporality {
    /// Successive data points repeat the starting timestamp
    Cumulative,
    /// Successive data points advance the starting timestamp
    Delta,
}

impl Default for TelemetryOptions {
    fn default() -> Self {
        TelemetryOptionsBuilder::default().build().unwrap()
    }
}

/// A log line (which ultimately came from a tracing event) exported from Core->Lang
#[derive(Debug)]
pub struct CoreLog {
    /// The module within core this message originated from
    pub target: String,
    /// Log message
    pub message: String,
    /// Time log was generated (not when it was exported to lang)
    pub timestamp: SystemTime,
    /// Message level
    pub level: Level,
    /// Arbitrary k/v pairs (span k/vs are collapsed with event k/vs here). We could change this
    /// to include them in `span_contexts` instead, but there's probably not much value for log
    /// forwarding.
    pub fields: HashMap<String, serde_json::Value>,
    /// A list of the outermost to the innermost span names
    pub span_contexts: Vec<String>,
}

impl CoreLog {
    /// Return timestamp as ms since epoch
    pub fn millis_since_epoch(&self) -> u128 {
        self.timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis()
    }
}
