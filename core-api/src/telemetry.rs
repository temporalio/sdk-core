pub mod metrics;

use crate::telemetry::metrics::CoreMeter;
use std::{
    collections::HashMap,
    fmt::Debug,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing_core::Level;
use url::Url;

pub static METRIC_PREFIX: &str = "temporal_";

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
}

/// Telemetry configuration options. Construct with [TelemetryOptionsBuilder]
#[derive(Debug, Clone, derive_builder::Builder)]
#[non_exhaustive]
pub struct TelemetryOptions {
    /// Optional logger - set as None to disable.
    #[builder(setter(into, strip_option), default)]
    pub logging: Option<Logger>,
    /// Optional metrics exporter - set as None to disable.
    #[builder(setter(into, strip_option), default)]
    pub metrics: Option<Arc<dyn CoreMeter>>,
    /// If set true (the default) explicitly attach a `service_name` label to all metrics. Turn this
    /// off if your collection system supports the `target_info` metric from the OpenMetrics spec.
    /// For more, see
    /// [here](https://github.com/OpenObservability/OpenMetrics/blob/main/specification/OpenMetrics.md#supporting-target-metadata-in-both-push-based-and-pull-based-systems)
    #[builder(default = "true")]
    pub attach_service_name: bool,
    /// A prefix to be applied to all core-created metrics. Defaults to "temporal_".
    #[builder(default = "METRIC_PREFIX.to_string()")]
    pub metric_prefix: String,
}

/// Options for exporting to an OpenTelemetry Collector
#[derive(Debug, Clone, derive_builder::Builder)]
pub struct OtelCollectorOptions {
    /// The url of the OTel collector to export telemetry and metrics to. Lang SDK should also
    /// export to this same collector.
    pub url: Url,
    /// Optional set of HTTP headers to send to the Collector, e.g for authentication.
    pub headers: HashMap<String, String>,
    /// Optionally specify how frequently metrics should be exported. Defaults to 1 second.
    #[builder(default = "Duration::from_secs(1)")]
    pub metric_periodicity: Duration,
    /// Specifies the aggregation temporality for metric export. Defaults to cumulative.
    #[builder(default = "MetricTemporality::Cumulative")]
    pub metric_temporality: MetricTemporality,
    // A map of tags to be applied to all metrics
    #[builder(default)]
    pub global_tags: HashMap<String, String>,
}

/// Options for exporting metrics to Prometheus
#[derive(Debug, Clone, derive_builder::Builder)]
pub struct PrometheusExporterOptions {
    pub socket_addr: SocketAddr,
    // A map of tags to be applied to all metrics
    #[builder(default)]
    pub global_tags: HashMap<String, String>,
    /// If set true, all counters will include a "_total" suffix
    #[builder(default = "false")]
    pub counters_total_suffix: bool,
    /// If set true, all histograms will include the unit in their name as a suffix.
    /// Ex: "_milliseconds".
    #[builder(default = "false")]
    pub unit_suffix: bool,
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
    // Push logs to Lang. Can used with temporal_sdk_core::telemetry::CoreLogBufferedConsumer to buffer.
    Push {
        /// An [EnvFilter](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/struct.EnvFilter.html) filter string.
        filter: String,
        /// Trait invoked on each log.
        consumer: Arc<dyn CoreLogConsumer>,
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

/// Consumer trait for use with push logger.
pub trait CoreLogConsumer: Send + Sync + Debug {
    /// Invoked synchronously for every single log.
    fn on_log(&self, log: CoreLog);
}
