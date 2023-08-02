//! This module helps with the initialization and management of telemetry. IE: Metrics and tracing.
//! Logs from core are all traces, which may be exported to the console, in memory, or externally.

mod log_export;
pub(crate) mod metrics;
mod prometheus_server;

use crate::telemetry::{
    log_export::{CoreLogExportLayer, CoreLogsOut},
    metrics::TemporalMeter,
    prometheus_server::PromServer,
};
use crossbeam::channel::Receiver;
use itertools::Itertools;
use once_cell::sync::OnceCell;
use opentelemetry::{
    metrics::{Meter, MeterProvider as MeterProviderT, MetricsError},
    runtime,
    sdk::Resource,
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::{
    data::Temporality, reader::TemporalitySelector, InstrumentKind, MeterProvider, PeriodicReader,
};
use parking_lot::Mutex;
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    convert::TryInto,
    env,
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use temporal_sdk_core_api::telemetry::{
    CoreLog, CoreTelemetry, Logger, MetricTemporality, MetricsExporter, OtelCollectorOptions,
    TelemetryOptions,
};
use tonic::metadata::MetadataMap;
use tracing::{Level, Subscriber};
use tracing_subscriber::{layer::SubscriberExt, EnvFilter, Layer};

const TELEM_SERVICE_NAME: &str = "temporal-core-sdk";

/// Help you construct an [EnvFilter] compatible filter string which will forward all core module
/// traces at `core_level` and all others (from 3rd party modules, etc) at `other_level`.
pub fn construct_filter_string(core_level: Level, other_level: Level) -> String {
    format!(
        "{other_level},temporal_sdk_core={core_level},temporal_client={core_level},temporal_sdk={core_level}"
    )
}

/// Holds initialized tracing/metrics exporters, etc
pub struct TelemetryInstance {
    metric_prefix: &'static str,
    logs_out: Option<Mutex<CoreLogsOut>>,
    metrics: Option<(MeterProvider, Meter)>,
    trace_subscriber: Arc<dyn Subscriber + Send + Sync>,
    prom_binding: Option<SocketAddr>,
    attach_service_name: bool,
    _keepalive_rx: Receiver<()>,
}

impl TelemetryInstance {
    fn new(
        trace_subscriber: Arc<dyn Subscriber + Send + Sync>,
        logs_out: Option<Mutex<CoreLogsOut>>,
        metric_prefix: &'static str,
        mut meter_provider: Option<MeterProvider>,
        prom_binding: Option<SocketAddr>,
        attach_service_name: bool,
        keepalive_rx: Receiver<()>,
    ) -> Self {
        let metrics = meter_provider.take().map(|mp| {
            let meter = mp.meter(TELEM_SERVICE_NAME);
            (mp, meter)
        });
        Self {
            metric_prefix,
            logs_out,
            metrics,
            trace_subscriber,
            prom_binding,
            attach_service_name,
            _keepalive_rx: keepalive_rx,
        }
    }

    /// Returns a trace subscriber which can be used with the tracing crate, or with our own
    /// [set_trace_subscriber_for_current_thread] function.
    pub fn trace_subscriber(&self) -> Arc<dyn Subscriber + Send + Sync> {
        self.trace_subscriber.clone()
    }

    /// Returns the address the Prometheus server is bound to if it is running
    pub fn prom_port(&self) -> Option<SocketAddr> {
        self.prom_binding
    }

    /// Returns our wrapper for OTel metric meters, can be used to, ex: initialize clients
    pub fn get_metric_meter(&self) -> Option<TemporalMeter> {
        let kvs = if self.attach_service_name {
            vec![KeyValue::new("service_name", TELEM_SERVICE_NAME)]
        } else {
            vec![]
        };
        self.metrics
            .as_ref()
            .map(|(_, m)| TemporalMeter::new(m, self.metric_prefix, Arc::new(kvs)))
    }
}

thread_local! {
    static SUB_GUARD: RefCell<Option<tracing::subscriber::DefaultGuard>> = RefCell::new(None);
}
/// Set the trace subscriber for the current thread. This must be done in every thread which uses
/// core stuff, otherwise traces/logs will not be collected on that thread. For example, if using
/// a multithreaded Tokio runtime, you should ensure that said runtime uses
/// [on_thread_start](https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.on_thread_start)
/// or a similar mechanism to call this for each thread within the runtime.
pub fn set_trace_subscriber_for_current_thread(sub: impl Subscriber + Send + Sync + 'static) {
    SUB_GUARD.with(|sg| {
        if sg.borrow().is_none() {
            let g = tracing::subscriber::set_default(sub);
            *sg.borrow_mut() = Some(g);
        }
    })
}

/// Undoes [set_trace_subscriber_for_current_thread]
pub fn remove_trace_subscriber_for_current_thread() {
    SUB_GUARD.with(|sg| sg.take());
}

fn metric_prefix(opts: &TelemetryOptions) -> &'static str {
    if opts.no_temporal_prefix_for_metrics {
        ""
    } else {
        "temporal_"
    }
}

impl CoreTelemetry for TelemetryInstance {
    fn fetch_buffered_logs(&self) -> Vec<CoreLog> {
        if let Some(logs_out) = self.logs_out.as_ref() {
            logs_out.lock().pop_iter().collect()
        } else {
            vec![]
        }
    }
}

/// Initialize tracing subscribers/output and logging export, returning a [TelemetryInstance]
/// which can be used to register default / global tracing subscribers.
///
/// You should only call this once per unique [TelemetryOptions]
///
/// See [TelemetryOptions] docs for more on configuration.
pub fn telemetry_init(opts: TelemetryOptions) -> Result<TelemetryInstance, anyhow::Error> {
    // This is a bit odd, but functional. It's desirable to create a separate tokio runtime for
    // metrics handling, since tests typically use a single-threaded runtime and initializing
    // pipeline requires us to know if the runtime is single or multithreaded, we will crash
    // in one case or the other. There does not seem to be a way to tell from the current runtime
    // handle if it is single or multithreaded. Additionally, we can isolate metrics work this
    // way which is nice.
    let (tx, rx) = crossbeam::channel::bounded(0);
    let (keepalive_tx, keepalive_rx) = crossbeam::channel::bounded(0);
    let jh = std::thread::spawn(move || -> Result<(), anyhow::Error> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .thread_name("telemetry")
            .worker_threads(2)
            .enable_all()
            .build()?;
        // Parts of telem dat ====
        let mut logs_out = None;
        let metric_prefix = metric_prefix(&opts);
        let mut prom_binding = None;
        // =======================

        // Tracing subscriber layers =========
        let mut console_pretty_layer = None;
        let mut console_compact_layer = None;
        let mut forward_layer = None;
        // ===================================

        if let Some(ref logger) = opts.logging {
            match logger {
                Logger::Console { filter } => {
                    // This is silly dupe but can't be avoided without boxing.
                    if env::var("TEMPORAL_CORE_PRETTY_LOGS").is_ok() {
                        console_pretty_layer = Some(
                            tracing_subscriber::fmt::layer()
                                .with_target(false)
                                .event_format(
                                    tracing_subscriber::fmt::format()
                                        .pretty()
                                        .with_source_location(false),
                                )
                                .with_filter(EnvFilter::new(filter)),
                        )
                    } else {
                        console_compact_layer = Some(
                            tracing_subscriber::fmt::layer()
                                .with_target(false)
                                .event_format(
                                    tracing_subscriber::fmt::format()
                                        .compact()
                                        .with_source_location(false),
                                )
                                .with_filter(EnvFilter::new(filter)),
                        )
                    }
                }
                Logger::Forward { filter } => {
                    let (export_layer, lo) = CoreLogExportLayer::new();
                    logs_out = Some(Mutex::new(lo));
                    forward_layer = Some(export_layer.with_filter(EnvFilter::new(filter)));
                }
            };
        };

        let meter_provider = if let Some(ref metrics) = opts.metrics {
            match metrics {
                MetricsExporter::Prometheus(addr) => {
                    let (srv, exporter) = runtime
                        .block_on(async { PromServer::new(*addr, SDKAggSelector::default()) })?;
                    prom_binding = Some(srv.bound_addr());
                    runtime.spawn(async move { srv.run().await });
                    Some(MeterProvider::builder().with_reader(exporter))
                }
                MetricsExporter::Otel(OtelCollectorOptions {
                    url,
                    headers,
                    metric_periodicity,
                }) => runtime.block_on(async {
                    let exporter = opentelemetry_otlp::MetricsExporter::new(
                        opentelemetry_otlp::TonicExporterBuilder::default()
                            .with_endpoint(url.to_string())
                            .with_metadata(MetadataMap::from_headers(headers.try_into()?)),
                        Box::new(metric_temporality_to_selector(opts.metric_temporality)),
                        Box::<SDKAggSelector>::default(),
                    )?;
                    let reader = PeriodicReader::builder(exporter, runtime::Tokio)
                        .with_interval(metric_periodicity.unwrap_or_else(|| Duration::from_secs(1)))
                        .build();
                    let mp = MeterProvider::builder().with_reader(reader);
                    Ok::<_, anyhow::Error>(Some(mp))
                })?,
            }
        } else {
            None
        };
        let meter_provider = meter_provider
            .map(|mp| {
                Ok::<_, MetricsError>(
                    augment_meter_provider_with_views(
                        mp.with_resource(default_resource(&opts.global_tags)),
                    )?
                    .build(),
                )
            })
            .transpose()?;

        let reg = tracing_subscriber::registry()
            .with(console_pretty_layer)
            .with(console_compact_layer)
            .with(forward_layer);

        #[cfg(feature = "tokio-console")]
        let reg = reg.with(console_subscriber::spawn());

        tx.send(TelemetryInstance::new(
            Arc::new(reg),
            logs_out,
            metric_prefix,
            meter_provider,
            prom_binding,
            opts.attach_service_name,
            keepalive_rx,
        ))
        .expect("Must be able to send telem instance out of thread");
        // Now keep the thread alive until the telemetry instance is dropped by trying to send
        // something forever
        let _ = keepalive_tx.send(());
        Ok(())
    });
    match rx.recv() {
        Ok(ti) => Ok(ti),
        Err(_) => {
            // Immediately join the thread since something went wrong in it
            jh.join().expect("Telemetry must init cleanly")?;
            // This can't happen. The rx channel can't be dropped unless the thread errored.
            unreachable!("Impossible error in telemetry init thread");
        }
    }
}

/// Initialize telemetry/tracing globally. Useful for testing. Only takes affect when called
/// the first time. Subsequent calls are ignored.
pub fn telemetry_init_global(opts: TelemetryOptions) -> Result<(), anyhow::Error> {
    static INITTED: AtomicBool = AtomicBool::new(false);
    if INITTED
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .is_ok()
    {
        let ti = telemetry_init(opts)?;
        tracing::subscriber::set_global_default(ti.trace_subscriber())?;
    }
    Ok(())
}

fn default_resource_kvs() -> &'static [KeyValue] {
    static INSTANCE: OnceCell<[KeyValue; 1]> = OnceCell::new();
    INSTANCE.get_or_init(|| [KeyValue::new("service.name", TELEM_SERVICE_NAME)])
}

fn default_resource(override_values: &HashMap<String, String>) -> Resource {
    let override_kvs = override_values
        .iter()
        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()));
    Resource::new(default_resource_kvs().iter().cloned()).merge(&Resource::new(override_kvs))
}

#[derive(Clone)]
struct ConstantTemporality(Temporality);
impl TemporalitySelector for ConstantTemporality {
    fn temporality(&self, _: InstrumentKind) -> Temporality {
        self.0
    }
}
fn metric_temporality_to_selector(
    t: MetricTemporality,
) -> impl TemporalitySelector + Send + Sync + Clone {
    match t {
        MetricTemporality::Cumulative => ConstantTemporality(Temporality::Cumulative),
        MetricTemporality::Delta => ConstantTemporality(Temporality::Delta),
    }
}

#[cfg(test)]
pub mod test_initters {
    use super::*;
    use temporal_sdk_core_api::telemetry::TelemetryOptionsBuilder;

    #[allow(dead_code)] // Not always used, called to enable for debugging when needed
    pub fn test_telem_console() {
        telemetry_init_global(
            TelemetryOptionsBuilder::default()
                .logging(Logger::Console {
                    filter: construct_filter_string(Level::DEBUG, Level::WARN),
                })
                .build()
                .unwrap(),
        )
        .unwrap();
    }
}
use crate::telemetry::metrics::{augment_meter_provider_with_views, SDKAggSelector};
#[cfg(test)]
pub use test_initters::*;

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
