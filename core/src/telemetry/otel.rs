use super::{
    default_buckets_for,
    metrics::{
        ACT_EXEC_LATENCY_NAME, ACT_SCHED_TO_START_LATENCY_NAME, DEFAULT_MS_BUCKETS,
        WF_E2E_LATENCY_NAME, WF_TASK_EXECUTION_LATENCY_NAME, WF_TASK_REPLAY_LATENCY_NAME,
        WF_TASK_SCHED_TO_START_LATENCY_NAME,
    },
    prometheus_server::PromServer,
    TELEM_SERVICE_NAME,
};
use crate::{abstractions::dbg_panic, telemetry::metrics::DEFAULT_S_BUCKETS};
use opentelemetry::{
    self,
    metrics::{Meter, MeterProvider as MeterProviderT},
    Key, KeyValue, Value,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    metrics::{
        data::Temporality,
        new_view,
        reader::{AggregationSelector, DefaultAggregationSelector, TemporalitySelector},
        Aggregation, AttributeSet, Instrument, InstrumentKind, MeterProviderBuilder,
        PeriodicReader, SdkMeterProvider, View,
    },
    runtime, Resource,
};
use parking_lot::RwLock;
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use temporal_sdk_core_api::telemetry::{
    metrics::{
        CoreMeter, Counter, Gauge, GaugeF64, Histogram, HistogramDuration, HistogramF64,
        MetricAttributes, MetricParameters, NewAttributes,
    },
    MetricTemporality, OtelCollectorOptions, PrometheusExporterOptions,
};
use tokio::task::AbortHandle;
use tonic::{metadata::MetadataMap, transport::ClientTlsConfig};

/// Chooses appropriate aggregators for our metrics
#[derive(Debug, Clone)]
struct SDKAggSelector {
    use_seconds: bool,
    default: DefaultAggregationSelector,
}

impl SDKAggSelector {
    fn new(use_seconds: bool) -> Self {
        Self {
            use_seconds,
            default: Default::default(),
        }
    }
}

impl AggregationSelector for SDKAggSelector {
    fn aggregation(&self, kind: InstrumentKind) -> Aggregation {
        match kind {
            InstrumentKind::Histogram => Aggregation::ExplicitBucketHistogram {
                boundaries: if self.use_seconds {
                    DEFAULT_S_BUCKETS.to_vec()
                } else {
                    DEFAULT_MS_BUCKETS.to_vec()
                },
                record_min_max: true,
            },
            _ => self.default.aggregation(kind),
        }
    }
}

fn histo_view(
    metric_name: &'static str,
    use_seconds: bool,
) -> opentelemetry::metrics::Result<Box<dyn View>> {
    let buckets = default_buckets_for(metric_name, use_seconds);
    new_view(
        Instrument::new().name(format!("*{metric_name}")),
        opentelemetry_sdk::metrics::Stream::new().aggregation(
            Aggregation::ExplicitBucketHistogram {
                boundaries: buckets.to_vec(),
                record_min_max: true,
            },
        ),
    )
}

pub(super) fn augment_meter_provider_with_defaults(
    mpb: MeterProviderBuilder,
    global_tags: &HashMap<String, String>,
    use_seconds: bool,
) -> opentelemetry::metrics::Result<MeterProviderBuilder> {
    // Some histograms are actually gauges, but we have to use histograms otherwise they forget
    // their value between collections since we don't use callbacks.
    Ok(mpb
        .with_view(histo_view(WF_E2E_LATENCY_NAME, use_seconds)?)
        .with_view(histo_view(WF_TASK_EXECUTION_LATENCY_NAME, use_seconds)?)
        .with_view(histo_view(WF_TASK_REPLAY_LATENCY_NAME, use_seconds)?)
        .with_view(histo_view(
            WF_TASK_SCHED_TO_START_LATENCY_NAME,
            use_seconds,
        )?)
        .with_view(histo_view(ACT_SCHED_TO_START_LATENCY_NAME, use_seconds)?)
        .with_view(histo_view(ACT_EXEC_LATENCY_NAME, use_seconds)?)
        .with_resource(default_resource(global_tags)))
}

/// OTel has no built-in synchronous Gauge. Histograms used to be able to serve that purpose, but
/// they broke that. Lovely. So, we need to implement one by hand.
pub(crate) struct MemoryGauge<U> {
    labels_to_values: Arc<RwLock<HashMap<AttributeSet, U>>>,
}

macro_rules! impl_memory_gauge {
    ($ty:ty, $gauge_fn:ident, $observe_fn:ident) => {
        impl MemoryGauge<$ty> {
            fn new(params: MetricParameters, meter: &Meter) -> Self {
                let gauge = meter
                    .$gauge_fn(params.name)
                    .with_unit(params.unit)
                    .with_description(params.description)
                    .init();
                let map = Arc::new(RwLock::new(HashMap::<AttributeSet, $ty>::new()));
                let map_c = map.clone();
                meter
                    .register_callback(&[gauge.as_any()], move |o| {
                        // This whole thing is... extra stupid.
                        // See https://github.com/open-telemetry/opentelemetry-rust/issues/1181
                        // The performance is likely bad here, but, given this is only called when
                        // metrics are exported it should be livable for now.
                        let map_rlock = map_c.read();
                        for (kvs, val) in map_rlock.iter() {
                            let kvs: Vec<_> = kvs
                                .iter()
                                .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                                .collect();
                            o.$observe_fn(&gauge, *val, kvs.as_slice())
                        }
                    })
                    .expect("instrument must exist we just created it");
                MemoryGauge {
                    labels_to_values: map,
                }
            }
        }
    };
}
impl_memory_gauge!(u64, u64_observable_gauge, observe_u64);
impl_memory_gauge!(f64, f64_observable_gauge, observe_f64);

impl<U> MemoryGauge<U> {
    fn record(&self, val: U, kvs: &[KeyValue]) {
        self.labels_to_values
            .write()
            .insert(AttributeSet::from(kvs), val);
    }
}

/// Create an OTel meter that can be used as a [CoreMeter] to export metrics over OTLP.
pub fn build_otlp_metric_exporter(
    opts: OtelCollectorOptions,
) -> Result<CoreOtelMeter, anyhow::Error> {
    let mut exporter =
        opentelemetry_otlp::TonicExporterBuilder::default().with_endpoint(opts.url.to_string());
    if opts.url.scheme() == "https" || opts.url.scheme() == "grpcs" {
        exporter = exporter.with_tls_config(ClientTlsConfig::new().with_native_roots());
    }
    let exporter = exporter
        .with_metadata(MetadataMap::from_headers((&opts.headers).try_into()?))
        .build_metrics_exporter(
            Box::new(SDKAggSelector::new(opts.use_seconds_for_durations)),
            Box::new(metric_temporality_to_selector(opts.metric_temporality)),
        )?;
    let reader = PeriodicReader::builder(exporter, runtime::Tokio)
        .with_interval(opts.metric_periodicity)
        .build();
    let mp = augment_meter_provider_with_defaults(
        MeterProviderBuilder::default().with_reader(reader),
        &opts.global_tags,
        opts.use_seconds_for_durations,
    )?
    .build();
    Ok::<_, anyhow::Error>(CoreOtelMeter {
        meter: mp.meter(TELEM_SERVICE_NAME),
        use_seconds_for_durations: opts.use_seconds_for_durations,
        _mp: mp,
    })
}

pub struct StartedPromServer {
    pub meter: Arc<CoreOtelMeter>,
    pub bound_addr: SocketAddr,
    pub abort_handle: AbortHandle,
}

/// Builds and runs a prometheus endpoint which can be scraped by prom instances for metrics export.
/// Returns the meter that can be used as a [CoreMeter].
///
/// Requires a Tokio runtime to exist, and will block briefly while binding the server endpoint.
pub fn start_prometheus_metric_exporter(
    opts: PrometheusExporterOptions,
) -> Result<StartedPromServer, anyhow::Error> {
    let (srv, exporter) =
        PromServer::new(&opts, SDKAggSelector::new(opts.use_seconds_for_durations))?;
    let meter_provider = augment_meter_provider_with_defaults(
        MeterProviderBuilder::default().with_reader(exporter),
        &opts.global_tags,
        opts.use_seconds_for_durations,
    )?
    .build();
    let bound_addr = srv.bound_addr()?;
    let handle = tokio::spawn(async move { srv.run().await });
    Ok(StartedPromServer {
        meter: Arc::new(CoreOtelMeter {
            meter: meter_provider.meter(TELEM_SERVICE_NAME),
            use_seconds_for_durations: opts.use_seconds_for_durations,
            _mp: meter_provider,
        }),
        bound_addr,
        abort_handle: handle.abort_handle(),
    })
}

#[derive(Debug)]
pub struct CoreOtelMeter {
    meter: Meter,
    use_seconds_for_durations: bool,
    // we have to hold on to the provider otherwise otel automatically shuts it down on drop
    // for whatever crazy reason
    _mp: SdkMeterProvider,
}

impl CoreMeter for CoreOtelMeter {
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        MetricAttributes::OTel {
            kvs: Arc::new(attribs.attributes.into_iter().map(KeyValue::from).collect()),
        }
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        if let MetricAttributes::OTel { mut kvs } = existing {
            Arc::make_mut(&mut kvs).extend(attribs.attributes.into_iter().map(Into::into));
            MetricAttributes::OTel { kvs }
        } else {
            dbg_panic!("Must use OTel attributes with an OTel metric implementation");
            existing
        }
    }

    fn counter(&self, params: MetricParameters) -> Arc<dyn Counter> {
        Arc::new(
            self.meter
                .u64_counter(params.name)
                .with_unit(params.unit)
                .with_description(params.description)
                .init(),
        )
    }

    fn histogram(&self, params: MetricParameters) -> Arc<dyn Histogram> {
        Arc::new(
            self.meter
                .u64_histogram(params.name)
                .with_unit(params.unit)
                .with_description(params.description)
                .init(),
        )
    }

    fn histogram_f64(&self, params: MetricParameters) -> Arc<dyn HistogramF64> {
        Arc::new(
            self.meter
                .f64_histogram(params.name)
                .with_unit(params.unit)
                .with_description(params.description)
                .init(),
        )
    }

    fn histogram_duration(&self, mut params: MetricParameters) -> Arc<dyn HistogramDuration> {
        Arc::new(if self.use_seconds_for_durations {
            params.unit = "s".into();
            DurationHistogram::Seconds(self.histogram_f64(params))
        } else {
            params.unit = "ms".into();
            DurationHistogram::Milliseconds(self.histogram(params))
        })
    }

    fn gauge(&self, params: MetricParameters) -> Arc<dyn Gauge> {
        Arc::new(MemoryGauge::<u64>::new(params, &self.meter))
    }

    fn gauge_f64(&self, params: MetricParameters) -> Arc<dyn GaugeF64> {
        Arc::new(MemoryGauge::<f64>::new(params, &self.meter))
    }
}

/// A histogram being used to record durations.
#[derive(Clone)]
enum DurationHistogram {
    Milliseconds(Arc<dyn Histogram>),
    Seconds(Arc<dyn HistogramF64>),
}
impl HistogramDuration for DurationHistogram {
    fn record(&self, value: Duration, attributes: &MetricAttributes) {
        match self {
            DurationHistogram::Milliseconds(h) => h.record(value.as_millis() as u64, attributes),
            DurationHistogram::Seconds(h) => h.record(value.as_secs_f64(), attributes),
        }
    }
}

impl Gauge for MemoryGauge<u64> {
    fn record(&self, value: u64, attributes: &MetricAttributes) {
        if let MetricAttributes::OTel { kvs } = attributes {
            self.record(value, kvs);
        } else {
            dbg_panic!("Must use OTel attributes with an OTel metric implementation");
        }
    }
}
impl GaugeF64 for MemoryGauge<f64> {
    fn record(&self, value: f64, attributes: &MetricAttributes) {
        if let MetricAttributes::OTel { kvs } = attributes {
            self.record(value, kvs);
        } else {
            dbg_panic!("Must use OTel attributes with an OTel metric implementation");
        }
    }
}

fn default_resource_instance() -> &'static Resource {
    use std::sync::OnceLock;

    static INSTANCE: OnceLock<Resource> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        let resource = Resource::default();
        if resource.get(Key::from("service.name")) == Some(Value::from("unknown_service")) {
            // otel spec recommends to leave service.name as unknown_service but we want to
            // maintain backwards compatability with existing library behaviour
            return resource.merge(&Resource::new([KeyValue::new(
                "service.name",
                TELEM_SERVICE_NAME,
            )]));
        }
        resource
    })
}

fn default_resource(override_values: &HashMap<String, String>) -> Resource {
    let override_kvs = override_values
        .iter()
        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()));
    default_resource_instance()
        .clone()
        .merge(&Resource::new(override_kvs))
}

#[derive(Clone)]
struct ConstantTemporality(Temporality);

impl TemporalitySelector for ConstantTemporality {
    fn temporality(&self, _: InstrumentKind) -> Temporality {
        self.0
    }
}

fn metric_temporality_to_selector(t: MetricTemporality) -> impl TemporalitySelector + Clone {
    match t {
        MetricTemporality::Cumulative => ConstantTemporality(Temporality::Cumulative),
        MetricTemporality::Delta => ConstantTemporality(Temporality::Delta),
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use opentelemetry::Key;

    #[test]
    pub(crate) fn default_resource_instance_service_name_default() {
        let resource = default_resource_instance();
        let service_name = resource.get(Key::from("service.name"));
        assert_eq!(service_name, Some(Value::from(TELEM_SERVICE_NAME)));
    }
}
