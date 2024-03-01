use super::{
    metrics::{
        ACT_EXEC_LATENCY_NAME, ACT_EXE_MS_BUCKETS, ACT_SCHED_TO_START_LATENCY_NAME,
        DEFAULT_MS_BUCKETS, TASK_SCHED_TO_START_MS_BUCKETS, WF_E2E_LATENCY_NAME,
        WF_LATENCY_MS_BUCKETS, WF_TASK_EXECUTION_LATENCY_NAME, WF_TASK_MS_BUCKETS,
        WF_TASK_REPLAY_LATENCY_NAME, WF_TASK_SCHED_TO_START_LATENCY_NAME,
    },
    prometheus_server::PromServer,
    TELEM_SERVICE_NAME,
};
use crate::abstractions::dbg_panic;

use opentelemetry::{
    self,
    metrics::{Meter, MeterProvider as MeterProviderT, Unit},
    KeyValue,
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    metrics::{
        data::Temporality,
        new_view,
        reader::{AggregationSelector, DefaultAggregationSelector, TemporalitySelector},
        Aggregation, Instrument, InstrumentKind, MeterProvider, MeterProviderBuilder,
        PeriodicReader, View,
    },
    runtime, AttributeSet, Resource,
};
use parking_lot::RwLock;
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use temporal_sdk_core_api::telemetry::{
    metrics::{
        CoreMeter, Counter, Gauge, Histogram, MetricAttributes, MetricParameters, NewAttributes,
    },
    MetricTemporality, OtelCollectorOptions, PrometheusExporterOptions,
};
use tokio::task::AbortHandle;
use tonic::metadata::MetadataMap;

/// Chooses appropriate aggregators for our metrics
#[derive(Debug, Clone, Default)]
pub struct SDKAggSelector {
    default: DefaultAggregationSelector,
}

impl AggregationSelector for SDKAggSelector {
    fn aggregation(&self, kind: InstrumentKind) -> Aggregation {
        match kind {
            InstrumentKind::Histogram => Aggregation::ExplicitBucketHistogram {
                boundaries: DEFAULT_MS_BUCKETS.to_vec(),
                record_min_max: true,
            },
            _ => self.default.aggregation(kind),
        }
    }
}

fn histo_view(
    metric_name: &'static str,
    buckets: &[f64],
) -> opentelemetry::metrics::Result<Box<dyn View>> {
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
) -> opentelemetry::metrics::Result<MeterProviderBuilder> {
    // Some histograms are actually gauges, but we have to use histograms otherwise they forget
    // their value between collections since we don't use callbacks.
    Ok(mpb
        .with_view(histo_view(WF_E2E_LATENCY_NAME, WF_LATENCY_MS_BUCKETS)?)
        .with_view(histo_view(
            WF_TASK_EXECUTION_LATENCY_NAME,
            WF_TASK_MS_BUCKETS,
        )?)
        .with_view(histo_view(WF_TASK_REPLAY_LATENCY_NAME, WF_TASK_MS_BUCKETS)?)
        .with_view(histo_view(
            WF_TASK_SCHED_TO_START_LATENCY_NAME,
            TASK_SCHED_TO_START_MS_BUCKETS,
        )?)
        .with_view(histo_view(
            ACT_SCHED_TO_START_LATENCY_NAME,
            TASK_SCHED_TO_START_MS_BUCKETS,
        )?)
        .with_view(histo_view(ACT_EXEC_LATENCY_NAME, ACT_EXE_MS_BUCKETS)?)
        .with_resource(default_resource(global_tags)))
}

/// OTel has no built-in synchronous Gauge. Histograms used to be able to serve that purpose, but
/// they broke that. Lovely. So, we need to implement one by hand.
pub(crate) struct MemoryGaugeU64 {
    labels_to_values: Arc<RwLock<HashMap<AttributeSet, u64>>>,
}

impl MemoryGaugeU64 {
    fn new(params: MetricParameters, meter: &Meter) -> Self {
        let gauge = meter
            .u64_observable_gauge(params.name)
            .with_unit(Unit::new(params.unit))
            .with_description(params.description)
            .init();
        let map = Arc::new(RwLock::new(HashMap::<AttributeSet, u64>::new()));
        let map_c = map.clone();
        meter
            .register_callback(&[gauge.as_any()], move |o| {
                // This whole thing is... extra stupid.
                // See https://github.com/open-telemetry/opentelemetry-rust/issues/1181
                // The performance is likely bad here, but, given this is only called when metrics
                // are exported it should be livable for now.
                let map_rlock = map_c.read();
                for (kvs, val) in map_rlock.iter() {
                    let kvs: Vec<_> = kvs
                        .iter()
                        .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                        .collect();
                    o.observe_u64(&gauge, *val, kvs.as_slice())
                }
            })
            .expect("instrument must exist we just created it");
        MemoryGaugeU64 {
            labels_to_values: map,
        }
    }
    fn record(&self, val: u64, kvs: &[KeyValue]) {
        self.labels_to_values
            .write()
            .insert(AttributeSet::from(kvs), val);
    }
}

/// Create an OTel meter that can be used as a [CoreMeter] to export metrics over OTLP.
pub fn build_otlp_metric_exporter(
    opts: OtelCollectorOptions,
) -> Result<CoreOtelMeter, anyhow::Error> {
    let exporter = opentelemetry_otlp::TonicExporterBuilder::default()
        .with_endpoint(opts.url.to_string())
        .with_metadata(MetadataMap::from_headers((&opts.headers).try_into()?))
        .build_metrics_exporter(
            Box::<SDKAggSelector>::default(),
            Box::new(metric_temporality_to_selector(opts.metric_temporality)),
        )?;
    let reader = PeriodicReader::builder(exporter, runtime::Tokio)
        .with_interval(opts.metric_periodicity)
        .build();
    let mp = augment_meter_provider_with_defaults(
        MeterProvider::builder().with_reader(reader),
        &opts.global_tags,
    )?
    .build();
    Ok::<_, anyhow::Error>(CoreOtelMeter(mp.meter(TELEM_SERVICE_NAME)))
}

pub struct StartedPromServer {
    pub meter: Arc<CoreOtelMeter>,
    pub bound_addr: SocketAddr,
    pub abort_handle: AbortHandle,
}

/// Builds and runs a prometheus endpoint which can be scraped by prom instances for metrics export.
/// Returns the meter that can be used as a [CoreMeter].
pub fn start_prometheus_metric_exporter(
    opts: PrometheusExporterOptions,
) -> Result<StartedPromServer, anyhow::Error> {
    let (srv, exporter) = PromServer::new(&opts, SDKAggSelector::default())?;
    let meter_provider = augment_meter_provider_with_defaults(
        MeterProvider::builder().with_reader(exporter),
        &opts.global_tags,
    )?
    .build();
    let bound_addr = srv.bound_addr();
    let handle = tokio::spawn(async move { srv.run().await });
    Ok(StartedPromServer {
        meter: Arc::new(CoreOtelMeter(meter_provider.meter(TELEM_SERVICE_NAME))),
        bound_addr,
        abort_handle: handle.abort_handle(),
    })
}

#[derive(Debug)]
pub struct CoreOtelMeter(Meter);

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
            self.0
                .u64_counter(params.name)
                .with_unit(Unit::new(params.unit))
                .with_description(params.description)
                .init(),
        )
    }

    fn histogram(&self, params: MetricParameters) -> Arc<dyn Histogram> {
        Arc::new(
            self.0
                .u64_histogram(params.name)
                .with_unit(Unit::new(params.unit))
                .with_description(params.description)
                .init(),
        )
    }

    fn gauge(&self, params: MetricParameters) -> Arc<dyn Gauge> {
        Arc::new(MemoryGaugeU64::new(params, &self.0))
    }
}

impl Gauge for MemoryGaugeU64 {
    fn record(&self, value: u64, attributes: &MetricAttributes) {
        if let MetricAttributes::OTel { kvs } = attributes {
            self.record(value, kvs);
        } else {
            dbg_panic!("Must use OTel attributes with an OTel metric implementation");
        }
    }
}

fn default_resource_kvs() -> &'static [KeyValue] {
    use once_cell::sync::OnceCell;

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

fn metric_temporality_to_selector(t: MetricTemporality) -> impl TemporalitySelector + Clone {
    match t {
        MetricTemporality::Cumulative => ConstantTemporality(Temporality::Cumulative),
        MetricTemporality::Delta => ConstantTemporality(Temporality::Delta),
    }
}
