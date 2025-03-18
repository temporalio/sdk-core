use super::{
    TELEM_SERVICE_NAME, default_buckets_for,
    metrics::{
        ACTIVITY_EXEC_LATENCY_HISTOGRAM_NAME, ACTIVITY_SCHED_TO_START_LATENCY_HISTOGRAM_NAME,
        DEFAULT_MS_BUCKETS, WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME,
        WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME,
        WORKFLOW_TASK_REPLAY_LATENCY_HISTOGRAM_NAME,
        WORKFLOW_TASK_SCHED_TO_START_LATENCY_HISTOGRAM_NAME,
    },
    prometheus_server::PromServer,
};
use crate::{abstractions::dbg_panic, telemetry::metrics::DEFAULT_S_BUCKETS};
use opentelemetry::{
    self, Key, KeyValue, Value, global,
    metrics::{Meter, MeterProvider as MeterProviderT},
};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{
    Resource,
    metrics::{
        Aggregation, Instrument, InstrumentKind, MeterProviderBuilder, PeriodicReader,
        SdkMeterProvider, View, data::Temporality, new_view, reader::TemporalitySelector,
    },
    runtime,
};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use temporal_sdk_core_api::telemetry::{
    HistogramBucketOverrides, MetricTemporality, OtelCollectorOptions, OtlpProtocol,
    PrometheusExporterOptions,
    metrics::{
        CoreMeter, Counter, Gauge, GaugeF64, Histogram, HistogramDuration, HistogramF64,
        MetricAttributes, MetricParameters, NewAttributes,
    },
};
use tokio::task::AbortHandle;
use tonic::{metadata::MetadataMap, transport::ClientTlsConfig};

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
    mut mpb: MeterProviderBuilder,
    global_tags: &HashMap<String, String>,
    use_seconds: bool,
    bucket_overrides: HistogramBucketOverrides,
) -> opentelemetry::metrics::Result<MeterProviderBuilder> {
    for (name, buckets) in bucket_overrides.overrides {
        mpb = mpb.with_view(new_view(
            Instrument::new().name(format!("*{name}")),
            opentelemetry_sdk::metrics::Stream::new().aggregation(
                Aggregation::ExplicitBucketHistogram {
                    boundaries: buckets,
                    record_min_max: true,
                },
            ),
        )?)
    }
    let mut mpb = mpb
        .with_view(histo_view(
            WORKFLOW_E2E_LATENCY_HISTOGRAM_NAME,
            use_seconds,
        )?)
        .with_view(histo_view(
            WORKFLOW_TASK_EXECUTION_LATENCY_HISTOGRAM_NAME,
            use_seconds,
        )?)
        .with_view(histo_view(
            WORKFLOW_TASK_REPLAY_LATENCY_HISTOGRAM_NAME,
            use_seconds,
        )?)
        .with_view(histo_view(
            WORKFLOW_TASK_SCHED_TO_START_LATENCY_HISTOGRAM_NAME,
            use_seconds,
        )?)
        .with_view(histo_view(
            ACTIVITY_SCHED_TO_START_LATENCY_HISTOGRAM_NAME,
            use_seconds,
        )?)
        .with_view(histo_view(
            ACTIVITY_EXEC_LATENCY_HISTOGRAM_NAME,
            use_seconds,
        )?);
    // Fallback default
    mpb = mpb.with_view(new_view(
        {
            let mut i = Instrument::new();
            i.kind = Some(InstrumentKind::Histogram);
            i
        },
        opentelemetry_sdk::metrics::Stream::new().aggregation(
            Aggregation::ExplicitBucketHistogram {
                boundaries: if use_seconds {
                    DEFAULT_S_BUCKETS.to_vec()
                } else {
                    DEFAULT_MS_BUCKETS.to_vec()
                },
                record_min_max: true,
            },
        ),
    )?);
    Ok(mpb.with_resource(default_resource(global_tags)))
}

/// Create an OTel meter that can be used as a [CoreMeter] to export metrics over OTLP.
pub fn build_otlp_metric_exporter(
    opts: OtelCollectorOptions,
) -> Result<CoreOtelMeter, anyhow::Error> {
    global::set_error_handler(|err| {
        tracing::error!("{}", err);
    })?;
    let exporter = match opts.protocol {
        OtlpProtocol::Grpc => {
            let mut exporter = opentelemetry_otlp::TonicExporterBuilder::default()
                .with_endpoint(opts.url.to_string());
            if opts.url.scheme() == "https" || opts.url.scheme() == "grpcs" {
                exporter = exporter.with_tls_config(ClientTlsConfig::new().with_native_roots());
            }
            exporter
                .with_metadata(MetadataMap::from_headers((&opts.headers).try_into()?))
                .build_metrics_exporter(Box::new(metric_temporality_to_selector(
                    opts.metric_temporality,
                )))?
        }
        OtlpProtocol::Http => opentelemetry_otlp::HttpExporterBuilder::default()
            .with_endpoint(opts.url.to_string())
            .with_headers(opts.headers)
            .build_metrics_exporter(Box::new(metric_temporality_to_selector(
                opts.metric_temporality,
            )))?,
    };
    let reader = PeriodicReader::builder(exporter, runtime::Tokio)
        .with_interval(opts.metric_periodicity)
        .build();
    let mp = augment_meter_provider_with_defaults(
        MeterProviderBuilder::default().with_reader(reader),
        &opts.global_tags,
        opts.use_seconds_for_durations,
        opts.histogram_bucket_overrides,
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
    let (srv, exporter) = PromServer::new(&opts)?;
    let meter_provider = augment_meter_provider_with_defaults(
        MeterProviderBuilder::default().with_reader(exporter),
        &opts.global_tags,
        opts.use_seconds_for_durations,
        opts.histogram_bucket_overrides,
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
    pub meter: Meter,
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
        Arc::new(
            self.meter
                .u64_gauge(params.name)
                .with_unit(params.unit)
                .with_description(params.description)
                .init(),
        )
    }

    fn gauge_f64(&self, params: MetricParameters) -> Arc<dyn GaugeF64> {
        Arc::new(
            self.meter
                .f64_gauge(params.name)
                .with_unit(params.unit)
                .with_description(params.description)
                .init(),
        )
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
