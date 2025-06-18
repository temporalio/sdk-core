use crate::api::telemetry::metrics::MetricAttributes;
use fmt::Debug;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use std::{fmt, sync::Arc};
use opentelemetry::KeyValue;
use opentelemetry::metrics::Meter;
use opentelemetry_sdk::InMemoryExporterError;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use temporal_sdk_core_api::telemetry::metrics::{
    CoreMeter, Counter, Gauge, GaugeF64, Histogram, HistogramDuration, HistogramF64,
    MetricParameters, NewAttributes,
};
use crate::abstractions::dbg_panic;

#[derive(Debug, Clone)]
pub struct InMemoryMeter {
    pub(crate) meter: Meter,
    pub(crate) in_memory_exporter: InMemoryMetricExporter,
    pub(crate) mp: SdkMeterProvider,

}

impl InMemoryMeter {
    /// TODO:
    pub fn new(in_memory_exporter: InMemoryMetricExporter, meter: Meter, mp: SdkMeterProvider) -> InMemoryMeter {
        Self {
            meter,
            in_memory_exporter,
            mp,
        }
    }
    
    /// TODO
    pub fn meter_provider(&self) -> SdkMeterProvider {
        self.mp.clone()
    }

    /// TODO
    pub fn in_mem_exporter(&self) -> InMemoryMetricExporter {
        self.in_memory_exporter.clone()
    }

    /// TODO
    pub fn get_metrics(&self) ->  Result<Vec<ResourceMetrics>, InMemoryExporterError> {
        // TODO: propogate error
        self.mp.force_flush().unwrap();
        self.in_memory_exporter.get_finished_metrics()
    }
}

impl CoreMeter for InMemoryMeter {
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
                .build(),
        )
    }

    fn histogram(&self, params: MetricParameters) -> Arc<dyn Histogram> {
        Arc::new(
            self.meter
                .u64_histogram(params.name)
                .with_unit(params.unit)
                .with_description(params.description)
                .build(),
        )
    }

    fn histogram_f64(&self, params: MetricParameters) -> Arc<dyn HistogramF64> {
        Arc::new(
            self.meter
                .f64_histogram(params.name)
                .with_unit(params.unit)
                .with_description(params.description)
                .build(),
        )
    }

    // TODO: fix
    fn histogram_duration(&self, mut params: MetricParameters) -> Arc<dyn HistogramDuration> {
        params.unit = "s".into();
        Arc::new(
            crate::telemetry::otel::DurationHistogram::Seconds(self.histogram_f64(params))
        )
    }

    fn gauge(&self, params: MetricParameters) -> Arc<dyn Gauge> {
        Arc::new(
            self.meter
                .u64_gauge(params.name)
                .with_unit(params.unit)
                .with_description(params.description)
                .build(),
        )
    }

    fn gauge_f64(&self, params: MetricParameters) -> Arc<dyn GaugeF64> {
        Arc::new(
            self.meter
                .f64_gauge(params.name)
                .with_unit(params.unit)
                .with_description(params.description)
                .build(),
        )
    }
}
