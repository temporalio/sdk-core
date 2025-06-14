use crate::api::telemetry::metrics::MetricAttributes;
use fmt::Debug;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use std::{fmt, sync::Arc};
use opentelemetry_sdk::InMemoryExporterError;
use opentelemetry_sdk::metrics::data::ResourceMetrics;
use temporal_sdk_core_api::telemetry::metrics::{
    CoreMeter, Counter, Gauge, GaugeF64, Histogram, HistogramDuration, HistogramF64,
    MetricParameters, NewAttributes,
};

#[derive(Debug)]
pub enum Exporter {
    Otel(Arc<dyn CoreMeter>),
    Prometheus(Arc<dyn CoreMeter>),
}

#[derive(Debug, Clone)]
pub struct InMemoryThing {
    pub(crate) in_memory_exporter: InMemoryMetricExporter,
    pub(crate) mp: SdkMeterProvider,
}

impl InMemoryThing {
    pub fn new(in_memory_exporter: InMemoryMetricExporter, mp: SdkMeterProvider) -> InMemoryThing {
        Self {
            in_memory_exporter,
            mp,
        }
    }
    pub fn meter_provider(&self) -> SdkMeterProvider {
        self.mp.clone()
    }

    pub fn in_mem_exporter(&self) -> InMemoryMetricExporter {
        self.in_memory_exporter.clone()
    }

    pub fn get_metrics(&self) ->  Result<Vec<ResourceMetrics>, InMemoryExporterError> {
        // TODO: propogate error
        self.mp.force_flush().unwrap();
        self.in_memory_exporter.get_finished_metrics()
    }
}

/// A composite meter that combines a otel/prom exporter with an in-memory store
#[derive(Debug)]
pub struct CompositeMeter {
    pub(crate) exporter: Exporter,
    pub(crate) in_memory_thing: InMemoryThing,
}

impl CompositeMeter {
    pub fn new(
        exporter: Exporter,
        in_memory_exporter: InMemoryMetricExporter,
        mp: SdkMeterProvider,
    ) -> Self {
        CompositeMeter {
            exporter,
            in_memory_thing: InMemoryThing {
                in_memory_exporter,
                mp,
            },
        }
    }

    pub fn force_flush(&self) -> OTelSdkResult {
        self.in_memory_thing.mp.force_flush()
    }

    pub fn meter_provider(&self) -> SdkMeterProvider {
        self.in_memory_thing.mp.clone()
    }

    pub fn in_mem_exporter(&self) -> InMemoryThing {
        self.in_memory_thing.clone()
    }

    // pub fn get_counter(&self)
}

impl CoreMeter for CompositeMeter {
    fn new_attributes(&self, attribs: NewAttributes) -> MetricAttributes {
        match &self.exporter {
            Exporter::Otel(meter) => meter.new_attributes(attribs),
            Exporter::Prometheus(meter) => meter.new_attributes(attribs),
        }
    }

    fn extend_attributes(
        &self,
        existing: MetricAttributes,
        attribs: NewAttributes,
    ) -> MetricAttributes {
        match &self.exporter {
            Exporter::Otel(meter) => meter.extend_attributes(existing, attribs),
            Exporter::Prometheus(meter) => meter.extend_attributes(existing, attribs),
        }
    }
    fn counter(&self, params: MetricParameters) -> Arc<dyn Counter> {
        match &self.exporter {
            Exporter::Otel(meter) => meter.counter(params),
            Exporter::Prometheus(meter) => meter.counter(params),
        }
    }
    fn histogram(&self, params: MetricParameters) -> Arc<dyn Histogram> {
        match &self.exporter {
            Exporter::Otel(meter) => meter.histogram(params),
            Exporter::Prometheus(meter) => meter.histogram(params),
        }
    }
    fn histogram_f64(&self, params: MetricParameters) -> Arc<dyn HistogramF64> {
        match &self.exporter {
            Exporter::Otel(meter) => meter.histogram_f64(params),
            Exporter::Prometheus(meter) => meter.histogram_f64(params),
        }
    }
    fn histogram_duration(&self, params: MetricParameters) -> Arc<dyn HistogramDuration> {
        match &self.exporter {
            Exporter::Otel(meter) => meter.histogram_duration(params),
            Exporter::Prometheus(meter) => meter.histogram_duration(params),
        }
    }
    fn gauge(&self, params: MetricParameters) -> Arc<dyn Gauge> {
        match &self.exporter {
            Exporter::Otel(meter) => meter.gauge(params),
            Exporter::Prometheus(meter) => meter.gauge(params),
        }
    }
    fn gauge_f64(&self, params: MetricParameters) -> Arc<dyn GaugeF64> {
        match &self.exporter {
            Exporter::Otel(meter) => meter.gauge_f64(params),
            Exporter::Prometheus(meter) => meter.gauge_f64(params),
        }
    }
}
