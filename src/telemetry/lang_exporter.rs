mod proto_serialization;

use crate::telemetry::lang_exporter::proto_serialization::metrics::{
    record_to_metric, sink, CheckpointedMetrics,
};
use opentelemetry::{
    metrics::Descriptor,
    sdk::export::{
        metrics::{CheckpointSet, ExportKind, ExportKindFor, ExportKindSelector, Exporter},
        trace::{ExportResult, SpanData, SpanExporter},
    },
    sdk::InstrumentationLibrary,
    trace::TraceError,
};
use std::sync::Arc;
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[derive(Default)]
pub struct OTelExportStreams {
    pub tracing: Option<Receiver<Vec<SpanData>>>,
    pub metrics: Option<Receiver<Vec<SpanData>>>,
}

#[derive(Debug)]
struct LangMetricsExporter {
    sender: Sender<()>,
    export_kind_selector: Arc<dyn ExportKindFor + Send + Sync>,
}

impl ExportKindFor for LangMetricsExporter {
    fn export_kind_for(&self, descriptor: &Descriptor) -> ExportKind {
        ExportKindSelector::Stateless.export_kind_for(descriptor)
    }
}

impl Exporter for LangMetricsExporter {
    fn export(&self, checkpoint_set: &mut dyn CheckpointSet) -> opentelemetry::metrics::Result<()> {
        let mut resource_metrics: Vec<CheckpointedMetrics> = Vec::default();
        checkpoint_set.try_for_each(self.export_kind_selector.as_ref(), &mut |record| {
            let metric_result = record_to_metric(record, self.export_kind_selector.as_ref());
            match metric_result {
                Ok(metrics) => {
                    resource_metrics.push((
                        record.resource().clone().into(),
                        InstrumentationLibrary::new("temporal-sdk-core-exporter", None),
                        metrics,
                    ));
                    Ok(())
                }
                Err(err) => Err(err),
            }
        })?;
        let resource_metrics = sink(resource_metrics);
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct LangSpanExporter {
    sender: Sender<Vec<SpanData>>,
}

impl LangSpanExporter {
    pub fn new(buffer_size: usize) -> (Self, Receiver<Vec<SpanData>>) {
        let (tx, rx) = channel(buffer_size);
        (Self { sender: tx }, rx)
    }
}

#[async_trait::async_trait]
impl SpanExporter for LangSpanExporter {
    async fn export(&mut self, batch: Vec<SpanData>) -> ExportResult {
        self.sender.send(batch).await.map_err(|e| {
            TraceError::Other(
                anyhow::anyhow!(
                    "Cannot export spans because receive half of export channel is closed: {:?}",
                    e
                )
                .into(),
            )
        })?;
        Ok(())
    }
}
