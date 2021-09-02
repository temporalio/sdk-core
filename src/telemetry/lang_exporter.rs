use opentelemetry::trace::TraceError;
use opentelemetry::{
    metrics::Descriptor,
    sdk::export::{
        metrics::{CheckpointSet, ExportKind, ExportKindFor, ExportKindSelector, Exporter},
        trace::{ExportResult, SpanData, SpanExporter},
    },
};
use tokio::sync::mpsc::{channel, Receiver, Sender};

#[derive(Default)]
pub struct OTelExportStreams {
    pub tracing: Option<Receiver<Vec<SpanData>>>,
    pub metrics: Option<Receiver<Vec<SpanData>>>,
}

#[derive(Debug)]
struct LangMetricsExporter {
    sender: Sender<()>,
}

impl ExportKindFor for LangMetricsExporter {
    fn export_kind_for(&self, descriptor: &Descriptor) -> ExportKind {
        ExportKindSelector::Stateless.export_kind_for(descriptor)
    }
}

impl Exporter for LangMetricsExporter {
    fn export(&self, checkpoint_set: &mut dyn CheckpointSet) -> opentelemetry::metrics::Result<()> {
        checkpoint_set.try_for_each(self, &mut |record| {
            // Construct the metric proto per record
            Ok(())
        })?;
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
