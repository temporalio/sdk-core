use anyhow::anyhow;
use bytes::BytesMut;
use futures::{stream::BoxStream, SinkExt, StreamExt};
use prost::bytes::Bytes;
use std::path::Path;
use temporal_sdk_core_api::worker::WorkerConfig;
use tokio::{fs::File, sync::mpsc::UnboundedReceiver};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

pub struct WFStateReplayData {
    pub config: WorkerConfig,
    pub inputs: BoxStream<'static, Result<BytesMut, std::io::Error>>,
}

pub async fn stream_to_file(
    config: &WorkerConfig,
    mut rcv: UnboundedReceiver<Vec<u8>>,
) -> Result<(), anyhow::Error> {
    let file = File::create("wf_inputs").await?;
    let mut transport = FramedWrite::new(file, ldc());
    // First write the worker config, since things like cache size affect how many evictions there
    // will be, etc.
    transport.send(rmp_serde::to_vec(config)?.into()).await?;
    while let Some(v) = rcv.recv().await {
        transport.send(Bytes::from(v)).await?;
    }
    Ok(())
}

pub async fn read_from_file(path: impl AsRef<Path>) -> Result<WFStateReplayData, anyhow::Error> {
    let file = File::open(path).await?;
    let mut framed_read = FramedRead::new(file, ldc());
    // Deserialize the worker config first
    let config = framed_read
        .next()
        .await
        .ok_or_else(|| anyhow!("Replay data file is empty"))??;
    let config = rmp_serde::from_slice(config.as_ref())?;

    Ok(WFStateReplayData {
        config,
        inputs: framed_read.boxed(),
    })
}

fn ldc() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .length_field_type::<u16>()
        .new_codec()
}
