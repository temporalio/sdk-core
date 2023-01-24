use futures::SinkExt;
use prost::bytes::Bytes;
use tokio::{fs::File, sync::mpsc::UnboundedReceiver};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub async fn stream_to_file(mut rcv: UnboundedReceiver<Vec<u8>>) -> Result<(), anyhow::Error> {
    let file = File::create("wf_inputs").await?;
    let ldc = LengthDelimitedCodec::builder()
        .length_field_type::<u16>()
        .new_codec();
    let mut transport = Framed::new(file, ldc);
    while let Some(v) = rcv.recv().await {
        transport.send(Bytes::from(v)).await?;
    }
    Ok(())
}
