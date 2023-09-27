use anyhow::{anyhow, bail};
use temporal_sdk_core_protos::temporal::api::{
    protocol::v1::{message::SequencingId, Message},
    update,
};

/// A decoded & verified of a [Message] that came with a WFT.
#[derive(Debug, Clone, PartialEq)]
pub struct IncomingProtocolMessage {
    id: String,
    protocol_instance_id: String,
    sequencing_id: Option<SequencingId>,
    body: IncomingProtocolMessageBody,
}
impl TryFrom<Message> for IncomingProtocolMessage {
    type Error = anyhow::Error;

    fn try_from(m: Message) -> Result<Self, Self::Error> {
        let body = m.body.try_into()?;
        Ok(Self {
            id: m.id,
            protocol_instance_id: m.protocol_instance_id,
            sequencing_id: m.sequencing_id,
            body,
        })
    }
}

/// All the protocol [Message] bodies Core understands that might come to us when receiving a new
/// WFT.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum IncomingProtocolMessageBody {
    UpdateRequest(update::v1::Request),
}

impl TryFrom<Option<prost_types::Any>> for IncomingProtocolMessageBody {
    type Error = anyhow::Error;

    fn try_from(v: Option<prost_types::Any>) -> Result<Self, Self::Error> {
        let v = v.ok_or_else(|| anyhow!("Protocol message body must be populated"))?;
        // Undo explicit type url checks when https://github.com/fdeantoni/prost-wkt/issues/48 is
        // fixed
        Ok(match v.type_url.as_str() {
            "type.googleapis.com/temporal.api.update.v1.Request" => {
                IncomingProtocolMessageBody::UpdateRequest(
                    v.unpack_as(update::v1::Request::default())?,
                )
            }
            o => bail!("Could not understand protocol message type {}", o),
        })
    }
}
