use anyhow::{anyhow, bail};
use std::collections::HashMap;
use temporal_sdk_core_protos::temporal::api::{
    common::v1::Payload,
    protocol::v1::{message::SequencingId, Message},
    update,
};

/// A decoded & verified of a [Message] that came with a WFT.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct IncomingProtocolMessage {
    pub(crate) id: String,
    pub(crate) protocol_instance_id: String,
    pub(crate) sequencing_id: Option<SequencingId>,
    pub(crate) body: IncomingProtocolMessageBody,
}

impl IncomingProtocolMessage {
    pub(crate) fn processable_after_event_id(&self) -> Option<i64> {
        match self.sequencing_id {
            None => Some(0),
            Some(SequencingId::EventId(id)) => Some(id),
            Some(SequencingId::CommandIndex(_)) => None,
        }
    }
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
    UpdateRequest(UpdateRequest),
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
                    v.unpack_as(update::v1::Request::default())?.try_into()?,
                )
            }
            o => bail!("Could not understand protocol message type {}", o),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UpdateRequest {
    pub(crate) name: String,
    pub(crate) headers: HashMap<String, Payload>,
    pub(crate) input: Vec<Payload>,
    pub(crate) meta: update::v1::Meta,
}

impl TryFrom<update::v1::Request> for UpdateRequest {
    type Error = anyhow::Error;

    fn try_from(r: update::v1::Request) -> Result<Self, Self::Error> {
        let inp = r
            .input
            .ok_or_else(|| anyhow!("Update request's `input` field must be populated"))?;
        let meta = r
            .meta
            .ok_or_else(|| anyhow!("Update request's `meta` field must be populated"))?;
        Ok(UpdateRequest {
            name: inp.name,
            headers: inp.header.map(Into::into).unwrap_or_default(),
            input: inp.args.map(|ps| ps.payloads).unwrap_or_default(),
            meta,
        })
    }
}
