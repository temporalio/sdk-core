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
    pub(crate) original: update::v1::Request,
}

impl UpdateRequest {
    pub(crate) fn name(&self) -> &str {
        &self
            .original
            .input
            .as_ref()
            .expect("Update request's `input` field must be populated")
            .name
    }

    pub(crate) fn headers(&self) -> HashMap<String, Payload> {
        self.original
            .input
            .as_ref()
            .expect("Update request's `input` field must be populated")
            .header
            .clone()
            .map(Into::into)
            .unwrap_or_default()
    }

    pub(crate) fn input(&self) -> Vec<Payload> {
        self.original
            .input
            .as_ref()
            .expect("Update request's `input` field must be populated")
            .args
            .clone()
            .map(|ps| ps.payloads)
            .unwrap_or_default()
    }

    pub(crate) fn meta(&self) -> &update::v1::Meta {
        self.original
            .meta
            .as_ref()
            .expect("Update request's `meta` field must be populated")
    }
}

impl TryFrom<update::v1::Request> for UpdateRequest {
    type Error = anyhow::Error;

    fn try_from(r: update::v1::Request) -> Result<Self, Self::Error> {
        if r.input.is_none() {
            return Err(anyhow!("Update request's `input` field must be populated"));
        }
        if r.meta.is_none() {
            return Err(anyhow!("Update request's `meta` field must be populated"));
        }
        Ok(UpdateRequest { original: r })
    }
}
