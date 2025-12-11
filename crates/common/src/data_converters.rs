//! Contains traits for and default implementations of data converters, codecs, and other
//! serialization related functionality.

use crate::protos::temporal::api::{common::v1::Payload, failure::v1::Failure};
use futures::{FutureExt, future::BoxFuture};
use std::{collections::HashMap, sync::Arc};

// TODO [rust-sdk-branch]: Use
#[allow(unused)]
pub struct DataConverter {
    payload_converter: PayloadConverter,
    failure_converter: Box<dyn FailureConverter>,
    codec: Box<dyn PayloadCodec>,
}
impl DataConverter {
    pub fn new(
        payload_converter: PayloadConverter,
        failure_converter: impl FailureConverter + 'static,
        codec: impl PayloadCodec + 'static,
    ) -> Self {
        Self {
            payload_converter,
            failure_converter: Box::new(failure_converter),
            codec: Box::new(codec),
        }
    }
}

pub enum SerializationContext {
    // TODO [rust-sdk-branch]: Fill in
    Workflow,
    Activity,
    Nexus,
    None,
}
#[derive(Clone)]
pub enum PayloadConverter {
    Serde(Arc<dyn ErasedSerdePayloadConverter>),
    // This variant signals the user wants to delegate to wrapper types
    UseWrappers,
    Composite(Arc<CompositePayloadConverter>),
}
impl PayloadConverter {
    pub fn serde_json() -> Self {
        Self::Serde(Arc::new(SerdeJsonPayloadConverter))
    }
    // TODO [rust-sdk-branch]: Proto binary, other standard built-ins
}

pub enum PayloadConversionError {
    WrongEncoding,
    EncodingError(Box<dyn std::error::Error>),
}

pub trait FailureConverter {
    fn to_failure(
        &self,
        error: Box<dyn std::error::Error>,
        payload_converter: &PayloadConverter,
        context: &SerializationContext,
    ) -> Result<Failure, PayloadConversionError>;

    fn to_error(
        &self,
        failure: Failure,
        payload_converter: &PayloadConverter,
        context: &SerializationContext,
    ) -> Result<Box<dyn std::error::Error>, PayloadConversionError>;
}
pub struct DefaultFailureConverter;
pub trait PayloadCodec {
    fn encode(
        &self,
        payloads: Vec<Payload>,
        context: &SerializationContext,
    ) -> BoxFuture<'static, Vec<Payload>>;
    fn decode(
        &self,
        payloads: Vec<Payload>,
        context: &SerializationContext,
    ) -> BoxFuture<'static, Vec<Payload>>;
}
pub struct DefaultPayloadCodec;

/// Indicates some type can be serialized for use with Temporal.
///
/// You don't need to implement this unless you are using a non-serde-compatible custom converter,
/// in which case you should implement the to/from_payload functions on some wrapper type.
pub trait TemporalSerializable {
    fn as_serde(&self) -> Option<&dyn erased_serde::Serialize> {
        None
    }
    fn to_payload(&self, _: &SerializationContext) -> Option<Payload> {
        None
    }
}
///
/// Indicates some type can be deserialized for use with Temporal.
///
/// You don't need to implement this unless you are using a non-serde-compatible custom converter,
/// in which case you should implement the to/from_payload functions on some wrapper type.
pub trait TemporalDeserializable: Sized {
    fn from_serde(
        _: &dyn ErasedSerdePayloadConverter,
        _: Payload,
        _: &SerializationContext,
    ) -> Option<Self> {
        None
    }
    fn from_payload(_: Payload, _: &SerializationContext) -> Option<Self> {
        None
    }
}

// TODO [rust-sdk-branch]: implement serialize/deserialize directly (should always bypass the
// converter).
#[derive(Clone)]
pub struct RawValue {
    pub payload: Payload,
}

pub trait GenericPayloadConverter {
    fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        val: &T,
        context: &SerializationContext,
    ) -> Result<Payload, PayloadConversionError>;
    #[allow(clippy::wrong_self_convention)]
    fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        payload: Payload,
        context: &SerializationContext,
    ) -> Result<T, PayloadConversionError>;
}

impl GenericPayloadConverter for PayloadConverter {
    fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        val: &T,
        context: &SerializationContext,
    ) -> Result<Payload, PayloadConversionError> {
        match self {
            PayloadConverter::Serde(pc) => {
                Ok(pc.to_payload(val.as_serde().ok_or_else(|| todo!())?, context)?)
            }
            PayloadConverter::UseWrappers => {
                Ok(T::to_payload(val, context).ok_or_else(|| todo!())?)
            }
            // Fairly clear how this would work
            PayloadConverter::Composite(_pc) => todo!(),
        }
    }

    fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        payload: Payload,
        context: &SerializationContext,
    ) -> Result<T, PayloadConversionError> {
        match self {
            PayloadConverter::Serde(pc) => {
                Ok(T::from_serde(pc.as_ref(), payload, context).ok_or_else(|| todo!())?)
            }
            PayloadConverter::UseWrappers => {
                Ok(T::from_payload(payload, context).ok_or_else(|| todo!())?)
            }
            PayloadConverter::Composite(_pc) => todo!(),
        }
    }
}

// TODO [rust-sdk-branch]: Potentially allow opt-out / no-serde compile flags
impl<T> TemporalSerializable for T
where
    T: serde::Serialize,
{
    fn as_serde(&self) -> Option<&dyn erased_serde::Serialize> {
        Some(self)
    }
}
impl<T> TemporalDeserializable for T
where
    T: serde::de::DeserializeOwned,
{
    fn from_serde(
        pc: &dyn ErasedSerdePayloadConverter,
        payload: Payload,
        context: &SerializationContext,
    ) -> Option<Self>
    where
        Self: Sized,
    {
        erased_serde::deserialize(&mut pc.from_payload(payload, context).ok()?).ok()
    }
}

struct SerdeJsonPayloadConverter;
impl ErasedSerdePayloadConverter for SerdeJsonPayloadConverter {
    fn to_payload(
        &self,
        value: &dyn erased_serde::Serialize,
        _context: &SerializationContext,
    ) -> Result<Payload, PayloadConversionError> {
        let as_json = serde_json::to_vec(value).map_err(|_| todo!())?;
        Ok(Payload {
            metadata: {
                let mut hm = HashMap::new();
                hm.insert("encoding".to_string(), b"json/plain".to_vec());
                hm
            },
            data: as_json,
        })
    }

    fn from_payload(
        &self,
        payload: Payload,
        _context: &SerializationContext,
    ) -> Result<Box<dyn erased_serde::Deserializer<'static>>, PayloadConversionError> {
        // TODO: Would check metadata
        let json_v: serde_json::Value =
            serde_json::from_slice(&payload.data).map_err(|_| todo!())?;
        Ok(Box::new(<dyn erased_serde::Deserializer>::erase(json_v)))
    }
}
pub trait ErasedSerdePayloadConverter: Send + Sync {
    fn to_payload(
        &self,
        value: &dyn erased_serde::Serialize,
        context: &SerializationContext,
    ) -> Result<Payload, PayloadConversionError>;
    #[allow(clippy::wrong_self_convention)]
    fn from_payload(
        &self,
        payload: Payload,
        context: &SerializationContext,
    ) -> Result<Box<dyn erased_serde::Deserializer<'static>>, PayloadConversionError>;
}

// TODO [rust-sdk-branch]: All prost things should be behind a compile flag

pub struct ProstSerializable<T: prost::Message>(T);
impl<T> TemporalSerializable for ProstSerializable<T>
where
    T: prost::Message + Default + 'static,
{
    fn to_payload(&self, _: &SerializationContext) -> Option<Payload> {
        let as_proto = prost::Message::encode_to_vec(&self.0);
        Some(Payload {
            metadata: {
                let mut hm = HashMap::new();
                hm.insert("encoding".to_string(), b"proto/binary".to_vec());
                hm
            },
            data: as_proto,
        })
    }
}
impl<T> TemporalDeserializable for ProstSerializable<T>
where
    T: prost::Message + Default + 'static,
{
    fn from_payload(p: Payload, _: &SerializationContext) -> Option<Self>
    where
        Self: Sized,
    {
        // TODO [rust-sdk-branch]: Check metadata
        Some(ProstSerializable(T::decode(p.data.as_slice()).ok()?))
    }
}

#[derive(Clone)]
pub struct CompositePayloadConverter {
    _converters: Vec<PayloadConverter>,
}

impl Default for DataConverter {
    fn default() -> Self {
        todo!()
    }
}
impl FailureConverter for DefaultFailureConverter {
    fn to_failure(
        &self,
        _: Box<dyn std::error::Error>,
        _: &PayloadConverter,
        _: &SerializationContext,
    ) -> Result<Failure, PayloadConversionError> {
        todo!()
    }
    fn to_error(
        &self,
        _: Failure,
        _: &PayloadConverter,
        _: &SerializationContext,
    ) -> Result<Box<dyn std::error::Error>, PayloadConversionError> {
        todo!()
    }
}
impl PayloadCodec for DefaultPayloadCodec {
    fn encode(
        &self,
        payloads: Vec<Payload>,
        _: &SerializationContext,
    ) -> BoxFuture<'static, Vec<Payload>> {
        async move { payloads }.boxed()
    }
    fn decode(
        &self,
        payloads: Vec<Payload>,
        _: &SerializationContext,
    ) -> BoxFuture<'static, Vec<Payload>> {
        async move { payloads }.boxed()
    }
}
