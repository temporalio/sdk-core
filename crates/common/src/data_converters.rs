//! Contains traits for and default implementations of data converters, codecs, and other
//! serialization related functionality.

use crate::protos::temporal::api::{common::v1::Payload, failure::v1::Failure};
use futures::{FutureExt, future::BoxFuture};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone)]
pub struct DataConverter {
    payload_converter: PayloadConverter,
    #[allow(dead_code)] // Will be used for failure conversion
    failure_converter: Arc<dyn FailureConverter + Send + Sync>,
    codec: Arc<dyn PayloadCodec + Send + Sync>,
}

impl std::fmt::Debug for DataConverter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataConverter")
            .field("payload_converter", &self.payload_converter)
            .finish_non_exhaustive()
    }
}
impl DataConverter {
    /// Create a new DataConverter with the given payload converter, failure converter, and codec.
    pub fn new(
        payload_converter: PayloadConverter,
        failure_converter: impl FailureConverter + Send + Sync + 'static,
        codec: impl PayloadCodec + Send + Sync + 'static,
    ) -> Self {
        Self {
            payload_converter,
            failure_converter: Arc::new(failure_converter),
            codec: Arc::new(codec),
        }
    }

    pub async fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        context: &SerializationContext,
        val: &T,
    ) -> Result<Payload, PayloadConversionError> {
        let payload = self.payload_converter.to_payload(context, val)?;
        let encoded = self.codec.encode(context, vec![payload]).await;
        encoded
            .into_iter()
            .next()
            .ok_or(PayloadConversionError::WrongEncoding)
    }

    pub async fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        context: &SerializationContext,
        payload: Payload,
    ) -> Result<T, PayloadConversionError> {
        let decoded = self.codec.decode(context, vec![payload]).await;
        let payload = decoded
            .into_iter()
            .next()
            .ok_or(PayloadConversionError::WrongEncoding)?;
        self.payload_converter.from_payload(context, payload)
    }

    /// Returns the payload converter component of this data converter.
    pub fn payload_converter(&self) -> &PayloadConverter {
        &self.payload_converter
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
    /// This variant signals the user wants to delegate to wrapper types
    UseWrappers,
    Composite(Arc<CompositePayloadConverter>),
}

impl std::fmt::Debug for PayloadConverter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PayloadConverter::Serde(_) => write!(f, "PayloadConverter::Serde(...)"),
            PayloadConverter::UseWrappers => write!(f, "PayloadConverter::UseWrappers"),
            PayloadConverter::Composite(_) => write!(f, "PayloadConverter::Composite(...)"),
        }
    }
}
impl PayloadConverter {
    pub fn serde_json() -> Self {
        Self::Serde(Arc::new(SerdeJsonPayloadConverter))
    }
    // TODO [rust-sdk-branch]: Proto binary, other standard built-ins
}

impl Default for PayloadConverter {
    fn default() -> Self {
        Self::Composite(Arc::new(CompositePayloadConverter {
            converters: vec![Self::UseWrappers, Self::serde_json()],
        }))
    }
}

#[derive(Debug)]
pub enum PayloadConversionError {
    WrongEncoding,
    EncodingError(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for PayloadConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PayloadConversionError::WrongEncoding => write!(f, "Wrong encoding"),
            PayloadConversionError::EncodingError(err) => write!(f, "Encoding error: {}", err),
        }
    }
}

impl std::error::Error for PayloadConversionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PayloadConversionError::WrongEncoding => None,
            PayloadConversionError::EncodingError(err) => Some(err.as_ref()),
        }
    }
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
        context: &SerializationContext,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>>;
    fn decode(
        &self,
        context: &SerializationContext,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>>;
}
pub struct DefaultPayloadCodec;

/// Indicates some type can be serialized for use with Temporal.
///
/// You don't need to implement this unless you are using a non-serde-compatible custom converter,
/// in which case you should implement the to/from_payload functions on some wrapper type.
pub trait TemporalSerializable {
    fn as_serde(&self) -> Result<&dyn erased_serde::Serialize, PayloadConversionError> {
        Err(PayloadConversionError::WrongEncoding)
    }
    fn to_payload(&self, _: &SerializationContext) -> Result<Payload, PayloadConversionError> {
        Err(PayloadConversionError::WrongEncoding)
    }
}

/// Indicates some type can be deserialized for use with Temporal.
///
/// You don't need to implement this unless you are using a non-serde-compatible custom converter,
/// in which case you should implement the to/from_payload functions on some wrapper type.
pub trait TemporalDeserializable: Sized {
    fn from_serde(
        _: &dyn ErasedSerdePayloadConverter,
        _: &SerializationContext,
        _: Payload,
    ) -> Result<Self, PayloadConversionError> {
        Err(PayloadConversionError::WrongEncoding)
    }
    fn from_payload(_: Payload, _: &SerializationContext) -> Result<Self, PayloadConversionError> {
        Err(PayloadConversionError::WrongEncoding)
    }
}

#[derive(Clone, Debug)]
pub struct RawValue {
    pub payload: Payload,
}

impl TemporalSerializable for RawValue {
    fn to_payload(&self, _: &SerializationContext) -> Result<Payload, PayloadConversionError> {
        Ok(self.payload.clone())
    }
}

impl TemporalDeserializable for RawValue {
    fn from_payload(p: Payload, _: &SerializationContext) -> Result<Self, PayloadConversionError> {
        Ok(RawValue { payload: p })
    }
}

pub trait GenericPayloadConverter {
    fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        context: &SerializationContext,
        val: &T,
    ) -> Result<Payload, PayloadConversionError>;
    #[allow(clippy::wrong_self_convention)]
    fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        context: &SerializationContext,
        payload: Payload,
    ) -> Result<T, PayloadConversionError>;
}

impl GenericPayloadConverter for PayloadConverter {
    fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        context: &SerializationContext,
        val: &T,
    ) -> Result<Payload, PayloadConversionError> {
        match self {
            PayloadConverter::Serde(pc) => pc.to_payload(context, val.as_serde()?),
            PayloadConverter::UseWrappers => T::to_payload(val, context),
            PayloadConverter::Composite(composite) => {
                for converter in &composite.converters {
                    match converter.to_payload(context, val) {
                        Ok(payload) => return Ok(payload),
                        Err(PayloadConversionError::WrongEncoding) => continue,
                        Err(e) => return Err(e),
                    }
                }
                Err(PayloadConversionError::WrongEncoding)
            }
        }
    }

    fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        context: &SerializationContext,
        payload: Payload,
    ) -> Result<T, PayloadConversionError> {
        match self {
            PayloadConverter::Serde(pc) => T::from_serde(pc.as_ref(), context, payload),
            PayloadConverter::UseWrappers => T::from_payload(payload, context),
            PayloadConverter::Composite(composite) => {
                for converter in &composite.converters {
                    match converter.from_payload(context, payload.clone()) {
                        Ok(val) => return Ok(val),
                        Err(PayloadConversionError::WrongEncoding) => continue,
                        Err(e) => return Err(e),
                    }
                }
                Err(PayloadConversionError::WrongEncoding)
            }
        }
    }
}

// TODO [rust-sdk-branch]: Potentially allow opt-out / no-serde compile flags
impl<T> TemporalSerializable for T
where
    T: serde::Serialize,
{
    fn as_serde(&self) -> Result<&dyn erased_serde::Serialize, PayloadConversionError> {
        Ok(self)
    }
}
impl<T> TemporalDeserializable for T
where
    T: serde::de::DeserializeOwned,
{
    fn from_serde(
        pc: &dyn ErasedSerdePayloadConverter,
        context: &SerializationContext,
        payload: Payload,
    ) -> Result<Self, PayloadConversionError>
    where
        Self: Sized,
    {
        let mut de = pc.from_payload(context, payload)?;
        erased_serde::deserialize(&mut de)
            .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))
    }
}

struct SerdeJsonPayloadConverter;
impl ErasedSerdePayloadConverter for SerdeJsonPayloadConverter {
    fn to_payload(
        &self,
        _: &SerializationContext,
        value: &dyn erased_serde::Serialize,
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
        _: &SerializationContext,
        payload: Payload,
    ) -> Result<Box<dyn erased_serde::Deserializer<'static>>, PayloadConversionError> {
        let encoding = payload.metadata.get("encoding").map(|v| v.as_slice());
        if encoding != Some(b"json/plain".as_slice()) {
            return Err(PayloadConversionError::WrongEncoding);
        }
        let json_v: serde_json::Value = serde_json::from_slice(&payload.data)
            .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))?;
        Ok(Box::new(<dyn erased_serde::Deserializer>::erase(json_v)))
    }
}
pub trait ErasedSerdePayloadConverter: Send + Sync {
    fn to_payload(
        &self,
        context: &SerializationContext,
        value: &dyn erased_serde::Serialize,
    ) -> Result<Payload, PayloadConversionError>;
    #[allow(clippy::wrong_self_convention)]
    fn from_payload(
        &self,
        context: &SerializationContext,
        payload: Payload,
    ) -> Result<Box<dyn erased_serde::Deserializer<'static>>, PayloadConversionError>;
}

// TODO [rust-sdk-branch]: All prost things should be behind a compile flag

pub struct ProstSerializable<T: prost::Message>(pub T);
impl<T> TemporalSerializable for ProstSerializable<T>
where
    T: prost::Message + Default + 'static,
{
    fn to_payload(&self, _: &SerializationContext) -> Result<Payload, PayloadConversionError> {
        let as_proto = prost::Message::encode_to_vec(&self.0);
        Ok(Payload {
            metadata: {
                let mut hm = HashMap::new();
                hm.insert("encoding".to_string(), b"binary/protobuf".to_vec());
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
    fn from_payload(p: Payload, _: &SerializationContext) -> Result<Self, PayloadConversionError>
    where
        Self: Sized,
    {
        let encoding = p.metadata.get("encoding").map(|v| v.as_slice());
        if encoding != Some(b"binary/protobuf".as_slice()) {
            return Err(PayloadConversionError::WrongEncoding);
        }
        T::decode(p.data.as_slice())
            .map(ProstSerializable)
            .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))
    }
}

#[derive(Clone)]
pub struct CompositePayloadConverter {
    converters: Vec<PayloadConverter>,
}

impl Default for DataConverter {
    fn default() -> Self {
        Self::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            DefaultPayloadCodec,
        )
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
        _: &SerializationContext,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        async move { payloads }.boxed()
    }
    fn decode(
        &self,
        _: &SerializationContext,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        async move { payloads }.boxed()
    }
}
