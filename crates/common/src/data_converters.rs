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
        data: &SerializationContextData,
        val: &T,
    ) -> Result<Payload, PayloadConversionError> {
        let context = SerializationContext {
            data,
            converter: &self.payload_converter,
        };
        let payload = self.payload_converter.to_payload(&context, val)?;
        let encoded = self.codec.encode(data, vec![payload]).await;
        encoded
            .into_iter()
            .next()
            .ok_or(PayloadConversionError::WrongEncoding)
    }

    pub async fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        data: &SerializationContextData,
        payload: Payload,
    ) -> Result<T, PayloadConversionError> {
        let context = SerializationContext {
            data,
            converter: &self.payload_converter,
        };
        let decoded = self.codec.decode(data, vec![payload]).await;
        let payload = decoded
            .into_iter()
            .next()
            .ok_or(PayloadConversionError::WrongEncoding)?;
        self.payload_converter.from_payload(&context, payload)
    }

    pub async fn to_payloads<T: TemporalSerializable + 'static>(
        &self,
        data: &SerializationContextData,
        val: &T,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        let context = SerializationContext {
            data,
            converter: &self.payload_converter,
        };
        let payloads = self.payload_converter.to_payloads(&context, val)?;
        Ok(self.codec.encode(data, payloads).await)
    }

    pub async fn from_payloads<T: TemporalDeserializable + 'static>(
        &self,
        data: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> Result<T, PayloadConversionError> {
        let context = SerializationContext {
            data,
            converter: &self.payload_converter,
        };
        let decoded = self.codec.decode(data, payloads).await;
        self.payload_converter.from_payloads(&context, decoded)
    }

    /// Returns the payload converter component of this data converter.
    pub fn payload_converter(&self) -> &PayloadConverter {
        &self.payload_converter
    }

    /// Returns the codec component of this data converter.
    pub fn codec(&self) -> &(dyn PayloadCodec + Send + Sync) {
        self.codec.as_ref()
    }
}

/// Data about the serialization context, indicating where the serialization is occurring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SerializationContextData {
    Workflow,
    Activity,
    Nexus,
    None,
}

/// Context for serialization operations, including the kind of context and the
/// payload converter for nested serialization.
#[derive(Clone, Copy)]
pub struct SerializationContext<'a> {
    pub data: &'a SerializationContextData,
    /// Allows nested types to serialize their contents using the same converter.
    pub converter: &'a PayloadConverter,
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
        context: &SerializationContextData,
    ) -> Result<Failure, PayloadConversionError>;

    fn to_error(
        &self,
        failure: Failure,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Result<Box<dyn std::error::Error>, PayloadConversionError>;
}
pub struct DefaultFailureConverter;
pub trait PayloadCodec {
    fn encode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>>;
    fn decode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>>;
}

impl<T: PayloadCodec> PayloadCodec for Arc<T> {
    fn encode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        (**self).encode(context, payloads)
    }
    fn decode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        (**self).decode(context, payloads)
    }
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
    fn to_payload(&self, _: &SerializationContext<'_>) -> Result<Payload, PayloadConversionError> {
        Err(PayloadConversionError::WrongEncoding)
    }
    /// Convert to multiple payloads. Override this for types representing multiple arguments.
    fn to_payloads(
        &self,
        ctx: &SerializationContext<'_>,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        Ok(vec![self.to_payload(ctx)?])
    }
}

/// Indicates some type can be deserialized for use with Temporal.
///
/// You don't need to implement this unless you are using a non-serde-compatible custom converter,
/// in which case you should implement the to/from_payload functions on some wrapper type.
pub trait TemporalDeserializable: Sized {
    fn from_serde(
        _: &dyn ErasedSerdePayloadConverter,
        _ctx: &SerializationContext<'_>,
        _: Payload,
    ) -> Result<Self, PayloadConversionError> {
        Err(PayloadConversionError::WrongEncoding)
    }
    fn from_payload(
        ctx: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<Self, PayloadConversionError> {
        let _ = (ctx, payload);
        Err(PayloadConversionError::WrongEncoding)
    }
    /// Convert from multiple payloads. Override this for types representing multiple arguments.
    fn from_payloads(
        ctx: &SerializationContext<'_>,
        payloads: Vec<Payload>,
    ) -> Result<Self, PayloadConversionError> {
        if payloads.len() != 1 {
            return Err(PayloadConversionError::WrongEncoding);
        }
        Self::from_payload(ctx, payloads.into_iter().next().unwrap())
    }
}

#[derive(Clone, Debug, Default)]
pub struct RawValue {
    pub payloads: Vec<Payload>,
}
impl RawValue {
    /// A RawValue representing no meaningful data, containing a single default payload.
    /// This ensures the value can still be serialized as a single payload.
    pub fn empty() -> Self {
        Self {
            payloads: vec![Payload::default()],
        }
    }

    /// Create a new RawValue from a vector of payloads.
    pub fn new(payloads: Vec<Payload>) -> Self {
        Self { payloads }
    }

    pub fn from_value<T: TemporalSerializable + 'static>(
        value: &T,
        converter: &PayloadConverter,
    ) -> RawValue {
        RawValue::new(vec![
            converter
                .to_payload(
                    &SerializationContext {
                        data: &SerializationContextData::None,
                        converter,
                    },
                    value,
                )
                .unwrap(),
        ])
    }

    pub fn to_value<T: TemporalDeserializable + 'static>(self, converter: &PayloadConverter) -> T {
        converter
            .from_payload(
                &SerializationContext {
                    data: &SerializationContextData::None,
                    converter,
                },
                self.payloads.into_iter().next().unwrap(),
            )
            .unwrap()
    }
}

impl TemporalSerializable for RawValue {
    fn to_payload(&self, _: &SerializationContext<'_>) -> Result<Payload, PayloadConversionError> {
        Ok(self.payloads.first().cloned().unwrap_or_default())
    }
    fn to_payloads(
        &self,
        _: &SerializationContext<'_>,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        Ok(self.payloads.clone())
    }
}

impl TemporalDeserializable for RawValue {
    fn from_payload(
        _: &SerializationContext<'_>,
        p: Payload,
    ) -> Result<Self, PayloadConversionError> {
        Ok(RawValue { payloads: vec![p] })
    }
    fn from_payloads(
        _: &SerializationContext<'_>,
        payloads: Vec<Payload>,
    ) -> Result<Self, PayloadConversionError> {
        Ok(RawValue { payloads })
    }
}

pub trait GenericPayloadConverter {
    fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        val: &T,
    ) -> Result<Payload, PayloadConversionError>;
    #[allow(clippy::wrong_self_convention)]
    fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<T, PayloadConversionError>;
    fn to_payloads<T: TemporalSerializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        val: &T,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        Ok(vec![self.to_payload(context, val)?])
    }
    #[allow(clippy::wrong_self_convention)]
    fn from_payloads<T: TemporalDeserializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        payloads: Vec<Payload>,
    ) -> Result<T, PayloadConversionError> {
        if payloads.len() != 1 {
            return Err(PayloadConversionError::WrongEncoding);
        }
        self.from_payload(context, payloads.into_iter().next().unwrap())
    }
}

impl GenericPayloadConverter for PayloadConverter {
    fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        val: &T,
    ) -> Result<Payload, PayloadConversionError> {
        let mut payloads = self.to_payloads(context, val)?;
        if payloads.len() != 1 {
            return Err(PayloadConversionError::WrongEncoding);
        }
        Ok(payloads.pop().unwrap())
    }

    fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<T, PayloadConversionError> {
        self.from_payloads(context, vec![payload])
    }

    fn to_payloads<T: TemporalSerializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        val: &T,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        match self {
            PayloadConverter::Serde(pc) => Ok(vec![pc.to_payload(context.data, val.as_serde()?)?]),
            PayloadConverter::UseWrappers => T::to_payloads(val, context),
            PayloadConverter::Composite(composite) => {
                for converter in &composite.converters {
                    match converter.to_payloads(context, val) {
                        Ok(payloads) => return Ok(payloads),
                        Err(PayloadConversionError::WrongEncoding) => continue,
                        Err(e) => return Err(e),
                    }
                }
                Err(PayloadConversionError::WrongEncoding)
            }
        }
    }

    fn from_payloads<T: TemporalDeserializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        payloads: Vec<Payload>,
    ) -> Result<T, PayloadConversionError> {
        // Handle empty payloads as unit type ()
        if payloads.is_empty() && std::any::TypeId::of::<T>() == std::any::TypeId::of::<()>() {
            let boxed: Box<dyn std::any::Any> = Box::new(());
            return Ok(*boxed.downcast::<T>().unwrap());
        }

        match self {
            PayloadConverter::Serde(pc) => {
                if payloads.len() != 1 {
                    return Err(PayloadConversionError::WrongEncoding);
                }
                T::from_serde(pc.as_ref(), context, payloads.into_iter().next().unwrap())
            }
            PayloadConverter::UseWrappers => T::from_payloads(context, payloads),
            PayloadConverter::Composite(composite) => {
                for converter in &composite.converters {
                    match converter.from_payloads(context, payloads.clone()) {
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
        context: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<Self, PayloadConversionError>
    where
        Self: Sized,
    {
        let mut de = pc.from_payload(context.data, payload)?;
        erased_serde::deserialize(&mut de)
            .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))
    }
}

struct SerdeJsonPayloadConverter;
impl ErasedSerdePayloadConverter for SerdeJsonPayloadConverter {
    fn to_payload(
        &self,
        _: &SerializationContextData,
        value: &dyn erased_serde::Serialize,
    ) -> Result<Payload, PayloadConversionError> {
        let as_json = serde_json::to_vec(value)
            .map_err(|e| PayloadConversionError::EncodingError(e.into()))?;
        Ok(Payload {
            metadata: {
                let mut hm = HashMap::new();
                hm.insert("encoding".to_string(), b"json/plain".to_vec());
                hm
            },
            data: as_json,
            external_payloads: vec![],
        })
    }

    fn from_payload(
        &self,
        _: &SerializationContextData,
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
        context: &SerializationContextData,
        value: &dyn erased_serde::Serialize,
    ) -> Result<Payload, PayloadConversionError>;
    #[allow(clippy::wrong_self_convention)]
    fn from_payload(
        &self,
        context: &SerializationContextData,
        payload: Payload,
    ) -> Result<Box<dyn erased_serde::Deserializer<'static>>, PayloadConversionError>;
}

// TODO [rust-sdk-branch]: All prost things should be behind a compile flag

pub struct ProstSerializable<T: prost::Message>(pub T);
impl<T> TemporalSerializable for ProstSerializable<T>
where
    T: prost::Message + Default + 'static,
{
    fn to_payload(&self, _: &SerializationContext<'_>) -> Result<Payload, PayloadConversionError> {
        let as_proto = prost::Message::encode_to_vec(&self.0);
        Ok(Payload {
            metadata: {
                let mut hm = HashMap::new();
                hm.insert("encoding".to_string(), b"binary/protobuf".to_vec());
                hm
            },
            data: as_proto,
            external_payloads: vec![],
        })
    }
}
impl<T> TemporalDeserializable for ProstSerializable<T>
where
    T: prost::Message + Default + 'static,
{
    fn from_payload(
        _: &SerializationContext<'_>,
        p: Payload,
    ) -> Result<Self, PayloadConversionError>
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
        _: &SerializationContextData,
    ) -> Result<Failure, PayloadConversionError> {
        todo!()
    }
    fn to_error(
        &self,
        _: Failure,
        _: &PayloadConverter,
        _: &SerializationContextData,
    ) -> Result<Box<dyn std::error::Error>, PayloadConversionError> {
        todo!()
    }
}
impl PayloadCodec for DefaultPayloadCodec {
    fn encode(
        &self,
        _: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        async move { payloads }.boxed()
    }
    fn decode(
        &self,
        _: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        async move { payloads }.boxed()
    }
}

/// Represents multiple arguments for workflows/activities that accept more than one argument.
/// Use this when interoperating with other language SDKs that allow multiple arguments.
macro_rules! impl_multi_args {
    ($name:ident; $count:expr; $($idx:tt: $ty:ident),+) => {
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $name<$($ty),+>($(pub $ty),+);

        impl<$($ty),+> TemporalSerializable for $name<$($ty),+>
        where
            $($ty: TemporalSerializable + 'static),+
        {
            fn to_payload(&self, _: &SerializationContext<'_>) -> Result<Payload, PayloadConversionError> {
                Err(PayloadConversionError::WrongEncoding)
            }
            fn to_payloads(
                &self,
                ctx: &SerializationContext<'_>,
            ) -> Result<Vec<Payload>, PayloadConversionError> {
                Ok(vec![$(ctx.converter.to_payload(ctx, &self.$idx)?),+])
            }
        }

        #[allow(non_snake_case)]
        impl<$($ty),+> From<($($ty),+,)> for $name<$($ty),+> {
            fn from(t: ($($ty),+,)) -> Self {
                $name($(t.$idx),+)
            }
        }

        impl<$($ty),+> TemporalDeserializable for $name<$($ty),+>
        where
            $($ty: TemporalDeserializable + 'static),+
        {
            fn from_payload(_: &SerializationContext<'_>, _: Payload) -> Result<Self, PayloadConversionError> {
                Err(PayloadConversionError::WrongEncoding)
            }
            fn from_payloads(
                ctx: &SerializationContext<'_>,
                payloads: Vec<Payload>,
            ) -> Result<Self, PayloadConversionError> {
                if payloads.len() != $count {
                    return Err(PayloadConversionError::WrongEncoding);
                }
                let mut iter = payloads.into_iter();
                Ok($name(
                    $(ctx.converter.from_payload::<$ty>(ctx, iter.next().unwrap())?),+
                ))
            }
        }
    };
}

impl_multi_args!(MultiArgs2; 2; 0: A, 1: B);
impl_multi_args!(MultiArgs3; 3; 0: A, 1: B, 2: C);
impl_multi_args!(MultiArgs4; 4; 0: A, 1: B, 2: C, 3: D);
impl_multi_args!(MultiArgs5; 5; 0: A, 1: B, 2: C, 3: D, 4: E);
impl_multi_args!(MultiArgs6; 6; 0: A, 1: B, 2: C, 3: D, 4: E, 5: F);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_payloads_as_unit_type() {
        let converter = PayloadConverter::default();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };

        let empty_payloads: Vec<Payload> = vec![];
        let result: Result<(), _> = converter.from_payloads(&ctx, empty_payloads);

        assert!(result.is_ok(), "Empty payloads should deserialize as ()");
    }

    #[test]
    fn multi_args_round_trip() {
        let converter = PayloadConverter::default();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };

        let args = MultiArgs2("hello".to_string(), 42i32);
        let payloads = converter.to_payloads(&ctx, &args).unwrap();
        assert_eq!(payloads.len(), 2);

        let result: MultiArgs2<String, i32> = converter.from_payloads(&ctx, payloads).unwrap();
        assert_eq!(result, args);
    }

    #[test]
    fn multi_args_from_tuple() {
        let args: MultiArgs2<String, i32> = ("hello".to_string(), 42i32).into();
        assert_eq!(args, MultiArgs2("hello".to_string(), 42));
    }
}
