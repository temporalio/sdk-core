use super::{PayloadConversionError, PayloadConverter, SerializationContextData};
use crate::protos::temporal::api::failure::v1::Failure;

/// Converts between Rust errors and Temporal [`Failure`] protobufs.
pub trait FailureConverter {
    /// Convert an error into a Temporal failure protobuf.
    fn to_failure(
        &self,
        error: Box<dyn std::error::Error>,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Result<Failure, PayloadConversionError>;

    /// Convert a Temporal failure protobuf back into a Rust error.
    fn to_error(
        &self,
        failure: Failure,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Result<Box<dyn std::error::Error>, PayloadConversionError>;
}

/// Default (currently unimplemented) failure converter.
pub struct DefaultFailureConverter;

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
