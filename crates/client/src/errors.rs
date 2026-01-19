//! Contains errors that can be returned by clients.

use http::uri::InvalidUri;
use temporalio_common::data_converters::PayloadConversionError;

/// Errors thrown while attempting to establish a connection to the server
#[derive(thiserror::Error, Debug)]
pub enum ClientInitError {
    /// Invalid URI. Configuration error, fatal.
    #[error("Invalid URI: {0:?}")]
    InvalidUri(#[from] InvalidUri),
    /// Invalid gRPC metadata headers. Configuration error.
    #[error("Invalid headers: {0}")]
    InvalidHeaders(#[from] InvalidHeaderError),
    /// Server connection error. Crashing and restarting the worker is likely best.
    #[error("Server connection error: {0:?}")]
    TonicTransportError(#[from] tonic::transport::Error),
    /// We couldn't successfully make the `get_system_info` call at connection time to establish
    /// server capabilities / verify server is responding.
    #[error("`get_system_info` call error after connection: {0:?}")]
    SystemInfoCallError(tonic::Status),
}

/// Errors thrown when a gRPC metadata header is invalid.
#[derive(thiserror::Error, Debug)]
pub enum InvalidHeaderError {
    /// A binary header key was invalid
    #[error("Invalid binary header key '{key}': {source}")]
    InvalidBinaryHeaderKey {
        /// The invalid key
        key: String,
        /// The source error from tonic
        source: tonic::metadata::errors::InvalidMetadataKey,
    },
    /// An ASCII header key was invalid
    #[error("Invalid ASCII header key '{key}': {source}")]
    InvalidAsciiHeaderKey {
        /// The invalid key
        key: String,
        /// The source error from tonic
        source: tonic::metadata::errors::InvalidMetadataKey,
    },
    /// An ASCII header value was invalid
    #[error("Invalid ASCII header value for key '{key}': {source}")]
    InvalidAsciiHeaderValue {
        /// The key
        key: String,
        /// The invalid value
        value: String,
        /// The source error from tonic
        source: tonic::metadata::errors::InvalidMetadataValue,
    },
}

/// Errors that can occur when starting a workflow.
#[derive(thiserror::Error, Debug)]
pub enum StartWorkflowError {
    /// Error converting the input to a payload.
    #[error("Failed to serialize workflow input: {0}")]
    PayloadConversion(#[from] PayloadConversionError),
    /// Server returned an error.
    #[error("Server error: {0}")]
    TonicStatus(#[from] tonic::Status),
}
