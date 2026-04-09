//! Contains errors that can be returned by clients.

use http::uri::InvalidUri;
use std::{error::Error, fmt};
use temporalio_common::{
    data_converters::{PayloadConversionError, TemporalError},
    protos::temporal::api::{common::v1::Payload, query::v1::QueryRejected},
};
use tonic::Code;

/// Errors thrown while attempting to establish a connection to the server
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ClientConnectError {
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
#[non_exhaustive]
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
#[non_exhaustive]
pub enum WorkflowStartError {
    /// The workflow already exists.
    #[error("Workflow already started with run ID: {run_id:?}")]
    AlreadyStarted {
        /// Run ID of the already-started workflow if this was raised by the client.
        run_id: Option<String>,
        /// The original gRPC status from the server.
        #[source]
        source: tonic::Status,
    },
    /// Error converting the input to a payload.
    #[error("Failed to serialize workflow input: {0}")]
    PayloadConversion(#[from] PayloadConversionError),
    /// An uncategorized rpc error from the server.
    #[error("Server error: {0}")]
    Rpc(#[from] tonic::Status),
}

/// Errors returned by query operations on [crate::WorkflowHandle].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowQueryError {
    /// The workflow was not found.
    #[error("Workflow not found")]
    NotFound(#[source] tonic::Status),

    /// The query was rejected based on the rejection condition.
    #[error("Query rejected: workflow status {:?}", .0.status)]
    Rejected(QueryRejected),

    /// Error serializing input or deserializing output.
    #[error("Payload conversion error: {0}")]
    PayloadConversion(#[from] PayloadConversionError),

    /// An uncategorized RPC error from the server.
    #[error("Server error: {0}")]
    Rpc(tonic::Status),

    /// Other errors.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl WorkflowQueryError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        if status.code() == Code::NotFound {
            Self::NotFound(status)
        } else {
            Self::Rpc(status)
        }
    }
}

/// Errors returned by update operations on [crate::WorkflowHandle].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowUpdateError {
    /// The workflow was not found.
    #[error("Workflow not found")]
    NotFound(#[source] tonic::Status),

    /// The update failed with an application-level failure.
    #[error("Update failed: {}", TemporalErrorChain(.0))]
    Failed(#[source] TemporalError),

    /// Error serializing input or deserializing output.
    #[error("Payload conversion error: {0}")]
    PayloadConversion(#[from] PayloadConversionError),

    /// An uncategorized RPC error from the server.
    #[error("Server error: {0}")]
    Rpc(tonic::Status),

    /// Other errors.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl WorkflowUpdateError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        if status.code() == Code::NotFound {
            Self::NotFound(status)
        } else {
            Self::Rpc(status)
        }
    }
}

/// Errors returned by workflow get_result operations.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowGetResultError {
    /// The workflow finished in failure.
    #[error("Workflow failed: {}", TemporalErrorChain(.0))]
    Failed(#[source] TemporalError),

    /// The workflow was cancelled.
    #[error("Workflow cancelled")]
    Cancelled {
        /// Details provided at cancellation time.
        details: Vec<Payload>,
    },

    /// The workflow was terminated.
    #[error("Workflow terminated")]
    Terminated {
        /// Details provided at termination time.
        details: Vec<Payload>,
    },

    /// The workflow timed out.
    #[error("Workflow timed out")]
    Timeout,

    /// The workflow continued as new.
    #[error("Workflow continued as new")]
    ContinuedAsNew,

    /// The workflow was not found.
    #[error("Workflow not found")]
    NotFound(#[source] tonic::Status),

    /// Error serializing input or deserializing output.
    #[error("Payload conversion error: {0}")]
    PayloadConversion(#[from] PayloadConversionError),

    /// An uncategorized RPC error from the server.
    #[error("Server error: {0}")]
    Rpc(tonic::Status),

    /// Other errors.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl From<WorkflowInteractionError> for WorkflowGetResultError {
    fn from(err: WorkflowInteractionError) -> Self {
        match err {
            WorkflowInteractionError::NotFound(s) => Self::NotFound(s),
            WorkflowInteractionError::PayloadConversion(e) => Self::PayloadConversion(e),
            WorkflowInteractionError::Rpc(s) => Self::Rpc(s),
            WorkflowInteractionError::Other(e) => Self::Other(e),
        }
    }
}

impl WorkflowGetResultError {
    /// Returns `true` if this error represents a workflow-level non-success outcome
    /// (Failed, Cancelled, Terminated, TimedOut, or ContinuedAsNew) rather than an
    /// infrastructure/RPC error.
    pub fn is_workflow_outcome(&self) -> bool {
        matches!(
            self,
            Self::Failed(_)
                | Self::Cancelled { .. }
                | Self::Terminated { .. }
                | Self::Timeout
                | Self::ContinuedAsNew
        )
    }
}

struct TemporalErrorChain<'a>(&'a TemporalError);

impl fmt::Display for TemporalErrorChain<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;
        let mut source = self.0.source();
        while let Some(err) = source {
            write!(f, ": {err}")?;
            source = err.source();
        }
        Ok(())
    }
}

/// Errors returned by client methods that don't need more specific error types.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ClientError {
    /// An uncategorized rpc error from the server.
    #[error("Server error: {0}")]
    Rpc(#[from] tonic::Status),
}

/// Errors returned by methods on [crate::WorkflowHandle] for general operations
/// like signal, cancel, terminate, describe, fetch_history, and get_result.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WorkflowInteractionError {
    /// The workflow was not found.
    #[error("Workflow not found")]
    NotFound(#[source] tonic::Status),

    /// Error serializing input or deserializing output.
    #[error("Payload conversion error: {0}")]
    PayloadConversion(#[from] PayloadConversionError),

    /// An uncategorized RPC error from the server.
    #[error("Server error: {0}")]
    Rpc(tonic::Status),

    /// Other errors.
    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

impl WorkflowInteractionError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        if status.code() == Code::NotFound {
            Self::NotFound(status)
        } else {
            Self::Rpc(status)
        }
    }
}

/// Errors that can occur when completing an activity asynchronously.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum AsyncActivityError {
    /// The activity was not found (e.g., already completed, cancelled, or never existed).
    #[error("Activity not found")]
    NotFound(#[source] tonic::Status),
    /// An uncategorized rpc error from the server.
    #[error("Server error: {0}")]
    Rpc(#[from] tonic::Status),
}

impl AsyncActivityError {
    pub(crate) fn from_status(status: tonic::Status) -> Self {
        if status.code() == Code::NotFound {
            Self::NotFound(status)
        } else {
            Self::Rpc(status)
        }
    }
}

/// Errors that can occur when constructing a [`crate::Client`].
///
/// Currently has no variants, but may be extended in the future.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ClientNewError {}

#[cfg(test)]
mod tests {
    use super::{WorkflowGetResultError, WorkflowUpdateError};
    use temporalio_common::{
        data_converters::TemporalError, protos::temporal::api::enums::v1::RetryState,
    };

    #[test]
    fn workflow_get_result_error_includes_nested_activity_cause_message() {
        let error = WorkflowGetResultError::Failed(TemporalError::Activity {
            message: "Activity task failed".into(),
            stack_trace: String::new(),
            scheduled_event_id: 1,
            started_event_id: 2,
            identity: "worker".into(),
            activity_type: "test-activity".into(),
            activity_id: "activity-id".into(),
            retry_state: RetryState::NonRetryableFailure,
            cause: Some(Box::new(TemporalError::Application {
                message: "boom".into(),
                stack_trace: String::new(),
                r#type: "TestError".into(),
                non_retryable: false,
                details: None,
                next_retry_delay: None,
                cause: None,
            })),
        });

        let rendered = error.to_string();
        assert!(rendered.contains("Workflow failed: Activity task failed"));
        assert!(rendered.contains("boom"));
    }

    #[test]
    fn workflow_update_error_includes_nested_child_workflow_cause_message() {
        let error = WorkflowUpdateError::Failed(TemporalError::ChildWorkflow {
            message: "Child workflow task failed".into(),
            stack_trace: String::new(),
            namespace: "default".into(),
            workflow_id: "child-id".into(),
            run_id: "child-run".into(),
            workflow_type: "child-type".into(),
            initiated_event_id: 3,
            started_event_id: 4,
            retry_state: RetryState::InProgress,
            cause: Some(Box::new(TemporalError::Application {
                message: "child boom".into(),
                stack_trace: String::new(),
                r#type: "ChildError".into(),
                non_retryable: false,
                details: None,
                next_retry_delay: None,
                cause: None,
            })),
        });

        let rendered = error.to_string();
        assert!(rendered.contains("Update failed: Child workflow task failed"));
        assert!(rendered.contains("child boom"));
    }
}
