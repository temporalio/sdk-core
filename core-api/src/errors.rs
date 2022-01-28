//! Error types exposed by public APIs

use prost_types::TimestampOutOfSystemRangeError;
use temporal_client::GatewayInitError;
use temporal_sdk_core_protos::coresdk::{
    activity_result::ActivityExecutionResult,
    workflow_activation::remove_from_cache::EvictionReason,
    workflow_completion::WorkflowActivationCompletion,
};

/// Errors thrown during initialization of [crate::Core]
#[derive(thiserror::Error, Debug)]
pub enum CoreInitError {
    /// Server connection error. Crashing and restarting the worker is likely best.
    #[error("Server connection error: {0:?}")]
    GatewayInitError(#[from] GatewayInitError),
    /// There was a problem initializing telemetry
    #[error("Telemetry initialization error: {0:?}")]
    TelemetryInitError(anyhow::Error),
}

/// Errors thrown by [crate::Core::poll_workflow_activation]
#[derive(thiserror::Error, Debug)]
pub enum PollWfError {
    /// [crate::Core::shutdown] was called, and there are no more replay tasks to be handled. Lang
    /// must call [crate::Core::complete_workflow_activation] for any remaining tasks, and then may
    /// exit.
    #[error("Core is shut down and there are no more workflow replay tasks")]
    ShutDown,
    /// Unhandled error when calling the temporal server. Core will attempt to retry any non-fatal
    /// errors, so lang should consider this fatal.
    #[error("Unhandled grpc error when workflow polling: {0:?}")]
    TonicError(#[from] tonic::Status),
    /// Unhandled error when completing a workflow during a poll -- this can happen when there is no
    /// work for lang to perform, but the server sent us a workflow task (EX: An activity completed
    /// even though we already cancelled it)
    #[error("Unhandled error when auto-completing workflow task: {0:?}")]
    AutocompleteError(#[from] CompleteWfError),
}

/// Errors thrown by [crate::Core::poll_activity_task]
#[derive(thiserror::Error, Debug)]
pub enum PollActivityError {
    /// [crate::Core::shutdown] was called, we will no longer fetch new activity tasks. Lang must
    /// ensure it is finished with any workflow replay, see [PollWfError::ShutDown]
    #[error("Core is shut down")]
    ShutDown,
    /// Unhandled error when calling the temporal server. Core will attempt to retry any non-fatal
    /// errors, so lang should consider this fatal.
    #[error("Unhandled grpc error when activity polling: {0:?}")]
    TonicError(#[from] tonic::Status),
}

/// Errors thrown by [crate::Core::complete_workflow_activation]
#[derive(thiserror::Error, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum CompleteWfError {
    /// Lang SDK sent us a malformed workflow completion. This likely means a bug in the lang sdk.
    #[error("Lang SDK sent us a malformed workflow completion ({reason}): {completion:?}")]
    MalformedWorkflowCompletion {
        /// Reason the completion was malformed
        reason: String,
        /// The completion, which may not be included to avoid unnecessary copies.
        completion: Option<WorkflowActivationCompletion>,
    },
    /// There is no worker registered for the queue being polled
    #[error("No worker registered for queue: {0}")]
    NoWorkerForQueue(String),
    /// Unhandled error when calling the temporal server. Core will attempt to retry any non-fatal
    /// errors, so lang should consider this fatal.
    #[error("Unhandled grpc error when completing workflow task: {0:?}")]
    TonicError(#[from] tonic::Status),
}

/// Errors thrown by [crate::Core::complete_activity_task]
#[derive(thiserror::Error, Debug)]
pub enum CompleteActivityError {
    /// Lang SDK sent us a malformed activity completion. This likely means a bug in the lang sdk.
    #[error("Lang SDK sent us a malformed activity completion ({reason}): {completion:?}")]
    MalformedActivityCompletion {
        /// Reason the completion was malformed
        reason: String,
        /// The completion, which may not be included to avoid unnecessary copies.
        completion: Option<ActivityExecutionResult>,
    },
    /// Unhandled error when calling the temporal server. Core will attempt to retry any non-fatal
    /// errors, so lang should consider this fatal.
    #[error("Unhandled grpc error when completing activity: {0:?}")]
    TonicError(#[from] tonic::Status),
    /// There is no worker registered or alive for the activity being completed
    #[error("No worker registered or alive for queue: {0}")]
    NoWorkerForQueue(String),
}

/// Errors thrown inside of workflow machines
#[derive(thiserror::Error, Debug)]
pub enum WFMachinesError {
    #[error("Nondeterminism error: {0}")]
    Nondeterminism(String),
    #[error("Fatal error in workflow machines: {0}")]
    Fatal(String),

    #[error("Unrecoverable network error while fetching history: {0}")]
    HistoryFetchingError(tonic::Status),

    /// Should always be caught internally and turned into a workflow task failure
    #[error("Unable to process partial event history because workflow is no longer cached.")]
    CacheMiss,
}

impl WFMachinesError {
    pub fn evict_reason(&self) -> EvictionReason {
        match self {
            WFMachinesError::Nondeterminism(_) => EvictionReason::Nondeterminism,
            WFMachinesError::Fatal(_) | WFMachinesError::HistoryFetchingError(_) => {
                EvictionReason::Fatal
            }
            WFMachinesError::CacheMiss => EvictionReason::CacheMiss,
        }
    }
}

impl From<TimestampOutOfSystemRangeError> for WFMachinesError {
    fn from(_: TimestampOutOfSystemRangeError) -> Self {
        Self::Fatal("Could not decode timestamp".to_string())
    }
}
