use crate::{
    protos::coresdk::activity_result::ActivityResult,
    protos::coresdk::workflow_completion::WfActivationCompletion,
    protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse,
    workflow::WorkflowError,
};
use tonic::codegen::http::uri::InvalidUri;

pub(crate) struct ShutdownErr;
pub(crate) struct WorkflowUpdateError {
    /// Underlying workflow error
    pub source: WorkflowError,
    /// The run id of the erring workflow
    pub run_id: String,
}

/// Errors thrown during initialization of [crate::Core]
#[derive(thiserror::Error, Debug)]
pub enum CoreInitError {
    /// Invalid URI. Configuration error, fatal.
    #[error("Invalid URI: {0:?}")]
    InvalidUri(#[from] InvalidUri),
    /// Server connection error. Crashing and restarting the worker is likely best.
    #[error("Server connection error: {0:?}")]
    TonicTransportError(#[from] tonic::transport::Error),
}

/// Errors thrown by [crate::Core::poll_workflow_task]
#[derive(thiserror::Error, Debug)]
pub enum PollWfError {
    /// There was an error specific to a workflow instance. The cached workflow should be deleted
    /// from lang side.
    #[error("There was an error with the workflow instance with run id ({run_id}): {source:?}")]
    WorkflowUpdateError {
        /// Underlying workflow error
        source: WorkflowError,
        /// The run id of the erring workflow
        run_id: String,
    },
    /// The server returned a malformed polling response. Either we aren't handling a valid form,
    /// or the server is bugging out. Likely fatal.
    #[error("Poll workflow response from server was malformed: {0:?}")]
    BadPollResponseFromServer(PollWorkflowTaskQueueResponse),
    /// [crate::Core::shutdown] was called, and there are no more replay tasks to be handled. Lang
    /// must call [crate::Core::complete_workflow_task] for any remaining tasks, and then may
    /// exit.
    #[error("Core is shut down and there are no more workflow replay tasks")]
    ShutDown,
    /// Unhandled error when calling the temporal server. Core will attempt to retry any non-fatal
    /// errors, so lang should consider this fatal.
    #[error("Unhandled error when calling the temporal server: {0:?}")]
    TonicError(#[from] tonic::Status),
    /// Unhandled error when completing a workflow during a poll -- this can happen when there is no
    /// work for lang to perform, but the server sent us a workflow task (EX: An activity completed
    /// even though we already cancelled it)
    #[error("Unhandled error when auto-completing workflow task: {0:?}")]
    AutocompleteError(#[from] CompleteWfError),
}

impl From<WorkflowUpdateError> for PollWfError {
    fn from(e: WorkflowUpdateError) -> Self {
        Self::WorkflowUpdateError {
            source: e.source,
            run_id: e.run_id,
        }
    }
}

impl From<ShutdownErr> for PollWfError {
    fn from(_: ShutdownErr) -> Self {
        Self::ShutDown
    }
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
    #[error("Unhandled error when calling the temporal server: {0:?}")]
    TonicError(#[from] tonic::Status),
}

impl From<ShutdownErr> for PollActivityError {
    fn from(_: ShutdownErr) -> Self {
        Self::ShutDown
    }
}

/// Errors thrown by [crate::Core::complete_workflow_task]
#[derive(thiserror::Error, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum CompleteWfError {
    /// Lang SDK sent us a malformed workflow completion. This likely means a bug in the lang sdk.
    #[error("Lang SDK sent us a malformed workflow completion ({reason}): {completion:?}")]
    MalformedWorkflowCompletion {
        /// Reason the completion was malformed
        reason: String,
        /// The completion, which may not be included to avoid unnecessary copies.
        completion: Option<WfActivationCompletion>,
    },
    /// There was an error specific to a workflow instance. The cached workflow should be deleted
    /// from lang side.
    #[error("There was an error with the workflow instance with run id ({run_id}): {source:?}")]
    WorkflowUpdateError {
        /// Underlying workflow error
        source: WorkflowError,
        /// The run id of the erring workflow
        run_id: String,
    },
    /// Unhandled error when calling the temporal server. Core will attempt to retry any non-fatal
    /// errors, so lang should consider this fatal.
    #[error("Unhandled error when calling the temporal server: {0:?}")]
    TonicError(#[from] tonic::Status),
}

impl From<WorkflowUpdateError> for CompleteWfError {
    fn from(e: WorkflowUpdateError) -> Self {
        Self::WorkflowUpdateError {
            source: e.source,
            run_id: e.run_id,
        }
    }
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
        completion: Option<ActivityResult>,
    },
    /// Unhandled error when calling the temporal server. Core will attempt to retry any non-fatal
    /// errors, so lang should consider this fatal.
    #[error("Unhandled error when calling the temporal server: {0:?}")]
    TonicError(#[from] tonic::Status),
}

/// Errors thrown by [crate::Core::record_activity_heartbeat]
#[derive(thiserror::Error, Debug)]
pub enum ActivityHeartbeatError {
    /// Heartbeat referenced an activity that we don't think exists. It may have completed already.
    #[error("Heartbeat has been sent for activity that either completed or never started on this worker.")]
    UnknownActivity,
    /// There was no heartbeat timeout set for the activity, but one is required to heartbeat.
    #[error("Heartbeat is only allowed on activities with heartbeat timeout.")]
    HeartbeatTimeoutNotSet,
    /// There was a set heartbeat timeout, but it was not parseable. A valid timeout is requried
    /// to heartbeat.
    #[error("Unable to parse activity heartbeat timeout.")]
    InvalidHeartbeatTimeout,
    /// Core is shutting down and thus new heartbeats are not accepted
    #[error("New heartbeat requests are not accepted while shutting down")]
    ShuttingDown,
}
