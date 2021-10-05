//! Error types exposed by public APIs

use crate::{machines::WFMachinesError, task_token::TaskToken, WorkerLookupErr};
use temporal_sdk_core_protos::{
    coresdk::{activity_result::ActivityResult, workflow_completion::WfActivationCompletion},
    temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse,
};
use tonic::codegen::http::uri::InvalidUri;

#[derive(Debug)]
pub(crate) struct WorkflowUpdateError {
    /// Underlying workflow error
    pub source: WFMachinesError,
    /// The run id of the erring workflow
    pub run_id: String,
    /// The task token associated with this update, if one existed yet.
    pub task_token: Option<TaskToken>,
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
    /// There was a problem initializing telemetry
    #[error("Telemetry initialization error: {0:?}")]
    TelemetryInitError(anyhow::Error),
}

/// Errors thrown by [crate::Core::poll_workflow_activation]
#[derive(thiserror::Error, Debug)]
pub enum PollWfError {
    /// There was an error specific to a workflow instance. The cached workflow should be deleted
    /// from lang side.
    #[error("There was an error with the workflow instance with run id ({run_id}): {source:?}")]
    WorkflowUpdateError {
        /// Underlying workflow error
        source: anyhow::Error,
        /// The run id of the erring workflow
        run_id: String,
    },
    /// The server returned a malformed polling response. Either we aren't handling a valid form,
    /// or the server is bugging out. Likely fatal.
    #[error("Poll workflow response from server was malformed: {0:?}")]
    BadPollResponseFromServer(Box<PollWorkflowTaskQueueResponse>),
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
    /// There is no worker registered for the queue being polled
    #[error("No worker registered for queue: {0}")]
    NoWorkerForQueue(String),
}

impl From<WorkflowUpdateError> for PollWfError {
    fn from(e: WorkflowUpdateError) -> Self {
        Self::WorkflowUpdateError {
            source: e.source.into(),
            run_id: e.run_id,
        }
    }
}

impl From<WorkerLookupErr> for PollWfError {
    fn from(e: WorkerLookupErr) -> Self {
        match e {
            WorkerLookupErr::Shutdown(_) => PollWfError::ShutDown,
            WorkerLookupErr::NoWorker(s) => PollWfError::NoWorkerForQueue(s),
        }
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
    #[error("Unhandled grpc error when activity polling: {0:?}")]
    TonicError(#[from] tonic::Status),
    /// There is no worker registered for the queue being polled
    #[error("No worker registered for queue: {0}")]
    NoWorkerForQueue(String),
}

impl From<WorkerLookupErr> for PollActivityError {
    fn from(e: WorkerLookupErr) -> Self {
        match e {
            WorkerLookupErr::Shutdown(_) => PollActivityError::ShutDown,
            WorkerLookupErr::NoWorker(s) => PollActivityError::NoWorkerForQueue(s),
        }
    }
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
        completion: Option<WfActivationCompletion>,
    },
    /// There was an error specific to a workflow instance. The cached workflow should be deleted
    /// from lang side.
    #[error("There was an error with the workflow instance with run id ({run_id}): {source:?}")]
    WorkflowUpdateError {
        /// Underlying workflow error
        source: anyhow::Error,
        /// The run id of the erring workflow
        run_id: String,
    },
    /// There is no worker registered for the queue being polled
    #[error("No worker registered for queue: {0}")]
    NoWorkerForQueue(String),
    /// Unhandled error when calling the temporal server. Core will attempt to retry any non-fatal
    /// errors, so lang should consider this fatal.
    #[error("Unhandled grpc error when completing workflow task: {0:?}")]
    TonicError(#[from] tonic::Status),
}

impl From<WorkflowUpdateError> for CompleteWfError {
    fn from(e: WorkflowUpdateError) -> Self {
        Self::WorkflowUpdateError {
            source: e.source.into(),
            run_id: e.run_id,
        }
    }
}

impl From<WorkerLookupErr> for CompleteWfError {
    fn from(e: WorkerLookupErr) -> Self {
        match e {
            WorkerLookupErr::Shutdown(s) => CompleteWfError::NoWorkerForQueue(s),
            WorkerLookupErr::NoWorker(s) => CompleteWfError::NoWorkerForQueue(s),
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
    #[error("Unhandled grpc error when completing activity: {0:?}")]
    TonicError(#[from] tonic::Status),
    /// There is no worker registered or alive for the activity being completed
    #[error("No worker registered or alive for queue: {0}")]
    NoWorkerForQueue(String),
}

impl From<WorkerLookupErr> for CompleteActivityError {
    fn from(e: WorkerLookupErr) -> Self {
        match e {
            WorkerLookupErr::Shutdown(s) => CompleteActivityError::NoWorkerForQueue(s),
            WorkerLookupErr::NoWorker(s) => CompleteActivityError::NoWorkerForQueue(s),
        }
    }
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

/// Errors thrown by [crate::Core::register_worker]
#[derive(thiserror::Error, Debug)]
pub enum WorkerRegistrationError {
    /// A worker has already been registered on this queue
    #[error("Worker already registered for queue: {0}")]
    WorkerAlreadyRegisteredForQueue(String),
}
