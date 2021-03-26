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
#[derive(thiserror::Error, Debug, displaydoc::Display)]
pub enum CoreInitError {
    /// Invalid URI: {0:?}
    InvalidUri(#[from] InvalidUri),
    /// Server connection error: {0:?}
    TonicTransportError(#[from] tonic::transport::Error),
}

/// Errors thrown by [crate::Core::poll_workflow_task]
#[derive(thiserror::Error, Debug, displaydoc::Display)]
pub enum PollWfError {
    /// There was an error specific to a workflow instance with id ({run_id}): {source:?}
    WorkflowUpdateError {
        /// Underlying workflow error
        source: WorkflowError,
        /// The run id of the erring workflow
        run_id: String,
    },
    /// Poll workflow response from server was malformed: {0:?}
    BadPollResponseFromServer(PollWorkflowTaskQueueResponse),
    /** [crate::Core::shutdown] was called, and there are no more replay tasks to be handled. You
     * must call [crate::Core::complete_workflow_task] for any remaining tasks, and then may
     * exit.*/
    ShuttingDown,
    /// Unhandled error when calling the temporal server: {0:?}
    TonicError(#[from] tonic::Status),
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
        Self::ShuttingDown
    }
}

/// Errors thrown by [crate::Core::poll_activity_task]
#[derive(thiserror::Error, Debug, displaydoc::Display)]
pub enum PollActivityError {
    /// [crate::Core::shutdown] was called, we will no longer fetch new activity tasks
    ShuttingDown,
    /// Unhandled error when calling the temporal server: {0:?}
    TonicError(#[from] tonic::Status),
}

impl From<ShutdownErr> for PollActivityError {
    fn from(_: ShutdownErr) -> Self {
        Self::ShuttingDown
    }
}

/// Errors thrown by [crate::Core::complete_workflow_task]
#[derive(thiserror::Error, Debug, displaydoc::Display)]
#[allow(clippy::large_enum_variant)]
pub enum CompleteWfError {
    /// Lang SDK sent us a malformed workflow completion ({reason}): {completion:?}
    MalformedWorkflowCompletion {
        /// Reason the completion was malformed
        reason: String,
        /// The completion, which may not be included to avoid unnecessary copies.
        completion: Option<WfActivationCompletion>,
    },
    /// There was an error specific to a workflow instance with id ({run_id}): {source:?}
    WorkflowUpdateError {
        /// Underlying workflow error
        source: WorkflowError,
        /// The run id of the erring workflow
        run_id: String,
    },
    /** There exists a pending command in this workflow's history which has not yet been handled.
     * When thrown from [crate::Core::complete_workflow_task], it means you should poll for a new
     * task, receive a new task token, and complete that new task. */
    UnhandledCommandWhenCompleting,
    /// Unhandled error when calling the temporal server: {0:?}
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
#[derive(thiserror::Error, Debug, displaydoc::Display)]
pub enum CompleteActivityError {
    /// Lang SDK sent us a malformed activity completion ({reason}): {completion:?}
    MalformedActivityCompletion {
        /// Reason the completion was malformed
        reason: String,
        /// The completion, which may not be included to avoid unnecessary copies.
        completion: Option<ActivityResult>,
    },
    /// Unhandled error when calling the temporal server: {0:?}
    TonicError(#[from] tonic::Status),
}
