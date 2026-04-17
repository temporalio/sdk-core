//! Shared error types used across Temporal SDK crates.

use crate::{
    data_converters::PayloadConversionError,
    protos::{
        coresdk::child_workflow::StartChildWorkflowExecutionFailedCause,
        temporal::api::{
            common::v1::{Payload, Payloads},
            enums::v1::ApplicationErrorCategory,
            failure::v1::{ApplicationFailureInfo, Failure, failure::FailureInfo},
        },
    },
};
use std::time::Duration;

/// User-authored application failure metadata that can be converted into a Temporal failure.
#[derive(Debug, bon::Builder)]
#[builder(start_fn = builder, state_mod(vis = "pub"))]
pub struct ApplicationFailure {
    #[builder(start_fn, into)]
    source: anyhow::Error,
    type_name: Option<String>,
    #[builder(default)]
    non_retryable: bool,
    next_retry_delay: Option<Duration>,
    #[builder(default = ApplicationErrorCategory::Unspecified)]
    category: ApplicationErrorCategory,
    details: Option<Payloads>,
    failure: Option<Failure>,
    cause: Option<Box<IncomingError>>,
}

impl ApplicationFailure {
    /// Construct a retryable application failure with no extra metadata.
    pub fn new(source: impl Into<anyhow::Error>) -> Self {
        Self {
            source: source.into(),
            type_name: None,
            non_retryable: false,
            next_retry_delay: None,
            category: ApplicationErrorCategory::Unspecified,
            details: None,
            failure: None,
            cause: None,
        }
    }

    /// Construct a non-retryable application failure with no extra metadata.
    pub fn non_retryable(source: impl Into<anyhow::Error>) -> Self {
        Self {
            non_retryable: true,
            ..Self::new(source)
        }
    }

    /// Returns the wrapped source error.
    pub fn source_error(&self) -> &anyhow::Error {
        &self.source
    }

    /// Returns the configured application failure type name, if any.
    pub fn type_name(&self) -> Option<&str> {
        self.type_name.as_deref()
    }

    /// Returns true if this failure should be treated as non-retryable.
    pub fn is_non_retryable(&self) -> bool {
        self.non_retryable
    }

    /// Returns the explicitly configured next retry delay, if any.
    pub fn next_retry_delay(&self) -> Option<Duration> {
        self.next_retry_delay
    }

    /// Returns the application error category.
    pub fn category(&self) -> ApplicationErrorCategory {
        self.category
    }

    /// Returns the raw encoded details payloads, if any.
    pub fn details(&self) -> Option<&Payloads> {
        self.details.as_ref()
    }

    /// Returns the original failure proto when this application failure was decoded from one.
    pub fn failure(&self) -> Option<&Failure> {
        self.failure.as_ref()
    }

    /// Returns the normalized cause, if any.
    pub fn cause(&self) -> Option<&IncomingError> {
        self.cause.as_deref()
    }

    pub(crate) fn from_failure(failure: Failure, cause: Option<IncomingError>) -> Self {
        let app_info = failure
            .maybe_application_failure()
            .cloned()
            .unwrap_or_default();
        let type_name = (!app_info.r#type.is_empty()).then_some(app_info.r#type.clone());
        Self {
            source: anyhow::anyhow!(failure.message.clone()),
            type_name,
            non_retryable: app_info.non_retryable,
            next_retry_delay: app_info.next_retry_delay.and_then(|d| d.try_into().ok()),
            category: app_info.category(),
            details: app_info.details,
            failure: Some(failure),
            cause: cause.map(Box::new),
        }
    }
}

impl std::fmt::Display for ApplicationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.source)
    }
}

impl std::error::Error for ApplicationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

impl From<anyhow::Error> for ApplicationFailure {
    fn from(value: anyhow::Error) -> Self {
        Self::new(value)
    }
}

impl From<ApplicationFailure> for Failure {
    fn from(value: ApplicationFailure) -> Self {
        if let Some(failure) = value.failure {
            return failure;
        }
        let mut failure = value
            .source
            .chain()
            .rfold(None, |cause, err| {
                Some(Failure {
                    message: err.to_string(),
                    cause: cause.map(Box::new),
                    ..Default::default()
                })
            })
            .unwrap_or_default();
        failure.failure_info = Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: value.type_name.unwrap_or_default(),
                non_retryable: value.non_retryable,
                details: value.details,
                next_retry_delay: value.next_retry_delay.and_then(|d| d.try_into().ok()),
                category: value.category as i32,
            },
        ));
        failure
    }
}

/// A typed outbound error surface used before encoding to a Temporal failure proto.
#[derive(Debug, thiserror::Error)]
pub enum OutgoingError {
    /// An error produced while completing an activity.
    #[error(transparent)]
    Activity(#[from] OutgoingActivityError),
    /// An error produced while failing a workflow execution.
    #[error(transparent)]
    Workflow(#[from] OutgoingWorkflowError),
}

/// A typed outbound activity error.
#[derive(Debug, thiserror::Error)]
pub enum OutgoingActivityError {
    /// An activity application failure.
    #[error(transparent)]
    Application(#[from] Box<ApplicationFailure>),
    /// An activity cancellation with optional details.
    #[error("Activity cancelled")]
    Cancelled {
        /// Optional cancellation details payload.
        details: Option<Payload>,
    },
}

/// A typed outbound workflow failure.
#[derive(Debug, thiserror::Error)]
pub enum OutgoingWorkflowError {
    /// A workflow application failure.
    #[error(transparent)]
    Application(#[from] Box<ApplicationFailure>),
    /// A workflow failure sourced from an activity execution.
    #[error(transparent)]
    ActivityExecution(#[from] Box<ActivityExecutionError>),
    /// A workflow failure sourced from a child-workflow execution.
    #[error(transparent)]
    ChildWorkflowExecution(#[from] Box<ChildWorkflowExecutionError>),
    /// A workflow failure sourced from child-workflow signaling.
    #[error(transparent)]
    ChildWorkflowSignal(#[from] Box<ChildWorkflowSignalError>),
}

impl From<anyhow::Error> for OutgoingWorkflowError {
    fn from(value: anyhow::Error) -> Self {
        Self::Application(Box::new(ApplicationFailure::new(value)))
    }
}

impl From<ApplicationFailure> for OutgoingWorkflowError {
    fn from(value: ApplicationFailure) -> Self {
        Self::Application(Box::new(value))
    }
}

impl From<ActivityExecutionError> for OutgoingWorkflowError {
    fn from(value: ActivityExecutionError) -> Self {
        Self::ActivityExecution(Box::new(value))
    }
}

impl From<ChildWorkflowExecutionError> for OutgoingWorkflowError {
    fn from(value: ChildWorkflowExecutionError) -> Self {
        Self::ChildWorkflowExecution(Box::new(value))
    }
}

impl From<ChildWorkflowSignalError> for OutgoingWorkflowError {
    fn from(value: ChildWorkflowSignalError) -> Self {
        Self::ChildWorkflowSignal(Box::new(value))
    }
}

/// A normalized incoming Temporal failure decoded from a protobuf [`Failure`].
#[derive(Debug)]
pub enum IncomingError {
    /// A decoded application failure.
    Application(ApplicationFailure),
    /// A decoded timeout failure.
    Timeout(TimeoutError),
    /// A decoded cancellation failure.
    Cancelled(CancelledError),
    /// A decoded terminated failure.
    Terminated(TerminatedError),
    /// A decoded server failure.
    Server(ServerError),
    /// A decoded reset-workflow failure.
    ResetWorkflow(ResetWorkflowError),
    /// A decoded activity failure wrapper.
    Activity(IncomingActivityError),
    /// A decoded child-workflow failure wrapper.
    ChildWorkflowExecution(IncomingChildWorkflowExecutionError),
    /// A decoded nexus operation failure wrapper.
    NexusOperationExecution(IncomingNexusOperationExecutionError),
    /// A decoded nexus handler failure wrapper.
    NexusHandler(IncomingNexusHandlerError),
}

impl IncomingError {
    /// Returns the original failure proto for this normalized error.
    pub fn failure(&self) -> &Failure {
        match self {
            IncomingError::Application(err) => err
                .failure()
                .expect("decoded application failures retain their original proto"),
            IncomingError::Timeout(err) => err.failure(),
            IncomingError::Cancelled(err) => err.failure(),
            IncomingError::Terminated(err) => err.failure(),
            IncomingError::Server(err) => err.failure(),
            IncomingError::ResetWorkflow(err) => err.failure(),
            IncomingError::Activity(err) => err.failure(),
            IncomingError::ChildWorkflowExecution(err) => err.failure(),
            IncomingError::NexusOperationExecution(err) => err.failure(),
            IncomingError::NexusHandler(err) => err.failure(),
        }
    }

    /// Returns the normalized cause, if any.
    pub fn cause(&self) -> Option<&IncomingError> {
        match self {
            IncomingError::Application(err) => err.cause(),
            IncomingError::Timeout(err) => err.cause(),
            IncomingError::Cancelled(err) => err.cause(),
            IncomingError::Terminated(err) => err.cause(),
            IncomingError::Server(err) => err.cause(),
            IncomingError::ResetWorkflow(err) => err.cause(),
            IncomingError::Activity(err) => err.cause(),
            IncomingError::ChildWorkflowExecution(err) => err.cause(),
            IncomingError::NexusOperationExecution(err) => err.cause(),
            IncomingError::NexusHandler(err) => err.cause(),
        }
    }

    /// Consumes this normalized error and returns the retained proto failure.
    pub fn into_failure(self) -> Failure {
        match self {
            IncomingError::Application(err) => err.into(),
            IncomingError::Timeout(err) => err.into_failure(),
            IncomingError::Cancelled(err) => err.into_failure(),
            IncomingError::Terminated(err) => err.into_failure(),
            IncomingError::Server(err) => err.into_failure(),
            IncomingError::ResetWorkflow(err) => err.into_failure(),
            IncomingError::Activity(err) => err.into_failure(),
            IncomingError::ChildWorkflowExecution(err) => err.into_failure(),
            IncomingError::NexusOperationExecution(err) => err.into_failure(),
            IncomingError::NexusHandler(err) => err.into_failure(),
        }
    }
}

macro_rules! incoming_failure_wrapper {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug)]
        pub struct $name {
            failure: Failure,
            cause: Option<Box<IncomingError>>,
        }

        impl $name {
            /// Creates a new normalized incoming error wrapper.
            pub fn new(failure: Failure, cause: Option<IncomingError>) -> Self {
                Self {
                    failure,
                    cause: cause.map(Box::new),
                }
            }

            /// Returns the original failure proto.
            pub fn failure(&self) -> &Failure {
                &self.failure
            }

            /// Returns the normalized cause, if any.
            pub fn cause(&self) -> Option<&IncomingError> {
                self.cause.as_deref()
            }

            /// Consumes this wrapper and returns the retained proto failure.
            pub fn into_failure(self) -> Failure {
                self.failure
            }
        }
    };
}

incoming_failure_wrapper!(TimeoutError, "A normalized timeout failure.");
incoming_failure_wrapper!(CancelledError, "A normalized cancellation failure.");
incoming_failure_wrapper!(TerminatedError, "A normalized terminated failure.");
incoming_failure_wrapper!(ServerError, "A normalized server failure.");
incoming_failure_wrapper!(ResetWorkflowError, "A normalized reset-workflow failure.");
incoming_failure_wrapper!(
    IncomingActivityError,
    "A normalized activity failure wrapper."
);
incoming_failure_wrapper!(
    IncomingChildWorkflowExecutionError,
    "A normalized child-workflow execution failure wrapper."
);
incoming_failure_wrapper!(
    IncomingNexusOperationExecutionError,
    "A normalized nexus operation failure wrapper."
);
incoming_failure_wrapper!(
    IncomingNexusHandlerError,
    "A normalized nexus handler failure wrapper."
);

/// Error type for activity execution outcomes.
#[derive(Debug, thiserror::Error)]
pub enum ActivityExecutionError {
    /// The activity failed with the given failure details.
    #[error("Activity failed: {}", .0.message)]
    Failed(Box<Failure>),
    /// The activity was cancelled.
    #[error("Activity cancelled: {}", .0.message)]
    Cancelled(Box<Failure>),
    /// Failed to serialize input or deserialize result payload.
    #[error("Payload conversion failed: {0}")]
    Serialization(#[from] PayloadConversionError),
}

impl ActivityExecutionError {
    /// Returns true if this error represents a timeout.
    pub fn is_timeout(&self) -> bool {
        match self {
            ActivityExecutionError::Failed(f) => f.is_timeout().is_some(),
            _ => false,
        }
    }
}

/// Error returned when a child workflow execution fails.
#[derive(Debug, thiserror::Error)]
pub enum ChildWorkflowExecutionError {
    /// The child workflow failed.
    #[error("Child workflow failed: {}", .0.message)]
    Failed(Box<Failure>),
    /// The child workflow was cancelled.
    #[error("Child workflow cancelled: {}", .0.message)]
    Cancelled(Box<Failure>),
    /// The child workflow failed to start (e.g., workflow ID already exists).
    #[error(
        "Child workflow start failed: workflow_id={workflow_id}, workflow_type={workflow_type}, cause={cause:?}"
    )]
    StartFailed {
        /// The workflow ID that was requested.
        workflow_id: String,
        /// The workflow type that was requested.
        workflow_type: String,
        /// The cause of the start failure.
        cause: StartChildWorkflowExecutionFailedCause,
    },
    /// Failed to serialize input or deserialize the child workflow result payload.
    #[error("Payload conversion failed: {0}")]
    Serialization(#[from] PayloadConversionError),
}

/// Error returned when signaling a child workflow fails.
#[derive(Debug, thiserror::Error)]
pub enum ChildWorkflowSignalError {
    /// The signal delivery failed.
    #[error("Child workflow signal failed: {}", .0.message)]
    Failed(Box<Failure>),
    /// Failed to serialize the signal input payload.
    #[error("Signal payload conversion failed: {0}")]
    Serialization(#[from] PayloadConversionError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protos::temporal::api::common::v1::Payload;

    #[test]
    fn constructors_set_retryability_defaults() {
        assert!(!ApplicationFailure::new(anyhow::anyhow!("retryable")).is_non_retryable());
        assert!(
            ApplicationFailure::non_retryable(anyhow::anyhow!("non-retryable")).is_non_retryable()
        );
    }

    #[test]
    fn conversion_preserves_application_metadata() {
        let payloads = Payloads {
            payloads: vec![Payload {
                data: b"details".to_vec(),
                ..Default::default()
            }],
        };
        let failure: Failure = ApplicationFailure::builder(anyhow::anyhow!("oops"))
            .type_name("MyType".to_owned())
            .non_retryable(true)
            .next_retry_delay(Duration::from_secs(3))
            .category(ApplicationErrorCategory::Benign)
            .details(payloads.clone())
            .build()
            .into();
        let Some(FailureInfo::ApplicationFailureInfo(info)) = failure.failure_info else {
            panic!("expected application failure info");
        };
        assert_eq!(failure.message, "oops");
        assert_eq!(info.r#type, "MyType");
        assert!(info.non_retryable);
        assert_eq!(info.details, Some(payloads));
        assert_eq!(info.category(), ApplicationErrorCategory::Benign);
        assert_eq!(info.next_retry_delay.unwrap().seconds, 3);
    }

    #[test]
    fn anyhow_workflow_errors_default_to_application_outgoing_errors() {
        let outgoing: OutgoingWorkflowError = anyhow::anyhow!("workflow boom").into();

        let OutgoingWorkflowError::Application(app) = outgoing else {
            panic!("plain workflow errors should default to application failures");
        };
        assert_eq!(app.to_string(), "workflow boom");
    }
}
