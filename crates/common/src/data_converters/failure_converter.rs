use super::{PayloadConversionError, PayloadConverter, SerializationContextData};
use crate::{
    error::{
        ActivityExecutionError, ApplicationFailure, ChildWorkflowExecutionError,
        ChildWorkflowSignalError,
    },
    protos::temporal::api::failure::v1::{
        ApplicationFailureInfo, Failure, failure::FailureInfo,
    },
};

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

/// Default failure converter.
pub struct DefaultFailureConverter;

enum ClassifiedFailure<'a> {
    Application(&'a ApplicationFailure),
    ActivityExecution(&'a ActivityExecutionError),
    ChildWorkflowExecution(&'a ChildWorkflowExecutionError),
    ChildWorkflowSignal(&'a ChildWorkflowSignalError),
    Generic(&'a (dyn std::error::Error + 'static)),
}

impl FailureConverter for DefaultFailureConverter {
    fn to_failure(
        &self,
        error: Box<dyn std::error::Error>,
        _: &PayloadConverter,
        _: &SerializationContextData,
    ) -> Result<Failure, PayloadConversionError> {
        Ok(encode_classified_failure(classify_error(error.as_ref())))
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

fn classify_error<'a>(err: &'a (dyn std::error::Error + 'static)) -> ClassifiedFailure<'a> {
    if let Some(app) = err.downcast_ref::<ApplicationFailure>() {
        ClassifiedFailure::Application(app)
    } else if let Some(activity) = err.downcast_ref::<ActivityExecutionError>() {
        ClassifiedFailure::ActivityExecution(activity)
    } else if let Some(child) = err.downcast_ref::<ChildWorkflowExecutionError>() {
        ClassifiedFailure::ChildWorkflowExecution(child)
    } else if let Some(signal) = err.downcast_ref::<ChildWorkflowSignalError>() {
        ClassifiedFailure::ChildWorkflowSignal(signal)
    } else if let Some(classified) = classify_source_chain(err) {
        classified
    } else {
        ClassifiedFailure::Generic(err)
    }
}

fn classify_source_chain<'a>(
    err: &'a (dyn std::error::Error + 'static),
) -> Option<ClassifiedFailure<'a>> {
    let mut current = err.source();
    while let Some(cause) = current {
        if let Some(app) = cause.downcast_ref::<ApplicationFailure>() {
            return Some(ClassifiedFailure::Application(app));
        } else if let Some(activity) = cause.downcast_ref::<ActivityExecutionError>() {
            return Some(ClassifiedFailure::ActivityExecution(activity));
        } else if let Some(child) = cause.downcast_ref::<ChildWorkflowExecutionError>() {
            return Some(ClassifiedFailure::ChildWorkflowExecution(child));
        } else {
            if let Some(signal) = cause
                .downcast_ref::<ChildWorkflowSignalError>()
                .map(ClassifiedFailure::ChildWorkflowSignal)
            {
                return Some(signal);
            }
        }
        current = cause.source();
    }
    None
}

fn encode_classified_failure(classified: ClassifiedFailure<'_>) -> Failure {
    match classified {
        ClassifiedFailure::Application(app) => encode_application_failure(app),
        ClassifiedFailure::ActivityExecution(activity) => encode_activity_execution_failure(activity),
        ClassifiedFailure::ChildWorkflowExecution(child) => {
            encode_child_workflow_execution_failure(child)
        }
        ClassifiedFailure::ChildWorkflowSignal(signal) => encode_child_workflow_signal_failure(signal),
        ClassifiedFailure::Generic(err) => encode_generic_application_failure(err),
    }
}

fn encode_application_failure(app: &ApplicationFailure) -> Failure {
    let mut failure = Failure {
        message: app.to_string(),
        cause: app.source_error().chain().next().map(encode_nested_failure).map(Box::new),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(ApplicationFailureInfo {
            r#type: app.type_name().unwrap_or_default().to_owned(),
            non_retryable: app.is_non_retryable(),
            details: app.details().cloned(),
            next_retry_delay: app.next_retry_delay().and_then(|d| d.try_into().ok()),
            category: app.category() as i32,
        })),
        ..Default::default()
    };
    if failure.message.is_empty() {
        failure.message = "Application failure".to_owned();
    }
    failure
}

fn encode_activity_execution_failure(err: &ActivityExecutionError) -> Failure {
    match err {
        ActivityExecutionError::Failed(failure) | ActivityExecutionError::Cancelled(failure) => {
            failure.as_ref().clone()
        }
        ActivityExecutionError::Serialization(err) => encode_generic_application_failure(err),
    }
}

fn encode_child_workflow_execution_failure(err: &ChildWorkflowExecutionError) -> Failure {
    match err {
        ChildWorkflowExecutionError::Failed(failure)
        | ChildWorkflowExecutionError::Cancelled(failure) => failure.as_ref().clone(),
        ChildWorkflowExecutionError::StartFailed { .. }
        | ChildWorkflowExecutionError::Serialization(_) => encode_generic_application_failure(err),
    }
}

fn encode_child_workflow_signal_failure(err: &ChildWorkflowSignalError) -> Failure {
    match err {
        ChildWorkflowSignalError::Failed(failure) => failure.as_ref().clone(),
        ChildWorkflowSignalError::Serialization(err) => encode_generic_application_failure(err),
    }
}

fn encode_nested_failure(err: &(dyn std::error::Error + 'static)) -> Failure {
    match classify_error(err) {
        ClassifiedFailure::Application(app) => encode_application_failure(app),
        ClassifiedFailure::ActivityExecution(activity) => encode_activity_execution_failure(activity),
        ClassifiedFailure::ChildWorkflowExecution(child) => {
            encode_child_workflow_execution_failure(child)
        }
        ClassifiedFailure::ChildWorkflowSignal(signal) => encode_child_workflow_signal_failure(signal),
        ClassifiedFailure::Generic(err) => encode_generic_application_failure(err),
    }
}

fn encode_generic_application_failure(err: &(dyn std::error::Error + 'static)) -> Failure {
    Failure {
        message: err.to_string(),
        cause: err.source().map(encode_nested_failure).map(Box::new),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo::default(),
        )),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protos::temporal::api::{
        common::v1::Payload,
        enums::v1::ApplicationErrorCategory,
        failure::v1::{ActivityFailureInfo, failure::FailureInfo},
    };
    #[derive(Debug, thiserror::Error)]
    #[error("plain boom")]
    struct PlainError;

    #[derive(Debug, thiserror::Error)]
    #[error("outer")]
    struct WrapperError(#[source] ActivityExecutionError);

    fn convert(err: impl Into<Box<dyn std::error::Error>>) -> Failure {
        DefaultFailureConverter
            .to_failure(
                err.into(),
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap()
    }

    #[test]
    fn plain_errors_become_application_failures() {
        let failure = convert(Box::new(PlainError) as Box<dyn std::error::Error>);
        assert_eq!(failure.message, "plain boom");
        assert!(matches!(
            failure.failure_info,
            Some(FailureInfo::ApplicationFailureInfo(_))
        ));
    }

    #[test]
    fn application_failures_preserve_metadata() {
        let failure = convert(Box::new(
            ApplicationFailure::builder(anyhow::anyhow!("app boom"))
                .type_name("MyType".to_owned())
                .non_retryable(true)
                .category(ApplicationErrorCategory::Benign)
                .details(crate::protos::temporal::api::common::v1::Payloads {
                    payloads: vec![Payload {
                        data: b"details".to_vec(),
                        ..Default::default()
                    }],
                })
                .build(),
        ) as Box<dyn std::error::Error>);
        let Some(FailureInfo::ApplicationFailureInfo(info)) = failure.failure_info else {
            panic!("expected application failure info");
        };
        assert_eq!(failure.message, "app boom");
        assert_eq!(info.r#type, "MyType");
        assert!(info.non_retryable);
        assert_eq!(info.category(), ApplicationErrorCategory::Benign);
        assert_eq!(info.details.unwrap().payloads[0].data, b"details".to_vec());
    }

    #[test]
    fn wrapped_activity_execution_errors_stay_activity_failures() {
        let activity_failure = Failure {
            message: "activity failed".to_owned(),
            failure_info: Some(FailureInfo::ActivityFailureInfo(
                ActivityFailureInfo::default(),
            )),
            ..Default::default()
        };
        let wrapped = WrapperError(ActivityExecutionError::Failed(Box::new(activity_failure.clone())));
        let converted = convert(Box::new(wrapped) as Box<dyn std::error::Error>);
        assert_eq!(converted, activity_failure);
    }

    #[test]
    fn application_failures_keep_special_causes_nested() {
        let activity_failure = Failure {
            message: "activity failed".to_owned(),
            failure_info: Some(FailureInfo::ActivityFailureInfo(
                ActivityFailureInfo::default(),
            )),
            ..Default::default()
        };
        let app = ApplicationFailure::new(anyhow::Error::new(ActivityExecutionError::Failed(
            Box::new(activity_failure.clone()),
        )));
        let converted = convert(Box::new(app) as Box<dyn std::error::Error>);
        assert!(matches!(
            converted.failure_info,
            Some(FailureInfo::ApplicationFailureInfo(_))
        ));
        assert_eq!(converted.cause.unwrap().as_ref(), &activity_failure);
    }

    #[test]
    fn start_failed_child_workflow_errors_fall_back_to_application_failures() {
        let failure = convert(Box::new(ChildWorkflowExecutionError::StartFailed {
            workflow_id: "wf-id".to_owned(),
            workflow_type: "wf-type".to_owned(),
            cause: crate::protos::coresdk::child_workflow::StartChildWorkflowExecutionFailedCause::WorkflowAlreadyExists,
        }) as Box<dyn std::error::Error>);
        assert!(matches!(
            failure.failure_info,
            Some(FailureInfo::ApplicationFailureInfo(_))
        ));
        assert!(failure.message.contains("Child workflow start failed"));
    }
}
