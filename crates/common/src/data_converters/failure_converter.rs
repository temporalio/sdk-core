use super::{PayloadConversionError, PayloadConverter, SerializationContextData};
use crate::{
    error::{
        ActivityExecutionError, ActivityFailureError, ApplicationFailure, CancelledError,
        ChildWorkflowExecutionError, ChildWorkflowFailureError, ChildWorkflowSignalError,
        ChildWorkflowSignalFailureError, IncomingError, IncomingNexusHandlerError,
        IncomingNexusOperationExecutionError, OutgoingActivityError, OutgoingError,
        OutgoingWorkflowError, ResetWorkflowError, ServerError, TerminatedError, TimeoutError,
    },
    protos::temporal::api::failure::v1::{
        ApplicationFailureInfo, CanceledFailureInfo, ChildWorkflowExecutionFailureInfo, Failure,
        failure::FailureInfo,
    },
};

/// Converts between Rust errors and Temporal [`Failure`] protobufs.
pub trait FailureConverter {
    /// Convert an error into a Temporal failure protobuf.
    fn to_failure(
        &self,
        error: OutgoingError,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Result<Failure, PayloadConversionError>;

    /// Convert a Temporal failure protobuf back into a Rust error.
    fn to_error(
        &self,
        failure: Failure,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Result<IncomingError, PayloadConversionError>;
}

/// Default failure converter.
pub struct DefaultFailureConverter;

/// Adapts a normalized incoming failure into a caller-facing error surface.
pub trait FailureDecodeHint {
    /// The caller-facing error type produced by this hint.
    type Output;

    /// Adapt a normalized incoming error to the caller-facing output.
    fn adapt(self, normalized: IncomingError) -> Self::Output;
}

/// Decode hint for activity execution results.
#[derive(Debug, Clone, Copy)]
pub struct ActivityExecutionDecodeHint {
    /// Whether the workflow-side resolution was cancelled rather than failed.
    pub cancelled: bool,
}

impl FailureDecodeHint for ActivityExecutionDecodeHint {
    type Output = ActivityExecutionError;

    fn adapt(self, normalized: IncomingError) -> Self::Output {
        match normalized {
            IncomingError::Activity(activity) => {
                if self.cancelled {
                    let (failure, cause) = activity.into_parts();
                    ActivityExecutionError::Cancelled(CancelledError::new(
                        failure,
                        CanceledFailureInfo::default(),
                        cause,
                    ))
                } else {
                    ActivityExecutionError::Failed(activity)
                }
            }
            other => match other {
                IncomingError::Cancelled(cancelled) if self.cancelled => {
                    ActivityExecutionError::Cancelled(cancelled)
                }
                other => {
                    let activity = ActivityFailureError::new(other.into_failure(), None);
                    ActivityExecutionError::Failed(activity)
                }
            },
        }
    }
}

/// Decode hint for child-workflow execution results.
#[derive(Debug, Clone, Copy)]
pub enum ChildWorkflowExecutionDecodeHint {
    /// The workflow-side resolution was failed.
    Failed,
    /// The workflow-side resolution was cancelled.
    Cancelled,
}

impl FailureDecodeHint for ChildWorkflowExecutionDecodeHint {
    type Output = ChildWorkflowExecutionError;

    fn adapt(self, normalized: IncomingError) -> Self::Output {
        match (self, normalized) {
            (
                ChildWorkflowExecutionDecodeHint::Failed,
                IncomingError::ChildWorkflowExecution(child),
            ) => ChildWorkflowExecutionError::Failed(child),
            (
                ChildWorkflowExecutionDecodeHint::Cancelled,
                IncomingError::ChildWorkflowExecution(child),
            ) => {
                let (failure, cause) = child.into_parts();
                ChildWorkflowExecutionError::Cancelled(CancelledError::new(
                    failure,
                    CanceledFailureInfo::default(),
                    cause,
                ))
            }
            (ChildWorkflowExecutionDecodeHint::Cancelled, IncomingError::Cancelled(cancelled)) => {
                ChildWorkflowExecutionError::Cancelled(cancelled)
            }
            (_, other) => ChildWorkflowExecutionError::Failed(ChildWorkflowFailureError::new(
                other.into_failure(),
                ChildWorkflowExecutionFailureInfo::default(),
                None,
            )),
        }
    }
}

/// Decode hint for child-workflow signal failures.
#[derive(Debug, Clone, Copy)]
pub struct ChildWorkflowSignalDecodeHint;

impl FailureDecodeHint for ChildWorkflowSignalDecodeHint {
    type Output = ChildWorkflowSignalError;

    fn adapt(self, normalized: IncomingError) -> Self::Output {
        let failure = normalized.failure().clone();
        let cause = failure.cause.clone().map(|cause| decode_failure(*cause));
        ChildWorkflowSignalError::Failed(ChildWorkflowSignalFailureError::new(
            failure, normalized, cause,
        ))
    }
}

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
        error: OutgoingError,
        _: &PayloadConverter,
        _: &SerializationContextData,
    ) -> Result<Failure, PayloadConversionError> {
        Ok(match error {
            OutgoingError::Activity(activity) => encode_outgoing_activity_error(activity),
            OutgoingError::Workflow(OutgoingWorkflowError::Application(app)) => {
                encode_application_failure(&app)
            }
            OutgoingError::Workflow(OutgoingWorkflowError::ActivityExecution(activity)) => {
                encode_activity_execution_failure(&activity)
            }
            OutgoingError::Workflow(OutgoingWorkflowError::ChildWorkflowExecution(child)) => {
                encode_child_workflow_execution_failure(&child)
            }
            OutgoingError::Workflow(OutgoingWorkflowError::ChildWorkflowSignal(signal)) => {
                encode_child_workflow_signal_failure(&signal)
            }
        })
    }

    fn to_error(
        &self,
        failure: Failure,
        _: &PayloadConverter,
        _: &SerializationContextData,
    ) -> Result<IncomingError, PayloadConversionError> {
        Ok(decode_failure(failure))
    }
}

fn classify_error<'a>(err: &'a (dyn std::error::Error + 'static)) -> ClassifiedFailure<'a> {
    if let Some(classified) = classify_known_error(err) {
        classified
    } else if let Some(classified) = classify_source_chain(err) {
        classified
    } else {
        ClassifiedFailure::Generic(err)
    }
}

fn classify_known_error<'a>(
    err: &'a (dyn std::error::Error + 'static),
) -> Option<ClassifiedFailure<'a>> {
    if let Some(app) = err.downcast_ref::<ApplicationFailure>() {
        Some(ClassifiedFailure::Application(app))
    } else if let Some(activity) = err.downcast_ref::<ActivityExecutionError>() {
        Some(ClassifiedFailure::ActivityExecution(activity))
    } else if let Some(child) = err.downcast_ref::<ChildWorkflowExecutionError>() {
        Some(ClassifiedFailure::ChildWorkflowExecution(child))
    } else {
        err.downcast_ref::<ChildWorkflowSignalError>()
            .map(ClassifiedFailure::ChildWorkflowSignal)
    }
}

fn classify_source_chain<'a>(
    err: &'a (dyn std::error::Error + 'static),
) -> Option<ClassifiedFailure<'a>> {
    let mut current = err.source();
    while let Some(cause) = current {
        if let Some(classified) = classify_known_error(cause) {
            return Some(classified);
        }
        current = cause.source();
    }
    None
}

fn encode_application_failure(app: &ApplicationFailure) -> Failure {
    let source = app.source_error().as_ref();
    let cause = match classify_error(source) {
        ClassifiedFailure::Application(_) | ClassifiedFailure::Generic(_) => {
            source.source().map(encode_nested_failure).map(Box::new)
        }
        _ => Some(Box::new(encode_nested_failure(source))),
    };
    let mut failure = Failure {
        message: app.to_string(),
        cause,
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo {
                r#type: app.type_name().unwrap_or_default().to_owned(),
                non_retryable: app.is_non_retryable(),
                details: app.details().cloned(),
                next_retry_delay: app.next_retry_delay().and_then(|d| d.try_into().ok()),
                category: app.category() as i32,
            },
        )),
        ..Default::default()
    };
    if failure.message.is_empty() {
        failure.message = "Application failure".to_owned();
    }
    failure
}

fn encode_activity_execution_failure(err: &ActivityExecutionError) -> Failure {
    match err {
        ActivityExecutionError::Failed(failure) => failure.failure().clone(),
        ActivityExecutionError::Cancelled(failure) => failure.failure().clone(),
        ActivityExecutionError::Serialization(err) => encode_generic_application_failure(err),
    }
}

fn encode_child_workflow_execution_failure(err: &ChildWorkflowExecutionError) -> Failure {
    match err {
        ChildWorkflowExecutionError::Failed(failure) => failure.failure().clone(),
        ChildWorkflowExecutionError::Cancelled(failure) => failure.failure().clone(),
        ChildWorkflowExecutionError::StartFailed { .. }
        | ChildWorkflowExecutionError::Serialization(_) => encode_generic_application_failure(err),
    }
}

fn encode_child_workflow_signal_failure(err: &ChildWorkflowSignalError) -> Failure {
    match err {
        ChildWorkflowSignalError::Failed(failure) => failure.failure().clone(),
        ChildWorkflowSignalError::Serialization(err) => encode_generic_application_failure(err),
    }
}

fn encode_outgoing_activity_error(err: OutgoingActivityError) -> Failure {
    match err {
        OutgoingActivityError::Application(app) => encode_application_failure(&app),
        OutgoingActivityError::Cancelled { details } => Failure {
            message: "Activity cancelled".to_string(),
            failure_info: Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
                details: details.map(Into::into),
            })),
            ..Default::default()
        },
    }
}

fn encode_nested_failure(err: &(dyn std::error::Error + 'static)) -> Failure {
    match classify_error(err) {
        ClassifiedFailure::Application(app) => encode_application_failure(app),
        ClassifiedFailure::ActivityExecution(activity) => {
            encode_activity_execution_failure(activity)
        }
        ClassifiedFailure::ChildWorkflowExecution(child) => {
            encode_child_workflow_execution_failure(child)
        }
        ClassifiedFailure::ChildWorkflowSignal(signal) => {
            encode_child_workflow_signal_failure(signal)
        }
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

fn decode_failure(failure: Failure) -> IncomingError {
    let cause = failure.cause.clone().map(|cause| decode_failure(*cause));
    match failure.failure_info.clone() {
        Some(FailureInfo::ApplicationFailureInfo(_)) | None => {
            IncomingError::Application(ApplicationFailure::from_failure(failure, cause))
        }
        Some(FailureInfo::TimeoutFailureInfo(failure_info)) => {
            IncomingError::Timeout(TimeoutError::new(failure, failure_info, cause))
        }
        Some(FailureInfo::CanceledFailureInfo(failure_info)) => {
            IncomingError::Cancelled(CancelledError::new(failure, failure_info, cause))
        }
        Some(FailureInfo::TerminatedFailureInfo(_)) => {
            IncomingError::Terminated(TerminatedError::new(failure, cause))
        }
        Some(FailureInfo::ServerFailureInfo(_)) => {
            IncomingError::Server(ServerError::new(failure, cause))
        }
        Some(FailureInfo::ResetWorkflowFailureInfo(_)) => {
            IncomingError::ResetWorkflow(ResetWorkflowError::new(failure, cause))
        }
        Some(FailureInfo::ActivityFailureInfo(_)) => {
            IncomingError::Activity(ActivityFailureError::new(failure, cause))
        }
        Some(FailureInfo::ChildWorkflowExecutionFailureInfo(failure_info)) => {
            IncomingError::ChildWorkflowExecution(ChildWorkflowFailureError::new(
                failure,
                failure_info,
                cause,
            ))
        }
        Some(FailureInfo::NexusOperationExecutionFailureInfo(_)) => {
            IncomingError::NexusOperationExecution(IncomingNexusOperationExecutionError::new(
                failure, cause,
            ))
        }
        Some(FailureInfo::NexusHandlerFailureInfo(_)) => {
            IncomingError::NexusHandler(IncomingNexusHandlerError::new(failure, cause))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protos::temporal::api::{
        common::v1::Payload,
        enums::v1::ApplicationErrorCategory,
        failure::v1::{
            ActivityFailureInfo, ChildWorkflowExecutionFailureInfo, NexusHandlerFailureInfo,
            NexusOperationFailureInfo, ResetWorkflowFailureInfo, ServerFailureInfo,
            TerminatedFailureInfo, TimeoutFailureInfo, failure::FailureInfo,
        },
    };
    use rstest::rstest;

    #[derive(Debug, Clone, Copy)]
    enum IncomingKind {
        Application,
        Timeout,
        Cancelled,
        Terminated,
        Server,
        ResetWorkflow,
        Activity,
        ChildWorkflowExecution,
        NexusOperationExecution,
        NexusHandler,
    }

    fn assert_incoming_kind(decoded: &IncomingError, expected: IncomingKind) {
        match expected {
            IncomingKind::Application => assert!(matches!(decoded, IncomingError::Application(_))),
            IncomingKind::Timeout => assert!(matches!(decoded, IncomingError::Timeout(_))),
            IncomingKind::Cancelled => assert!(matches!(decoded, IncomingError::Cancelled(_))),
            IncomingKind::Terminated => assert!(matches!(decoded, IncomingError::Terminated(_))),
            IncomingKind::Server => assert!(matches!(decoded, IncomingError::Server(_))),
            IncomingKind::ResetWorkflow => {
                assert!(matches!(decoded, IncomingError::ResetWorkflow(_)))
            }
            IncomingKind::Activity => assert!(matches!(decoded, IncomingError::Activity(_))),
            IncomingKind::ChildWorkflowExecution => {
                assert!(matches!(decoded, IncomingError::ChildWorkflowExecution(_)))
            }
            IncomingKind::NexusOperationExecution => {
                assert!(matches!(decoded, IncomingError::NexusOperationExecution(_)))
            }
            IncomingKind::NexusHandler => {
                assert!(matches!(decoded, IncomingError::NexusHandler(_)))
            }
        }
    }

    fn convert(err: OutgoingWorkflowError) -> Failure {
        DefaultFailureConverter
            .to_failure(
                OutgoingError::Workflow(err),
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap()
    }

    #[test]
    fn application_failures_preserve_metadata() {
        let failure = convert(OutgoingWorkflowError::Application(Box::new(
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
        )));
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
    fn application_failures_do_not_duplicate_their_source_as_cause() {
        let failure = convert(OutgoingWorkflowError::Application(Box::new(
            ApplicationFailure::new(anyhow::anyhow!("app boom")),
        )));

        assert_eq!(failure.message, "app boom");
        assert!(failure.cause.is_none());
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
            ActivityFailureError::new(activity_failure.clone(), None),
        )));
        let converted = convert(OutgoingWorkflowError::Application(Box::new(app)));
        assert!(matches!(
            converted.failure_info,
            Some(FailureInfo::ApplicationFailureInfo(_))
        ));
        assert_eq!(converted.cause.unwrap().as_ref(), &activity_failure);
    }

    #[test]
    fn start_failed_child_workflow_errors_fall_back_to_application_failures() {
        let failure = convert(OutgoingWorkflowError::ChildWorkflowExecution(Box::new(
            ChildWorkflowExecutionError::StartFailed {
            workflow_id: "wf-id".to_owned(),
            workflow_type: "wf-type".to_owned(),
            cause: crate::protos::coresdk::child_workflow::StartChildWorkflowExecutionFailedCause::WorkflowAlreadyExists,
        })));
        assert!(matches!(
            failure.failure_info,
            Some(FailureInfo::ApplicationFailureInfo(_))
        ));
        assert!(failure.message.contains("Child workflow start failed"));
    }

    #[test]
    fn application_failures_decode_with_metadata_and_proto() {
        let failure = Failure {
            message: "app boom".to_owned(),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo {
                    r#type: "MyType".to_owned(),
                    non_retryable: true,
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        let decoded = DefaultFailureConverter
            .to_error(
                failure.clone(),
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap();

        let IncomingError::Application(app) = decoded else {
            panic!("expected application error");
        };
        assert_eq!(app.type_name(), Some("MyType"));
        assert!(app.is_non_retryable());
        assert_eq!(app.failure(), Some(&failure));
    }

    #[test]
    fn application_failures_decode_with_normalized_cause() {
        let failure = Failure {
            message: "app boom".to_owned(),
            cause: Some(Box::new(Failure {
                message: "timed out".to_owned(),
                failure_info: Some(FailureInfo::TimeoutFailureInfo(
                    TimeoutFailureInfo::default(),
                )),
                ..Default::default()
            })),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo::default(),
            )),
            ..Default::default()
        };

        let decoded = DefaultFailureConverter
            .to_error(
                failure.clone(),
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap();

        let IncomingError::Application(app) = decoded else {
            panic!("expected application error");
        };
        assert_eq!(app.failure(), Some(&failure));
        assert!(matches!(app.cause(), Some(IncomingError::Timeout(_))));
    }

    #[rstest]
    #[case(
        FailureInfo::ApplicationFailureInfo(ApplicationFailureInfo::default()),
        IncomingKind::Application
    )]
    #[case(
        FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo::default()),
        IncomingKind::Timeout
    )]
    #[case(
        FailureInfo::CanceledFailureInfo(CanceledFailureInfo::default()),
        IncomingKind::Cancelled
    )]
    #[case(
        FailureInfo::TerminatedFailureInfo(TerminatedFailureInfo::default()),
        IncomingKind::Terminated
    )]
    #[case(
        FailureInfo::ServerFailureInfo(ServerFailureInfo::default()),
        IncomingKind::Server
    )]
    #[case(
        FailureInfo::ResetWorkflowFailureInfo(ResetWorkflowFailureInfo::default()),
        IncomingKind::ResetWorkflow
    )]
    #[case(
        FailureInfo::ActivityFailureInfo(ActivityFailureInfo::default()),
        IncomingKind::Activity
    )]
    #[case(
        FailureInfo::ChildWorkflowExecutionFailureInfo(
            ChildWorkflowExecutionFailureInfo::default()
        ),
        IncomingKind::ChildWorkflowExecution
    )]
    #[case(
        FailureInfo::NexusOperationExecutionFailureInfo(NexusOperationFailureInfo::default()),
        IncomingKind::NexusOperationExecution
    )]
    #[case(
        FailureInfo::NexusHandlerFailureInfo(NexusHandlerFailureInfo::default()),
        IncomingKind::NexusHandler
    )]
    fn failure_info_decodes_to_expected_incoming_error(
        #[case] failure_info: FailureInfo,
        #[case] expected: IncomingKind,
    ) {
        let failure = Failure {
            message: "boom".to_owned(),
            failure_info: Some(failure_info),
            ..Default::default()
        };

        let decoded = DefaultFailureConverter
            .to_error(
                failure.clone(),
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap();

        assert_incoming_kind(&decoded, expected);
        assert_eq!(decoded.failure(), &failure);
    }

    #[test]
    fn activity_decode_hint_preserves_timeout_reason() {
        let failure = Failure {
            message: "activity failed".to_owned(),
            cause: Some(Box::new(Failure {
                message: "timed out".to_owned(),
                failure_info: Some(FailureInfo::TimeoutFailureInfo(
                    TimeoutFailureInfo::default(),
                )),
                ..Default::default()
            })),
            failure_info: Some(FailureInfo::ActivityFailureInfo(
                ActivityFailureInfo::default(),
            )),
            ..Default::default()
        };
        let data_converter = crate::data_converters::DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            crate::data_converters::DefaultPayloadCodec,
        );

        let decoded = data_converter
            .to_error(
                &SerializationContextData::Workflow,
                failure.clone(),
                ActivityExecutionDecodeHint { cancelled: false },
            )
            .unwrap();

        let ActivityExecutionError::Failed(decoded_failure) = decoded else {
            panic!("expected failed activity execution error");
        };
        assert_eq!(decoded_failure.failure(), &failure);
        assert!(matches!(
            decoded_failure.cause(),
            Some(IncomingError::Timeout(_))
        ));
    }

    #[test]
    fn activity_cancelled_decode_hint_preserves_top_level_cancellation() {
        let failure = Failure {
            message: "activity cancelled".to_owned(),
            failure_info: Some(FailureInfo::CanceledFailureInfo(
                CanceledFailureInfo::default(),
            )),
            ..Default::default()
        };
        let data_converter = crate::data_converters::DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            crate::data_converters::DefaultPayloadCodec,
        );

        let decoded = data_converter
            .to_error(
                &SerializationContextData::Workflow,
                failure.clone(),
                ActivityExecutionDecodeHint { cancelled: true },
            )
            .unwrap();

        let ActivityExecutionError::Cancelled(decoded_failure) = decoded else {
            panic!("expected cancelled activity execution error");
        };
        assert_eq!(decoded_failure.failure(), &failure);
    }

    #[test]
    fn activity_cancelled_decode_hint_falls_back_to_failed_for_non_cancelled_proto() {
        let failure = Failure {
            message: "timed out".to_owned(),
            failure_info: Some(FailureInfo::TimeoutFailureInfo(
                TimeoutFailureInfo::default(),
            )),
            ..Default::default()
        };
        let data_converter = crate::data_converters::DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            crate::data_converters::DefaultPayloadCodec,
        );

        let decoded = data_converter
            .to_error(
                &SerializationContextData::Workflow,
                failure.clone(),
                ActivityExecutionDecodeHint { cancelled: true },
            )
            .unwrap();

        let ActivityExecutionError::Failed(decoded_failure) = decoded else {
            panic!("expected failed activity execution error");
        };
        assert_eq!(decoded_failure.failure(), &failure);
        assert!(decoded_failure.cause().is_none());
    }

    #[test]
    fn timeout_error_exposes_timeout_info_fields() {
        let heartbeat_details = crate::protos::temporal::api::common::v1::Payloads {
            payloads: vec![Payload {
                data: b"hb".to_vec(),
                ..Default::default()
            }],
        };
        let failure = Failure {
            message: "timed out".to_owned(),
            failure_info: Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
                timeout_type: crate::protos::temporal::api::enums::v1::TimeoutType::Heartbeat
                    .into(),
                last_heartbeat_details: Some(heartbeat_details.clone()),
            })),
            ..Default::default()
        };

        let decoded = DefaultFailureConverter
            .to_error(
                failure.clone(),
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap();

        let IncomingError::Timeout(timeout) = decoded else {
            panic!("expected timeout error");
        };
        assert_eq!(
            timeout.timeout_type(),
            crate::protos::temporal::api::enums::v1::TimeoutType::Heartbeat
        );
        assert_eq!(timeout.last_heartbeat_details(), Some(&heartbeat_details));
        assert_eq!(timeout.failure(), &failure);
    }

    #[test]
    fn cancelled_error_exposes_details() {
        let details = crate::protos::temporal::api::common::v1::Payloads {
            payloads: vec![Payload {
                data: b"cancel".to_vec(),
                ..Default::default()
            }],
        };
        let failure = Failure {
            message: "cancelled".to_owned(),
            failure_info: Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
                details: Some(details.clone()),
            })),
            ..Default::default()
        };

        let decoded = DefaultFailureConverter
            .to_error(
                failure.clone(),
                &PayloadConverter::default(),
                &SerializationContextData::Workflow,
            )
            .unwrap();

        let IncomingError::Cancelled(cancelled) = decoded else {
            panic!("expected cancelled error");
        };
        assert_eq!(cancelled.details(), Some(&details));
        assert_eq!(cancelled.failure(), &failure);
    }

    #[test]
    fn child_workflow_decode_hint_preserves_child_failure_proto() {
        let failure = Failure {
            message: "child workflow failed".to_owned(),
            failure_info: Some(FailureInfo::ChildWorkflowExecutionFailureInfo(
                ChildWorkflowExecutionFailureInfo {
                    namespace: "default".to_owned(),
                    workflow_execution: Some(
                        crate::protos::temporal::api::common::v1::WorkflowExecution {
                            workflow_id: "child-id".to_owned(),
                            run_id: "run-id".to_owned(),
                        },
                    ),
                    workflow_type: Some(crate::protos::temporal::api::common::v1::WorkflowType {
                        name: "child-type".to_owned(),
                    }),
                    initiated_event_id: 11,
                    started_event_id: 22,
                    retry_state: crate::protos::temporal::api::enums::v1::RetryState::Timeout
                        .into(),
                },
            )),
            ..Default::default()
        };
        let data_converter = crate::data_converters::DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            crate::data_converters::DefaultPayloadCodec,
        );

        let decoded = data_converter
            .to_error(
                &SerializationContextData::Workflow,
                failure.clone(),
                ChildWorkflowExecutionDecodeHint::Failed,
            )
            .unwrap();

        let ChildWorkflowExecutionError::Failed(decoded_failure) = decoded else {
            panic!("expected failed child-workflow execution error");
        };
        assert_eq!(decoded_failure.failure(), &failure);
        assert_eq!(decoded_failure.namespace(), "default");
        assert_eq!(
            decoded_failure
                .workflow_execution()
                .map(|wf| wf.workflow_id.as_str()),
            Some("child-id")
        );
        assert_eq!(
            decoded_failure
                .workflow_execution()
                .map(|wf| wf.run_id.as_str()),
            Some("run-id")
        );
        assert_eq!(
            decoded_failure.workflow_type().map(|wf| wf.name.as_str()),
            Some("child-type")
        );
        assert_eq!(decoded_failure.initiated_event_id(), 11);
        assert_eq!(decoded_failure.started_event_id(), 22);
        assert_eq!(
            decoded_failure.retry_state(),
            crate::protos::temporal::api::enums::v1::RetryState::Timeout
        );
    }

    #[test]
    fn child_workflow_cancelled_decode_hint_preserves_child_failure_proto() {
        let failure = Failure {
            message: "child workflow cancelled".to_owned(),
            failure_info: Some(FailureInfo::ChildWorkflowExecutionFailureInfo(
                ChildWorkflowExecutionFailureInfo::default(),
            )),
            ..Default::default()
        };
        let data_converter = crate::data_converters::DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            crate::data_converters::DefaultPayloadCodec,
        );

        let decoded = data_converter
            .to_error(
                &SerializationContextData::Workflow,
                failure.clone(),
                ChildWorkflowExecutionDecodeHint::Cancelled,
            )
            .unwrap();

        let ChildWorkflowExecutionError::Cancelled(decoded_failure) = decoded else {
            panic!("expected cancelled child-workflow execution error");
        };
        assert_eq!(decoded_failure.failure(), &failure);
        assert!(decoded_failure.cause().is_none());
    }

    #[test]
    fn child_workflow_cancelled_decode_hint_falls_back_to_failed_for_non_cancelled_proto() {
        let failure = Failure {
            message: "timed out".to_owned(),
            failure_info: Some(FailureInfo::TimeoutFailureInfo(
                TimeoutFailureInfo::default(),
            )),
            ..Default::default()
        };
        let data_converter = crate::data_converters::DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            crate::data_converters::DefaultPayloadCodec,
        );

        let decoded = data_converter
            .to_error(
                &SerializationContextData::Workflow,
                failure.clone(),
                ChildWorkflowExecutionDecodeHint::Cancelled,
            )
            .unwrap();

        let ChildWorkflowExecutionError::Failed(decoded_failure) = decoded else {
            panic!("expected failed child-workflow execution error");
        };
        assert_eq!(decoded_failure.failure(), &failure);
        assert!(decoded_failure.cause().is_none());
    }

    #[test]
    fn child_workflow_signal_decode_hint_preserves_failure_proto() {
        let failure = Failure {
            message: "child workflow signal failed".to_owned(),
            cause: Some(Box::new(Failure {
                message: "timed out".to_owned(),
                failure_info: Some(FailureInfo::TimeoutFailureInfo(
                    TimeoutFailureInfo::default(),
                )),
                ..Default::default()
            })),
            ..Default::default()
        };
        let data_converter = crate::data_converters::DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            crate::data_converters::DefaultPayloadCodec,
        );

        let decoded = data_converter
            .to_error(
                &SerializationContextData::Workflow,
                failure.clone(),
                ChildWorkflowSignalDecodeHint,
            )
            .unwrap();

        let ChildWorkflowSignalError::Failed(decoded_failure) = decoded else {
            panic!("expected failed child-workflow signal error");
        };
        assert_eq!(decoded_failure.failure(), &failure);
        assert!(matches!(
            decoded_failure.error(),
            IncomingError::Application(_)
        ));
        assert!(matches!(
            decoded_failure.cause(),
            Some(IncomingError::Timeout(_))
        ));
        assert!(std::error::Error::source(&decoded_failure).is_some());
    }

    #[test]
    fn outgoing_cancelled_activity_errors_encode_to_cancelled_failures() {
        let failure = DefaultFailureConverter
            .to_failure(
                OutgoingError::Activity(OutgoingActivityError::Cancelled { details: None }),
                &PayloadConverter::default(),
                &SerializationContextData::Activity,
            )
            .unwrap();

        assert_eq!(failure.message, "Activity cancelled");
        assert!(matches!(
            failure.failure_info,
            Some(FailureInfo::CanceledFailureInfo(_))
        ));
    }
}
