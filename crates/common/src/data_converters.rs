//! Contains traits for and default implementations of data converters, codecs, and other
//! serialization related functionality.

use crate::protos::temporal::api::{
    common::v1::{Payload, Payloads},
    enums::v1::{NexusHandlerErrorRetryBehavior, RetryState, TimeoutType},
    failure::v1::{
        ActivityFailureInfo, ApplicationFailureInfo, CanceledFailureInfo,
        ChildWorkflowExecutionFailureInfo, Failure, NexusHandlerFailureInfo,
        NexusOperationFailureInfo, ServerFailureInfo, TerminatedFailureInfo, TimeoutFailureInfo,
        failure::FailureInfo,
    },
};
use futures::{FutureExt, future::BoxFuture};
use std::{collections::HashMap, sync::Arc};
use tracing::warn;

/// Combines a [`PayloadConverter`], [`FailureConverter`], and [`PayloadCodec`] to handle all
/// serialization needs for communicating with the Temporal server.
#[derive(Clone)]
pub struct DataConverter {
    payload_converter: PayloadConverter,
    failure_converter: Arc<dyn FailureConverter + Send + Sync>,
    codec: Arc<dyn PayloadCodec + Send + Sync>,
}

impl std::fmt::Debug for DataConverter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataConverter")
            .field("payload_converter", &self.payload_converter)
            .finish_non_exhaustive()
    }
}
impl DataConverter {
    /// Create a new DataConverter with the given payload converter, failure converter, and codec.
    pub fn new(
        payload_converter: PayloadConverter,
        failure_converter: impl FailureConverter + Send + Sync + 'static,
        codec: impl PayloadCodec + Send + Sync + 'static,
    ) -> Self {
        Self {
            payload_converter,
            failure_converter: Arc::new(failure_converter),
            codec: Arc::new(codec),
        }
    }

    /// Serialize a value into a single payload, applying the codec.
    pub async fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        data: &SerializationContextData,
        val: &T,
    ) -> Result<Payload, PayloadConversionError> {
        let context = SerializationContext {
            data,
            converter: &self.payload_converter,
        };
        let payload = self.payload_converter.to_payload(&context, val)?;
        let encoded = self.codec.encode(data, vec![payload]).await;
        encoded
            .into_iter()
            .next()
            .ok_or(PayloadConversionError::WrongEncoding)
    }

    /// Deserialize a value from a single payload, applying the codec.
    pub async fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        data: &SerializationContextData,
        payload: Payload,
    ) -> Result<T, PayloadConversionError> {
        let context = SerializationContext {
            data,
            converter: &self.payload_converter,
        };
        let decoded = self.codec.decode(data, vec![payload]).await;
        let payload = decoded
            .into_iter()
            .next()
            .ok_or(PayloadConversionError::WrongEncoding)?;
        self.payload_converter.from_payload(&context, payload)
    }

    /// Serialize a value into multiple payloads (e.g. for multi-arg support), applying the codec.
    pub async fn to_payloads<T: TemporalSerializable + 'static>(
        &self,
        data: &SerializationContextData,
        val: &T,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        let context = SerializationContext {
            data,
            converter: &self.payload_converter,
        };
        let payloads = self.payload_converter.to_payloads(&context, val)?;
        Ok(self.codec.encode(data, payloads).await)
    }

    /// Deserialize a value from multiple payloads (e.g. for multi-arg support), applying the codec.
    pub async fn from_payloads<T: TemporalDeserializable + 'static>(
        &self,
        data: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> Result<T, PayloadConversionError> {
        let context = SerializationContext {
            data,
            converter: &self.payload_converter,
        };
        let decoded = self.codec.decode(data, payloads).await;
        self.payload_converter.from_payloads(&context, decoded)
    }

    /// Returns the payload converter component of this data converter.
    pub fn payload_converter(&self) -> &PayloadConverter {
        &self.payload_converter
    }

    /// Returns the failure converter component of this data converter.
    pub fn failure_converter(&self) -> &(dyn FailureConverter + Send + Sync) {
        self.failure_converter.as_ref()
    }

    /// Returns the codec component of this data converter.
    pub fn codec(&self) -> &(dyn PayloadCodec + Send + Sync) {
        self.codec.as_ref()
    }

    /// Convert a [`Failure`] proto into an error using only the
    /// failure converter (no codec). Use [`decode_failure`](Self::decode_failure)
    /// for the full pipeline including codec.
    pub fn to_error(&self, failure: Failure, context: &SerializationContextData) -> TemporalError {
        self.failure_converter
            .to_error(failure, &self.payload_converter, context)
    }

    /// Convert an error into a [`Failure`] proto using only the failure
    /// converter (no codec). The codec is applied separately by the
    /// `PayloadVisitable` visitor on outgoing completions.
    pub fn to_failure(
        &self,
        error: Box<dyn std::error::Error + Send + Sync>,
        context: &SerializationContextData,
    ) -> Failure {
        self.failure_converter
            .to_failure(error, &self.payload_converter, context)
    }

    /// Decode a [`Failure`] proto into an error, applying the codec
    /// to embedded payloads before running the failure converter.
    pub async fn decode_failure(
        &self,
        failure: Failure,
        context: &SerializationContextData,
    ) -> TemporalError {
        let decoded = Self::apply_codec_to_failure(failure, context, |ctx, payloads| {
            self.codec.decode(ctx, payloads)
        })
        .await;
        self.failure_converter
            .to_error(decoded, &self.payload_converter, context)
    }

    /// Recursively apply a codec operation (encode or decode) to all payloads
    /// embedded in a [`Failure`]: `encoded_attributes`, detail payloads in
    /// failure info variants, and causes.
    async fn apply_codec_to_failure<F, Fut>(
        failure: Failure,
        context: &SerializationContextData,
        codec_fn: F,
    ) -> Failure
    where
        F: Fn(&SerializationContextData, Vec<Payload>) -> Fut + Copy,
        Fut: std::future::Future<Output = Vec<Payload>>,
    {
        let Failure {
            message,
            source,
            stack_trace,
            encoded_attributes,
            cause,
            failure_info,
            ..
        } = failure;
        let cause_context = Self::nested_failure_context(*context, failure_info.as_ref());

        let encoded_attributes = match encoded_attributes {
            Some(ea) => codec_fn(context, vec![ea]).await.into_iter().next(),
            None => None,
        };

        let failure_info = match failure_info {
            Some(FailureInfo::ApplicationFailureInfo(mut app)) => {
                let mut d = app.details.take().unwrap_or_default();
                d.payloads = codec_fn(context, d.payloads).await;
                app.details = Some(d);
                Some(FailureInfo::ApplicationFailureInfo(app))
            }
            Some(FailureInfo::TimeoutFailureInfo(mut t)) => {
                let mut d = t.last_heartbeat_details.take().unwrap_or_default();
                d.payloads = codec_fn(context, d.payloads).await;
                t.last_heartbeat_details = Some(d);
                Some(FailureInfo::TimeoutFailureInfo(t))
            }
            Some(FailureInfo::CanceledFailureInfo(mut c)) => {
                let mut d = c.details.take().unwrap_or_default();
                d.payloads = codec_fn(context, d.payloads).await;
                c.details = Some(d);
                Some(FailureInfo::CanceledFailureInfo(c))
            }
            Some(FailureInfo::ResetWorkflowFailureInfo(mut r)) => {
                let mut d = r.last_heartbeat_details.take().unwrap_or_default();
                d.payloads = codec_fn(context, d.payloads).await;
                r.last_heartbeat_details = Some(d);
                Some(FailureInfo::ResetWorkflowFailureInfo(r))
            }
            other => other,
        };

        let cause = match cause {
            Some(c) => Some(Box::new(
                Box::pin(Self::apply_codec_to_failure(*c, &cause_context, codec_fn)).await,
            )),
            None => None,
        };

        Failure {
            message,
            source,
            stack_trace,
            encoded_attributes,
            cause,
            failure_info,
        }
    }

    fn nested_failure_context(
        context: SerializationContextData,
        failure_info: Option<&FailureInfo>,
    ) -> SerializationContextData {
        match failure_info {
            Some(FailureInfo::ActivityFailureInfo(_)) => SerializationContextData::Activity,
            Some(FailureInfo::ChildWorkflowExecutionFailureInfo(_)) => {
                SerializationContextData::Workflow
            }
            Some(FailureInfo::NexusOperationExecutionFailureInfo(_)) => {
                SerializationContextData::Nexus
            }
            _ => context,
        }
    }
}

/// Data about the serialization context, indicating where the serialization is occurring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SerializationContextData {
    /// Serialization is occurring in a workflow context.
    Workflow,
    /// Serialization is occurring in an activity context.
    Activity,
    /// Serialization is occurring in a nexus context.
    Nexus,
    /// No specific serialization context.
    None,
}

/// Context for serialization operations, including the kind of context and the
/// payload converter for nested serialization.
#[derive(Clone, Copy)]
pub struct SerializationContext<'a> {
    /// The kind of serialization context (workflow, activity, etc.).
    pub data: &'a SerializationContextData,
    /// Allows nested types to serialize their contents using the same converter.
    pub converter: &'a PayloadConverter,
}
/// Converts values to and from [`Payload`]s using different encoding strategies.
#[derive(Clone)]
pub enum PayloadConverter {
    /// Uses a serde-based converter for encoding/decoding.
    Serde(Arc<dyn ErasedSerdePayloadConverter>),
    /// This variant signals the user wants to delegate to wrapper types
    UseWrappers,
    /// Tries multiple converters in order until one succeeds.
    Composite(Arc<CompositePayloadConverter>),
}

impl std::fmt::Debug for PayloadConverter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PayloadConverter::Serde(_) => write!(f, "PayloadConverter::Serde(...)"),
            PayloadConverter::UseWrappers => write!(f, "PayloadConverter::UseWrappers"),
            PayloadConverter::Composite(_) => write!(f, "PayloadConverter::Composite(...)"),
        }
    }
}
impl PayloadConverter {
    /// Create a payload converter that uses JSON serialization via serde.
    pub fn serde_json() -> Self {
        Self::Serde(Arc::new(SerdeJsonPayloadConverter))
    }
    // TODO [rust-sdk-branch]: Proto binary, other standard built-ins
}

impl Default for PayloadConverter {
    fn default() -> Self {
        Self::Composite(Arc::new(CompositePayloadConverter {
            converters: vec![Self::UseWrappers, Self::serde_json()],
        }))
    }
}

/// Errors that can occur during payload conversion.
#[derive(Debug)]
pub enum PayloadConversionError {
    /// The payload's encoding does not match what the converter expects.
    WrongEncoding,
    /// An error occurred during encoding or decoding.
    EncodingError(Box<dyn std::error::Error + Send + Sync>),
}

impl std::fmt::Display for PayloadConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PayloadConversionError::WrongEncoding => write!(f, "Wrong encoding"),
            PayloadConversionError::EncodingError(err) => write!(f, "Encoding error: {}", err),
        }
    }
}

impl std::error::Error for PayloadConversionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PayloadConversionError::WrongEncoding => None,
            PayloadConversionError::EncodingError(err) => Some(err.as_ref()),
        }
    }
}

/// Converts between Rust errors and Temporal [`Failure`] protobufs.
///
/// Implementations must be infallible — conversion should always succeed,
/// falling back to a reasonable default (e.g. wrapping as
/// `ApplicationFailureInfo`) rather than returning an error. This matches
/// every other Temporal SDK.
pub trait FailureConverter {
    /// Convert an error into a Temporal failure protobuf.
    fn to_failure(
        &self,
        error: Box<dyn std::error::Error + Send + Sync>,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> Failure;

    /// Convert a Temporal failure protobuf back into a Rust error.
    fn to_error(
        &self,
        failure: Failure,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> TemporalError;
}
/// Default failure converter that maps between Temporal [`Failure`] protobufs
/// and Rust error types.
pub struct DefaultFailureConverter;

/// An error produced by the failure converter, representing a Temporal
/// [`Failure`] proto as an error.
#[derive(Debug, thiserror::Error)]
pub enum TemporalError {
    /// Application-level failure — the primary error type users throw from
    /// workflows and activities.
    #[error("{message}")]
    Application {
        /// Human-readable error message.
        message: String,
        /// Stack trace from the originating SDK, if available.
        stack_trace: String,
        /// Application error type string.
        r#type: String,
        /// Whether this error is non-retryable.
        non_retryable: bool,
        /// Serialized detail payloads.
        details: Option<Payloads>,
        /// Override for the next retry delay.
        next_retry_delay: Option<prost_types::Duration>,
        /// Recursive cause.
        #[source]
        cause: Option<Box<TemporalError>>,
    },
    /// A timeout occurred (activity start-to-close, schedule-to-close, etc.).
    #[error("{message}")]
    Timeout {
        /// Human-readable error message.
        message: String,
        /// Stack trace from the originating SDK, if available.
        stack_trace: String,
        /// Which kind of timeout.
        timeout_type: TimeoutType,
        /// Last heartbeat details before the timeout.
        last_heartbeat_details: Option<Payloads>,
        /// Recursive cause.
        #[source]
        cause: Option<Box<TemporalError>>,
    },
    /// The operation was cancelled.
    #[error("{message}")]
    Cancelled {
        /// Human-readable error message.
        message: String,
        /// Stack trace from the originating SDK, if available.
        stack_trace: String,
        /// Cancellation detail payloads.
        details: Option<Payloads>,
        /// Recursive cause.
        #[source]
        cause: Option<Box<TemporalError>>,
    },
    /// The workflow or activity was terminated.
    #[error("{message}")]
    Terminated {
        /// Human-readable error message.
        message: String,
        /// Stack trace from the originating SDK, if available.
        stack_trace: String,
        /// Recursive cause.
        #[source]
        cause: Option<Box<TemporalError>>,
    },
    /// An error originated at the Temporal server.
    #[error("{message}")]
    Server {
        /// Human-readable error message.
        message: String,
        /// Stack trace from the originating SDK, if available.
        stack_trace: String,
        /// Whether this error is non-retryable.
        non_retryable: bool,
        /// Recursive cause.
        #[source]
        cause: Option<Box<TemporalError>>,
    },
    /// An activity execution failed. The original error is available as the
    /// cause.
    #[error("{message}")]
    Activity {
        /// Human-readable error message.
        message: String,
        /// Stack trace from the originating SDK, if available.
        stack_trace: String,
        /// Scheduled event ID.
        scheduled_event_id: i64,
        /// Started event ID.
        started_event_id: i64,
        /// Worker identity.
        identity: String,
        /// Activity type name.
        activity_type: String,
        /// Activity ID.
        activity_id: String,
        /// Retry state at the time of failure.
        retry_state: RetryState,
        /// Recursive cause (typically the underlying application error).
        #[source]
        cause: Option<Box<TemporalError>>,
    },
    /// A child workflow execution failed.
    #[error("{message}")]
    ChildWorkflow {
        /// Human-readable error message.
        message: String,
        /// Stack trace from the originating SDK, if available.
        stack_trace: String,
        /// Child workflow namespace.
        namespace: String,
        /// Child workflow ID.
        workflow_id: String,
        /// Child workflow run ID.
        run_id: String,
        /// Child workflow type name.
        workflow_type: String,
        /// Initiated event ID.
        initiated_event_id: i64,
        /// Started event ID.
        started_event_id: i64,
        /// Retry state at the time of failure.
        retry_state: RetryState,
        /// Recursive cause.
        #[source]
        cause: Option<Box<TemporalError>>,
    },
    /// A Nexus operation failed.
    #[error("{message}")]
    NexusOperation {
        /// Human-readable error message.
        message: String,
        /// Stack trace from the originating SDK, if available.
        stack_trace: String,
        /// Scheduled event ID.
        scheduled_event_id: i64,
        /// Nexus endpoint name.
        endpoint: String,
        /// Nexus service name.
        service: String,
        /// Nexus operation name.
        operation: String,
        /// Operation token (may be empty for sync completions).
        operation_token: String,
        /// Recursive cause.
        #[source]
        cause: Option<Box<TemporalError>>,
    },
    /// A Nexus handler produced an error.
    #[error("{message}")]
    NexusHandler {
        /// Human-readable error message.
        message: String,
        /// Stack trace from the originating SDK, if available.
        stack_trace: String,
        /// Nexus error type.
        r#type: String,
        /// Retry behavior.
        retry_behavior: NexusHandlerErrorRetryBehavior,
        /// Recursive cause.
        #[source]
        cause: Option<Box<TemporalError>>,
    },
    /// A failure with no specific `failure_info`, or an unmodeled variant.
    #[error("{message}")]
    Generic {
        /// Human-readable error message.
        message: String,
        /// Stack trace from the originating SDK, if available.
        stack_trace: String,
        /// Recursive cause.
        #[source]
        cause: Option<Box<TemporalError>>,
    },
    /// An opaque error from a custom failure converter that doesn't map to a
    /// known Temporal failure type.
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

impl TemporalError {
    /// The human-readable error message, if this is a known Temporal failure
    /// variant. Returns `None` for [`Other`](Self::Other) — use `Display` to
    /// get a message from any variant.
    pub fn message(&self) -> Option<&str> {
        match self {
            Self::Application { message, .. }
            | Self::Timeout { message, .. }
            | Self::Cancelled { message, .. }
            | Self::Terminated { message, .. }
            | Self::Server { message, .. }
            | Self::Activity { message, .. }
            | Self::ChildWorkflow { message, .. }
            | Self::NexusOperation { message, .. }
            | Self::NexusHandler { message, .. }
            | Self::Generic { message, .. } => Some(message),
            Self::Other(_) => None,
        }
    }

    /// The stack trace, if this is a known Temporal failure variant. Returns
    /// `None` for [`Other`](Self::Other).
    pub fn stack_trace(&self) -> Option<&str> {
        match self {
            Self::Application { stack_trace, .. }
            | Self::Timeout { stack_trace, .. }
            | Self::Cancelled { stack_trace, .. }
            | Self::Terminated { stack_trace, .. }
            | Self::Server { stack_trace, .. }
            | Self::Activity { stack_trace, .. }
            | Self::ChildWorkflow { stack_trace, .. }
            | Self::NexusOperation { stack_trace, .. }
            | Self::NexusHandler { stack_trace, .. }
            | Self::Generic { stack_trace, .. } => Some(stack_trace),
            Self::Other(_) => None,
        }
    }

    /// Returns the cause of this error, if any.
    pub fn cause(&self) -> Option<&TemporalError> {
        match self {
            Self::Application { cause, .. }
            | Self::Timeout { cause, .. }
            | Self::Cancelled { cause, .. }
            | Self::Terminated { cause, .. }
            | Self::Server { cause, .. }
            | Self::Activity { cause, .. }
            | Self::ChildWorkflow { cause, .. }
            | Self::NexusOperation { cause, .. }
            | Self::NexusHandler { cause, .. }
            | Self::Generic { cause, .. } => cause.as_deref(),
            Self::Other(_) => None,
        }
    }

    fn from_failure(mut failure: Failure, payload_converter: &PayloadConverter) -> Self {
        // If encoded_attributes is present, decode message (and stack_trace)
        // from the payload — the top-level fields were cleared for encryption.
        if let Some(payload) = failure.encoded_attributes.take() {
            let ctx = SerializationContext {
                data: &SerializationContextData::None,
                converter: payload_converter,
            };
            match payload_converter.from_payload::<serde_json::Value>(&ctx, payload) {
                Ok(attrs) => {
                    if let Some(msg) = attrs.get("message").and_then(|v| v.as_str()) {
                        failure.message = msg.to_owned();
                    }
                    if let Some(st) = attrs.get("stack_trace").and_then(|v| v.as_str()) {
                        failure.stack_trace = st.to_owned();
                    }
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        "Failed to decode encoded_attributes on Failure proto, \
                         falling back to top-level message"
                    );
                }
            }
        }

        let cause = failure
            .cause
            .map(|c| Box::new(Self::from_failure(*c, payload_converter)));

        let stack_trace = failure.stack_trace;

        match failure.failure_info {
            Some(FailureInfo::ApplicationFailureInfo(info)) => Self::Application {
                message: failure.message,
                stack_trace,
                r#type: info.r#type,
                non_retryable: info.non_retryable,
                details: info.details,
                next_retry_delay: info.next_retry_delay,
                cause,
            },
            Some(FailureInfo::TimeoutFailureInfo(info)) => Self::Timeout {
                message: failure.message,
                stack_trace,
                timeout_type: info.timeout_type(),
                last_heartbeat_details: info.last_heartbeat_details,
                cause,
            },
            Some(FailureInfo::CanceledFailureInfo(info)) => Self::Cancelled {
                message: failure.message,
                stack_trace,
                details: info.details,
                cause,
            },
            Some(FailureInfo::TerminatedFailureInfo(_)) => Self::Terminated {
                message: failure.message,
                stack_trace,
                cause,
            },
            Some(FailureInfo::ServerFailureInfo(info)) => Self::Server {
                message: failure.message,
                stack_trace,
                non_retryable: info.non_retryable,
                cause,
            },
            Some(FailureInfo::ActivityFailureInfo(info)) => {
                let retry_state = info.retry_state();
                Self::Activity {
                    message: failure.message,
                    stack_trace,
                    scheduled_event_id: info.scheduled_event_id,
                    started_event_id: info.started_event_id,
                    identity: info.identity,
                    activity_type: info.activity_type.map(|t| t.name).unwrap_or_default(),
                    activity_id: info.activity_id,
                    retry_state,
                    cause,
                }
            }
            Some(FailureInfo::ChildWorkflowExecutionFailureInfo(info)) => {
                let retry_state = info.retry_state();
                let (workflow_id, run_id) = info
                    .workflow_execution
                    .map(|e| (e.workflow_id, e.run_id))
                    .unwrap_or_default();
                Self::ChildWorkflow {
                    message: failure.message,
                    stack_trace,
                    namespace: info.namespace,
                    workflow_id,
                    run_id,
                    workflow_type: info.workflow_type.map(|t| t.name).unwrap_or_default(),
                    initiated_event_id: info.initiated_event_id,
                    started_event_id: info.started_event_id,
                    retry_state,
                    cause,
                }
            }
            Some(FailureInfo::NexusOperationExecutionFailureInfo(info)) => Self::NexusOperation {
                message: failure.message,
                stack_trace,
                scheduled_event_id: info.scheduled_event_id,
                endpoint: info.endpoint,
                service: info.service,
                operation: info.operation,
                operation_token: info.operation_token,
                cause,
            },
            Some(FailureInfo::NexusHandlerFailureInfo(info)) => {
                let retry_behavior = info.retry_behavior();
                Self::NexusHandler {
                    message: failure.message,
                    stack_trace,
                    r#type: info.r#type,
                    retry_behavior,
                    cause,
                }
            }
            Some(FailureInfo::ResetWorkflowFailureInfo(_)) | None => Self::Generic {
                message: failure.message,
                stack_trace,
                cause,
            },
        }
    }

    fn into_failure(self) -> Failure {
        let (message, stack_trace, failure_info, cause) = match self {
            Self::Application {
                message,
                stack_trace,
                r#type,
                non_retryable,
                details,
                next_retry_delay,
                cause,
            } => (
                message,
                stack_trace,
                Some(FailureInfo::ApplicationFailureInfo(
                    ApplicationFailureInfo {
                        r#type,
                        non_retryable,
                        details,
                        next_retry_delay,
                        ..Default::default()
                    },
                )),
                cause,
            ),
            Self::Timeout {
                message,
                stack_trace,
                timeout_type,
                last_heartbeat_details,
                cause,
            } => (
                message,
                stack_trace,
                Some(FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
                    timeout_type: timeout_type.into(),
                    last_heartbeat_details,
                })),
                cause,
            ),
            Self::Cancelled {
                message,
                stack_trace,
                details,
                cause,
            } => (
                message,
                stack_trace,
                Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
                    details,
                })),
                cause,
            ),
            Self::Terminated {
                message,
                stack_trace,
                cause,
            } => (
                message,
                stack_trace,
                Some(FailureInfo::TerminatedFailureInfo(TerminatedFailureInfo {})),
                cause,
            ),
            Self::Server {
                message,
                stack_trace,
                non_retryable,
                cause,
            } => (
                message,
                stack_trace,
                Some(FailureInfo::ServerFailureInfo(ServerFailureInfo {
                    non_retryable,
                })),
                cause,
            ),
            Self::Activity {
                message,
                stack_trace,
                scheduled_event_id,
                started_event_id,
                identity,
                activity_type,
                activity_id,
                retry_state,
                cause,
            } => (
                message,
                stack_trace,
                Some(FailureInfo::ActivityFailureInfo(ActivityFailureInfo {
                    scheduled_event_id,
                    started_event_id,
                    identity,
                    activity_type: Some(crate::protos::temporal::api::common::v1::ActivityType {
                        name: activity_type,
                    }),
                    activity_id,
                    retry_state: retry_state.into(),
                })),
                cause,
            ),
            Self::ChildWorkflow {
                message,
                stack_trace,
                namespace,
                workflow_id,
                run_id,
                workflow_type,
                initiated_event_id,
                started_event_id,
                retry_state,
                cause,
            } => (
                message,
                stack_trace,
                Some(FailureInfo::ChildWorkflowExecutionFailureInfo(
                    ChildWorkflowExecutionFailureInfo {
                        namespace,
                        workflow_execution: Some(
                            crate::protos::temporal::api::common::v1::WorkflowExecution {
                                workflow_id,
                                run_id,
                            },
                        ),
                        workflow_type: Some(
                            crate::protos::temporal::api::common::v1::WorkflowType {
                                name: workflow_type,
                            },
                        ),
                        initiated_event_id,
                        started_event_id,
                        retry_state: retry_state.into(),
                    },
                )),
                cause,
            ),
            Self::NexusOperation {
                message,
                stack_trace,
                scheduled_event_id,
                endpoint,
                service,
                operation,
                operation_token,
                cause,
            } => (
                message,
                stack_trace,
                Some(FailureInfo::NexusOperationExecutionFailureInfo(
                    NexusOperationFailureInfo {
                        scheduled_event_id,
                        endpoint,
                        service,
                        operation,
                        operation_token,
                        ..Default::default()
                    },
                )),
                cause,
            ),
            Self::NexusHandler {
                message,
                stack_trace,
                r#type,
                retry_behavior,
                cause,
            } => (
                message,
                stack_trace,
                Some(FailureInfo::NexusHandlerFailureInfo(
                    NexusHandlerFailureInfo {
                        r#type,
                        retry_behavior: retry_behavior.into(),
                    },
                )),
                cause,
            ),
            Self::Generic {
                message,
                stack_trace,
                cause,
            } => (message, stack_trace, None, cause),
            Self::Other(e) => (
                e.to_string(),
                String::new(),
                Some(FailureInfo::ApplicationFailureInfo(
                    ApplicationFailureInfo::default(),
                )),
                None,
            ),
        };

        Failure {
            message,
            stack_trace,
            source: "RustSDK".into(),
            failure_info,
            cause: cause.map(|c| Box::new(c.into_failure())),
            ..Default::default()
        }
    }
}

/// Encodes and decodes payloads, enabling encryption or compression.
pub trait PayloadCodec {
    /// Encode payloads before they are sent to the server.
    fn encode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>>;
    /// Decode payloads after they are received from the server.
    fn decode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>>;
}

impl<T: PayloadCodec> PayloadCodec for Arc<T> {
    fn encode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        (**self).encode(context, payloads)
    }
    fn decode(
        &self,
        context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        (**self).decode(context, payloads)
    }
}

/// A no-op codec that passes payloads through unchanged.
pub struct DefaultPayloadCodec;

/// Indicates some type can be serialized for use with Temporal.
///
/// You don't need to implement this unless you are using a non-serde-compatible custom converter,
/// in which case you should implement the to/from_payload functions on some wrapper type.
pub trait TemporalSerializable {
    /// Return a reference to this value as a serde-serializable trait object.
    fn as_serde(&self) -> Result<&dyn erased_serde::Serialize, PayloadConversionError> {
        Err(PayloadConversionError::WrongEncoding)
    }
    /// Convert this value into a single [`Payload`].
    fn to_payload(&self, _: &SerializationContext<'_>) -> Result<Payload, PayloadConversionError> {
        Err(PayloadConversionError::WrongEncoding)
    }
    /// Convert to multiple payloads. Override this for types representing multiple arguments.
    fn to_payloads(
        &self,
        ctx: &SerializationContext<'_>,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        Ok(vec![self.to_payload(ctx)?])
    }
}

/// Indicates some type can be deserialized for use with Temporal.
///
/// You don't need to implement this unless you are using a non-serde-compatible custom converter,
/// in which case you should implement the to/from_payload functions on some wrapper type.
pub trait TemporalDeserializable: Sized {
    /// Deserialize from a serde-based payload converter.
    fn from_serde(
        _: &dyn ErasedSerdePayloadConverter,
        _ctx: &SerializationContext<'_>,
        _: Payload,
    ) -> Result<Self, PayloadConversionError> {
        Err(PayloadConversionError::WrongEncoding)
    }
    /// Deserialize from a single [`Payload`].
    fn from_payload(
        ctx: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<Self, PayloadConversionError> {
        let _ = (ctx, payload);
        Err(PayloadConversionError::WrongEncoding)
    }
    /// Convert from multiple payloads. Override this for types representing multiple arguments.
    fn from_payloads(
        ctx: &SerializationContext<'_>,
        payloads: Vec<Payload>,
    ) -> Result<Self, PayloadConversionError> {
        if payloads.len() != 1 {
            return Err(PayloadConversionError::WrongEncoding);
        }
        Self::from_payload(ctx, payloads.into_iter().next().unwrap())
    }
}

/// An unconverted set of payloads, used when the caller wants to defer deserialization.
#[derive(Clone, Debug, Default)]
pub struct RawValue {
    /// The underlying payloads.
    pub payloads: Vec<Payload>,
}
impl RawValue {
    /// A RawValue representing no meaningful data, containing a single default payload.
    /// This ensures the value can still be serialized as a single payload.
    pub fn empty() -> Self {
        Self {
            payloads: vec![Payload::default()],
        }
    }

    /// Create a new RawValue from a vector of payloads.
    pub fn new(payloads: Vec<Payload>) -> Self {
        Self { payloads }
    }

    /// Create a [`RawValue`] by serializing a value with the given converter.
    pub fn from_value<T: TemporalSerializable + 'static>(
        value: &T,
        converter: &PayloadConverter,
    ) -> RawValue {
        RawValue::new(vec![
            converter
                .to_payload(
                    &SerializationContext {
                        data: &SerializationContextData::None,
                        converter,
                    },
                    value,
                )
                .unwrap(),
        ])
    }

    /// Deserialize this [`RawValue`] into a typed value using the given converter.
    pub fn to_value<T: TemporalDeserializable + 'static>(self, converter: &PayloadConverter) -> T {
        converter
            .from_payload(
                &SerializationContext {
                    data: &SerializationContextData::None,
                    converter,
                },
                self.payloads.into_iter().next().unwrap(),
            )
            .unwrap()
    }
}

impl TemporalSerializable for RawValue {
    fn to_payload(&self, _: &SerializationContext<'_>) -> Result<Payload, PayloadConversionError> {
        Ok(self.payloads.first().cloned().unwrap_or_default())
    }
    fn to_payloads(
        &self,
        _: &SerializationContext<'_>,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        Ok(self.payloads.clone())
    }
}

impl TemporalDeserializable for RawValue {
    fn from_payload(
        _: &SerializationContext<'_>,
        p: Payload,
    ) -> Result<Self, PayloadConversionError> {
        Ok(RawValue { payloads: vec![p] })
    }
    fn from_payloads(
        _: &SerializationContext<'_>,
        payloads: Vec<Payload>,
    ) -> Result<Self, PayloadConversionError> {
        Ok(RawValue { payloads })
    }
}

/// Generic interface for converting between typed values and [`Payload`]s.
pub trait GenericPayloadConverter {
    /// Serialize a value into a single [`Payload`].
    fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        val: &T,
    ) -> Result<Payload, PayloadConversionError>;
    /// Deserialize a value from a single [`Payload`].
    #[allow(clippy::wrong_self_convention)]
    fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<T, PayloadConversionError>;
    /// Serialize a value into multiple [`Payload`]s.
    fn to_payloads<T: TemporalSerializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        val: &T,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        Ok(vec![self.to_payload(context, val)?])
    }
    /// Deserialize a value from multiple [`Payload`]s.
    #[allow(clippy::wrong_self_convention)]
    fn from_payloads<T: TemporalDeserializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        payloads: Vec<Payload>,
    ) -> Result<T, PayloadConversionError> {
        if payloads.len() != 1 {
            return Err(PayloadConversionError::WrongEncoding);
        }
        self.from_payload(context, payloads.into_iter().next().unwrap())
    }
}

impl GenericPayloadConverter for PayloadConverter {
    fn to_payload<T: TemporalSerializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        val: &T,
    ) -> Result<Payload, PayloadConversionError> {
        // If a single payload is explicitly needed for `()`, then produce a null payload
        if std::any::TypeId::of::<T>() == std::any::TypeId::of::<()>() {
            return Ok(Payload {
                metadata: {
                    let mut hm = HashMap::new();
                    hm.insert("encoding".to_string(), b"binary/null".to_vec());
                    hm
                },
                data: vec![],
                external_payloads: vec![],
            });
        }
        let mut payloads = self.to_payloads(context, val)?;
        if payloads.len() != 1 {
            return Err(PayloadConversionError::WrongEncoding);
        }
        Ok(payloads.pop().unwrap())
    }

    fn from_payload<T: TemporalDeserializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<T, PayloadConversionError> {
        self.from_payloads(context, vec![payload])
    }

    fn to_payloads<T: TemporalSerializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        val: &T,
    ) -> Result<Vec<Payload>, PayloadConversionError> {
        match self {
            PayloadConverter::Serde(pc) => {
                // Since Rust SDK uses () to denote no input, we must match other SDKs by producing
                // no payloads for it.
                if std::any::TypeId::of::<T>() == std::any::TypeId::of::<()>() {
                    Ok(Vec::new())
                } else {
                    Ok(vec![pc.to_payload(context.data, val.as_serde()?)?])
                }
            }
            PayloadConverter::UseWrappers => T::to_payloads(val, context),
            PayloadConverter::Composite(composite) => {
                for converter in &composite.converters {
                    match converter.to_payloads(context, val) {
                        Ok(payloads) => return Ok(payloads),
                        Err(PayloadConversionError::WrongEncoding) => continue,
                        Err(e) => return Err(e),
                    }
                }
                Err(PayloadConversionError::WrongEncoding)
            }
        }
    }

    fn from_payloads<T: TemporalDeserializable + 'static>(
        &self,
        context: &SerializationContext<'_>,
        payloads: Vec<Payload>,
    ) -> Result<T, PayloadConversionError> {
        // Accept empty payloads (no args) and a single binary/null payload (result from a
        // workflow/update with () return type as ().
        if std::any::TypeId::of::<T>() == std::any::TypeId::of::<()>()
            && is_unit_payloads(&payloads)
        {
            let boxed: Box<dyn std::any::Any> = Box::new(());
            return Ok(*boxed.downcast::<T>().unwrap());
        }

        match self {
            PayloadConverter::Serde(pc) => {
                if payloads.len() != 1 {
                    return Err(PayloadConversionError::WrongEncoding);
                }
                T::from_serde(pc.as_ref(), context, payloads.into_iter().next().unwrap())
            }
            PayloadConverter::UseWrappers => T::from_payloads(context, payloads),
            PayloadConverter::Composite(composite) => {
                for converter in &composite.converters {
                    match converter.from_payloads(context, payloads.clone()) {
                        Ok(val) => return Ok(val),
                        Err(PayloadConversionError::WrongEncoding) => continue,
                        Err(e) => return Err(e),
                    }
                }
                Err(PayloadConversionError::WrongEncoding)
            }
        }
    }
}

fn is_unit_payloads(payloads: &[Payload]) -> bool {
    match payloads {
        [] => true,
        [payload] => {
            payload.data.is_empty()
                && payload
                    .metadata
                    .get("encoding")
                    .map(|encoding| encoding == b"binary/null")
                    .unwrap_or(false)
        }
        _ => false,
    }
}

// TODO [rust-sdk-branch]: Potentially allow opt-out / no-serde compile flags
impl<T> TemporalSerializable for T
where
    T: serde::Serialize,
{
    fn as_serde(&self) -> Result<&dyn erased_serde::Serialize, PayloadConversionError> {
        Ok(self)
    }
}
impl<T> TemporalDeserializable for T
where
    T: serde::de::DeserializeOwned,
{
    fn from_serde(
        pc: &dyn ErasedSerdePayloadConverter,
        context: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<Self, PayloadConversionError>
    where
        Self: Sized,
    {
        let mut de = pc.from_payload(context.data, payload)?;
        erased_serde::deserialize(&mut de)
            .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))
    }
}

struct SerdeJsonPayloadConverter;
impl ErasedSerdePayloadConverter for SerdeJsonPayloadConverter {
    fn to_payload(
        &self,
        _: &SerializationContextData,
        value: &dyn erased_serde::Serialize,
    ) -> Result<Payload, PayloadConversionError> {
        let as_json = serde_json::to_vec(value)
            .map_err(|e| PayloadConversionError::EncodingError(e.into()))?;
        Ok(Payload {
            metadata: {
                let mut hm = HashMap::new();
                hm.insert("encoding".to_string(), b"json/plain".to_vec());
                hm
            },
            data: as_json,
            external_payloads: vec![],
        })
    }

    fn from_payload(
        &self,
        _: &SerializationContextData,
        payload: Payload,
    ) -> Result<Box<dyn erased_serde::Deserializer<'static>>, PayloadConversionError> {
        let encoding = payload.metadata.get("encoding").map(|v| v.as_slice());
        if encoding != Some(b"json/plain".as_slice()) {
            return Err(PayloadConversionError::WrongEncoding);
        }
        let json_v: serde_json::Value = serde_json::from_slice(&payload.data)
            .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))?;
        Ok(Box::new(<dyn erased_serde::Deserializer>::erase(json_v)))
    }
}
/// Type-erased serde-based payload converter for use behind `dyn` trait objects.
pub trait ErasedSerdePayloadConverter: Send + Sync {
    /// Serialize a type-erased serde value into a [`Payload`].
    fn to_payload(
        &self,
        context: &SerializationContextData,
        value: &dyn erased_serde::Serialize,
    ) -> Result<Payload, PayloadConversionError>;
    /// Deserialize a [`Payload`] into a type-erased serde deserializer.
    #[allow(clippy::wrong_self_convention)]
    fn from_payload(
        &self,
        context: &SerializationContextData,
        payload: Payload,
    ) -> Result<Box<dyn erased_serde::Deserializer<'static>>, PayloadConversionError>;
}

// TODO [rust-sdk-branch]: All prost things should be behind a compile flag

/// Wrapper for protobuf messages that implements [`TemporalSerializable`]/[`TemporalDeserializable`]
/// using `binary/protobuf` encoding.
pub struct ProstSerializable<T: prost::Message>(pub T);
impl<T> TemporalSerializable for ProstSerializable<T>
where
    T: prost::Message + Default + 'static,
{
    fn to_payload(&self, _: &SerializationContext<'_>) -> Result<Payload, PayloadConversionError> {
        let as_proto = prost::Message::encode_to_vec(&self.0);
        Ok(Payload {
            metadata: {
                let mut hm = HashMap::new();
                hm.insert("encoding".to_string(), b"binary/protobuf".to_vec());
                hm
            },
            data: as_proto,
            external_payloads: vec![],
        })
    }
}
impl<T> TemporalDeserializable for ProstSerializable<T>
where
    T: prost::Message + Default + 'static,
{
    fn from_payload(
        _: &SerializationContext<'_>,
        p: Payload,
    ) -> Result<Self, PayloadConversionError>
    where
        Self: Sized,
    {
        let encoding = p.metadata.get("encoding").map(|v| v.as_slice());
        if encoding != Some(b"binary/protobuf".as_slice()) {
            return Err(PayloadConversionError::WrongEncoding);
        }
        T::decode(p.data.as_slice())
            .map(ProstSerializable)
            .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))
    }
}

/// A payload converter that delegates to an ordered list of inner converters.
#[derive(Clone)]
pub struct CompositePayloadConverter {
    converters: Vec<PayloadConverter>,
}

impl Default for DataConverter {
    fn default() -> Self {
        Self::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            DefaultPayloadCodec,
        )
    }
}

impl FailureConverter for DefaultFailureConverter {
    fn to_failure(
        &self,
        error: Box<dyn std::error::Error + Send + Sync>,
        _payload_converter: &PayloadConverter,
        _context: &SerializationContextData,
    ) -> Failure {
        match error.downcast::<TemporalError>() {
            Ok(tf) => tf.into_failure(),
            Err(error) => {
                let mut failure = generic_error_to_application_failure(error.as_ref());
                failure.source = "RustSDK".into();
                failure
            }
        }
    }

    fn to_error(
        &self,
        failure: Failure,
        payload_converter: &PayloadConverter,
        _context: &SerializationContextData,
    ) -> TemporalError {
        TemporalError::from_failure(failure, payload_converter)
    }
}

fn generic_error_to_application_failure(error: &dyn std::error::Error) -> Failure {
    Failure {
        message: error.to_string(),
        failure_info: Some(FailureInfo::ApplicationFailureInfo(
            ApplicationFailureInfo::default(),
        )),
        cause: error
            .source()
            .map(|cause| Box::new(generic_error_to_application_failure(cause))),
        ..Default::default()
    }
}

impl PayloadCodec for DefaultPayloadCodec {
    fn encode(
        &self,
        _: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        async move { payloads }.boxed()
    }
    fn decode(
        &self,
        _: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        async move { payloads }.boxed()
    }
}

/// Represents multiple arguments for workflows/activities that accept more than one argument.
/// Use this when interoperating with other language SDKs that allow multiple arguments.
macro_rules! impl_multi_args {
    ($name:ident; $count:expr; $($idx:tt: $ty:ident),+) => {
        #[doc = concat!("Wrapper for ", stringify!($count), " typed arguments, enabling multi-arg serialization.")]
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $name<$($ty),+>($(pub $ty),+);

        impl<$($ty),+> TemporalSerializable for $name<$($ty),+>
        where
            $($ty: TemporalSerializable + 'static),+
        {
            fn to_payload(&self, _: &SerializationContext<'_>) -> Result<Payload, PayloadConversionError> {
                Err(PayloadConversionError::WrongEncoding)
            }
            fn to_payloads(
                &self,
                ctx: &SerializationContext<'_>,
            ) -> Result<Vec<Payload>, PayloadConversionError> {
                Ok(vec![$(ctx.converter.to_payload(ctx, &self.$idx)?),+])
            }
        }

        #[allow(non_snake_case)]
        impl<$($ty),+> From<($($ty),+,)> for $name<$($ty),+> {
            fn from(t: ($($ty),+,)) -> Self {
                $name($(t.$idx),+)
            }
        }

        impl<$($ty),+> TemporalDeserializable for $name<$($ty),+>
        where
            $($ty: TemporalDeserializable + 'static),+
        {
            fn from_payload(_: &SerializationContext<'_>, _: Payload) -> Result<Self, PayloadConversionError> {
                Err(PayloadConversionError::WrongEncoding)
            }
            fn from_payloads(
                ctx: &SerializationContext<'_>,
                payloads: Vec<Payload>,
            ) -> Result<Self, PayloadConversionError> {
                if payloads.len() != $count {
                    return Err(PayloadConversionError::WrongEncoding);
                }
                let mut iter = payloads.into_iter();
                Ok($name(
                    $(ctx.converter.from_payload::<$ty>(ctx, iter.next().unwrap())?),+
                ))
            }
        }
    };
}

impl_multi_args!(MultiArgs2; 2; 0: A, 1: B);
impl_multi_args!(MultiArgs3; 3; 0: A, 1: B, 2: C);
impl_multi_args!(MultiArgs4; 4; 0: A, 1: B, 2: C, 3: D);
impl_multi_args!(MultiArgs5; 5; 0: A, 1: B, 2: C, 3: D, 4: E);
impl_multi_args!(MultiArgs6; 6; 0: A, 1: B, 2: C, 3: D, 4: E, 5: F);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protos::temporal::api::failure::v1::{
        ActivityFailureInfo, ApplicationFailureInfo, CanceledFailureInfo, ServerFailureInfo,
        TerminatedFailureInfo, TimeoutFailureInfo, failure::FailureInfo,
    };
    use assert_matches::assert_matches;
    use rstest::rstest;
    use std::error::Error as _;

    #[test]
    fn test_empty_payloads_as_unit_type() {
        let converter = PayloadConverter::default();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };

        let empty_payloads: Vec<Payload> = vec![];
        let result: Result<(), _> = converter.from_payloads(&ctx, empty_payloads);

        assert!(result.is_ok(), "Empty payloads should deserialize as ()");
    }

    #[test]
    fn test_unit_type_roundtrip_serde() {
        let converter = PayloadConverter::serde_json();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };

        let payloads = converter.to_payloads(&ctx, &()).unwrap();
        assert!(payloads.is_empty());

        let result: () = converter.from_payloads(&ctx, payloads).unwrap();
        assert_eq!(result, ());
    }

    #[test]
    fn test_unit_composite_roundtrip() {
        let converter = PayloadConverter::default();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };

        let payloads = converter.to_payloads(&ctx, &()).unwrap();
        assert!(payloads.is_empty());

        let result: () = converter.from_payloads(&ctx, payloads).unwrap();
        assert_eq!(result, ());
    }

    #[test]
    fn test_unit_to_payload_roundtrip() {
        let converter = PayloadConverter::default();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };

        let mut payloads = vec![converter.to_payload(&ctx, &()).unwrap()];
        assert!(is_unit_payloads(&payloads));
        let result: () = converter
            .from_payload(&ctx, payloads.pop().unwrap())
            .unwrap();
        assert_eq!(result, ());
    }

    #[test]
    fn test_unit_use_wrappers_returns_wrong_encoding() {
        let converter = PayloadConverter::UseWrappers;
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };

        let result = converter.to_payloads(&ctx, &());
        assert!(
            matches!(result, Err(PayloadConversionError::WrongEncoding)),
            "{result:?}"
        );
    }

    #[test]
    fn multi_args_round_trip() {
        let converter = PayloadConverter::default();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };

        let args = MultiArgs2("hello".to_string(), 42i32);
        let payloads = converter.to_payloads(&ctx, &args).unwrap();
        assert_eq!(payloads.len(), 2);

        let result: MultiArgs2<String, i32> = converter.from_payloads(&ctx, payloads).unwrap();
        assert_eq!(result, args);
    }

    #[test]
    fn multi_args_from_tuple() {
        let args: MultiArgs2<String, i32> = ("hello".to_string(), 42i32).into();
        assert_eq!(args, MultiArgs2("hello".to_string(), 42));
    }

    #[test]
    fn plain_error_becomes_application_failure() {
        let err: Box<dyn std::error::Error + Send + Sync> =
            "something went wrong".to_string().into();
        let failure = DefaultFailureConverter.to_failure(
            err,
            &PayloadConverter::default(),
            &SerializationContextData::Workflow,
        );

        assert_eq!(failure.message, "something went wrong");
        assert_matches!(
            failure.failure_info,
            Some(FailureInfo::ApplicationFailureInfo(_))
        );
    }

    #[test]
    fn source_field_is_set() {
        let err: Box<dyn std::error::Error + Send + Sync> = "test".to_string().into();
        let failure = DefaultFailureConverter.to_failure(
            err,
            &PayloadConverter::default(),
            &SerializationContextData::Workflow,
        );

        assert_eq!(failure.source, "RustSDK");
    }

    #[rstest]
    #[case::application(
        "app error",
        FailureInfo::ApplicationFailureInfo(ApplicationFailureInfo {
            r#type: "MyError".into(),
            non_retryable: true,
            ..Default::default()
        })
    )]
    #[case::timeout(
        "timed out",
        FailureInfo::TimeoutFailureInfo(TimeoutFailureInfo {
            timeout_type: 1,
            ..Default::default()
        })
    )]
    #[case::canceled(
        "canceled",
        FailureInfo::CanceledFailureInfo(CanceledFailureInfo::default())
    )]
    #[case::terminated(
        "terminated",
        FailureInfo::TerminatedFailureInfo(TerminatedFailureInfo {})
    )]
    #[case::server(
        "server error",
        FailureInfo::ServerFailureInfo(ServerFailureInfo { non_retryable: true })
    )]
    fn failure_type_round_trips(#[case] message: &str, #[case] info: FailureInfo) {
        let converter = DefaultFailureConverter;
        let pc = PayloadConverter::default();
        let ctx = SerializationContextData::Workflow;

        let original = Failure {
            message: message.into(),
            failure_info: Some(info.clone()),
            ..Default::default()
        };

        let error = converter.to_error(original.clone(), &pc, &ctx);
        assert_eq!(error.to_string(), message);

        let round_tripped = converter.to_failure(Box::new(error), &pc, &ctx);
        assert_eq!(round_tripped.failure_info, Some(info));
        assert_eq!(round_tripped.message, original.message);
    }

    // -- cause chain --

    #[test]
    fn cause_chain_is_preserved() {
        let inner = Failure {
            message: "root cause".into(),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo::default(),
            )),
            ..Default::default()
        };
        let outer = Failure {
            message: "outer error".into(),
            cause: Some(Box::new(inner)),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo::default(),
            )),
            ..Default::default()
        };

        let error = DefaultFailureConverter.to_error(
            outer,
            &PayloadConverter::default(),
            &SerializationContextData::Workflow,
        );

        let source = error.source().expect("error should have a source");
        assert_eq!(source.to_string(), "root cause");
    }

    #[derive(Debug, thiserror::Error)]
    #[error("inner cause")]
    struct InnerCause;

    #[derive(Debug, thiserror::Error)]
    #[error("outer wrapper")]
    struct OuterCause {
        #[source]
        source: InnerCause,
    }

    #[test]
    fn plain_error_source_chain_is_preserved_in_failure() {
        let pc = PayloadConverter::default();
        let ctx = SerializationContextData::Workflow;
        let error = OuterCause { source: InnerCause };

        let failure = DefaultFailureConverter.to_failure(Box::new(error), &pc, &ctx);

        assert_eq!(failure.message, "outer wrapper");
        assert_matches!(
            failure.failure_info,
            Some(FailureInfo::ApplicationFailureInfo(_))
        );
        let cause = failure
            .cause
            .as_ref()
            .expect("failure should preserve causes");
        assert_eq!(cause.message, "inner cause");
        assert_matches!(
            cause.failure_info,
            Some(FailureInfo::ApplicationFailureInfo(_))
        );

        let round_tripped = DefaultFailureConverter.to_error(failure, &pc, &ctx);
        let source = round_tripped.source().expect("error should have a source");
        assert_eq!(source.to_string(), "inner cause");
    }

    #[test]
    fn deeply_nested_cause_chain() {
        let pc = PayloadConverter::default();
        let ctx = SerializationContextData::Workflow;

        let level0 = Failure {
            message: "level0".into(),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo::default(),
            )),
            ..Default::default()
        };
        let level1 = Failure {
            message: "level1".into(),
            cause: Some(Box::new(level0)),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo::default(),
            )),
            ..Default::default()
        };
        let level2 = Failure {
            message: "level2".into(),
            cause: Some(Box::new(level1)),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo::default(),
            )),
            ..Default::default()
        };

        let error = DefaultFailureConverter.to_error(level2, &pc, &ctx);

        let e1 = error.source().expect("should have level1");
        assert_eq!(e1.to_string(), "level1");
        let e0 = e1.source().expect("should have level0");
        assert_eq!(e0.to_string(), "level0");
    }

    // -- cross-SDK --

    #[test]
    fn cross_sdk_failure_deserializes() {
        let foreign = Failure {
            message: "something failed in TypeScript".into(),
            source: "TypeScriptSDK".into(),
            stack_trace: "at someFunction (file.ts:10:5)".into(),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo {
                    r#type: "Error".into(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        let error = DefaultFailureConverter.to_error(
            foreign,
            &PayloadConverter::default(),
            &SerializationContextData::Workflow,
        );
        assert_eq!(error.to_string(), "something failed in TypeScript");

        assert_eq!(error.stack_trace(), Some("at someFunction (file.ts:10:5)"));
    }

    #[test]
    fn failure_with_no_info_deserializes() {
        let bare = Failure {
            message: "bare failure".into(),
            ..Default::default()
        };

        let error = DefaultFailureConverter.to_error(
            bare,
            &PayloadConverter::default(),
            &SerializationContextData::Workflow,
        );
        assert_eq!(error.to_string(), "bare failure");
    }

    struct UpperCaseFailureConverter;
    impl FailureConverter for UpperCaseFailureConverter {
        fn to_failure(
            &self,
            error: Box<dyn std::error::Error + Send + Sync>,
            _: &PayloadConverter,
            _: &SerializationContextData,
        ) -> Failure {
            Failure {
                message: error.to_string().to_uppercase(),
                failure_info: Some(FailureInfo::ApplicationFailureInfo(
                    ApplicationFailureInfo::default(),
                )),
                ..Default::default()
            }
        }

        fn to_error(
            &self,
            failure: Failure,
            _: &PayloadConverter,
            _: &SerializationContextData,
        ) -> TemporalError {
            TemporalError::Other(failure.message.to_lowercase().into())
        }
    }

    #[test]
    fn custom_failure_converter_is_used() {
        let custom = UpperCaseFailureConverter;
        let pc = PayloadConverter::default();
        let ctx = SerializationContextData::Workflow;

        let err: Box<dyn std::error::Error + Send + Sync> = "hello world".to_string().into();
        let failure = custom.to_failure(err, &pc, &ctx);
        assert_eq!(failure.message, "HELLO WORLD");

        let error = custom.to_error(failure, &pc, &ctx);
        assert_eq!(error.to_string(), "hello world");
    }

    #[test]
    fn encoded_attributes_hides_message_and_stack_trace() {
        let encoded_msg = serde_json::to_vec(&serde_json::json!({
            "message": "secret error",
            "stack_trace": "at secret_fn (secret.rs:42)"
        }))
        .unwrap();

        let failure = Failure {
            message: "Encoded failure".into(),
            stack_trace: String::new(),
            encoded_attributes: Some(Payload {
                metadata: {
                    let mut hm = HashMap::new();
                    hm.insert("encoding".to_string(), b"json/plain".to_vec());
                    hm
                },
                data: encoded_msg,
                external_payloads: vec![],
            }),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo::default(),
            )),
            ..Default::default()
        };

        let error = DefaultFailureConverter.to_error(
            failure,
            &PayloadConverter::default(),
            &SerializationContextData::Workflow,
        );

        assert_eq!(error.to_string(), "secret error");
    }

    #[test]
    fn non_json_encoded_attributes_falls_back_to_message() {
        let failure = Failure {
            message: "fallback message".into(),
            encoded_attributes: Some(Payload {
                metadata: {
                    let mut hm = HashMap::new();
                    hm.insert("encoding".to_string(), b"binary/protobuf".to_vec());
                    hm
                },
                data: vec![0xFF, 0xFE, 0x00],
                external_payloads: vec![],
            }),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo::default(),
            )),
            ..Default::default()
        };

        let error = DefaultFailureConverter.to_error(
            failure,
            &PayloadConverter::default(),
            &SerializationContextData::Workflow,
        );

        assert_eq!(error.to_string(), "fallback message");
    }

    #[test]
    fn stack_trace_round_trips() {
        let converter = DefaultFailureConverter;
        let pc = PayloadConverter::default();
        let ctx = SerializationContextData::Workflow;

        let original = Failure {
            message: "oops".into(),
            stack_trace: "at my_fn (lib.rs:42)".into(),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo::default(),
            )),
            ..Default::default()
        };

        let error = converter.to_error(original, &pc, &ctx);
        assert_eq!(error.stack_trace(), Some("at my_fn (lib.rs:42)"));

        let round_tripped = converter.to_failure(Box::new(error), &pc, &ctx);
        assert_eq!(round_tripped.stack_trace, "at my_fn (lib.rs:42)");
    }

    /// A codec that XOR-encodes payload data, used to verify that
    /// `DataConverter::to_error`/`to_failure` apply the codec to failure payloads.
    struct XorFailureCodec(u8);
    impl PayloadCodec for XorFailureCodec {
        fn encode(
            &self,
            _: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Vec<Payload>> {
            let key = self.0;
            async move {
                payloads
                    .into_iter()
                    .map(|mut p| {
                        p.data.iter_mut().for_each(|b| *b ^= key);
                        p
                    })
                    .collect()
            }
            .boxed()
        }
        fn decode(
            &self,
            _: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Vec<Payload>> {
            // XOR is its own inverse
            let key = self.0;
            async move {
                payloads
                    .into_iter()
                    .map(|mut p| {
                        p.data.iter_mut().for_each(|b| *b ^= key);
                        p
                    })
                    .collect()
            }
            .boxed()
        }
    }

    struct ContextAwareFailureCodec;
    impl PayloadCodec for ContextAwareFailureCodec {
        fn encode(
            &self,
            _: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Vec<Payload>> {
            async move { payloads }.boxed()
        }

        fn decode(
            &self,
            context: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Vec<Payload>> {
            let prefix = match context {
                SerializationContextData::Workflow => b"wf:".as_slice(),
                SerializationContextData::Activity => b"act:".as_slice(),
                SerializationContextData::Nexus => b"nex:".as_slice(),
                SerializationContextData::None => b"none:".as_slice(),
            }
            .to_vec();

            async move {
                payloads
                    .into_iter()
                    .map(|mut payload| {
                        if payload.data.starts_with(&prefix) {
                            payload.data.drain(..prefix.len());
                        }
                        payload
                    })
                    .collect()
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn decode_failure_applies_codec_to_detail_payloads() {
        let dc = DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            XorFailureCodec(0xAB),
        );
        let ctx = SerializationContextData::Workflow;

        // Build a Failure proto with XOR-encoded detail payloads (simulating
        // what the server would send after the send-path PayloadVisitable
        // visitor encoded them).
        let plaintext = b"\"some detail\"".to_vec();
        let encoded_data: Vec<u8> = plaintext.iter().map(|b| b ^ 0xAB).collect();
        let encoded_payload = Payload {
            metadata: {
                let mut hm = HashMap::new();
                hm.insert("encoding".to_string(), b"json/plain".to_vec());
                hm
            },
            data: encoded_data,
            external_payloads: vec![],
        };
        let failure = Failure {
            message: "test".into(),
            failure_info: Some(FailureInfo::ApplicationFailureInfo(
                ApplicationFailureInfo {
                    r#type: "TestError".into(),
                    details: Some(Payloads {
                        payloads: vec![encoded_payload],
                    }),
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        // decode_failure should decode the XOR'd payloads back to plaintext
        let error = dc.decode_failure(failure, &ctx).await;
        match &error {
            TemporalError::Application { details, .. } => {
                let payloads = details.as_ref().unwrap();
                assert_eq!(
                    payloads.payloads[0].data, plaintext,
                    "decoded detail payload should match original plaintext"
                );
            }
            other => panic!("expected Application, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn decode_failure_uses_nested_activity_context_for_activity_causes() {
        let dc = DataConverter::new(
            PayloadConverter::default(),
            DefaultFailureConverter,
            ContextAwareFailureCodec,
        );
        let plaintext = b"\"activity detail\"".to_vec();
        let mut encoded_data = b"act:".to_vec();
        encoded_data.extend_from_slice(&plaintext);
        let failure = Failure {
            message: "Activity task failed".into(),
            cause: Some(Box::new(Failure {
                message: "application failure".into(),
                failure_info: Some(FailureInfo::ApplicationFailureInfo(
                    ApplicationFailureInfo {
                        r#type: "TestError".into(),
                        details: Some(Payloads {
                            payloads: vec![Payload {
                                metadata: {
                                    let mut hm = HashMap::new();
                                    hm.insert("encoding".to_string(), b"json/plain".to_vec());
                                    hm
                                },
                                data: encoded_data,
                                external_payloads: vec![],
                            }],
                        }),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            })),
            failure_info: Some(FailureInfo::ActivityFailureInfo(ActivityFailureInfo {
                activity_type: Some(crate::protos::temporal::api::common::v1::ActivityType {
                    name: "test-activity".into(),
                }),
                activity_id: "activity-id".into(),
                ..Default::default()
            })),
            ..Default::default()
        };

        let error = dc
            .decode_failure(failure, &SerializationContextData::Workflow)
            .await;

        match error {
            TemporalError::Activity {
                cause: Some(cause), ..
            } => match cause.as_ref() {
                TemporalError::Application {
                    details: Some(details),
                    ..
                } => {
                    assert_eq!(details.payloads[0].data, plaintext);
                }
                other => panic!("expected Application cause, got {other:?}"),
            },
            other => panic!("expected Activity, got {other:?}"),
        }
    }

    /// Mimics the proto structure produced by the local activity state machine
    /// when a local activity is cancelled via TryCancel. The outer failure has
    /// `ActivityFailureInfo` and the cause has `CanceledFailureInfo`.
    #[test]
    fn la_cancel_produces_activity_with_cancelled_cause() {
        let converter = DefaultFailureConverter;
        let pc = PayloadConverter::default();
        let ctx = SerializationContextData::Workflow;

        // Inner failure: what Cancellation::from_details(None) produces
        let cancel_cause = Failure {
            message: "Activity cancelled".into(),
            failure_info: Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
                details: None,
            })),
            ..Default::default()
        };

        // Outer failure: what wrap_fail! produces around the cancel cause
        let wrapped = Failure {
            message: "Local Activity cancelled".into(),
            cause: Some(Box::new(cancel_cause)),
            failure_info: Some(FailureInfo::ActivityFailureInfo(ActivityFailureInfo {
                activity_type: Some(crate::protos::temporal::api::common::v1::ActivityType {
                    name: "echo".into(),
                }),
                activity_id: "1".into(),
                retry_state: RetryState::CancelRequested.into(),
                ..Default::default()
            })),
            ..Default::default()
        };

        let te = converter.to_error(wrapped, &pc, &ctx);

        // Should be Activity { cause: Some(Cancelled { .. }) }
        assert_matches!(&te, TemporalError::Activity {
            message,
            activity_type,
            cause: Some(cause),
            ..
        } => {
            assert_eq!(message, "Local Activity cancelled");
            assert_eq!(activity_type, "echo");
            assert_matches!(cause.as_ref(), TemporalError::Cancelled { message, .. } => {
                assert_eq!(message, "Activity cancelled");
            });
        });
    }

    /// `Cancellation::from_details(None)` produces a Failure with
    /// CanceledFailureInfo, which correctly converts to TemporalError::Cancelled.
    #[test]
    fn cancellation_from_details_produces_cancelled() {
        let converter = DefaultFailureConverter;
        let pc = PayloadConverter::default();
        let ctx = SerializationContextData::Workflow;

        let cancel = Failure {
            message: "Activity cancelled".into(),
            failure_info: Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo {
                details: None,
            })),
            ..Default::default()
        };
        let te = converter.to_error(cancel, &pc, &ctx);

        assert_matches!(&te, TemporalError::Cancelled { message, .. } => {
            assert_eq!(message, "Activity cancelled");
        });
    }
}
