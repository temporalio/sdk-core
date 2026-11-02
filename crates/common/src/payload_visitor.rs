//! Payload visitor infrastructure for applying codecs to proto messages.
//!
//! This module provides a visitor pattern for traversing proto messages and transforming
//! payload fields. It's used to apply `PayloadCodec` encode/decode operations at the
//! boundary between the SDK and Core.

use crate::{
    data_converters::{PayloadCodec, PayloadConversionError, SerializationContextData},
    protos::temporal::api::common::v1::{Payload, Payloads},
};
use futures::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture, stream};
use std::num::NonZeroUsize;

/// Represents a payload field in a proto message.
/// Payloads within the same field may be processed together by the codec.
pub struct PayloadField<'a> {
    /// The fully-qualified field path (e.g.,
    /// `coresdk.workflow_commands.ScheduleActivity.arguments`)
    pub path: &'static str,
    /// The payload data
    pub data: PayloadFieldData<'a>,
}

/// The payload data within a field, varying by field type.
pub enum PayloadFieldData<'a> {
    /// A singular [Payload] field
    Single(&'a mut Payload),
    /// A repeated [Payload] field
    Repeated(&'a mut Vec<Payload>),
    /// A [Payloads] message field
    Payloads(&'a mut Payloads),
}

/// Async visitor for transforming payload fields.
///
/// Takes `&self` because several fields of one message may be visited at once.
pub trait AsyncPayloadVisitor {
    /// Visit a payload field, potentially transforming it. Returning an error stops traversal.
    fn visit<'a>(
        &'a self,
        field: PayloadField<'a>,
    ) -> BoxFuture<'a, Result<(), PayloadConversionError>>;
}

/// Trait for messages that contain Payload fields (directly or transitively).
/// Generated via codegen for all relevant proto message types.
pub trait PayloadVisitable: Send {
    /// Append every payload field in this message, in traversal order.
    fn payload_fields_mut<'a>(&'a mut self, fields: &mut Vec<PayloadField<'a>>);

    /// Visit all payload fields in this message, running at most `concurrency` visits at once.
    /// The visitor is called once per field, receiving the field's payload(s).
    /// Traversal stops and returns the first visitor error.
    fn visit_payloads_mut<'a>(
        &'a mut self,
        visitor: &'a (dyn AsyncPayloadVisitor + Sync),
        concurrency: NonZeroUsize,
    ) -> BoxFuture<'a, Result<(), PayloadConversionError>> {
        let mut fields = Vec::new();
        self.payload_fields_mut(&mut fields);
        stream::iter(fields)
            .map(Ok::<_, PayloadConversionError>)
            .try_for_each_concurrent(concurrency.get(), |field| visitor.visit(field))
            .boxed()
    }
}

/// Check if a field path represents search attributes that must not be transformed.
/// Search attributes must remain server-readable for indexing.
fn is_search_attributes_path(path: &str) -> bool {
    // All search attributes go through the SearchAttributes message which has indexed_fields
    path.contains("SearchAttributes.indexed_fields")
}

/// Whether a field at this path may be transformed: encoded, decoded, or offloaded.
pub(crate) fn should_transform(path: &str) -> bool {
    !is_search_attributes_path(path)
}

/// Implement [`AsyncPayloadVisitor`] for a visitor that applies one batch operation to every
/// transformable field, given `|visitor, payloads| body`. The body takes the field's payloads as an
/// owned `Vec<Payload>` and evaluates to the payloads that replace them.
macro_rules! payload_visitor {
    ($visitor:ty, |$this:ident, $payloads:ident| $body:expr) => {
        impl $crate::payload_visitor::AsyncPayloadVisitor for $visitor {
            fn visit<'a>(
                &'a self,
                field: $crate::payload_visitor::PayloadField<'a>,
            ) -> ::futures::future::BoxFuture<
                'a,
                Result<(), $crate::data_converters::PayloadConversionError>,
            > {
                Box::pin(async move {
                    if !$crate::payload_visitor::should_transform(field.path) {
                        return Ok(());
                    }
                    let $this = self;
                    match field.data {
                        $crate::payload_visitor::PayloadFieldData::Single(payload) => {
                            let $payloads = vec![::std::mem::take(payload)];
                            if let Some(p) = $body.into_iter().next() {
                                *payload = p;
                            }
                        }
                        $crate::payload_visitor::PayloadFieldData::Repeated(payloads) => {
                            let $payloads = ::std::mem::take(payloads);
                            *payloads = $body;
                        }
                        $crate::payload_visitor::PayloadFieldData::Payloads(message) => {
                            let $payloads = ::std::mem::take(&mut message.payloads);
                            message.payloads = $body;
                        }
                    }
                    Ok(())
                })
            }
        }
    };
}
pub(crate) use payload_visitor;

/// Visitor that encodes payloads using a codec.
pub struct EncodeVisitor<'a> {
    codec: &'a (dyn PayloadCodec + Send + Sync),
    context: &'a SerializationContextData,
}

payload_visitor!(EncodeVisitor<'_>, |this, payloads| {
    this.codec.encode(this.context, payloads).await?
});

/// Visitor that decodes payloads using a codec.
pub struct DecodeVisitor<'a> {
    codec: &'a (dyn PayloadCodec + Send + Sync),
    context: &'a SerializationContextData,
}

payload_visitor!(DecodeVisitor<'_>, |this, payloads| {
    this.codec.decode(this.context, payloads).await?
});

/// Encode all payloads in a message using the given codec, running at most `concurrency` codec
/// calls at once.
pub async fn encode_payloads<M: PayloadVisitable + Send>(
    msg: &mut M,
    codec: &(dyn PayloadCodec + Send + Sync),
    context: &SerializationContextData,
    concurrency: NonZeroUsize,
) -> Result<(), PayloadConversionError> {
    let visitor = EncodeVisitor { codec, context };
    msg.visit_payloads_mut(&visitor, concurrency).await
}

/// Decode all payloads in a message using the given codec, running at most `concurrency` codec
/// calls at once.
pub async fn decode_payloads<M: PayloadVisitable + Send>(
    msg: &mut M,
    codec: &(dyn PayloadCodec + Send + Sync),
    context: &SerializationContextData,
    concurrency: NonZeroUsize,
) -> Result<(), PayloadConversionError> {
    let visitor = DecodeVisitor { codec, context };
    msg.visit_payloads_mut(&visitor, concurrency).await
}

// Manual impl for Payload - visits itself as a single payload
impl PayloadVisitable for Payload {
    fn payload_fields_mut<'a>(&'a mut self, fields: &mut Vec<PayloadField<'a>>) {
        fields.push(PayloadField {
            path: "temporal.api.common.v1.Payload",
            data: PayloadFieldData::Single(self),
        });
    }
}

// Manual impl for Payloads - visits itself as a Payloads field
impl PayloadVisitable for Payloads {
    fn payload_fields_mut<'a>(&'a mut self, fields: &mut Vec<PayloadField<'a>>) {
        fields.push(PayloadField {
            path: "temporal.api.common.v1.Payloads",
            data: PayloadFieldData::Payloads(self),
        });
    }
}

// Include the generated PayloadVisitable implementations
include!(concat!(env!("OUT_DIR"), "/payload_visitor_impl.rs"));

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        data_converters::WorkflowSerializationContext,
        protos::{
            coresdk::{
                activity_result::{
                    ActivityResolution, Success, activity_resolution::Status as ActivityStatus,
                },
                workflow_activation::{
                    InitializeWorkflow, ResolveActivity, WorkflowActivation, WorkflowActivationJob,
                    workflow_activation_job::Variant,
                },
                workflow_commands::{
                    ContinueAsNewWorkflowExecution, ScheduleActivity, StartChildWorkflowExecution,
                    UpsertWorkflowSearchAttributes, WorkflowCommand,
                    workflow_command::Variant as CmdVariant,
                },
                workflow_completion::{
                    WorkflowActivationCompletion, workflow_activation_completion::Status,
                },
            },
            temporal::api::{
                common::v1::{Memo, SearchAttributes},
                failure::v1::failure::FailureInfo,
                workflow::v1::WorkflowExecutionInfo,
                workflowservice::v1::DescribeWorkflowExecutionResponse,
            },
        },
    };
    use futures::FutureExt;
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use temporalio_common_wasm::{
        data_converters::{DefaultFailureConverter, FailureConverter, PayloadConverter},
        error::{ApplicationFailure, OutgoingError, OutgoingWorkflowError},
    };

    struct MarkingCodec;
    impl PayloadCodec for MarkingCodec {
        fn encode(
            &self,
            _: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            async move {
                Ok(payloads
                    .into_iter()
                    .map(|mut p| {
                        p.metadata.insert("encoded".to_string(), b"true".to_vec());
                        p
                    })
                    .collect())
            }
            .boxed()
        }

        fn decode(
            &self,
            _: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            async move {
                Ok(payloads
                    .into_iter()
                    .map(|mut p| {
                        p.metadata.insert("decoded".to_string(), b"true".to_vec());
                        p
                    })
                    .collect())
            }
            .boxed()
        }
    }

    #[derive(Default)]
    struct FailingCodec {
        encode_calls: AtomicUsize,
        decode_calls: AtomicUsize,
    }

    impl PayloadCodec for FailingCodec {
        fn encode(
            &self,
            _: &SerializationContextData,
            _: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            self.encode_calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Err(PayloadConversionError::EncodingError(
                    "visitor encode failed".into(),
                ))
            }
            .boxed()
        }

        fn decode(
            &self,
            _: &SerializationContextData,
            _: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            self.decode_calls.fetch_add(1, Ordering::SeqCst);
            async move {
                Err(PayloadConversionError::EncodingError(
                    "visitor decode failed".into(),
                ))
            }
            .boxed()
        }
    }

    struct PathRecordingVisitor {
        visited_paths: Mutex<Vec<String>>,
    }
    impl PathRecordingVisitor {
        fn new() -> Self {
            Self {
                visited_paths: Mutex::new(Vec::new()),
            }
        }

        fn paths(&self) -> Vec<String> {
            self.visited_paths.lock().unwrap().clone()
        }
    }

    impl AsyncPayloadVisitor for PathRecordingVisitor {
        fn visit<'a>(
            &'a self,
            field: PayloadField<'a>,
        ) -> BoxFuture<'a, Result<(), PayloadConversionError>> {
            let path = field.path.to_string();
            self.visited_paths.lock().unwrap().push(path);
            async move { Ok(()) }.boxed()
        }
    }

    /// Marks payloads like [`MarkingCodec`] while recording the most encodes ever in flight. Each
    /// encode yields once, so every encode the visitor has started is in flight before any finishes.
    #[derive(Default)]
    struct InFlightCodec {
        in_flight: Arc<AtomicUsize>,
        max_in_flight: Arc<AtomicUsize>,
    }
    impl PayloadCodec for InFlightCodec {
        fn encode(
            &self,
            context: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            let in_flight = Arc::clone(&self.in_flight);
            let max_in_flight = Arc::clone(&self.max_in_flight);
            let marked = MarkingCodec.encode(context, payloads);
            async move {
                let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                max_in_flight.fetch_max(now, Ordering::SeqCst);
                tokio::task::yield_now().await;
                in_flight.fetch_sub(1, Ordering::SeqCst);
                marked.await
            }
            .boxed()
        }

        fn decode(
            &self,
            _: &SerializationContextData,
            payloads: Vec<Payload>,
        ) -> BoxFuture<'static, Result<Vec<Payload>, PayloadConversionError>> {
            async move { Ok(payloads) }.boxed()
        }
    }

    const ONE: NonZeroUsize = NonZeroUsize::MIN;

    fn make_payload(data: &str) -> Payload {
        Payload {
            metadata: HashMap::new(),
            data: data.as_bytes().to_vec(),
            external_payloads: vec![],
        }
    }

    fn is_encoded(p: &Payload) -> bool {
        p.metadata.contains_key("encoded")
    }

    fn is_decoded(p: &Payload) -> bool {
        p.metadata.contains_key("decoded")
    }

    #[tokio::test]
    async fn test_direct_visitor_records_paths() {
        let mut activation = WorkflowActivation {
            run_id: "test-run".to_string(),
            jobs: vec![WorkflowActivationJob {
                variant: Some(Variant::InitializeWorkflow(InitializeWorkflow {
                    workflow_type: "test-workflow".to_string(),
                    arguments: vec![make_payload("input1")],
                    headers: {
                        let mut h = HashMap::new();
                        h.insert("header-key".to_string(), make_payload("header-value"));
                        h
                    },
                    memo: Some(Memo {
                        fields: {
                            let mut m = HashMap::new();
                            m.insert("memo-key".to_string(), make_payload("memo-value"));
                            m
                        },
                    }),
                    ..Default::default()
                })),
            }],
            ..Default::default()
        };

        let visitor = PathRecordingVisitor::new();
        activation.visit_payloads_mut(&visitor, ONE).await.unwrap();

        let paths = visitor.paths();
        assert!(
            paths
                .iter()
                .any(|p| p.contains("InitializeWorkflow.arguments")),
            "should visit arguments, got: {:?}",
            paths
        );
        assert!(
            paths
                .iter()
                .any(|p| p.contains("InitializeWorkflow.headers")),
            "should visit headers, got: {:?}",
            paths
        );
        assert!(
            paths.iter().any(|p| p.contains("Memo.fields")),
            "should visit memo fields, got: {:?}",
            paths
        );
    }

    #[tokio::test]
    async fn test_encode_workflow_activation_completion_with_schedule_activity() {
        let mut completion = WorkflowActivationCompletion {
            run_id: "test-run".to_string(),
            status: Some(Status::Successful(
                crate::protos::coresdk::workflow_completion::Success {
                    commands: vec![WorkflowCommand {
                        variant: Some(CmdVariant::ScheduleActivity(ScheduleActivity {
                            activity_id: "act-1".to_string(),
                            activity_type: "test-activity".to_string(),
                            arguments: vec![make_payload("arg1"), make_payload("arg2")],
                            headers: {
                                let mut h = HashMap::new();
                                h.insert("header-key".to_string(), make_payload("header-value"));
                                h
                            },
                            ..Default::default()
                        })),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        encode_payloads(
            &mut completion,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
            ONE,
        )
        .await
        .unwrap();

        let status = completion.status.as_ref().unwrap();
        let Status::Successful(success) = status else {
            panic!("Expected successful status")
        };
        let cmd = &success.commands[0];
        let CmdVariant::ScheduleActivity(schedule) = cmd.variant.as_ref().unwrap() else {
            panic!("Expected ScheduleActivity")
        };

        assert!(is_encoded(&schedule.arguments[0]), "arg1 should be encoded");
        assert!(is_encoded(&schedule.arguments[1]), "arg2 should be encoded");
        assert!(
            is_encoded(schedule.headers.get("header-key").unwrap()),
            "header should be encoded"
        );
    }

    #[tokio::test]
    async fn test_decode_workflow_activation_with_initialize() {
        let mut activation = WorkflowActivation {
            run_id: "test-run".to_string(),
            jobs: vec![WorkflowActivationJob {
                variant: Some(Variant::InitializeWorkflow(InitializeWorkflow {
                    workflow_type: "test-workflow".to_string(),
                    arguments: vec![make_payload("input1"), make_payload("input2")],
                    headers: {
                        let mut h = HashMap::new();
                        h.insert("header-key".to_string(), make_payload("header-value"));
                        h
                    },
                    ..Default::default()
                })),
            }],
            ..Default::default()
        };

        decode_payloads(
            &mut activation,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
            ONE,
        )
        .await
        .unwrap();

        let job = &activation.jobs[0];
        let Variant::InitializeWorkflow(init) = job.variant.as_ref().unwrap() else {
            panic!("Expected InitializeWorkflow")
        };

        assert!(is_decoded(&init.arguments[0]), "arg1 should be decoded");
        assert!(is_decoded(&init.arguments[1]), "arg2 should be decoded");
        assert!(
            is_decoded(init.headers.get("header-key").unwrap()),
            "header should be decoded"
        );
    }

    #[tokio::test]
    async fn test_decode_workflow_activation_with_resolve_activity() {
        let mut activation = WorkflowActivation {
            run_id: "test-run".to_string(),
            jobs: vec![WorkflowActivationJob {
                variant: Some(Variant::ResolveActivity(ResolveActivity {
                    seq: 1,
                    result: Some(ActivityResolution {
                        status: Some(ActivityStatus::Completed(Success {
                            result: Some(make_payload("activity-result")),
                        })),
                    }),
                    ..Default::default()
                })),
            }],
            ..Default::default()
        };

        decode_payloads(
            &mut activation,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
            ONE,
        )
        .await
        .unwrap();

        let job = &activation.jobs[0];
        let Variant::ResolveActivity(resolve) = job.variant.as_ref().unwrap() else {
            panic!("Expected ResolveActivity")
        };
        let ActivityStatus::Completed(success) =
            resolve.result.as_ref().unwrap().status.as_ref().unwrap()
        else {
            panic!("Expected Completed status")
        };

        assert!(
            is_decoded(success.result.as_ref().unwrap()),
            "activity result should be decoded"
        );
    }

    #[tokio::test]
    async fn test_search_attributes_skipped_on_encode() {
        // Test that search attributes are NOT encoded (they must remain server-readable)
        let mut completion = WorkflowActivationCompletion {
            run_id: "test-run".to_string(),
            status: Some(Status::Successful(
                crate::protos::coresdk::workflow_completion::Success {
                    commands: vec![
                        // UpsertWorkflowSearchAttributes command
                        WorkflowCommand {
                            variant: Some(CmdVariant::UpsertWorkflowSearchAttributes(
                                UpsertWorkflowSearchAttributes {
                                    search_attributes: Some(SearchAttributes {
                                        indexed_fields: {
                                            let mut sa = HashMap::new();
                                            sa.insert(
                                                "CustomField".to_string(),
                                                make_payload("search-value"),
                                            );
                                            sa
                                        },
                                    }),
                                },
                            )),
                            ..Default::default()
                        },
                        // ContinueAsNewWorkflowExecution command
                        WorkflowCommand {
                            variant: Some(CmdVariant::ContinueAsNewWorkflowExecution(
                                ContinueAsNewWorkflowExecution {
                                    arguments: vec![make_payload("continue-arg")],
                                    search_attributes: Some(SearchAttributes {
                                        indexed_fields: {
                                            let mut sa = HashMap::new();
                                            sa.insert(
                                                "CustomField".to_string(),
                                                make_payload("continue-search-value"),
                                            );
                                            sa
                                        },
                                    }),
                                    ..Default::default()
                                },
                            )),
                            ..Default::default()
                        },
                        // StartChildWorkflowExecution command
                        WorkflowCommand {
                            variant: Some(CmdVariant::StartChildWorkflowExecution(
                                StartChildWorkflowExecution {
                                    seq: 1,
                                    workflow_type: "child-workflow".to_string(),
                                    input: vec![make_payload("child-arg")],
                                    search_attributes: Some(SearchAttributes {
                                        indexed_fields: {
                                            let mut sa = HashMap::new();
                                            sa.insert(
                                                "CustomField".to_string(),
                                                make_payload("child-search-value"),
                                            );
                                            sa
                                        },
                                    }),
                                    ..Default::default()
                                },
                            )),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            )),
            ..Default::default()
        };

        encode_payloads(
            &mut completion,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
            ONE,
        )
        .await
        .unwrap();

        let status = completion.status.as_ref().unwrap();
        let Status::Successful(success) = status else {
            panic!("Expected successful status")
        };

        // UpsertWorkflowSearchAttributes - search attributes should NOT be encoded
        let CmdVariant::UpsertWorkflowSearchAttributes(upsert) =
            success.commands[0].variant.as_ref().unwrap()
        else {
            panic!("Expected UpsertWorkflowSearchAttributes")
        };
        let sa = upsert.search_attributes.as_ref().unwrap();
        assert!(
            !is_encoded(sa.indexed_fields.get("CustomField").unwrap()),
            "search attributes should NOT be encoded"
        );

        // ContinueAsNewWorkflowExecution - arguments encoded, search attributes NOT
        let CmdVariant::ContinueAsNewWorkflowExecution(continue_as_new) =
            success.commands[1].variant.as_ref().unwrap()
        else {
            panic!("Expected ContinueAsNewWorkflowExecution")
        };
        assert!(
            is_encoded(&continue_as_new.arguments[0]),
            "arguments should be encoded"
        );
        let sa = continue_as_new.search_attributes.as_ref().unwrap();
        assert!(
            !is_encoded(sa.indexed_fields.get("CustomField").unwrap()),
            "search attributes should NOT be encoded"
        );

        // StartChildWorkflowExecution - input encoded, search attributes NOT
        let CmdVariant::StartChildWorkflowExecution(start_child) =
            success.commands[2].variant.as_ref().unwrap()
        else {
            panic!("Expected StartChildWorkflowExecution")
        };
        assert!(is_encoded(&start_child.input[0]), "input should be encoded");
        let sa = start_child.search_attributes.as_ref().unwrap();
        assert!(
            !is_encoded(sa.indexed_fields.get("CustomField").unwrap()),
            "search attributes should NOT be encoded"
        );
    }

    #[tokio::test]
    async fn test_search_attributes_skipped_on_decode() {
        let mut response = DescribeWorkflowExecutionResponse {
            workflow_execution_info: Some(WorkflowExecutionInfo {
                memo: Some(Memo {
                    fields: {
                        let mut memo = HashMap::new();
                        memo.insert("tracked".to_string(), make_payload("memo-value"));
                        memo
                    },
                }),
                search_attributes: Some(SearchAttributes {
                    indexed_fields: {
                        let mut sa = HashMap::new();
                        sa.insert("CustomField".to_string(), make_payload("search-value"));
                        sa
                    },
                }),
                ..Default::default()
            }),
            ..Default::default()
        };

        decode_payloads(
            &mut response,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
            ONE,
        )
        .await
        .unwrap();

        let info = response.workflow_execution_info.as_ref().unwrap();
        assert!(
            is_decoded(info.memo.as_ref().unwrap().fields.get("tracked").unwrap()),
            "memo should be decoded"
        );
        assert!(
            !is_decoded(
                info.search_attributes
                    .as_ref()
                    .unwrap()
                    .indexed_fields
                    .get("CustomField")
                    .unwrap()
            ),
            "search attributes should NOT be decoded"
        );
    }

    #[tokio::test]
    async fn test_encode_single_payload() {
        let mut payload = make_payload("test-data");

        encode_payloads(
            &mut payload,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
            ONE,
        )
        .await
        .unwrap();

        assert!(is_encoded(&payload), "single payload should be encoded");
    }

    #[tokio::test]
    async fn test_decode_single_payload() {
        let mut payload = make_payload("test-data");

        decode_payloads(
            &mut payload,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
            ONE,
        )
        .await
        .unwrap();

        assert!(is_decoded(&payload), "single payload should be decoded");
    }

    #[tokio::test]
    async fn test_encode_payloads_message() {
        let mut payloads = Payloads {
            payloads: vec![make_payload("p1"), make_payload("p2"), make_payload("p3")],
        };

        encode_payloads(
            &mut payloads,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
            ONE,
        )
        .await
        .unwrap();

        for (i, p) in payloads.payloads.iter().enumerate() {
            assert!(is_encoded(p), "payload {} should be encoded", i);
        }
    }

    #[tokio::test]
    async fn test_codec_errors_stop_subsequent_codec_invocations() {
        let codec = FailingCodec::default();
        let mut activation = WorkflowActivation {
            jobs: vec![WorkflowActivationJob {
                variant: Some(Variant::InitializeWorkflow(InitializeWorkflow {
                    arguments: vec![make_payload("input")],
                    headers: HashMap::from([("header".to_string(), make_payload("value"))]),
                    memo: Some(Memo {
                        fields: HashMap::from([("memo".to_string(), make_payload("memo-value"))]),
                    }),
                    ..Default::default()
                })),
            }],
            ..Default::default()
        };

        let err = decode_payloads(
            &mut activation,
            &codec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
            ONE,
        )
        .await
        .unwrap_err();

        assert_eq!(err.to_string(), "Encoding error: visitor decode failed");
        assert_eq!(codec.decode_calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_encode_failure_encodes_application_failure_details() {
        let mut failure = DefaultFailureConverter::default().to_failure(
            OutgoingError::Workflow(OutgoingWorkflowError::Application(Box::new(
                ApplicationFailure::builder(anyhow::anyhow!("app boom"))
                    .details(crate::data_converters::RawValue::new(vec![make_payload(
                        "detail",
                    )]))
                    .build(),
            ))),
            &PayloadConverter::default(),
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
        );

        encode_payloads(
            &mut failure,
            &MarkingCodec,
            &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
            ONE,
        )
        .await
        .unwrap();

        let Some(FailureInfo::ApplicationFailureInfo(info)) = failure.failure_info else {
            panic!("expected application failure info")
        };
        assert!(is_encoded(&info.details.unwrap().payloads[0]));
    }

    #[tokio::test]
    async fn fields_are_visited_concurrently_up_to_the_limit() {
        for limit in [1, 3] {
            let mut completion = WorkflowActivationCompletion::from_cmds(
                "run",
                (0..5)
                    .map(|i| {
                        CmdVariant::ScheduleActivity(ScheduleActivity {
                            seq: i,
                            arguments: vec![make_payload(&format!("arg-{i}"))],
                            ..Default::default()
                        })
                    })
                    .collect(),
            );
            let codec = InFlightCodec::default();

            encode_payloads(
                &mut completion,
                &codec,
                &SerializationContextData::Workflow(WorkflowSerializationContext::new()),
                NonZeroUsize::new(limit).unwrap(),
            )
            .await
            .unwrap();

            assert_eq!(codec.max_in_flight.load(Ordering::SeqCst), limit);
            let Some(Status::Successful(success)) = completion.status else {
                panic!("expected successful status")
            };
            // Visits finish out of order, so each result must still land in its own command.
            for (i, cmd) in success.commands.iter().enumerate() {
                let Some(CmdVariant::ScheduleActivity(schedule)) = &cmd.variant else {
                    panic!("expected ScheduleActivity")
                };
                assert!(is_encoded(&schedule.arguments[0]));
                assert_eq!(schedule.arguments[0].data, format!("arg-{i}").into_bytes());
            }
        }
    }
}
