use crate::common::{CoreWfStarter, TestWorker, get_integ_connection, integ_namespace};
use futures::{FutureExt, future::BoxFuture};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_client::{
    Client, ClientOptions, UntypedWorkflow, WorkflowGetResultError, WorkflowStartOptions,
};
use temporalio_common::{
    data_converters::{
        DataConverter, DefaultFailureConverter, FailureConverter, MultiArgs2, PayloadCodec,
        PayloadConversionError, PayloadConverter, RawValue, SerializationContext,
        SerializationContextData, TemporalDeserializable, TemporalError, TemporalSerializable,
    },
    protos::{
        DEFAULT_WORKFLOW_TYPE, canned_histories,
        temporal::api::{
            common::v1::Payload, enums::v1::WorkflowTaskFailedCause,
            failure::v1::failure::FailureInfo, history::v1::history_event::Attributes,
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, CancellableFuture, ChildWorkflowExecutionError, ChildWorkflowOptions,
    WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use temporalio_sdk_core::test_help::{
    MockPollCfg, build_mock_pollers, mock_worker, mock_worker_client,
};

#[derive(Clone, Debug)]
struct TrackedWrapper(TrackedValue);
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct TrackedValue {
    data: String,
    serialize_count: u32,
    deserialize_count: u32,
}

impl TrackedValue {
    fn new(data: String) -> Self {
        Self {
            data,
            serialize_count: 0,
            deserialize_count: 0,
        }
    }
}

impl TemporalSerializable for TrackedWrapper {
    fn to_payload(&self, _: &SerializationContext<'_>) -> Result<Payload, PayloadConversionError> {
        let mut wire = self.0.clone();
        wire.serialize_count += 1;
        let json = serde_json::to_vec(&wire)
            .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))?;
        Ok(Payload {
            metadata: {
                let mut hm = std::collections::HashMap::new();
                hm.insert("encoding".to_string(), b"json/plain".to_vec());
                hm
            },
            data: json,
            external_payloads: vec![],
        })
    }
}

impl TemporalDeserializable for TrackedWrapper {
    fn from_payload(
        _: &SerializationContext<'_>,
        payload: Payload,
    ) -> Result<Self, PayloadConversionError> {
        let wire: TrackedValue = serde_json::from_slice(&payload.data)
            .map_err(|e| PayloadConversionError::EncodingError(Box::new(e)))?;
        let mut val = wire.clone();
        val.deserialize_count += 1;
        Ok(TrackedWrapper(val))
    }
}

struct TestActivities;
#[activities]
impl TestActivities {
    #[activity]
    async fn process_tracked(
        _ctx: ActivityContext,
        mut input: TrackedWrapper,
    ) -> Result<TrackedWrapper, ActivityError> {
        input.0.data = format!("activity-processed:{}", input.0.data);
        Ok(input)
    }
}

#[workflow]
#[derive(Default)]
struct DataConverterTestWorkflow;
#[workflow_methods]
impl DataConverterTestWorkflow {
    #[run]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: TrackedWrapper,
    ) -> WorkflowResult<TrackedWrapper> {
        let output = ctx
            .start_activity(
                TestActivities::process_tracked,
                input,
                ActivityOptions {
                    start_to_close_timeout: Some(Duration::from_secs(5)),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        Ok(output)
    }
}

#[tokio::test]
async fn data_converter_tracks_serialization_points() {
    let wf_name = DataConverterTestWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(TestActivities);
    starter
        .sdk_config
        .register_workflow::<DataConverterTestWorkflow>();
    let mut worker = starter.worker().await;

    let input = TrackedValue::new("test-input".to_string());
    assert_eq!(input.serialize_count, 0);
    assert_eq!(input.deserialize_count, 0);

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            DataConverterTestWorkflow::run,
            TrackedWrapper(input),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let output = handle.get_result(Default::default()).await.unwrap().0;

    assert!(
        output.data.contains("activity-processed:test-input"),
        "Activity should have processed the input, got: {}",
        output.data
    );

    // Expected path:
    // 1. Client serializes input (serialize_count: 0 -> 1)
    // 2. Workflow deserializes input (deserialize_count: 0 -> 1)
    // 3. Workflow serializes for activity call (serialize_count: 1 -> 2)
    // 4. Activity deserializes input (deserialize_count: 1 -> 2)
    // 5. Activity serializes result (serialize_count: 2 -> 3)
    // 6. Workflow deserializes activity result (deserialize_count: 2 -> 3)
    // 7. Workflow serializes final result (serialize_count: 3 -> 4)
    // 8. Client deserializes result (deserialize_count: 3 -> 4)

    assert!(
        output.serialize_count == 4,
        "Value should have been serialized 4 times, was serialized {} times",
        output.serialize_count
    );
    assert!(
        output.deserialize_count == 4,
        "Value should have been deserialized 4 times, was deserialized {} times",
        output.deserialize_count
    );
}

#[workflow]
#[derive(Default)]
struct MultiArgs2Workflow;
#[workflow_methods]
impl MultiArgs2Workflow {
    #[run]
    async fn run(
        _ctx: &mut WorkflowContext<Self>,
        input: MultiArgs2<String, i32>,
    ) -> WorkflowResult<String> {
        Ok(format!("received: {} and {}", input.0, input.1))
    }
}

#[tokio::test]
async fn multi_args_serializes_as_multiple_payloads() {
    let wf_name = MultiArgs2Workflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_workflow::<MultiArgs2Workflow>();
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    let input = MultiArgs2("hello".to_string(), 42);

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            MultiArgs2Workflow::run,
            input,
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let output = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(output, "received: hello and 42");

    // Verify the workflow history contains multiple payloads in the input
    let client = starter.get_client().await;
    let events = client
        .get_workflow_handle::<UntypedWorkflow>(wf_name)
        .fetch_history(Default::default())
        .await
        .unwrap()
        .into_events();

    let workflow_started_event = events
        .iter()
        .find_map(|e| {
            if let Attributes::WorkflowExecutionStartedEventAttributes(attrs) =
                e.attributes.as_ref().unwrap()
            {
                Some(attrs)
            } else {
                None
            }
        })
        .expect("Should find WorkflowExecutionStarted event");

    let input_payloads = workflow_started_event
        .input
        .as_ref()
        .expect("Should have input payloads");

    assert_eq!(
        input_payloads.payloads.len(),
        2,
        "MultiArgs2<A, B> should produce 2 payloads, got {}",
        input_payloads.payloads.len()
    );

    // Verify the content of each payload
    let first_payload_data: String =
        serde_json::from_slice(&input_payloads.payloads[0].data).unwrap();
    assert_eq!(first_payload_data, "hello");

    let second_payload_data: i32 =
        serde_json::from_slice(&input_payloads.payloads[1].data).unwrap();
    assert_eq!(second_payload_data, 42);
}

#[workflow]
#[derive(Default)]
struct FailingWorkflow;
#[workflow_methods]
impl FailingWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>, message: String) -> WorkflowResult<String> {
        Err(temporalio_sdk::WorkflowTermination::Failed(
            anyhow::anyhow!("{}", message),
        ))
    }
}

#[tokio::test]
async fn failing_workflow_produces_failure_with_message() {
    let wf_name = FailingWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_workflow::<FailingWorkflow>();
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            FailingWorkflow::run,
            "intentional failure".to_string(),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let res = handle.get_result(Default::default()).await.unwrap_err();
    if let WorkflowGetResultError::Failed(ref te) = res {
        let message = te.message().unwrap();
        assert!(
            message.contains("intentional failure"),
            "failure message should contain the workflow error, got: {message}",
        );
    } else {
        panic!("expected Failed, got: {res:?}");
    }
}

#[workflow]
#[derive(Default)]
struct FailingActivityWorkflow;
#[workflow_methods]
impl FailingActivityWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, _input: String) -> WorkflowResult<String> {
        ctx.start_activity(
            FailingActivities::always_fail,
            "activity input".to_string(),
            ActivityOptions {
                start_to_close_timeout: Some(Duration::from_secs(5)),
                retry_policy: Some(
                    temporalio_common::protos::temporal::api::common::v1::RetryPolicy {
                        maximum_attempts: 1,
                        ..Default::default()
                    },
                ),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| {
            temporalio_sdk::WorkflowTermination::Failed(anyhow::anyhow!("activity failed: {}", e))
        })
    }
}

struct FailingActivities;
#[activities]
impl FailingActivities {
    #[activity]
    async fn always_fail(_ctx: ActivityContext, _input: String) -> Result<String, ActivityError> {
        Err(ActivityError::NonRetryable(
            "activity went boom".to_string().into(),
        ))
    }
}

#[tokio::test]
async fn activity_failure_propagates_through_workflow() {
    let wf_name = FailingActivityWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<FailingActivityWorkflow>();
    starter.sdk_config.register_activities(FailingActivities);
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            FailingActivityWorkflow::run,
            "trigger".to_string(),
            WorkflowStartOptions::new(task_queue, wf_name.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let res = handle.get_result(Default::default()).await.unwrap_err();
    if let WorkflowGetResultError::Failed(ref te) = res {
        let message = te.message().unwrap();
        assert!(
            message.contains("activity failed"),
            "workflow failure should mention the activity error, got: {message}",
        );
    } else {
        panic!("expected Failed, got: {res:?}");
    }
}

/// A codec that XORs payload data with a key and tracks encode/decode operations.
struct XorCodec {
    key: u8,
    encode_count: AtomicUsize,
    decode_count: AtomicUsize,
}

impl XorCodec {
    fn new(key: u8) -> Self {
        Self {
            key,
            encode_count: AtomicUsize::new(0),
            decode_count: AtomicUsize::new(0),
        }
    }

    fn encode_count(&self) -> usize {
        self.encode_count.load(Ordering::SeqCst)
    }

    fn decode_count(&self) -> usize {
        self.decode_count.load(Ordering::SeqCst)
    }
}

impl PayloadCodec for XorCodec {
    fn encode(
        &self,
        _context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        let count = payloads.len();
        eprintln!("XorCodec::encode called with {} payloads", count);
        self.encode_count.fetch_add(count, Ordering::SeqCst);
        let key = self.key;
        async move {
            payloads
                .into_iter()
                .map(|mut p| {
                    p.data = p.data.iter().map(|b| b ^ key).collect();
                    p.metadata.insert("xor_encoded".to_string(), vec![key]);
                    p
                })
                .collect()
        }
        .boxed()
    }

    fn decode(
        &self,
        _context: &SerializationContextData,
        payloads: Vec<Payload>,
    ) -> BoxFuture<'static, Vec<Payload>> {
        let count = payloads.len();
        eprintln!("XorCodec::decode called with {} payloads", count);
        self.decode_count.fetch_add(count, Ordering::SeqCst);
        let key = self.key;
        async move {
            payloads
                .into_iter()
                .map(|mut p| {
                    if p.metadata.remove("xor_encoded").is_some() {
                        p.data = p.data.iter().map(|b| b ^ key).collect();
                    }
                    p
                })
                .collect()
        }
        .boxed()
    }
}

#[tokio::test]
async fn codec_encodes_and_decodes_payloads() {
    let wf_name = DataConverterTestWorkflow::name();
    let codec = Arc::new(XorCodec::new(0x42));

    // Create a client with our custom codec
    let connection = get_integ_connection(None).await;
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter,
        codec.clone(),
    );
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(data_converter)
        .build();
    let client = Client::new(connection, client_opts).unwrap();

    let mut starter = CoreWfStarter::new_with_overrides(wf_name, None, Some(client));
    starter.sdk_config.register_activities(TestActivities);
    starter.sdk_config.task_types = WorkerTaskTypes::all();
    starter
        .sdk_config
        .register_workflow::<DataConverterTestWorkflow>();
    // Use task queue name as workflow ID to avoid collisions with parallel tests
    let wf_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let input = TrackedValue::new("codec-test".to_string());
    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            DataConverterTestWorkflow::run,
            TrackedWrapper(input),
            WorkflowStartOptions::new(task_queue, wf_id).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let output = handle.get_result(Default::default()).await.unwrap().0;

    // Verify the workflow processed correctly (activity echoed the data)
    assert!(
        output.data.contains("activity-processed:codec-test"),
        "Activity should have processed the input, got: {}",
        output.data
    );

    // Verify the codec was used for both encoding and decoding
    assert!(
        codec.encode_count() > 0,
        "Codec should have encoded payloads, but encode_count was 0"
    );
    assert!(
        codec.decode_count() > 0,
        "Codec should have decoded payloads, but decode_count was 0"
    );
}

/// Apply `f` to every failure in the cause chain.
fn walk_failure_chain(
    failure: &mut temporalio_common::protos::temporal::api::failure::v1::Failure,
    mut f: impl FnMut(&mut temporalio_common::protos::temporal::api::failure::v1::Failure),
) {
    let mut curr = Some(failure);
    while let Some(node) = curr {
        f(node);
        curr = node.cause.as_deref_mut();
    }
}

/// Custom failure converter that wraps the default, uppercasing outgoing
/// messages and lowercasing incoming ones.
struct UpperCaseFailureConverter;

impl temporalio_common::data_converters::FailureConverter for UpperCaseFailureConverter {
    fn to_failure(
        &self,
        error: Box<dyn std::error::Error + Send + Sync>,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> temporalio_common::protos::temporal::api::failure::v1::Failure {
        let mut failure = DefaultFailureConverter.to_failure(error, payload_converter, context);
        walk_failure_chain(&mut failure, |f| f.message = f.message.to_uppercase());
        failure
    }

    fn to_error(
        &self,
        mut failure: temporalio_common::protos::temporal::api::failure::v1::Failure,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> temporalio_common::data_converters::TemporalError {
        walk_failure_chain(&mut failure, |f| f.message = f.message.to_lowercase());
        DefaultFailureConverter.to_error(failure, payload_converter, context)
    }
}

struct PrefixFailureConverter {
    outgoing_prefix: &'static str,
    incoming_prefix: &'static str,
}

impl FailureConverter for PrefixFailureConverter {
    fn to_failure(
        &self,
        error: Box<dyn std::error::Error + Send + Sync>,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> temporalio_common::protos::temporal::api::failure::v1::Failure {
        let mut failure = DefaultFailureConverter.to_failure(error, payload_converter, context);
        if !self.outgoing_prefix.is_empty() {
            walk_failure_chain(&mut failure, |f| {
                f.message = format!("{}{}", self.outgoing_prefix, f.message);
            });
        }
        failure
    }

    fn to_error(
        &self,
        mut failure: temporalio_common::protos::temporal::api::failure::v1::Failure,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> TemporalError {
        if !self.incoming_prefix.is_empty() {
            walk_failure_chain(&mut failure, |f| {
                f.message = format!("{}{}", self.incoming_prefix, f.message);
            });
        }
        DefaultFailureConverter.to_error(failure, payload_converter, context)
    }
}

struct GenericFailureConverter;

impl FailureConverter for GenericFailureConverter {
    fn to_failure(
        &self,
        error: Box<dyn std::error::Error + Send + Sync>,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> temporalio_common::protos::temporal::api::failure::v1::Failure {
        let mut failure = DefaultFailureConverter.to_failure(error, payload_converter, context);
        failure.message = format!("generic:{}", failure.message);
        failure.failure_info = None;
        failure
    }

    fn to_error(
        &self,
        failure: temporalio_common::protos::temporal::api::failure::v1::Failure,
        payload_converter: &PayloadConverter,
        context: &SerializationContextData,
    ) -> TemporalError {
        DefaultFailureConverter.to_error(failure, payload_converter, context)
    }
}

#[workflow]
#[derive(Default)]
struct CustomConverterFailWorkflow;
#[workflow_methods]
impl CustomConverterFailWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>, message: String) -> WorkflowResult<String> {
        Err(temporalio_sdk::WorkflowTermination::Failed(
            anyhow::anyhow!("{}", message),
        ))
    }
}

#[tokio::test]
async fn custom_failure_converter_applied_to_workflow_failure() {
    let wf_name = CustomConverterFailWorkflow::name();

    let connection = get_integ_connection(None).await;
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        UpperCaseFailureConverter,
        temporalio_common::data_converters::DefaultPayloadCodec,
    );
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(data_converter)
        .build();
    let client = Client::new(connection, client_opts).unwrap();

    let mut starter = CoreWfStarter::new_with_overrides(wf_name, None, Some(client));
    starter
        .sdk_config
        .register_workflow::<CustomConverterFailWorkflow>();
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let wf_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            CustomConverterFailWorkflow::run,
            "should be uppercased".to_string(),
            WorkflowStartOptions::new(task_queue, wf_id.clone()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    // Fetch the raw failure from history to see what the worker actually sent.
    let client = starter.get_client().await;
    let events = client
        .get_workflow_handle::<UntypedWorkflow>(&wf_id)
        .fetch_history(Default::default())
        .await
        .unwrap()
        .into_events();

    let failed_attrs = events
        .iter()
        .find_map(|e| {
            if let Attributes::WorkflowExecutionFailedEventAttributes(attrs) =
                e.attributes.as_ref().unwrap()
            {
                Some(attrs)
            } else {
                None
            }
        })
        .expect("Should find WorkflowExecutionFailed event");

    let failure = failed_attrs.failure.as_ref().expect("should have failure");

    // The custom converter uppercases, so the server-side failure message must
    // be all-uppercase. Without the converter wired up, this would contain the
    // raw lowercase message.
    assert_eq!(
        failure.message, "SHOULD BE UPPERCASED",
        "Failure message on server should reflect custom converter (uppercase), got: {}",
        failure.message,
    );
}

#[workflow]
struct PanickingOnceWorkflow {
    did_panic: Arc<AtomicUsize>,
}

#[workflow_methods(factory_only)]
impl PanickingOnceWorkflow {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.timer(Duration::from_secs(1)).await;
        if ctx.state(|wf| wf.did_panic.fetch_add(1, Ordering::SeqCst)) == 0 {
            panic!("workflow panic marker");
        }
        Ok(())
    }
}

#[tokio::test]
async fn custom_failure_converter_applied_to_workflow_panic_failures() {
    let wf_id = "workflow-panic-failure-converter";
    let history = canned_histories::workflow_fails_with_failure_after_timer("1");
    let mock_client = mock_worker_client();
    let mut mock_cfg = MockPollCfg::from_resp_batches(wf_id, history, [1, 2, 2], mock_client);
    mock_cfg.using_rust_sdk = true;
    mock_cfg.num_expected_fails = 1;
    mock_cfg.expect_fail_wft_matcher = Box::new(|_, cause, failure| {
        *cause == WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
            && failure.as_ref().is_some_and(|failure| {
                failure.message
                    == "panic-converted:Workflow function panicked: workflow panic marker"
                    && matches!(
                        failure.failure_info,
                        Some(FailureInfo::ApplicationFailureInfo(_))
                    )
            })
    });

    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|cfg| {
        cfg.max_cached_workflows = 1;
        cfg.ignore_evicts_on_shutdown = false;
    });
    let core = mock_worker(mock);
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        PrefixFailureConverter {
            outgoing_prefix: "panic-converted:",
            incoming_prefix: "",
        },
        temporalio_common::data_converters::DefaultPayloadCodec,
    );
    let mut worker = TestWorker::new(temporalio_sdk::Worker::new_from_core(
        Arc::new(core),
        data_converter,
    ));

    let did_panic = Arc::new(AtomicUsize::new(0));
    let did_panic_clone = did_panic.clone();
    worker.register_workflow_with_factory(move || PanickingOnceWorkflow {
        did_panic: did_panic_clone.clone(),
    });
    worker
        .submit_wf(
            DEFAULT_WORKFLOW_TYPE,
            vec![],
            WorkflowStartOptions::new("fake_tq".to_owned(), wf_id.to_owned()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct FailWithDetailsWorkflow;
#[workflow_methods]
impl FailWithDetailsWorkflow {
    #[run]
    async fn run(_ctx: &mut WorkflowContext<Self>, _input: String) -> WorkflowResult<String> {
        use temporalio_common::{
            data_converters::TemporalError, protos::temporal::api::common::v1::Payloads,
        };

        // Build detail payloads with a known plaintext marker.
        let detail_payload = Payload {
            metadata: {
                let mut hm = std::collections::HashMap::new();
                hm.insert("encoding".to_string(), b"json/plain".to_vec());
                hm
            },
            data: b"\"detail-marker-plaintext\"".to_vec(),
            external_payloads: vec![],
        };

        let tf = TemporalError::Application {
            message: "fail with details".to_string(),
            stack_trace: String::new(),
            r#type: String::new(),
            non_retryable: false,
            details: Some(Payloads {
                payloads: vec![detail_payload],
            }),
            next_retry_delay: None,
            cause: None,
        };

        Err(temporalio_sdk::WorkflowTermination::Failed(tf.into()))
    }
}

#[tokio::test]
async fn codec_applied_to_outgoing_workflow_failure_payloads() {
    let wf_name = FailWithDetailsWorkflow::name();
    let codec = Arc::new(XorCodec::new(0x42));

    let connection = get_integ_connection(None).await;
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter,
        codec.clone(),
    );
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(data_converter)
        .build();
    let client = Client::new(connection, client_opts).unwrap();

    let mut starter = CoreWfStarter::new_with_overrides(wf_name, None, Some(client));
    starter
        .sdk_config
        .register_workflow::<FailWithDetailsWorkflow>();
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let wf_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            FailWithDetailsWorkflow::run,
            "trigger".to_string(),
            WorkflowStartOptions::new(task_queue, wf_id.clone()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    // Fetch raw history — the server stores whatever the worker sent, so if the
    // codec was applied the detail payloads will NOT contain our plaintext marker.
    let client = starter.get_client().await;
    let events = client
        .get_workflow_handle::<UntypedWorkflow>(&wf_id)
        .fetch_history(Default::default())
        .await
        .unwrap()
        .into_events();

    let failed_attrs = events
        .iter()
        .find_map(|e| {
            if let Attributes::WorkflowExecutionFailedEventAttributes(attrs) =
                e.attributes.as_ref().unwrap()
            {
                Some(attrs)
            } else {
                None
            }
        })
        .expect("Should find WorkflowExecutionFailed event");

    let failure = failed_attrs.failure.as_ref().expect("should have failure");

    // Dig into ApplicationFailureInfo to find the detail payloads.
    use temporalio_common::protos::temporal::api::failure::v1::failure::FailureInfo;
    let details = match failure.failure_info.as_ref() {
        Some(FailureInfo::ApplicationFailureInfo(info)) => info.details.as_ref(),
        other => panic!("Expected ApplicationFailureInfo, got: {:?}", other),
    };

    let detail_payloads = details.expect("should have detail payloads");
    assert!(
        !detail_payloads.payloads.is_empty(),
        "should have at least one detail payload"
    );

    // The plaintext marker should NOT appear in the raw data if the codec was applied.
    let raw_data = &detail_payloads.payloads[0].data;
    let raw_str = String::from_utf8_lossy(raw_data);
    assert!(
        !raw_str.contains("detail-marker-plaintext"),
        "Detail payload on server should be encoded by the codec, but found plaintext: {}",
        raw_str,
    );
}

// ---------------------------------------------------------------------------
// Send path: codec is applied to outgoing activity failure payloads
// ---------------------------------------------------------------------------

struct FailWithDetailsActivities;
#[activities]
impl FailWithDetailsActivities {
    #[activity]
    async fn fail_with_details(
        _ctx: ActivityContext,
        _input: String,
    ) -> Result<String, ActivityError> {
        use temporalio_common::{
            data_converters::TemporalError, protos::temporal::api::common::v1::Payloads,
        };

        let detail_payload = Payload {
            metadata: {
                let mut hm = std::collections::HashMap::new();
                hm.insert("encoding".to_string(), b"json/plain".to_vec());
                hm
            },
            data: b"\"activity-detail-plaintext\"".to_vec(),
            external_payloads: vec![],
        };

        let tf = TemporalError::Application {
            message: "activity fail with details".to_string(),
            stack_trace: String::new(),
            r#type: String::new(),
            non_retryable: true,
            details: Some(Payloads {
                payloads: vec![detail_payload],
            }),
            next_retry_delay: None,
            cause: None,
        };

        Err(ActivityError::NonRetryable(tf.into()))
    }
}

struct PanickingActivities;
#[activities]
impl PanickingActivities {
    #[activity]
    async fn always_panic(_ctx: ActivityContext, _input: String) -> Result<String, ActivityError> {
        panic!("activity panic marker");
    }
}

#[workflow]
#[derive(Default)]
struct ActivityFailDetailsWorkflow;
#[workflow_methods]
impl ActivityFailDetailsWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, _input: String) -> WorkflowResult<String> {
        ctx.start_activity(
            FailWithDetailsActivities::fail_with_details,
            "trigger".to_string(),
            ActivityOptions {
                start_to_close_timeout: Some(Duration::from_secs(5)),
                retry_policy: Some(
                    temporalio_common::protos::temporal::api::common::v1::RetryPolicy {
                        maximum_attempts: 1,
                        ..Default::default()
                    },
                ),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| {
            temporalio_sdk::WorkflowTermination::Failed(anyhow::anyhow!("activity failed: {}", e))
        })
    }
}

#[workflow]
#[derive(Default)]
struct ActivityPanicWorkflow;
#[workflow_methods]
impl ActivityPanicWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, _input: String) -> WorkflowResult<String> {
        let err = ctx
            .start_activity(
                PanickingActivities::always_panic,
                "trigger".to_string(),
                ActivityOptions {
                    start_to_close_timeout: Some(Duration::from_secs(5)),
                    retry_policy: Some(
                        temporalio_common::protos::temporal::api::common::v1::RetryPolicy {
                            maximum_attempts: 1,
                            ..Default::default()
                        },
                    ),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();

        Ok(err.to_string())
    }
}

#[tokio::test]
async fn codec_applied_to_outgoing_activity_failure_payloads() {
    let wf_name = ActivityFailDetailsWorkflow::name();
    let codec = Arc::new(XorCodec::new(0x42));

    let connection = get_integ_connection(None).await;
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter,
        codec.clone(),
    );
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(data_converter)
        .build();
    let client = Client::new(connection, client_opts).unwrap();

    let mut starter = CoreWfStarter::new_with_overrides(wf_name, None, Some(client));
    starter
        .sdk_config
        .register_workflow::<ActivityFailDetailsWorkflow>();
    starter
        .sdk_config
        .register_activities(FailWithDetailsActivities);
    let wf_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            ActivityFailDetailsWorkflow::run,
            "trigger".to_string(),
            WorkflowStartOptions::new(task_queue, wf_id.clone()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    // Fetch raw history to inspect the ActivityTaskFailed event.
    let client = starter.get_client().await;
    let events = client
        .get_workflow_handle::<UntypedWorkflow>(&wf_id)
        .fetch_history(Default::default())
        .await
        .unwrap()
        .into_events();

    let activity_failed_attrs = events
        .iter()
        .find_map(|e| {
            if let Attributes::ActivityTaskFailedEventAttributes(attrs) =
                e.attributes.as_ref().unwrap()
            {
                Some(attrs)
            } else {
                None
            }
        })
        .expect("Should find ActivityTaskFailed event");

    let failure = activity_failed_attrs
        .failure
        .as_ref()
        .expect("should have failure");

    // Walk through cause chain — the activity failure wraps an application failure.
    use temporalio_common::protos::temporal::api::failure::v1::failure::FailureInfo;
    let app_failure = failure
        .cause
        .as_ref()
        .map(|c| c.as_ref())
        .unwrap_or(failure);

    let details = match app_failure.failure_info.as_ref() {
        Some(FailureInfo::ApplicationFailureInfo(info)) => info.details.as_ref(),
        other => panic!("Expected ApplicationFailureInfo, got: {:?}", other),
    };

    let detail_payloads = details.expect("should have detail payloads");
    assert!(
        !detail_payloads.payloads.is_empty(),
        "should have at least one detail payload"
    );

    let raw_data = &detail_payloads.payloads[0].data;
    let raw_str = String::from_utf8_lossy(raw_data);
    assert!(
        !raw_str.contains("activity-detail-plaintext"),
        "Activity failure detail payload on server should be encoded by codec, \
         but found plaintext: {}",
        raw_str,
    );
}

#[tokio::test]
async fn custom_failure_converter_applied_to_activity_panic_failures() {
    let wf_name = ActivityPanicWorkflow::name();

    let connection = get_integ_connection(None).await;
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        PrefixFailureConverter {
            outgoing_prefix: "activity-converted:",
            incoming_prefix: "",
        },
        temporalio_common::data_converters::DefaultPayloadCodec,
    );
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(data_converter)
        .build();
    let client = Client::new(connection, client_opts).unwrap();

    let mut starter = CoreWfStarter::new_with_overrides(wf_name, None, Some(client));
    starter
        .sdk_config
        .register_workflow::<ActivityPanicWorkflow>();
    starter.sdk_config.register_activities(PanickingActivities);
    let wf_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ActivityPanicWorkflow::run,
            "trigger".to_string(),
            WorkflowStartOptions::new(task_queue, wf_id.clone()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    handle.get_result(Default::default()).await.unwrap();

    let client = starter.get_client().await;
    let events = client
        .get_workflow_handle::<UntypedWorkflow>(&wf_id)
        .fetch_history(Default::default())
        .await
        .unwrap()
        .into_events();

    let activity_failed_attrs = events
        .iter()
        .find_map(|e| {
            if let Attributes::ActivityTaskFailedEventAttributes(attrs) =
                e.attributes.as_ref().unwrap()
            {
                Some(attrs)
            } else {
                None
            }
        })
        .expect("Should find ActivityTaskFailed event");

    let failure = activity_failed_attrs
        .failure
        .as_ref()
        .expect("should have failure");
    let app_failure = failure.cause.as_deref().unwrap_or(failure);
    assert_eq!(
        app_failure.message,
        "activity-converted:Activity function panicked: activity panic marker",
    );
}

// ---------------------------------------------------------------------------
// Receive path: activity failures are converted through the failure converter
// before reaching workflow code
// ---------------------------------------------------------------------------

/// Workflow that captures the activity error message into its result so the
/// test can inspect it from outside.
#[workflow]
#[derive(Default)]
struct ActivityFailConverterWorkflow;
#[workflow_methods]
impl ActivityFailConverterWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, _input: String) -> WorkflowResult<String> {
        let err = ctx
            .start_activity(
                FailingActivities::always_fail,
                "input".to_string(),
                ActivityOptions {
                    start_to_close_timeout: Some(Duration::from_secs(5)),
                    retry_policy: Some(
                        temporalio_common::protos::temporal::api::common::v1::RetryPolicy {
                            maximum_attempts: 1,
                            ..Default::default()
                        },
                    ),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();

        Ok(err.to_string())
    }
}

#[workflow]
#[derive(Default)]
struct NonRetryableActivityWithCustomConverterWorkflow;
#[workflow_methods]
impl NonRetryableActivityWithCustomConverterWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, _input: String) -> WorkflowResult<String> {
        let err = ctx
            .start_activity(
                FailingActivities::always_fail,
                "input".to_string(),
                ActivityOptions {
                    start_to_close_timeout: Some(Duration::from_secs(5)),
                    retry_policy: Some(
                        temporalio_common::protos::temporal::api::common::v1::RetryPolicy {
                            initial_interval: Some(temporalio_common::prost_dur!(from_millis(10))),
                            backoff_coefficient: 1.0,
                            maximum_attempts: 5,
                            ..Default::default()
                        },
                    ),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();

        Ok(err.to_string())
    }
}

#[workflow]
#[derive(Default)]
struct ChildStartCancelledFromHistoryWorkflow;

#[workflow_methods]
impl ChildStartCancelledFromHistoryWorkflow {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let start = ctx.child_workflow(
            temporalio_common::UntypedWorkflow::new("child"),
            RawValue::new(vec![]),
            ChildWorkflowOptions {
                workflow_id: "child-id-1".to_owned(),
                cancel_type:
                    temporalio_common::protos::coresdk::child_workflow::ChildWorkflowCancellationType::WaitCancellationCompleted,
                ..Default::default()
            },
        );

        start.cancel();
        let err = start
            .await
            .expect_err("child start should resolve as cancelled");

        match err {
            ChildWorkflowExecutionError::Cancelled { source, .. } => match source.as_ref() {
                TemporalError::ChildWorkflow {
                    workflow_id,
                    cause: Some(cause),
                    ..
                } => {
                    assert_eq!(workflow_id, "child-id-1");
                    assert!(
                        cause.message().unwrap_or_default().starts_with("decoded:"),
                        "expected decoded cancellation cause, got {cause:?}"
                    );
                }
                other => panic!("expected child workflow metadata, got {other:?}"),
            },
            other => panic!("expected cancelled child start, got {other:?}"),
        }

        Ok(())
    }
}

#[tokio::test]
async fn activity_failure_converted_through_failure_converter() {
    let wf_name = ActivityFailConverterWorkflow::name();

    let connection = get_integ_connection(None).await;
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        UpperCaseFailureConverter,
        temporalio_common::data_converters::DefaultPayloadCodec,
    );
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(data_converter)
        .build();
    let client = Client::new(connection, client_opts).unwrap();

    let mut starter = CoreWfStarter::new_with_overrides(wf_name, None, Some(client));
    starter
        .sdk_config
        .register_workflow::<ActivityFailConverterWorkflow>();
    starter.sdk_config.register_activities(FailingActivities);
    let wf_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ActivityFailConverterWorkflow::run,
            "trigger".to_string(),
            WorkflowStartOptions::new(task_queue, wf_id).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();
    assert_eq!(result, "Activity failed: activity went boom");
}

#[tokio::test]
async fn custom_failure_converter_preserves_non_retryable_activity_errors() {
    let wf_name = NonRetryableActivityWithCustomConverterWorkflow::name();

    let connection = get_integ_connection(None).await;
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        GenericFailureConverter,
        temporalio_common::data_converters::DefaultPayloadCodec,
    );
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(data_converter)
        .build();
    let client = Client::new(connection, client_opts).unwrap();

    let mut starter = CoreWfStarter::new_with_overrides(wf_name, None, Some(client));
    starter
        .sdk_config
        .register_workflow::<NonRetryableActivityWithCustomConverterWorkflow>();
    starter.sdk_config.register_activities(FailingActivities);
    let wf_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            NonRetryableActivityWithCustomConverterWorkflow::run,
            "trigger".to_string(),
            WorkflowStartOptions::new(task_queue, wf_id.clone()).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();
    assert!(
        result.contains("activity went boom"),
        "workflow should still observe the activity failure, got: {result}",
    );

    let client = starter.get_client().await;
    let events = client
        .get_workflow_handle::<UntypedWorkflow>(&wf_id)
        .fetch_history(Default::default())
        .await
        .unwrap()
        .into_events();

    let activity_started_count = events
        .iter()
        .filter(|e| {
            matches!(
                e.attributes.as_ref(),
                Some(Attributes::ActivityTaskStartedEventAttributes(_))
            )
        })
        .count();
    assert_eq!(
        activity_started_count, 1,
        "ActivityError::NonRetryable must suppress retries even when a custom failure converter \
         returns a non-application failure",
    );

    let activity_failed_attrs = events
        .iter()
        .find_map(|e| {
            if let Attributes::ActivityTaskFailedEventAttributes(attrs) =
                e.attributes.as_ref().unwrap()
            {
                Some(attrs)
            } else {
                None
            }
        })
        .expect("Should find ActivityTaskFailed event");
    let failure = activity_failed_attrs
        .failure
        .as_ref()
        .expect("should have failure");
    let app_info = std::iter::successors(Some(failure), |f| f.cause.as_deref())
        .find_map(|f| match f.failure_info.as_ref() {
            Some(FailureInfo::ApplicationFailureInfo(info)) => Some(info),
            _ => None,
        })
        .expect("Should find ApplicationFailureInfo in failure chain");
    assert!(
        app_info.non_retryable,
        "ActivityError::NonRetryable must leave a non-retryable marker on the failure chain",
    );
}

#[tokio::test]
async fn child_start_cancellation_converted_through_failure_converter() {
    let mut history = temporalio_common::protos::TestHistoryBuilder::default();
    history.add_by_type(
        temporalio_common::protos::temporal::api::enums::v1::EventType::WorkflowExecutionStarted,
    );
    history.add_full_wf_task();
    history.add_workflow_execution_completed();

    let mut mock_cfg = MockPollCfg::from_hist_builder(history);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(|wft| {
            assert_eq!(wft.commands.len(), 1);
            assert_matches!(
                wft.commands[0].command_type(),
                temporalio_common::protos::temporal::api::enums::v1::CommandType::CompleteWorkflowExecution
            );
        });
    });
    mock_cfg.using_rust_sdk = true;

    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|cfg| {
        cfg.max_cached_workflows = 1;
        cfg.ignore_evicts_on_shutdown = false;
    });
    let core = mock_worker(mock);
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        PrefixFailureConverter {
            outgoing_prefix: "",
            incoming_prefix: "decoded:",
        },
        temporalio_common::data_converters::DefaultPayloadCodec,
    );
    let mut worker = TestWorker::new(temporalio_sdk::Worker::new_from_core(
        Arc::new(core),
        data_converter,
    ));
    worker.register_workflow::<ChildStartCancelledFromHistoryWorkflow>();
    worker.run_until_done().await.unwrap();
}

// ---------------------------------------------------------------------------
// Receive path: codec decodes activity failure payloads before the workflow
// sees them
// ---------------------------------------------------------------------------

/// Activity that fails with detail payloads (used by the codec receive test).
struct CodecFailActivities;
#[activities]
impl CodecFailActivities {
    #[activity]
    async fn fail_with_encoded_details(
        _ctx: ActivityContext,
        _input: String,
    ) -> Result<String, ActivityError> {
        use temporalio_common::{
            data_converters::TemporalError, protos::temporal::api::common::v1::Payloads,
        };

        let detail_payload = Payload {
            metadata: {
                let mut hm = std::collections::HashMap::new();
                hm.insert("encoding".to_string(), b"json/plain".to_vec());
                hm
            },
            data: b"\"readable-detail\"".to_vec(),
            external_payloads: vec![],
        };

        let tf = TemporalError::Application {
            message: "activity with details".to_string(),
            stack_trace: String::new(),
            r#type: String::new(),
            non_retryable: true,
            details: Some(Payloads {
                payloads: vec![detail_payload],
            }),
            next_retry_delay: None,
            cause: None,
        };

        Err(ActivityError::NonRetryable(tf.into()))
    }
}

/// Workflow that catches the activity error and extracts the detail payload
/// to return as its result, so the test can verify the payload was decoded.
#[workflow]
#[derive(Default)]
struct ActivityCodecDecodeWorkflow;
#[workflow_methods]
impl ActivityCodecDecodeWorkflow {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, _input: String) -> WorkflowResult<String> {
        use temporalio_common::data_converters::TemporalError;

        let err = ctx
            .start_activity(
                CodecFailActivities::fail_with_encoded_details,
                "input".to_string(),
                ActivityOptions {
                    start_to_close_timeout: Some(Duration::from_secs(5)),
                    retry_policy: Some(
                        temporalio_common::protos::temporal::api::common::v1::RetryPolicy {
                            maximum_attempts: 1,
                            ..Default::default()
                        },
                    ),
                    ..Default::default()
                },
            )
            .await
            .unwrap_err();

        // Try to extract the detail payload from the error. If the failure
        // converter + codec are wired up on the receive path, the error will be
        // a TemporalError with decoded detail payloads.
        let err_str = format!("{}", err);
        if let temporalio_sdk::ActivityExecutionError::Failed { source: e, .. } = err
            && let TemporalError::Activity {
                cause: Some(tf), ..
            } = e.as_ref()
            && let TemporalError::Application {
                details: Some(payloads),
                ..
            } = tf.as_ref()
            && let Some(p) = payloads.payloads.first()
        {
            let detail_str = String::from_utf8_lossy(&p.data);
            return Ok(format!("detail:{}", detail_str));
        }

        Ok(format!("raw_error:{}", err_str))
    }
}

#[tokio::test]
async fn codec_decodes_activity_failure_payloads_on_receive() {
    let wf_name = ActivityCodecDecodeWorkflow::name();
    let codec = Arc::new(XorCodec::new(0x42));

    let connection = get_integ_connection(None).await;
    let data_converter = DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter,
        codec.clone(),
    );
    let client_opts = ClientOptions::new(integ_namespace())
        .data_converter(data_converter)
        .build();
    let client = Client::new(connection, client_opts).unwrap();

    let mut starter = CoreWfStarter::new_with_overrides(wf_name, None, Some(client));
    starter
        .sdk_config
        .register_workflow::<ActivityCodecDecodeWorkflow>();
    starter.sdk_config.register_activities(CodecFailActivities);
    let wf_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    let task_queue = starter.get_task_queue().to_owned();
    let handle = worker
        .submit_workflow(
            ActivityCodecDecodeWorkflow::run,
            "trigger".to_string(),
            WorkflowStartOptions::new(task_queue, wf_id).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let result = handle.get_result(Default::default()).await.unwrap();

    // If the codec + failure converter are wired up on the receive path, the
    // workflow should see a TemporalError with the decoded detail payload
    // containing the original "readable-detail" string.
    assert!(
        result.starts_with("detail:"),
        "Workflow should receive a TemporalError with decoded detail payloads, got: {}",
        result,
    );
    assert!(
        result.contains("readable-detail"),
        "Detail payload should be decoded (readable), got: {}",
        result,
    );
}
