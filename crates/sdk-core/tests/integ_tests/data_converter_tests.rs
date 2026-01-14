use crate::common::{CoreWfStarter, get_integ_connection, integ_namespace};
use assert_matches::assert_matches;
use futures::{FutureExt, future::BoxFuture};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_client::{
    Client, ClientOptions, WorkflowClientTrait, WorkflowExecutionResult, WorkflowOptions,
};
use temporalio_common::{
    data_converters::{
        DataConverter, DefaultFailureConverter, MultiArgs2, PayloadCodec, PayloadConversionError,
        PayloadConverter, SerializationContext, SerializationContextData, TemporalDeserializable,
        TemporalSerializable,
    },
    protos::temporal::api::{common::v1::Payload, history::v1::history_event::Attributes},
    worker::WorkerTaskTypes,
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, WfExitValue, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
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
        &mut self,
        ctx: &mut WorkflowContext,
        input: TrackedWrapper,
    ) -> WorkflowResult<TrackedWrapper> {
        let result = ctx
            .start_activity(
                TestActivities::process_tracked,
                input,
                ActivityOptions {
                    start_to_close_timeout: Some(Duration::from_secs(5)),
                    ..Default::default()
                },
            )?
            .await;

        // TODO [rust-sdk-branch] Will go away after activity starts return deserialized result
        let payload = result.unwrap_ok_payload();
        let converter = PayloadConverter::default();
        let ctx = SerializationContext {
            data: &SerializationContextData::Workflow,
            converter: &converter,
        };
        let output = TrackedWrapper::from_payload(&ctx, payload)?;

        Ok(WfExitValue::Normal(output))
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

    let handle = worker
        .submit_workflow(
            DataConverterTestWorkflow::run,
            wf_name.to_owned(),
            TrackedWrapper(input),
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let result = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    let output = assert_matches!(result, WorkflowExecutionResult::Succeeded(v) => v).0;

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
        &mut self,
        _ctx: &mut WorkflowContext,
        input: MultiArgs2<String, i32>,
    ) -> WorkflowResult<String> {
        Ok(WfExitValue::Normal(format!(
            "received: {} and {}",
            input.0, input.1
        )))
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

    let handle = worker
        .submit_workflow(
            MultiArgs2Workflow::run,
            wf_name.to_owned(),
            input,
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let result = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    let output = assert_matches!(result, WorkflowExecutionResult::Succeeded(v) => v);
    assert_eq!(output, "received: hello and 42");

    // Verify the workflow history contains multiple payloads in the input
    let client = starter.get_client().await;
    let history = client
        .get_workflow_execution_history(wf_name.to_owned(), None, vec![])
        .await
        .unwrap()
        .history
        .unwrap();

    let workflow_started_event = history
        .events
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
    let client = Client::new(connection, client_opts);

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
    let handle = worker
        .submit_workflow(
            DataConverterTestWorkflow::run,
            wf_id,
            TrackedWrapper(input),
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let result = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    let output = assert_matches!(result, WorkflowExecutionResult::Succeeded(v) => v).0;

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
