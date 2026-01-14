use crate::common::CoreWfStarter;
use assert_matches::assert_matches;
use std::time::Duration;
use temporalio_client::{WorkflowClientTrait, WorkflowExecutionResult, WorkflowOptions};
use temporalio_common::{
    data_converters::{
        MultiArgs2, PayloadConversionError, PayloadConverter, SerializationContext,
        SerializationContextData, TemporalDeserializable, TemporalSerializable,
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
