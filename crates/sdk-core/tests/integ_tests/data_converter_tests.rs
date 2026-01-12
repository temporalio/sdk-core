use crate::common::CoreWfStarter;
use assert_matches::assert_matches;
use std::time::Duration;
use temporalio_client::{WorkflowExecutionResult, WorkflowOptions};
use temporalio_common::{
    data_converters::{
        PayloadConversionError, SerializationContext, TemporalDeserializable, TemporalSerializable,
    },
    protos::temporal::api::common::v1::Payload,
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
    fn to_payload(&self, _: &SerializationContext) -> Result<Payload, PayloadConversionError> {
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
        payload: Payload,
        _: &SerializationContext,
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
        let output = TrackedWrapper::from_payload(payload, &SerializationContext::Workflow)?;

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
