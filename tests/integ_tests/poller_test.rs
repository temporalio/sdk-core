use rand;
use rand::Rng;
use std::convert::TryFrom;
use std::time::Duration;
use temporal_sdk_core::protos::temporal::api::common::v1::WorkflowType;
use temporal_sdk_core::protos::temporal::api::taskqueue::v1::TaskQueue;
use temporal_sdk_core::protos::temporal::api::workflowservice::v1::StartWorkflowExecutionRequest;
use temporal_sdk_core::{Core, CoreInitOptions, ServerGatewayOptions, Url};

const TASK_QUEUE: &str = "test-tq";
const NAMESPACE: &str = "default";

const TARGET_URI: &'static str = "http://localhost:7233";

// TODO try to consolidate this into the SDK code so we don't need to create another runtime.
#[tokio::main]
async fn create_workflow() {
    let mut rng = rand::thread_rng();
    let workflow_id: u8 = rng.gen();
    let request_id: u8 = rng.gen();
    let gateway_opts = ServerGatewayOptions {
        namespace: NAMESPACE.to_string(),
        identity: "none".to_string(),
        worker_binary_id: "".to_string(),
        long_poll_timeout: Duration::from_secs(60),
    };
    let mut gateway = gateway_opts
        .connect(Url::try_from(TARGET_URI).unwrap())
        .await
        .unwrap();
    gateway
        .service
        .start_workflow_execution(StartWorkflowExecutionRequest {
            namespace: NAMESPACE.to_string(),
            workflow_id: workflow_id.to_string(),
            workflow_type: Some(WorkflowType {
                name: "test-workflow".to_string(),
            }),
            task_queue: Some(TaskQueue {
                name: TASK_QUEUE.to_string(),
                kind: 0,
            }),
            request_id: request_id.to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
}

#[test]
fn empty_poll() {
    create_workflow();
    let core = temporal_sdk_core::init(CoreInitOptions {
        target_url: Url::try_from(TARGET_URI).unwrap(),
        namespace: NAMESPACE.to_string(),
        identity: "none".to_string(),
        worker_binary_id: "".to_string(),
        runtime: None,
    })
    .unwrap();

    dbg!(core.poll_task(TASK_QUEUE).unwrap());
}
