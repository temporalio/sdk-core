use rand::{self, Rng};
use std::{convert::TryFrom, env, time::Duration};
use temporal_sdk_core::{
    protos::{
        coresdk::CompleteTaskReq,
        temporal::api::command::v1::{
            CompleteWorkflowExecutionCommandAttributes, StartTimerCommandAttributes,
        },
        temporal::api::common::v1::WorkflowType,
        temporal::api::taskqueue::v1::TaskQueue,
        temporal::api::workflowservice::v1::StartWorkflowExecutionRequest,
    },
    Core, CoreInitOptions, ServerGatewayOptions, Url,
};

const TASK_QUEUE: &str = "test-tq";
const NAMESPACE: &str = "default";

// TODO try to consolidate this into the SDK code so we don't need to create another runtime.
#[tokio::main]
async fn create_workflow() -> (String, String, ServerGatewayOptions) {
    let temporal_server_address = match env::var("TEMPORAL_SERVICE_ADDRESS") {
        Ok(addr) => addr,
        Err(_) => "http://localhost:7233".to_owned(),
    };

    let mut rng = rand::thread_rng();
    let url = Url::try_from(&*temporal_server_address).unwrap();
    let workflow_id: u32 = rng.gen();
    let request_id: u32 = rng.gen();
    let gateway_opts = ServerGatewayOptions {
        namespace: NAMESPACE.to_string(),
        identity: "none".to_string(),
        worker_binary_id: "".to_string(),
        long_poll_timeout: Duration::from_secs(60),
        target_url: url,
    };
    let mut gateway = gateway_opts.connect().await.unwrap();
    let response = gateway
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
    (
        workflow_id.to_string(),
        response.into_inner().run_id,
        gateway_opts,
    )
}

#[test]
fn timer_workflow() {
    let (workflow_id, run_id, gateway_opts) = dbg!(create_workflow());
    let core = temporal_sdk_core::init(CoreInitOptions { gateway_opts }).unwrap();
    let mut rng = rand::thread_rng();
    let timer_id: String = rng.gen::<u32>().to_string();
    let task = dbg!(core.poll_task(TASK_QUEUE).unwrap());
    core.complete_task(CompleteTaskReq::ok_from_api_attrs(
        StartTimerCommandAttributes {
            timer_id: timer_id.to_string(),
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
            ..Default::default()
        }
        .into(),
        task.task_token,
    ))
    .unwrap();
    dbg!("sent completion w/ start timer");
    let task = dbg!(core.poll_task(TASK_QUEUE).unwrap());
    core.complete_task(CompleteTaskReq::ok_from_api_attrs(
        CompleteWorkflowExecutionCommandAttributes { result: None }.into(),
        task.task_token,
    ))
    .unwrap();
    dbg!(
        "sent workflow done, completed workflow",
        workflow_id,
        run_id
    );
}
