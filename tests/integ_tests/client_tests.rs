use assert_matches::assert_matches;
use std::{collections::HashMap, time::Duration};
use temporal_client::{Client, WorkflowClientTrait, WorkflowOptions, WorkflowService};
use temporal_sdk_core_protos::temporal::api::{
    common::v1::WorkflowExecution,
    enums::v1::{UpdateWorkflowExecutionLifecycleStage, WorkflowIdReusePolicy},
    update,
    update::v1::WaitPolicy,
    workflowservice::v1::{DescribeNamespaceRequest, UpdateWorkflowExecutionRequest},
};
use temporal_sdk_core_test_utils::{get_integ_server_options, CoreWfStarter, NAMESPACE};
use tonic::{Code, Request};

#[tokio::test]
async fn can_use_retry_client() {
    // Not terribly interesting by itself but can be useful for manually inspecting metrics etc
    let mut core = CoreWfStarter::new("retry_client");
    let retry_client = core.get_client().await;
    for _ in 0..10 {
        retry_client.list_namespaces().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn can_use_retry_raw_client() {
    let opts = get_integ_server_options();
    let mut client = opts.connect_no_namespace(None).await.unwrap();
    client
        .describe_namespace(DescribeNamespaceRequest {
            namespace: NAMESPACE.to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn calls_get_system_info() {
    let opts = get_integ_server_options();
    let raw_client = opts.connect_no_namespace(None).await.unwrap();
    assert!(raw_client.get_client().capabilities().is_some());
}

#[tokio::test]
async fn per_call_timeout_respected_whole_client() {
    let opts = get_integ_server_options();
    let mut raw_client = opts.connect_no_namespace(None).await.unwrap();
    let mut hm = HashMap::new();
    hm.insert("grpc-timeout".to_string(), "0S".to_string());
    raw_client.get_client().set_headers(hm);
    let err = raw_client
        .describe_namespace(DescribeNamespaceRequest {
            namespace: NAMESPACE.to_string(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_matches!(err.code(), Code::DeadlineExceeded | Code::Cancelled);
}

#[tokio::test]
async fn per_call_timeout_respected_one_call() {
    let opts = get_integ_server_options();
    let mut client = opts.connect_no_namespace(None).await.unwrap();
    // Start a workflow (we don't need to actually make any progress on it)
    let wfc = Client::new(client.clone().into_inner(), NAMESPACE.to_string());
    wfc.start_workflow(
        vec![],
        "whatever".to_string(),
        "test-rpc-timeout".to_string(),
        "whatever".to_string(),
        None,
        WorkflowOptions {
            run_timeout: Some(Duration::from_secs(10)),
            id_reuse_policy: WorkflowIdReusePolicy::TerminateIfRunning,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let mut req = Request::new(UpdateWorkflowExecutionRequest {
        namespace: NAMESPACE.to_string(),
        wait_policy: Some(WaitPolicy {
            lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed.into(),
        }),
        workflow_execution: Some(WorkflowExecution {
            workflow_id: "test-rpc-timeout".to_string(),
            run_id: "".to_string(),
        }),
        request: Some(update::v1::Request {
            meta: Some(update::v1::Meta {
                update_id: "".into(),
                identity: "aaaa".to_string(),
            }),
            input: Some(update::v1::Input {
                header: None,
                name: "update".to_string(),
                args: None,
            }),
        }),
        ..Default::default()
    });
    req.set_timeout(Duration::from_millis(500));
    let start = std::time::Instant::now();
    let res = client.update_workflow_execution(req).await;
    assert_matches!(
        res.unwrap_err().code(),
        Code::DeadlineExceeded | Code::Cancelled
    );
    assert!(start.elapsed() < Duration::from_secs(1));
}
