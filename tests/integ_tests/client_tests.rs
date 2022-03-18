use std::time::Duration;
use temporal_client::{RetryClient, WorkflowService};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::DescribeNamespaceRequest;
use temporal_sdk_core_test_utils::{get_integ_server_options, CoreWfStarter, NAMESPACE};

#[tokio::test]
async fn can_use_retry_client() {
    // Not terribly interesting by itself but can be useful for manually inspecting metrics etc
    let mut core = CoreWfStarter::new("retry_client");
    let retry_client = core.get_worker().await.workflow_client();
    for _ in 0..10 {
        retry_client.list_namespaces().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn can_use_retry_raw_client() {
    let opts = get_integ_server_options();
    let raw_client = opts.connect_no_namespace(None).await.unwrap();
    let mut retry_client = RetryClient::new(raw_client, opts.retry_config);
    retry_client
        .describe_namespace(DescribeNamespaceRequest {
            namespace: NAMESPACE.to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
}
