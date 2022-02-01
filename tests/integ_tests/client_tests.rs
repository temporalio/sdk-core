use std::time::Duration;
use temporal_client::{retry_call, RetryGateway};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::ListNamespacesRequest;
use temporal_sdk_core_test_utils::{get_integ_server_options, CoreWfStarter};

#[tokio::test]
async fn can_use_retry_gateway() {
    // Not terribly interesting by itself but can be useful for manually inspecting metrics etc
    let mut core = CoreWfStarter::new("retry_gateway");
    let retry_client = core.get_worker().await.server_gateway();
    for _ in 0..10 {
        retry_client.list_namespaces().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn can_use_retry_gateway_raw_client() {
    let opts = get_integ_server_options();
    let raw_client = opts.connect_raw(None).await.unwrap();
    let retry_client = RetryGateway::new(raw_client, opts.retry_config);
    retry_call!(
        retry_client,
        list_namespaces,
        ListNamespacesRequest::default()
    )
    .unwrap();
}
