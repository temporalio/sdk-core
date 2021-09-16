use std::time::Duration;
use temporal_sdk_core::ServerGatewayApis;
use test_utils::get_integ_server_options;

#[tokio::test]
async fn can_use_retry_gateway() {
    // Not terribly interesting by itself but can be useful for manually inspecting metrics etc
    let sgopts = get_integ_server_options();
    let retry_client = sgopts.connect().await.unwrap();
    for _ in 0..10 {
        retry_client.list_namespaces().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
