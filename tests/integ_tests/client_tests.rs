use std::time::Duration;
use temporal_sdk_core_test_utils::CoreWfStarter;

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
