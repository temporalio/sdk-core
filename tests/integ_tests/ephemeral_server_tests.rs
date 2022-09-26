use std::time::{SystemTime, UNIX_EPOCH};
use temporal_client::{ClientOptionsBuilder, TestService, WorkflowService};
use temporal_sdk_core::ephemeral_server::{
    EphemeralExe, EphemeralExeVersion, EphemeralServer, TemporaliteConfigBuilder,
    TestServerConfigBuilder,
};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::DescribeNamespaceRequest;
use temporal_sdk_core_test_utils::{default_cached_download, NAMESPACE};
use url::Url;

#[tokio::test]
async fn temporalite_default() {
    let config = TemporaliteConfigBuilder::default()
        .exe(default_cached_download())
        .build()
        .unwrap();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn temporalite_fixed() {
    let config = TemporaliteConfigBuilder::default()
        .exe(fixed_cached_download("v0.1.1"))
        .build()
        .unwrap();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn temporalite_shutdown_port_reuse() {
    // Start, test shutdown, do again immediately on same port to ensure we can
    // reuse after shutdown
    let config = TemporaliteConfigBuilder::default()
        .exe(default_cached_download())
        .port(Some(10123))
        .build()
        .unwrap();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;
    server.shutdown().await.unwrap();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_server_default() {
    let config = TestServerConfigBuilder::default()
        .exe(default_cached_download())
        .build()
        .unwrap();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_server_fixed() {
    let config = TestServerConfigBuilder::default()
        .exe(fixed_cached_download("v1.16.0"))
        .build()
        .unwrap();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_server_shutdown_port_reuse() {
    // Start, test shutdown, do again immediately on same port to ensure we can
    // reuse after shutdown
    let config = TestServerConfigBuilder::default()
        .exe(default_cached_download())
        .port(Some(10124))
        .build()
        .unwrap();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;
    server.shutdown().await.unwrap();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;
    server.shutdown().await.unwrap();
}

fn fixed_cached_download(version: &str) -> EphemeralExe {
    EphemeralExe::CachedDownload {
        version: EphemeralExeVersion::Fixed(version.to_string()),
        dest_dir: None,
    }
}

async fn assert_ephemeral_server(server: &EphemeralServer) {
    // Connect and describe namespace
    let mut client = ClientOptionsBuilder::default()
        .identity("integ_tester".to_string())
        .target_url(Url::try_from(&*format!("http://{}", server.target)).unwrap())
        .client_name("temporal-core".to_string())
        .client_version("0.1.0".to_string())
        .build()
        .unwrap()
        .connect_no_namespace(None, None)
        .await
        .unwrap();
    let resp = client
        .describe_namespace(DescribeNamespaceRequest {
            namespace: NAMESPACE.to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(resp.into_inner().namespace_info.unwrap().name == "default");

    // If it has test service, make sure we can use it too
    if server.has_test_service {
        let resp = client.get_current_time(()).await.unwrap();
        // Make sure it's within 5 mins of now
        let resp_seconds = resp.get_ref().time.as_ref().unwrap().seconds as u64;
        let curr_seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(curr_seconds - 300 < resp_seconds && curr_seconds + 300 > resp_seconds);
    }
}
