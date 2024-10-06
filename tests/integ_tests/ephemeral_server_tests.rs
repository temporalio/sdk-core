use futures_util::stream;
use futures_util::TryStreamExt;
use std::time::{SystemTime, UNIX_EPOCH};
use temporal_client::{ClientOptionsBuilder, TestService, WorkflowService};
use temporal_sdk_core::ephemeral_server::{
    EphemeralExe, EphemeralExeVersion, EphemeralServer, TemporalDevServerConfigBuilder,
    TestServerConfigBuilder,
};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::DescribeNamespaceRequest;
use temporal_sdk_core_test_utils::{default_cached_download, NAMESPACE};
use url::Url;

#[tokio::test]
async fn temporal_cli_default() {
    let config = TemporalDevServerConfigBuilder::default()
        .exe(default_cached_download())
        .build()
        .unwrap();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;

    // Make sure process is there on start and not there after shutdown
    let pid = sysinfo::Pid::from_u32(server.child_process_id().unwrap());
    assert!(sysinfo::System::new_all().process(pid).is_some());
    server.shutdown().await.unwrap();
    assert!(sysinfo::System::new_all().process(pid).is_none());
}

#[tokio::test]
async fn temporal_cli_fixed() {
    let config = TemporalDevServerConfigBuilder::default()
        .exe(fixed_cached_download("v0.4.0"))
        .build()
        .unwrap();
    let mut server = config.start_server().await.unwrap();
    assert_ephemeral_server(&server).await;
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn temporal_cli_shutdown_port_reuse() {
    // Start, test shutdown, do again immediately on same port to ensure we can
    // reuse after shutdown
    let config = TemporalDevServerConfigBuilder::default()
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

// This test will fail on Linux until https://github.com/temporalio/cli/pull/564
// gets released (presumably in 0.12.1). To test locally, build CLI manually
// and use that specific binary instead:
// ```
//   .exe(EphemeralExe::ExistingPath(
//       "/usr/local/bin/temporal".to_string(),
//   ))
// ```
#[tokio::test]
#[ignore]
async fn temporal_cli_concurrent_starts() -> Result<(), Box<dyn std::error::Error>> {
    stream::iter((0..80).map(|_| {
        TemporalDevServerConfigBuilder::default()
            .exe(default_cached_download())
            .build()
            .map_err(anyhow::Error::from)
    }))
    .try_for_each_concurrent(8, |config| async move {
        let mut server = config.start_server().await?;
        server.shutdown().await?;
        Ok(())
    })
    .await?;

    Ok(())
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
        .connect_no_namespace(None)
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
