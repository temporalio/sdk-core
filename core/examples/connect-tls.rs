/// Manually run to verify tls works against cloud. You will need certs in place in the
/// indicated directory.
use std::str::FromStr;

use temporal_client::{ClientOptionsBuilder, ClientTlsConfig, TlsConfig, WorkflowClientTrait};
use temporal_sdk_core_test_utils::init_integ_telem;
use url::Url;

#[tokio::main]
async fn main() {
    init_integ_telem();
    let root = tokio::fs::read("../.cloud_certs/ca.pem").await.unwrap();
    let client_cert = tokio::fs::read("../.cloud_certs/client.pem").await.unwrap();
    let client_private_key = tokio::fs::read("../.cloud_certs/client.key").await.unwrap();
    let sgo = ClientOptionsBuilder::default()
        .target_url(Url::from_str("https://spencer.temporal-dev.tmprl.cloud:7233").unwrap())
        .client_name("tls_tester")
        .client_version("clientver")
        .tls_cfg(TlsConfig {
            server_root_ca_cert: Some(root),
            // Not necessary, but illustrates functionality for people using proxies, etc.
            domain: Some("spencer.temporal-dev.tmprl.cloud".to_string()),
            client_tls_config: Some(ClientTlsConfig {
                client_cert,
                client_private_key,
            }),
        })
        .build()
        .unwrap();
    let con = sgo
        .connect("spencer.temporal-dev".to_string(), None)
        .await
        .unwrap();
    dbg!(con
        .list_workflow_executions(100, vec![], "".to_string())
        .await
        .unwrap());
}
