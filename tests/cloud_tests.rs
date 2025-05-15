use std::env;
use std::str::FromStr;
use temporal_client::{
    Client, ClientOptionsBuilder, ClientTlsConfig, RetryClient, TlsConfig, WorkflowClientTrait,
};
use url::Url;

async fn get_client() -> RetryClient<Client> {
    let cloud_addr = env::var("TEMPORAL_CLOUD_ADDRESS").unwrap();
    let cloud_key = env::var("TEMPORAL_CLIENT_KEY").unwrap();

    let client_cert = env::var("TEMPORAL_CLIENT_CERT")
        .expect("TEMPORAL_CLIENT_CERT must be set")
        .replace("\\n", "\n")
        .into_bytes();
    let client_private_key = cloud_key.replace("\\n", "\n").into_bytes();
    let sgo = ClientOptionsBuilder::default()
        .target_url(Url::from_str(&cloud_addr).unwrap())
        .client_name("tls_tester")
        .client_version("clientver")
        .tls_cfg(TlsConfig {
            client_tls_config: Some(ClientTlsConfig {
                client_cert,
                client_private_key,
            }),
            ..Default::default()
        })
        .build()
        .unwrap();
    sgo.connect(
        env::var("TEMPORAL_CLOUD_NAMESPACE").expect("TEMPORAL_CLOUD_NAMESPACE must be set"),
        None,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn tls_test() {
    let con = get_client().await;
    con.list_workflow_executions(100, vec![], "".to_string())
        .await
        .unwrap();
}

#[tokio::test]
async fn grpc_message_too_large_test() {
    // TODO
}
