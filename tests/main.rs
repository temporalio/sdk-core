//! Integration tests

#[cfg(test)]
mod integ_tests {
    use std::str::FromStr;
    use temporal_sdk_core::{
        ClientTlsConfig, ServerGatewayApis, ServerGatewayOptionsBuilder, TlsConfig,
    };
    use temporal_sdk_core_test_utils::NAMESPACE;
    use url::Url;

    mod client_tests;
    mod heartbeat_tests;
    mod polling_tests;
    mod queries_tests;
    mod workflow_tests;

    // TODO: Currently ignored because starting up the docker image with TLS requires some hoop
    //  jumping. We should upgrade CI to be able to do that but this was manually run against
    //  https://github.com/temporalio/customization-samples/tree/master/tls/tls-simple
    #[tokio::test]
    #[ignore]
    async fn tls_test() {
        // Load certs/keys
        let root = tokio::fs::read(
            "/home/sushi/dev/temporal/customization-samples/tls/tls-simple/certs/ca.cert",
        )
        .await
        .unwrap();
        let client_cert = tokio::fs::read(
            "/home/sushi/dev/temporal/customization-samples/tls/tls-simple/certs/client.pem",
        )
        .await
        .unwrap();
        let client_private_key = tokio::fs::read(
            "/home/sushi/dev/temporal/customization-samples/tls/tls-simple/certs/client.key",
        )
        .await
        .unwrap();
        let sgo = ServerGatewayOptionsBuilder::default()
            .target_url(Url::from_str("https://localhost:7233").unwrap())
            .worker_binary_id("binident".to_string())
            .tls_cfg(TlsConfig {
                server_root_ca_cert: Some(root),
                domain: Some("tls-sample".to_string()),
                client_tls_config: Some(ClientTlsConfig {
                    client_cert,
                    client_private_key,
                }),
            })
            .build()
            .unwrap();
        let con = sgo.connect(NAMESPACE.to_string(), None).await.unwrap();
        con.list_namespaces().await.unwrap();
    }
}
