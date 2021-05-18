//! Integration tests

#[cfg(test)]
mod integ_tests {
    use std::{str::FromStr, time::Duration};
    use temporal_sdk_core::{
        protos::temporal::api::workflowservice::v1::ListNamespacesRequest, ClientTlsConfig,
        ServerGatewayOptions, TlsConfig,
    };
    use test_utils::NAMESPACE;
    use url::Url;

    mod heartbeat_tests;
    mod polling_tests;
    mod workflow_tests;

    // TODO: Currently ignored because starting up the docker image with TLS requires some hoop
    //  jumping. We should upgrade CI to be able to do that but this was manually run against
    //  https://github.com/temporalio/customization-samples/tree/master/tls/tls-simple
    #[tokio::test]
    #[ignore]
    async fn tls_test() {
        // Load certs/keys
        let root =
            tokio::fs::read("/home/sushi/dev/temporal/customization-samples/tls/tls-full/certs/cluster/ca/server-intermediate-ca.pem")
                .await
                .unwrap();
        let client_cert = tokio::fs::read(
            "/home/sushi/dev/temporal/customization-samples/tls/tls-full/certs/client/accounting/client-accounting-namespace-chain.pem",
        )
        .await
        .unwrap();
        let client_private_key = tokio::fs::read(
            "/home/sushi/dev/temporal/customization-samples/tls/tls-full/certs/client/accounting/client-accounting-namespace.key",
        )
        .await
        .unwrap();
        let sgo = ServerGatewayOptions {
            target_url: Url::from_str("https://localhost:7233").unwrap(),
            namespace: NAMESPACE.to_string(),
            identity: "ident".to_string(),
            worker_binary_id: "binident".to_string(),
            long_poll_timeout: Duration::from_secs(60),
            tls_cfg: Some(TlsConfig {
                server_root_ca_cert: Some(root),
                domain: Some("accounting.cluster-x.contoso.com".to_string()),
                client_tls_config: Some(ClientTlsConfig {
                    client_cert,
                    client_private_key,
                }),
            }),
        };
        let mut con = sgo.connect().await.unwrap();
        con.service
            .list_namespaces(ListNamespacesRequest::default())
            .await
            .unwrap();
    }
}
