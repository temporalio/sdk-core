//! Integration tests

#[macro_use]
extern crate rstest;
#[macro_use]
extern crate temporal_sdk_core_test_utils;

#[cfg(test)]
mod integ_tests {
    mod activity_functions;
    mod client_tests;
    mod ephemeral_server_tests;
    mod heartbeat_tests;
    mod metrics_tests;
    mod polling_tests;
    mod queries_tests;
    mod update_tests;
    mod visibility_tests;
    mod workflow_tests;

    use std::str::FromStr;
    use temporal_client::WorkflowService;
    use temporal_sdk_core::{
        init_worker, ClientOptionsBuilder, ClientTlsConfig, CoreRuntime, TlsConfig,
        WorkflowClientTrait,
    };
    use temporal_sdk_core_api::worker::WorkerConfigBuilder;
    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::ListNamespacesRequest;
    use temporal_sdk_core_test_utils::{
        get_integ_server_options, get_integ_telem_options, init_integ_telem,
    };
    use url::Url;

    // Create a worker like a bridge would (unwraps aside)
    #[tokio::test]
    #[ignore] // Really a compile time check more than anything
    async fn lang_bridge_example() {
        let opts = get_integ_server_options();
        let runtime = CoreRuntime::new_assume_tokio(get_integ_telem_options()).unwrap();
        let mut retrying_client = opts
            .connect_no_namespace(runtime.telemetry().get_temporal_metric_meter())
            .await
            .unwrap();

        let _worker = init_worker(
            &runtime,
            WorkerConfigBuilder::default()
                .namespace("default")
                .task_queue("Wheee!")
                .build()
                .unwrap(),
            // clone the client if you intend to use it later. Strip off the retry wrapper since
            // worker will assert its own
            retrying_client.clone(),
        );

        // Do things with worker or client
        let _ = retrying_client
            .list_namespaces(ListNamespacesRequest::default())
            .await;
    }

    // Manually run to verify tls works against cloud. You will need certs in place in the
    // indicated directory.
    #[tokio::test]
    #[ignore]
    async fn tls_test() {
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
}
