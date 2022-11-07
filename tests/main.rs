//! Integration tests
//!
//! Note that integ tests which want to use the server (nearly all of them) *need* to use the
//! `#[rstest]` macro and accept the TODO fixture to support auto setup & teardown of ephemeral
//! local servers.

#[macro_use]
extern crate rstest;

#[cfg(test)]
mod integ_tests {
    #[macro_export]
    macro_rules! prost_dur {
        ($dur_call:ident $args:tt) => {
            std::time::Duration::$dur_call$args
                .try_into()
                .expect("test duration fits")
        };
    }
    mod client_tests;
    mod ephemeral_server_tests;
    mod heartbeat_tests;
    mod metrics_tests;
    mod polling_tests;
    mod queries_tests;
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
        get_integ_server_options, get_integ_telem_options, NAMESPACE,
    };
    use url::Url;

    // Create a worker like a bridge would (unwraps aside)
    #[tokio::test]
    #[ignore] // Really a compile time check more than anything
    async fn lang_bridge_example() {
        let opts = get_integ_server_options();
        let runtime = CoreRuntime::new_assume_tokio(get_integ_telem_options()).unwrap();
        let mut retrying_client = opts
            .connect_no_namespace(runtime.metric_meter(), None)
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
        let sgo = ClientOptionsBuilder::default()
            .target_url(Url::from_str("https://localhost:7233").unwrap())
            .client_name("tls_tester")
            .client_version("clientver")
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
        let con = sgo
            .connect(NAMESPACE.to_string(), None, None)
            .await
            .unwrap();
        con.list_namespaces().await.unwrap();
    }
}
