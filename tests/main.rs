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
    mod worker_tests;
    mod workflow_tests;

    use std::{env, str::FromStr};
    use temporal_client::WorkflowService;
    use temporal_sdk_core::{
        init_worker, ClientOptionsBuilder, ClientTlsConfig, CoreRuntime, TlsConfig,
        WorkflowClientTrait,
    };
    use temporal_sdk_core_api::worker::WorkerConfigBuilder;
    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::ListNamespacesRequest;
    use temporal_sdk_core_test_utils::{get_integ_server_options, get_integ_telem_options};
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

    #[tokio::test]
    async fn tls_test() {
        let cloud_addr = env::var("TEMPORAL_CLOUD_ADDRESS");
        let cloud_key = env::var("TEMPORAL_CLIENT_KEY");

        let (cloud_addr, cloud_key) = if let (Ok(c), Ok(ck)) = (cloud_addr, cloud_key) {
            if ck.is_empty() {
                return; // secret not present in github, could be a fork, just skip
            }
            (c, ck)
        } else {
            // Skip the test
            return;
        };

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
        let con = sgo
            .connect(
                env::var("TEMPORAL_CLOUD_NAMESPACE").expect("TEMPORAL_CLOUD_NAMESPACE must be set"),
                None,
            )
            .await
            .unwrap();
        con.list_workflow_executions(100, vec![], "".to_string())
            .await
            .unwrap();
    }
}
