#[cfg(test)]
mod integ_tests {
    use std::str::FromStr;
    use std::{convert::TryFrom, env, future::Future, sync::Arc, time::Duration};
    use temporal_sdk_core::{
        protos::temporal::api::workflowservice::v1::ListNamespacesRequest, ClientTlsConfig, Core,
        CoreInitOptions, ServerGatewayApis, ServerGatewayOptions, TlsConfig,
    };
    use url::Url;

    mod polling_tests;
    mod simple_wf_tests;

    const NAMESPACE: &str = "default";
    type GwApi = Arc<dyn ServerGatewayApis>;

    pub async fn create_workflow(
        core: &dyn Core,
        task_q: &str,
        workflow_id: &str,
        wf_type: Option<&str>,
    ) -> String {
        with_gw(core, |gw: GwApi| async move {
            gw.start_workflow(
                NAMESPACE.to_owned(),
                task_q.to_owned(),
                workflow_id.to_owned(),
                wf_type.unwrap_or("test-workflow").to_owned(),
                None,
            )
            .await
            .unwrap()
            .run_id
        })
        .await
    }

    // TODO: Builder pattern
    pub async fn create_workflow_custom_timeout(
        core: &dyn Core,
        task_q: &str,
        workflow_id: &str,
        task_timeout: Duration,
    ) -> String {
        with_gw(core, |gw: GwApi| async move {
            gw.start_workflow(
                NAMESPACE.to_owned(),
                task_q.to_owned(),
                workflow_id.to_owned(),
                "test-workflow".to_owned(),
                Some(task_timeout),
            )
            .await
            .unwrap()
            .run_id
        })
        .await
    }

    pub async fn with_gw<F: FnOnce(GwApi) -> Fout, Fout: Future>(
        core: &dyn Core,
        fun: F,
    ) -> Fout::Output {
        let gw = core.server_gateway();
        fun(gw).await
    }

    pub fn get_integ_server_options(task_q: &str) -> ServerGatewayOptions {
        let temporal_server_address = match env::var("TEMPORAL_SERVICE_ADDRESS") {
            Ok(addr) => addr,
            Err(_) => "http://localhost:7233".to_owned(),
        };
        let url = Url::try_from(&*temporal_server_address).unwrap();
        ServerGatewayOptions {
            namespace: NAMESPACE.to_string(),
            task_queue: task_q.to_string(),
            identity: "integ_tester".to_string(),
            worker_binary_id: "".to_string(),
            long_poll_timeout: Duration::from_secs(60),
            target_url: url,
            tls_cfg: None,
        }
    }

    pub async fn get_integ_core(task_q: &str) -> impl Core {
        let gateway_opts = get_integ_server_options(task_q);
        temporal_sdk_core::init(CoreInitOptions {
            gateway_opts,
            evict_after_pending_cleared: false,
            max_outstanding_workflow_tasks: 5,
            max_outstanding_activities: 5,
        })
        .await
        .unwrap()
    }

    // TODO: Currently ignored because starting up the docker image with TLS requires some hoop
    //  jumping. We should upgrade CI to be able to do that but this was manually run against
    //  https://github.com/temporalio/customization-samples/tree/master/tls/tls-simple
    #[tokio::test]
    #[ignore]
    async fn tls_test() {
        let sgo = ServerGatewayOptions {
            target_url: Url::from_str("https://localhost:7233").unwrap(),
            namespace: NAMESPACE.to_string(),
            task_queue: "whatever".to_string(),
            identity: "ident".to_string(),
            worker_binary_id: "binident".to_string(),
            long_poll_timeout: Duration::from_secs(60),
            tls_cfg: Some(TlsConfig {
                server_root_ca_cert:
                    "/home/sushi/dev/temporal/customization-samples/tls/tls-simple/certs/ca.cert"
                        .into(),
                domain: "tls-sample".to_string(),
                client_tls_config: Some(ClientTlsConfig {
                    client_cert:
                    "/home/sushi/dev/temporal/customization-samples/tls/tls-simple/certs/client.pem"
                        .into(),
                    client_private_key:
                    "/home/sushi/dev/temporal/customization-samples/tls/tls-simple/certs/client.key"
                        .into(),
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
