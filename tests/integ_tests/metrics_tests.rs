use temporal_client::WorkflowService;
use temporal_sdk_core::CoreRuntime;
use temporal_sdk_core_api::telemetry::MetricsExporter;
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::ListNamespacesRequest;
use temporal_sdk_core_test_utils::{get_integ_server_options, get_integ_telem_options};

#[tokio::test]
async fn prometheus_metrics_exported() {
    let mut telemopts = get_integ_telem_options();
    let addr = "127.0.0.1:10919";
    telemopts.metrics = Some(MetricsExporter::Prometheus(addr.parse().unwrap()));
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();
    let opts = get_integ_server_options();
    let mut raw_client = opts
        .connect_no_namespace(rt.metric_meter(), None)
        .await
        .unwrap();
    assert!(raw_client.get_client().capabilities().is_some());

    let _ = raw_client
        .list_namespaces(ListNamespacesRequest::default())
        .await
        .unwrap();

    let body = reqwest::get(format!("http://{}/metrics", addr))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains(
        "request_latency_count{operation=\"ListNamespaces\",service_name=\"temporal-core-sdk\"} 1"
    ));
    assert!(body.contains(
        "request_latency_count{operation=\"GetSystemInfo\",service_name=\"temporal-core-sdk\"} 1"
    ));
}
