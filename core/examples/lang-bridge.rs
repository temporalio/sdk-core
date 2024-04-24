use temporal_client::WorkflowService;
use temporal_sdk_core::{init_worker, CoreRuntime};
use temporal_sdk_core_api::worker::WorkerConfigBuilder;
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::ListNamespacesRequest;
use temporal_sdk_core_test_utils::{get_integ_server_options, get_integ_telem_options};

// Create a worker like a bridge would (unwraps aside)
#[tokio::main]
async fn main() {
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
