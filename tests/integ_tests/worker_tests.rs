use assert_matches::assert_matches;
use temporal_sdk_core::{init_worker, CoreRuntime};
use temporal_sdk_core_api::{errors::WorkerValidationError, worker::WorkerConfigBuilder, Worker};
use temporal_sdk_core_test_utils::{get_integ_server_options, get_integ_telem_options};

#[tokio::test]
async fn worker_validation_fails_on_nonexistent_namespace() {
    let opts = get_integ_server_options();
    let runtime = CoreRuntime::new_assume_tokio(get_integ_telem_options()).unwrap();
    let retrying_client = opts
        .connect_no_namespace(runtime.telemetry().get_temporal_metric_meter())
        .await
        .unwrap();

    let worker = init_worker(
        &runtime,
        WorkerConfigBuilder::default()
            .namespace("i_dont_exist")
            .task_queue("Wheee!")
            .worker_build_id("blah")
            .build()
            .unwrap(),
        retrying_client,
    )
    .unwrap();

    let res = worker.validate().await;
    assert_matches!(
        res,
        Err(WorkerValidationError::NamespaceDescribeError { .. })
    );
}
