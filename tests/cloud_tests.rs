use temporal_client::WorkflowClientTrait;
use temporal_sdk_core_test_utils::get_cloud_client;

mod shared_tests;

#[tokio::test]
async fn tls_test() {
    let con = get_cloud_client().await;
    con.list_workflow_executions(100, vec![], "".to_string())
        .await
        .unwrap();
}

#[tokio::test]
async fn grpc_message_too_large_test() {
    shared_tests::grpc_message_too_large().await
}

// Needs https://github.com/temporalio/temporal/pull/8143 to be rolled out in cloud to pass
// #[tokio::test]
// async fn priority_values_sent_to_server() {
//     shared_tests::priority::priority_values_sent_to_server().await
// }
