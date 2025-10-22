// All non-main.rs tests ignore dead common code so that the linter doesn't complain about about it.
#[allow(dead_code)]
mod common;
mod shared_tests;

use common::get_cloud_client;
use temporalio_client::WorkflowClientTrait;

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

#[tokio::test]
async fn priority_values_sent_to_server() {
    shared_tests::priority::priority_values_sent_to_server().await
}
