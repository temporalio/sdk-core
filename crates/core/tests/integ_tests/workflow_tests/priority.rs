use crate::shared_tests;

#[tokio::test]
async fn priority_values_sent_to_server() {
    shared_tests::priority::priority_values_sent_to_server().await
}
