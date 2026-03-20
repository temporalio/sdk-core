use temporalio_client::{Client, UntypedWorkflow, WorkflowStartOptions};
use temporalio_common::data_converters::RawValue;

// UntypedWorkflow must not be usable with the typed start_workflow API.
// Users should use start_untyped_workflow instead.
async fn should_not_compile(client: &Client) {
    let opts = WorkflowStartOptions::new("q".to_owned(), "id".to_owned()).build();
    let _ = client
        .start_workflow::<UntypedWorkflow>(RawValue::empty(), opts)
        .await;
}

fn main() {}
