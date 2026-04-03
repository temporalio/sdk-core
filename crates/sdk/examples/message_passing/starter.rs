mod workflows;

use std::str::FromStr;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowExecuteUpdateOptions,
    WorkflowGetResultOptions, WorkflowQueryOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_sdk_core::Url;
use workflows::MessagePassingWorkflow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("TEMPORAL_SERVICE_ADDRESS")
        .unwrap_or_else(|_| "http://localhost:7233".to_string());
    let namespace = std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".to_string());

    let connection =
        Connection::connect(ConnectionOptions::new(Url::from_str(&address)?).build()).await?;
    let client = Client::new(connection, ClientOptions::new(namespace).build())?;

    let handle = client
        .start_workflow(
            MessagePassingWorkflow::run,
            10,
            WorkflowStartOptions::new("message-passing", "message-passing-workflow-id").build(),
        )
        .await?;

    println!("Started workflow, run_id: {:?}", handle.run_id());

    handle
        .signal(
            MessagePassingWorkflow::increment,
            5,
            WorkflowSignalOptions::default(),
        )
        .await?;
    println!("Sent signal: increment(5)");

    let counter = handle
        .query(
            MessagePassingWorkflow::get_counter,
            (),
            WorkflowQueryOptions::default(),
        )
        .await?;
    println!("Query result: counter = {counter}");

    let old = handle
        .execute_update(
            MessagePassingWorkflow::set_counter,
            10,
            WorkflowExecuteUpdateOptions::default(),
        )
        .await?;
    println!("Update result: old counter = {old}");

    let result = handle
        .get_result(WorkflowGetResultOptions::default())
        .await?;
    println!("Workflow result: {result}");

    Ok(())
}
