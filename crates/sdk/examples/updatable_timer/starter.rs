mod workflows;

use std::str::FromStr;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_sdk_core::Url;
use workflows::UpdatableTimerWorkflow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("TEMPORAL_SERVICE_ADDRESS")
        .unwrap_or_else(|_| "http://localhost:7233".to_string());
    let namespace = std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".to_string());

    let connection =
        Connection::connect(ConnectionOptions::new(Url::from_str(&address)?).build()).await?;
    let client = Client::new(connection, ClientOptions::new(namespace).build())?;

    let far_future_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 3_600_000;

    let handle = client
        .start_workflow(
            UpdatableTimerWorkflow::run,
            far_future_ms,
            WorkflowStartOptions::new("updatable-timer", "updatable-timer-workflow-id").build(),
        )
        .await?;

    println!("Started workflow, run_id: {:?}", handle.run_id());
    println!("Initial deadline: {far_future_ms}");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let near_future_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 2_000;

    handle
        .signal(
            UpdatableTimerWorkflow::update_deadline,
            near_future_ms,
            WorkflowSignalOptions::default(),
        )
        .await?;

    println!("Updated deadline to: {near_future_ms}");

    let result = handle
        .get_result(WorkflowGetResultOptions::default())
        .await?;
    println!("Workflow result: {result}");

    Ok(())
}
