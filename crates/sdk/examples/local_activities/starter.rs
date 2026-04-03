mod workflows;

use std::str::FromStr;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowStartOptions,
};
use temporalio_sdk_core::Url;
use workflows::LocalActivitiesWorkflow;

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
            LocalActivitiesWorkflow::run,
            "Temporal".to_string(),
            WorkflowStartOptions::new("local-activities", "local-activities-workflow-id").build(),
        )
        .await?;

    println!("Started workflow, run_id: {:?}", handle.run_id());

    let result: (String, String) = handle
        .get_result(WorkflowGetResultOptions::default())
        .await?;
    println!("Remote activity result: {}", result.0);
    println!("Local activity result: {}", result.1);

    Ok(())
}
