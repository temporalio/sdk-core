mod workflows;

use std::str::FromStr;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowStartOptions,
};
use temporalio_sdk_core::Url;
use workflows::SagaWorkflow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("TEMPORAL_SERVICE_ADDRESS")
        .unwrap_or_else(|_| "http://localhost:7233".to_string());
    let namespace = std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".to_string());

    let connection =
        Connection::connect(ConnectionOptions::new(Url::from_str(&address)?).build()).await?;
    let client = Client::new(connection, ClientOptions::new(namespace).build())?;

    let trip_id = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "trip-123".to_string());

    let handle = client
        .start_workflow(
            SagaWorkflow::run,
            trip_id.clone(),
            WorkflowStartOptions::new("saga", format!("saga-{trip_id}")).build(),
        )
        .await?;

    println!("Started workflow, run_id: {:?}", handle.run_id());

    match handle.get_result(WorkflowGetResultOptions::default()).await {
        Ok(bookings) => println!("Bookings: {bookings:?}"),
        Err(e) => println!("Workflow failed (compensations were run): {e}"),
    }

    Ok(())
}
