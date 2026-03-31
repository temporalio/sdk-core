mod workflows;

use std::str::FromStr;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowCancelOptions,
    WorkflowGetResultOptions, WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use workflows::CancellationWorkflow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("TEMPORAL_SERVICE_ADDRESS")
        .unwrap_or_else(|_| "http://localhost:7233".to_string());
    let namespace = std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".to_string());

    let runtime = CoreRuntime::new_assume_tokio(
        RuntimeOptions::builder()
            .telemetry_options(TelemetryOptions::builder().build())
            .build()?,
    )?;
    let connection =
        Connection::connect(ConnectionOptions::new(Url::from_str(&address)?).build()).await?;
    let client = Client::new(connection, ClientOptions::new(namespace).build())?;
    let _ = &runtime;

    let handle = client
        .start_workflow(
            CancellationWorkflow::run,
            (),
            WorkflowStartOptions::new("cancellation", "cancellation-workflow-id").build(),
        )
        .await?;

    println!("Started workflow, run_id: {:?}", handle.run_id());

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    println!("Requesting cancellation...");
    handle.cancel(WorkflowCancelOptions::default()).await?;

    let result = handle
        .get_result(WorkflowGetResultOptions::default())
        .await?;
    println!("Workflow result: {result}");

    Ok(())
}
