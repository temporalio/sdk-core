mod workflows;

use std::str::FromStr;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowStartOptions,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use workflows::ContinueAsNewWorkflow;

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
    let _ = &runtime;
    let connection =
        Connection::connect(ConnectionOptions::new(Url::from_str(&address)?).build()).await?;
    let client = Client::new(connection, ClientOptions::new(namespace).build())?;

    let handle = client
        .start_workflow(
            ContinueAsNewWorkflow::run,
            (1u32, 5u32),
            WorkflowStartOptions::new("continue-as-new", "continue-as-new-workflow-id").build(),
        )
        .await?;

    println!("Started workflow, run_id: {:?}", handle.run_id());

    let result = handle
        .get_result(WorkflowGetResultOptions::default())
        .await?;
    println!("Workflow result: {result}");

    Ok(())
}
