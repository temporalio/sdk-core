mod workflows;

use std::str::FromStr;
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};
use workflows::{GreetingChildWorkflow, ParentWorkflow};

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

    let worker_options = WorkerOptions::new("child-workflows")
        .register_workflow::<ParentWorkflow>()
        .register_workflow::<GreetingChildWorkflow>()
        .task_types(WorkerTaskTypes::workflow_only())
        .build();

    let mut worker = Worker::new(&runtime, client, worker_options)?;
    worker.run().await?;

    Ok(())
}
