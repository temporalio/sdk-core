mod codec;
mod workflows;

use std::str::FromStr;
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::data_converters::{
    DataConverter, DefaultFailureConverter, PayloadConverter,
};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk::{Worker, WorkerOptions};
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

use codec::EncryptionCodec;
use workflows::{EncryptionActivities, EncryptionWorkflow};

const ENCRYPTION_KEY: &[u8] = b"my-secret-encryption-key-32bytes";

fn make_data_converter() -> DataConverter {
    DataConverter::new(
        PayloadConverter::default(),
        DefaultFailureConverter,
        EncryptionCodec {
            key: ENCRYPTION_KEY.to_vec(),
        },
    )
}

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
    let client = Client::new(
        connection,
        ClientOptions::new(namespace)
            .data_converter(make_data_converter())
            .build(),
    )?;

    let worker_options = WorkerOptions::new("encryption")
        .register_workflow::<EncryptionWorkflow>()
        .register_activities(EncryptionActivities)
        .build();

    let mut worker = Worker::new(&runtime, client, worker_options)?;
    worker.run().await?;

    Ok(())
}
