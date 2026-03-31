use std::str::FromStr;

use futures_util::StreamExt;
use temporalio_client::schedules::{
    CreateScheduleOptions, ListSchedulesOptions, ScheduleAction, ScheduleOverlapPolicy,
    ScheduleSpec,
};
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_common::telemetry::TelemetryOptions;
use temporalio_sdk_core::{CoreRuntime, RuntimeOptions, Url};

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

    let schedule_id = "demo-schedule";
    let action =
        ScheduleAction::start_workflow("ScheduledWorkflow", "schedules", "scheduled-workflow");
    let spec = ScheduleSpec::from_interval(std::time::Duration::from_secs(10));
    let opts = CreateScheduleOptions::builder()
        .action(action)
        .spec(spec)
        .trigger_immediately(true)
        .build();

    let handle = client.create_schedule(schedule_id, opts).await?;
    println!("Created schedule: {schedule_id}");

    let desc = handle.describe().await?;
    println!("Schedule is paused: {}", desc.paused());

    let mut stream = client.list_schedules(ListSchedulesOptions::default());
    if let Some(entry) = stream.next().await {
        let entry = entry?;
        println!("Found schedule: {:?}", entry);
    }

    handle.trigger(ScheduleOverlapPolicy::Unspecified).await?;
    println!("Triggered schedule");

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    handle.delete().await?;
    println!("Deleted schedule");

    Ok(())
}
