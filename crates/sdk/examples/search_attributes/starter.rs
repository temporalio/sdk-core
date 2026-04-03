mod workflows;

use std::{collections::HashMap, str::FromStr};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowGetResultOptions,
    WorkflowStartOptions,
};
use temporalio_common::protos::coresdk::AsJsonPayloadExt;
use temporalio_sdk_core::Url;
use workflows::SearchAttributesWorkflow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("TEMPORAL_SERVICE_ADDRESS")
        .unwrap_or_else(|_| "http://localhost:7233".to_string());
    let namespace = std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "default".to_string());

    let connection =
        Connection::connect(ConnectionOptions::new(Url::from_str(&address)?).build()).await?;
    let client = Client::new(connection, ClientOptions::new(namespace).build())?;

    let mut search_attrs = HashMap::new();
    search_attrs.insert(
        "CustomKeywordField".to_string(),
        "initial-value".as_json_payload()?,
    );

    let handle = client
        .start_workflow(
            SearchAttributesWorkflow::run,
            (),
            WorkflowStartOptions::new("search-attributes", "search-attributes-workflow-id")
                .search_attributes(search_attrs)
                .build(),
        )
        .await?;

    println!("Started workflow, run_id: {:?}", handle.run_id());

    let result = handle
        .get_result(WorkflowGetResultOptions::default())
        .await?;
    println!("Workflow result: {result}");

    Ok(())
}
