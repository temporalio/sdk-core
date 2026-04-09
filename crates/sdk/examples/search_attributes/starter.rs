mod workflows;

use std::collections::HashMap;
use temporalio_client::{
    Client, ClientOptions, Connection, WorkflowGetResultOptions, WorkflowStartOptions,
    envconfig::LoadClientConfigProfileOptions,
};
use temporalio_common::protos::coresdk::AsJsonPayloadExt;
use workflows::SearchAttributesWorkflow;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (conn_opts, client_opts) =
        ClientOptions::load_from_config(LoadClientConfigProfileOptions::default())?;
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(connection, client_opts)?;

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
