//! Use this binary to fetch histories as proto-encoded binary. The first argument must be a
//! workflow ID. A run id may optionally be provided as the second arg. The history is written to
//! `{workflow_id}_history.bin`.
//!
//! We can use `clap` if this needs more arguments / other stuff later on.

use prost::Message;
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, WorkflowClientTrait,
};
use url::Url;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let copts = ConnectionOptions::new(Url::try_from("http://localhost:7233").unwrap())
        .client_name("histfetch")
        .client_version("0.0")
        .build();
    let connection = Connection::connect(copts).await?;
    let client = Client::new(
        connection,
        ClientOptions::builder().namespace("default").build(),
    );
    let wf_id = std::env::args()
        .nth(1)
        .expect("must provide workflow id as only argument");
    let run_id = std::env::args().nth(2);
    let hist = client
        .get_workflow_execution_history(wf_id.clone(), run_id, vec![])
        .await?
        .history
        .expect("history field must be populated");
    // Serialize history to file
    let byteified = hist.encode_to_vec();
    tokio::fs::write(format!("{wf_id}_history.bin"), &byteified).await?;
    Ok(())
}
