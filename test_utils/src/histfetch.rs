//! Use this binary to fetch histories as proto-encoded binary. The only provided argument is a
//! workflow ID. The history is written to `{workflow_id}_history.bin`.
//!
//! We can use `clap` if this needs more arguments / other stuff later on.

use prost::Message;
use temporal_client::ServerGatewayApis;
use test_utils::get_integ_server_options;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let gw_opts = get_integ_server_options();
    let gateway = gw_opts.connect(None).await?;
    let wf_id = std::env::args()
        .nth(1)
        .expect("must provide workflow id as only argument");
    let hist = gateway
        .get_workflow_execution_history(wf_id.clone(), None, vec![])
        .await?
        .history
        .expect("history field must be populated");
    // Serialize history to file
    let byteified = hist.encode_to_vec();
    tokio::fs::write(format!("{}_history.bin", wf_id), &byteified).await?;
    Ok(())
}
