use prost::Message;
use temporal_client::ServerGatewayApis;
use test_utils::get_integ_server_options;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let gw_opts = get_integ_server_options();
    let gateway = gw_opts.connect(None).await?;
    let hist = gateway
        .get_workflow_execution_history("fail_wf_task_PITR/89Y".to_string(), None, vec![])
        .await?
        .history
        .expect("history field must be populated");
    // Serialize history to file
    let byteified = hist.encode_to_vec();
    tokio::fs::write("history_bytes.bin", &byteified).await?;
    Ok(())
}
