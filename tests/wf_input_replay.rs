use anyhow::Context;
use clap::Parser;
use futures_util::StreamExt;
use std::path::PathBuf;
use temporal_sdk_core::replay_wf_state_inputs;
use temporal_sdk_core_test_utils::{init_integ_telem, wf_input_saver::read_from_file};

#[derive(clap::Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Path to file containing the saved wf input data
    replay_data: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    init_integ_telem();
    let Cli { replay_data } = Cli::parse();
    let replay_dat = read_from_file(replay_data)
        .await
        .context("while reading replay data file")?;

    replay_wf_state_inputs(
        replay_dat.config,
        replay_dat
            .inputs
            .map(|r| r.expect("Reading bytes from file works").to_vec()),
    )
    .await;

    Ok(())
}
