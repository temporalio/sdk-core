use anyhow::{anyhow, bail};
use std::{
    env,
    env::args,
    path::{Path, PathBuf},
    process::Stdio,
};
use temporal_sdk_core::ephemeral_server::{TemporaliteConfigBuilder, TestServerConfigBuilder};
use temporal_sdk_core_test_utils::{
    default_cached_download, INTEG_SERVER_TARGET_ENV_VAR, INTEG_TEMPORALITE_USED_ENV_VAR,
    INTEG_TEST_SERVER_USED_ENV_VAR,
};
use tokio::{self, process::Command};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let server_type = env::var("INTEG_SERVER_TYPE")
        .unwrap_or_else(|_| "temporalite".to_string())
        .to_lowercase();
    // Try building first, so that we error early on build failures & don't start server
    let status = Command::new(&cargo)
        .args(["test", "--test", "integ_tests", "--no-run"])
        .status()
        .await?;
    if !status.success() {
        bail!("Building integration tests failed!");
    }

    // Move to clap if we start doing any more complicated input
    let (server, envs) = if server_type == "test-server" {
        let config = TestServerConfigBuilder::default()
            .exe(default_cached_download())
            .build()?;
        println!("Using java test server");
        (
            Some(config.start_server_with_output(Stdio::null()).await?),
            vec![(INTEG_TEST_SERVER_USED_ENV_VAR, "true")],
        )
    } else if server_type == "temporalite" {
        let config = TemporaliteConfigBuilder::default()
            .exe(default_cached_download())
            .build()?;
        println!("Using temporalite");
        (
            Some(config.start_server_with_output(Stdio::null()).await?),
            vec![(INTEG_TEMPORALITE_USED_ENV_VAR, "true")],
        )
    } else {
        println!("Not starting up a server. One should be running already.");
        (None, vec![])
    };

    // Run the integ tests, passing through arguments
    let mut args = args();
    // Shift off binary name
    args.next();
    let mut cmd = Command::new(&cargo);
    if let Some(srv) = server.as_ref() {
        cmd.env(
            INTEG_SERVER_TARGET_ENV_VAR,
            format!("http://{}", &srv.target),
        );
    }
    let status = cmd
        .envs(envs)
        .current_dir(project_root())
        .args(
            ["test", "--test", "integ_tests"]
                .into_iter()
                .map(ToString::to_string)
                .chain(args),
        )
        .status()
        .await?;

    if let Some(mut srv) = server {
        srv.shutdown().await?;
    }
    if status.success() {
        Ok(())
    } else {
        Err(anyhow!("Integ tests failed!"))
    }
}

fn project_root() -> PathBuf {
    Path::new(&env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(1)
        .unwrap()
        .to_path_buf()
}
