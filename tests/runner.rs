use anyhow::{anyhow, bail};
use clap::Parser;
use std::{
    env,
    path::{Path, PathBuf},
    process::Stdio,
};
use temporal_sdk_core::ephemeral_server::{
    TemporalDevServerConfigBuilder, TestServerConfigBuilder,
};
use temporal_sdk_core_test_utils::{
    default_cached_download, INTEG_SERVER_TARGET_ENV_VAR, INTEG_TEMPORAL_DEV_SERVER_USED_ENV_VAR,
    INTEG_TEST_SERVER_USED_ENV_VAR, SEARCH_ATTR_INT, SEARCH_ATTR_TXT,
};
use tokio::{self, process::Command};

#[derive(clap::Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Test harness to run. Anything defined as a `[[test]]` in core's `Cargo.toml` is valid.
    #[arg(short, long, default_value = "integ_tests")]
    test_name: String,

    /// What kind of server to auto-launch, if any
    #[arg(short, long, value_enum, default_value = "temporal-cli")]
    server_kind: ServerKind,

    /// Arguments to pass through to the `cargo test` command. Ex: `--release`
    #[arg(short, long, allow_hyphen_values(true))]
    cargo_test_args: Vec<String>,

    /// The rest of the arguments will be passed through to the test harness
    harness_args: Vec<String>,
}

#[derive(Copy, Clone, PartialEq, Eq, clap::ValueEnum)]
enum ServerKind {
    /// Use Temporal-cli
    TemporalCLI,
    /// Use the Java test server
    TestServer,
    /// Do not automatically start any server
    External,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let Cli {
        test_name,
        server_kind,
        cargo_test_args,
        harness_args,
    } = Cli::parse();
    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    // Try building first, so that we error early on build failures & don't start server
    let test_args_preamble = ["test", "--test", &test_name]
        .into_iter()
        .map(ToString::to_string)
        .chain(cargo_test_args)
        .collect::<Vec<_>>();
    let status = Command::new(&cargo)
        .args([test_args_preamble.as_slice(), &["--no-run".to_string()]].concat())
        .status()
        .await?;
    if !status.success() {
        bail!("Building integration tests failed!");
    }

    let (server, envs) = match server_kind {
        ServerKind::TemporalCLI => {
            let config = TemporalDevServerConfigBuilder::default()
                .exe(default_cached_download())
                .extra_args(vec![
                    // TODO: Delete when temporalCLI enables it by default.
                    "--dynamic-config-value".to_string(),
                    "system.enableEagerWorkflowStart=true".to_string(),
                    "--search-attribute".to_string(),
                    format!("{SEARCH_ATTR_TXT}=Text"),
                    "--search-attribute".to_string(),
                    format!("{SEARCH_ATTR_INT}=Int"),
                ])
                .ui(true)
                .build()?;
            println!("Using temporal CLI");
            (
                Some(
                    config
                        .start_server_with_output(Stdio::null(), Stdio::null())
                        .await?,
                ),
                vec![(INTEG_TEMPORAL_DEV_SERVER_USED_ENV_VAR, "true")],
            )
        }
        ServerKind::TestServer => {
            let config = TestServerConfigBuilder::default()
                .exe(default_cached_download())
                .build()?;
            println!("Using java test server");
            (
                Some(
                    config
                        .start_server_with_output(Stdio::null(), Stdio::null())
                        .await?,
                ),
                vec![(INTEG_TEST_SERVER_USED_ENV_VAR, "true")],
            )
        }
        ServerKind::External => {
            println!("Not starting up a server. One should be running already.");
            (None, vec![])
        }
    };

    let mut cmd = Command::new(&cargo);
    if let Some(srv) = server.as_ref() {
        println!("Running on {}", srv.target);
        cmd.env(
            INTEG_SERVER_TARGET_ENV_VAR,
            format!("http://{}", &srv.target),
        );
    }
    let status = cmd
        .envs(envs)
        .current_dir(project_root())
        .args(
            test_args_preamble
                .into_iter()
                .chain(["--".to_string()])
                .chain(harness_args),
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
