use anyhow::{anyhow, bail};
use clap::Parser;
use std::{
    env,
    path::{Path, PathBuf},
    process::Stdio,
};
use temporal_sdk_core::ephemeral_server::{
    EphemeralExe, EphemeralExeVersion, TemporalDevServerConfigBuilder, TestServerConfigBuilder,
};
use temporal_sdk_core_test_utils::{
    INTEG_SERVER_TARGET_ENV_VAR, INTEG_TEMPORAL_DEV_SERVER_USED_ENV_VAR,
    INTEG_TEST_SERVER_USED_ENV_VAR, SEARCH_ATTR_INT, SEARCH_ATTR_TXT, default_cached_download,
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

    #[arg(long)]
    /// If set, only run the build, not any tests
    just_build: bool,

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
        just_build,
        harness_args,
    } = Cli::parse();
    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    // Try building first, so that we error early on build failures & don't start server
    let test_args_preamble = [
        "test",
        "--features",
        "temporal-sdk-core-protos/serde_serialize",
        "--test",
        &test_name,
    ]
    .into_iter()
    .map(ToString::to_string)
    .chain(cargo_test_args)
    .collect::<Vec<_>>();
    let mut build_cmd = Command::new(&cargo);
    strip_cargo_env_vars(&mut build_cmd);
    let status = build_cmd
        .args([test_args_preamble.as_slice(), &["--no-run".to_string()]].concat())
        .status()
        .await?;
    if !status.success() {
        bail!("Building integration tests failed!");
    }
    if just_build {
        return Ok(());
    }

    let (server, envs) = match server_kind {
        ServerKind::TemporalCLI => {
            let cli_version = if let Some(ver_override) = option_env!("CLI_VERSION_OVERRIDE") {
                EphemeralExe::CachedDownload {
                    version: EphemeralExeVersion::Fixed(ver_override.to_owned()),
                    dest_dir: None,
                    ttl: None,
                }
            } else {
                default_cached_download()
            };
            let config = TemporalDevServerConfigBuilder::default()
                .exe(cli_version)
                .extra_args(vec![
                    // TODO: Delete when temporalCLI enables it by default.
                    "--dynamic-config-value".to_string(),
                    "system.enableEagerWorkflowStart=true".to_string(),
                    "--dynamic-config-value".to_string(),
                    "system.enableNexus=true".to_string(),
                    "--dynamic-config-value".to_owned(),
                    "frontend.workerVersioningWorkflowAPIs=true".to_owned(),
                    "--dynamic-config-value".to_owned(),
                    "frontend.workerVersioningDataAPIs=true".to_owned(),
                    "--dynamic-config-value".to_owned(),
                    "system.enableDeploymentVersions=true".to_owned(),
                    "--http-port".to_string(),
                    "7243".to_string(),
                    "--search-attribute".to_string(),
                    format!("{SEARCH_ATTR_TXT}=Text"),
                    "--search-attribute".to_string(),
                    format!("{SEARCH_ATTR_INT}=Int"),
                ])
                .ui(true)
                .build()?;
            println!("Using temporal CLI: {:?}", config);
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
            println!("========================================================");
            println!("Not starting up a server. One should be running already.");
            println!("========================================================");
            (None, vec![])
        }
    };

    let mut cmd = Command::new(&cargo);
    strip_cargo_env_vars(&mut cmd);
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

/// Some env vars inherited from the fact that this is called with cargo can cause the *nested*
/// cargo calls to do unnecessary recompiles. Fix that.
fn strip_cargo_env_vars(build_cmd: &mut Command) {
    let envs = env::vars().filter(|(k, _)| k.starts_with("CARGO_"));
    for (k, _) in envs {
        build_cmd.env_remove(k);
    }
}

fn project_root() -> PathBuf {
    Path::new(&env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(1)
        .unwrap()
        .to_path_buf()
}
