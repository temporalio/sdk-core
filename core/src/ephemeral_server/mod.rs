//! This module implements support for downloading and running ephemeral test
//! servers useful for testing.

use anyhow::anyhow;
use flate2::read::GzDecoder;
use futures::StreamExt;
use serde::Deserialize;
use std::{
    fs::OpenOptions,
    path::{Path, PathBuf},
};
use temporal_client::ClientOptionsBuilder;
use tokio::{
    task::spawn_blocking,
    time::{sleep, Duration},
};
use tokio_util::io::{StreamReader, SyncIoBridge};
use url::Url;
use zip::read::read_zipfile_from_stream;

#[cfg(target_family = "unix")]
use std::os::unix::fs::OpenOptionsExt;
use std::process::Stdio;

/// Configuration for Temporalite.
#[derive(Debug, Clone, derive_builder::Builder)]
pub struct TemporaliteConfig {
    /// Required path to executable or download info.
    pub exe: EphemeralExe,
    /// Namespace to use.
    #[builder(default = "\"default\".to_owned()")]
    pub namespace: String,
    /// IP to bind to.
    #[builder(default = "\"127.0.0.1\".to_owned()")]
    pub ip: String,
    /// Port to use or obtains a free one if none given.
    #[builder(default)]
    pub port: Option<u16>,
    /// Sqlite DB filename if persisting or non-persistent if none.
    #[builder(default)]
    pub db_filename: Option<String>,
    /// Whether to enable the UI.
    #[builder(default)]
    pub ui: bool,
    /// Log format and level
    #[builder(default = "(\"pretty\".to_owned(), \"warn\".to_owned())")]
    pub log: (String, String),
    /// Additional arguments to Temporalite.
    #[builder(default)]
    pub extra_args: Vec<String>,
}

impl TemporaliteConfig {
    /// Start a Temporalite server.
    pub async fn start_server(&self) -> anyhow::Result<EphemeralServer> {
        self.start_server_with_output(Stdio::inherit()).await
    }

    /// Start a Temporalite server with configurable stdout destination.
    pub async fn start_server_with_output(&self, output: Stdio) -> anyhow::Result<EphemeralServer> {
        // Get exe path
        let exe_path = self.exe.get_or_download("temporalite").await?;

        // Get free port if not already given
        let port = self.port.unwrap_or_else(|| get_free_port(&self.ip));

        // Build arg set
        let mut args = vec![
            "start".to_owned(),
            "--port".to_owned(),
            port.to_string(),
            "--namespace".to_owned(),
            self.namespace.clone(),
            "--ip".to_owned(),
            self.ip.clone(),
            "--log-format".to_owned(),
            self.log.0.clone(),
            "--log-level".to_owned(),
            self.log.1.clone(),
        ];
        if let Some(db_filename) = &self.db_filename {
            args.push("--filename".to_owned());
            args.push(db_filename.clone());
        } else {
            args.push("--ephemeral".to_owned());
        }
        if !self.ui {
            args.push("--headless".to_owned());
        }
        args.extend(self.extra_args.clone());

        // Start
        EphemeralServer::start(EphemeralServerConfig {
            exe_path,
            port,
            args,
            has_test_service: false,
            output,
        })
        .await
    }
}

/// Configuration for the test server.
#[derive(Debug, Clone, derive_builder::Builder)]
pub struct TestServerConfig {
    /// Required path to executable or download info.
    pub exe: EphemeralExe,
    /// Port to use or obtains a free one if none given.
    #[builder(default)]
    pub port: Option<u16>,
    /// Additional arguments to the test server.
    #[builder(default)]
    pub extra_args: Vec<String>,
}

impl TestServerConfig {
    /// Start a test server.
    pub async fn start_server(&self) -> anyhow::Result<EphemeralServer> {
        self.start_server_with_output(Stdio::inherit()).await
    }

    /// Start a test server with configurable stdout.
    pub async fn start_server_with_output(&self, output: Stdio) -> anyhow::Result<EphemeralServer> {
        // Get exe path
        let exe_path = self.exe.get_or_download("temporal-test-server").await?;

        // Get free port if not already given
        let port = self.port.unwrap_or_else(|| get_free_port("0.0.0.0"));

        // Build arg set
        let mut args = vec![port.to_string()];
        args.extend(self.extra_args.clone());

        // Start
        EphemeralServer::start(EphemeralServerConfig {
            exe_path,
            port,
            args,
            has_test_service: true,
            output,
        })
        .await
    }
}

struct EphemeralServerConfig {
    exe_path: PathBuf,
    port: u16,
    args: Vec<String>,
    has_test_service: bool,
    output: Stdio,
}

/// Server that will be stopped when dropped.
#[derive(Debug)]
pub struct EphemeralServer {
    /// gRPC target host:port for the server frontend.
    pub target: String,
    /// Whether the target implements the gRPC TestService
    pub has_test_service: bool,
    child: tokio::process::Child,
}

impl EphemeralServer {
    async fn start(config: EphemeralServerConfig) -> anyhow::Result<EphemeralServer> {
        // Start process
        // TODO(cretz): Offer stdio suppression?
        let child = tokio::process::Command::new(config.exe_path)
            .args(config.args)
            .stdin(Stdio::null())
            .stdout(config.output)
            .spawn()?;
        let target = format!("127.0.0.1:{}", config.port);
        let target_url = format!("http://{}", target);
        let success = Ok(EphemeralServer {
            target,
            has_test_service: config.has_test_service,
            child,
        });

        // Try to connect every 100ms for 5s
        // TODO(cretz): Some other way, e.g. via stdout, to know whether the
        // server is up?
        let client_options = ClientOptionsBuilder::default()
            .identity("online_checker".to_owned())
            .target_url(Url::parse(&target_url)?)
            .client_name("online-checker".to_owned())
            .client_version("0.1.0".to_owned())
            .build()?;
        for _ in 0..50 {
            sleep(Duration::from_millis(100)).await;
            if client_options
                .connect_no_namespace(None, None)
                .await
                .is_ok()
            {
                return success;
            }
        }
        Err(anyhow!("Failed connecting to test server after 5 seconds"))
    }

    /// Shutdown the server (i.e. kill the child process). This does not attempt
    /// a kill if the child process appears completed, but such a check is not
    /// atomic so a kill could still fail as completed if completed just before
    /// kill.
    #[cfg(not(target_family = "unix"))]
    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        // Only kill if there is a PID
        if self.child.id().is_some() {
            Ok(self.child.kill().await?)
        } else {
            Ok(())
        }
    }

    /// Shutdown the server (i.e. kill the child process). This does not attempt
    /// a kill if the child process appears completed, but such a check is not
    /// atomic so a kill could still fail as completed if completed just before
    /// kill.
    #[cfg(target_family = "unix")]
    pub async fn shutdown(&mut self) -> anyhow::Result<()> {
        // For whatever reason, Tokio is not properly waiting on result
        // after sending kill in some cases which is causing defunct zombie
        // processes to remain and kill() to hang. Therefore, we are sending
        // SIGKILL and waiting on the process ourselves using a low-level call.
        //
        // WARNING: This is based on empirical evidence starting a Python test
        // run on Linux with Python 3.7 (does not happen on Python 3.10 nor does
        // it happen on Temporalite nor does it happen in Rust integration
        // tests). Don't alter without running that scenario. EX: SIGINT works but not SIGKILL
        if let Some(pid) = self.child.id() {
            let nix_pid = nix::unistd::Pid::from_raw(pid as i32);
            Ok(spawn_blocking(move || {
                nix::sys::signal::kill(nix_pid, nix::sys::signal::Signal::SIGINT)?;
                nix::sys::wait::waitpid(Some(nix_pid), None)
            })
            .await?
            .map(|_| ())?)
        } else {
            Ok(())
        }
    }
}

/// Where to find an executable. Can be a path or download.
#[derive(Debug, Clone)]
pub enum EphemeralExe {
    /// Existing path on the filesystem for the executable.
    ExistingPath(String),
    /// Download the executable if not already there.
    CachedDownload {
        /// Which version to download.
        version: EphemeralExeVersion,
        /// Destination directory or the user temp directory if none set.
        dest_dir: Option<String>,
    },
}

/// Which version of the exe to download.
#[derive(Debug, Clone)]
pub enum EphemeralExeVersion {
    /// Use a default version for the given SDK name and version.
    Default {
        /// Name of the SDK to get the default for.
        sdk_name: String,
        /// Version of the SDK to get the default for.
        sdk_version: String,
    },
    /// Specific version.
    Fixed(String),
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct DownloadInfo {
    archive_url: String,
    file_to_extract: String,
}

impl EphemeralExe {
    async fn get_or_download(&self, artifact_name: &str) -> anyhow::Result<PathBuf> {
        match self {
            EphemeralExe::ExistingPath(exe_path) => {
                let path = PathBuf::from(exe_path);
                if !path.exists() {
                    return Err(anyhow!("Exe path does not exist"));
                }
                Ok(path)
            }
            EphemeralExe::CachedDownload { version, dest_dir } => {
                let dest_dir = dest_dir
                    .as_ref()
                    .map(PathBuf::from)
                    .unwrap_or_else(std::env::temp_dir);
                let (platform, out_ext) = match std::env::consts::OS {
                    "windows" => ("windows", ".exe"),
                    "macos" => ("darwin", ""),
                    _ => ("linux", ""),
                };
                // Create dest file based on SDK name/version or fixed version
                let dest = dest_dir.join(match version {
                    EphemeralExeVersion::Default {
                        sdk_name,
                        sdk_version,
                    } => format!("{}-{}-{}{}", artifact_name, sdk_name, sdk_version, out_ext),
                    EphemeralExeVersion::Fixed(version) => {
                        format!("{}-{}{}", artifact_name, version, out_ext)
                    }
                });
                debug!(
                    "Lazily downloading or using existing exe at {}",
                    dest.display()
                );

                // If it already exists, skip
                if dest.exists() {
                    return Ok(dest);
                }

                // Get info about the proper archive and in-archive file
                let arch = match std::env::consts::ARCH {
                    "x86_64" => "amd64",
                    "arm" | "aarch64" => "arm64",
                    other => return Err(anyhow!("Unsupported arch: {}", other)),
                };
                let mut get_info_params = vec![("arch", arch), ("platform", platform)];
                let version_name = match version {
                    EphemeralExeVersion::Default {
                        sdk_name,
                        sdk_version,
                    } => {
                        get_info_params.push(("sdk-name", sdk_name.as_str()));
                        get_info_params.push(("sdk-version", sdk_version.as_str()));
                        "default"
                    }
                    EphemeralExeVersion::Fixed(version) => version,
                };
                let client = reqwest::Client::new();
                let info: DownloadInfo = client
                    .get(format!(
                        "https://temporal.download/{}/{}",
                        artifact_name, version_name
                    ))
                    .query(&get_info_params)
                    .send()
                    .await?
                    .json()
                    .await?;

                // Attempt download, looping because it could have waited for
                // concurrent one to finish
                loop {
                    if lazy_download_exe(
                        &client,
                        &info.archive_url,
                        Path::new(&info.file_to_extract),
                        &dest,
                    )
                    .await?
                    {
                        return Ok(dest);
                    }
                }
            }
        }
    }
}

fn get_free_port(bind_ip: &str) -> u16 {
    // Can just ask OS to give us a port then close socket. OS's don't give that
    // port back to anyone else anytime soon.
    std::net::TcpListener::bind(format!("{}:0", bind_ip))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Returns false if we successfully waited for another download to complete, or
/// true if the destination is known to exist. Should call again if false is
/// returned.
async fn lazy_download_exe(
    client: &reqwest::Client,
    uri: &str,
    file_to_extract: &Path,
    dest: &Path,
) -> anyhow::Result<bool> {
    // If it already exists, do not extract
    if dest.exists() {
        return Ok(true);
    }

    // We only want to download if we're not already downloading. To avoid some
    // kind of global lock, we'll just create the file eagerly w/ a temp
    // filename and delete it on failure or move it on success. If the temp file
    // already exists, we'll wait a bit and re-run this.
    let temp_dest_str = format!("{}{}", dest.to_str().unwrap(), ".downloading");
    let temp_dest = Path::new(&temp_dest_str);
    // Try to open file, using a file mode on unix families
    #[cfg(target_family = "unix")]
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o755)
        .open(temp_dest);
    #[cfg(not(target_family = "unix"))]
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(temp_dest);
    // This match only gets Ok if the file was downloaded and extracted to the
    // temporary path
    match file {
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            // Since it already exists, we'll try once a second for 20 seconds
            // to wait for it to be done, then return false so the caller can
            // try again.
            for _ in 0..20 {
                sleep(Duration::from_secs(1)).await;
                if !temp_dest.exists() {
                    return Ok(false);
                }
            }
            Err(anyhow!(
                "Temp download file at {} not complete after 20 seconds. \
                Make sure another download isn't running for too long and delete the temp file.",
                temp_dest.display()
            ))
        }
        Err(err) => Err(err.into()),
        // If the dest was added since, just remove temp file
        Ok(_) if dest.exists() => {
            std::fs::remove_file(temp_dest)?;
            return Ok(true);
        }
        // Download and extract the binary
        Ok(mut temp_file) => {
            info!("Downloading {} to {}", uri, dest.display());
            download_and_extract(client, uri, file_to_extract, &mut temp_file)
                .await
                .map_err(|err| {
                    // Failed to download, just remove file
                    if let Err(err) = std::fs::remove_file(temp_dest) {
                        warn!(
                            "Failed removing temp file at {}: {:?}",
                            temp_dest.display(),
                            err
                        );
                    }
                    err
                })
        }
    }?;
    // Now that file should be dropped, we can rename
    std::fs::rename(temp_dest, dest)?;
    Ok(true)
}

async fn download_and_extract(
    client: &reqwest::Client,
    uri: &str,
    file_to_extract: &Path,
    dest: &mut std::fs::File,
) -> anyhow::Result<()> {
    // Start download. We are using streaming here to extract the file from the
    // tarball or zip instead of loading into memory for Cursor/Seek.
    let resp = client.get(uri).send().await?;
    // We have to map the error type to an io error
    let stream = resp
        .bytes_stream()
        .map(|item| item.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err)));

    // Since our tar/zip impls use sync IO, we have to create a bridge and run
    // in a blocking closure.
    let mut reader = SyncIoBridge::new(StreamReader::new(stream));
    let tarball = if uri.ends_with(".tar.gz") {
        true
    } else if uri.ends_with(".zip") {
        false
    } else {
        return Err(anyhow!("URI not .tar.gz or .zip"));
    };
    let file_to_extract = file_to_extract.to_path_buf();
    let mut dest = dest.try_clone()?;

    spawn_blocking(move || {
        if tarball {
            for entry in tar::Archive::new(GzDecoder::new(reader)).entries()? {
                let mut entry = entry?;
                if entry.path()? == file_to_extract {
                    std::io::copy(&mut entry, &mut dest)?;
                    return Ok(());
                }
            }
            Err(anyhow!("Unable to find file in tarball"))
        } else {
            loop {
                // This is the way to stream a zip file without creating an archive
                // that requires Seek.
                if let Some(mut file) = read_zipfile_from_stream(&mut reader)? {
                    // If this is the file we're expecting, extract it
                    if file.enclosed_name() == Some(&file_to_extract) {
                        std::io::copy(&mut file, &mut dest)?;
                        return Ok(());
                    }
                } else {
                    return Err(anyhow!("Unable to find file in zip"));
                }
            }
        }
    })
    .await?
}
