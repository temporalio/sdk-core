use anyhow::anyhow;
use flate2::read::GzDecoder;
use hyper::{body, body::Buf, Client};
#[cfg(target_family = "unix")]
use std::os::unix::fs::OpenOptionsExt;
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};
use temporal_client::ClientOptionsBuilder;
use tokio::time::{sleep, Duration};
use url::Url;
use zip::read::read_zipfile_from_stream;

#[derive(Debug, Clone, derive_builder::Builder)]
pub struct TemporaliteServerConfig {
    #[builder(default)]
    pub namespace: String,

    #[builder(default)]
    pub ip: Option<String>,

    #[builder(default)]
    pub port: Option<u16>,

    #[builder(default)]
    pub db_filename: Option<String>,

    #[builder(default)]
    pub ui: bool,

    #[builder(default)]
    pub extra_args: Vec<String>,

    #[builder(default)]
    pub exe_path: Option<String>,

    #[builder(default)]
    pub download_version: Option<String>,
}

impl TemporaliteServerConfigBuilder {
    pub fn to_args() -> Vec<String> {
        todo!()
    }
}

pub struct TemporaliteServer {}

impl TemporaliteServer {
    // fn start
}

#[derive(Debug, Clone, derive_builder::Builder)]
pub struct TestServerConfig {
    #[builder(default)]
    pub port: Option<u16>,

    #[builder(default)]
    pub extra_args: Vec<String>,

    #[builder(default)]
    pub exe_path: Option<String>,

    #[builder(default)]
    pub download_version: Option<String>,

    #[builder(default)]
    pub download_dest_dir: Option<String>,
}

pub struct TestServer {
    target: String,
    child: tokio::process::Child,
}

impl TestServer {
    pub async fn start(config: TestServerConfig) -> anyhow::Result<TestServer> {
        // Use exe path or download and extract
        let exe_path = if let Some(exe_path) = config.exe_path {
            let path = PathBuf::from(exe_path);
            if !path.exists() {
                return Err(anyhow!("Exe path does not exist"));
            }
            path
        } else if let Some(download_version) = config.download_version {
            // Use given dest dir or use temp dir
            let dest_dir = if let Some(download_dest_dir) = config.download_dest_dir {
                PathBuf::from(download_dest_dir)
            } else {
                std::env::temp_dir()
            };

            // Build URI from platform specific components
            let (platform, download_ext, out_ext) = match std::env::consts::OS {
                "windows" => ("windows", ".zip", ".exe"),
                "macos" => ("macOS", ".tar.gz", ""),
                _ => ("linux", ".tar.gz", ""),
            };
            // We intentionally always choose amd64 even in cases of ARM
            // processors because we don't have native ARM binaries yet and some
            // systems like M1 can run the amd64 one
            let name = format!(
                "temporal-test-server_{}_{}_amd64",
                download_version, platform
            );
            let uri = format!(
                "https://github.com/temporalio/sdk-java/releases/download/v{}/{}{}",
                download_version, name, download_ext
            );
            let dest = dest_dir.join(format!(
                "temporal-test-server-{}{}",
                download_version, out_ext
            ));
            let file_to_extract: PathBuf = [name, format!("temporal-test-server{}", out_ext)]
                .iter()
                .collect();

            info!(
                "Downloading {} to extract test server to {}",
                uri,
                dest.display()
            );
            lazy_download_exe(&uri, &file_to_extract, &dest).await?;
            dest
        } else {
            return Err(anyhow!("Must have either exe path or download version"));
        };

        // Get port
        let port = config.port.unwrap_or_else(get_free_port);

        // Start process
        // TODO(cretz): Offer stdio suppression?
        let mut args = vec![port.to_string()];
        args.extend(config.extra_args);
        let child = tokio::process::Command::new(exe_path)
            .args(args)
            .kill_on_drop(true)
            .spawn()?;

        // Try to connect every 100ms for 5s
        let target = format!("127.0.0.1:{}", port);
        let target_url = format!("http://{}", target);
        let client_options = ClientOptionsBuilder::default()
            .target_url(Url::parse(&target_url)?)
            .build()?;
        for _ in 0..50 {
            sleep(Duration::from_millis(100)).await;
            if client_options
                .connect_no_namespace(None, None)
                .await
                .is_ok()
            {
                return Ok(TestServer { target, child });
            }
        }
        Err(anyhow!("Failed connecting to test server after 5 seconds"))
    }
}

fn get_free_port() -> u16 {
    // Can just ask OS to give us a port then close socket. OS's don't give that
    // port back to anyone else anytime soon.
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn lazy_download_exe(uri: &str, file_to_extract: &Path, dest: &Path) -> anyhow::Result<()> {
    loop {
        if lazy_download_exe_attempt(uri, file_to_extract, dest).await? {
            return Ok(());
        }
    }
}

// Returns false if we successfully waited for another download to complete, or
// true if the destination is known to exist. Should call again if false is
// returned.
async fn lazy_download_exe_attempt(
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
    // This match only gets Ok if the file was downloaded and extracted to the
    // temporary path
    match new_executable_file(temp_dest) {
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            // Since it already exists, we'll try once a second for 20 seconds
            // to wait for it to be done, then recurse
            for _ in 0..20 {
                sleep(Duration::from_secs(1)).await;
                if !temp_dest.exists() {
                    return Ok(false);
                }
            }
            Err(anyhow!(
                "Temp download file at {} not complete after 20 seconds",
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
        Ok(mut temp_file) => download_and_extract(uri, file_to_extract, &mut temp_file)
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
            }),
    }?;
    // Now that file should be dropped, we can rename
    std::fs::rename(temp_dest, dest)?;
    Ok(true)
}

#[cfg(target_family = "unix")]
fn new_executable_file(file: &Path) -> std::io::Result<File> {
    OpenOptions::new().create_new(true).mode(0o755).open(file)
}

#[cfg(not(target_family = "unix"))]
fn new_executable_file(file: &Path) -> std::io::Result<File> {
    OpenOptions::new().create_new(true).open(file)
}

async fn download_and_extract(
    uri: &str,
    file_to_extract: &Path,
    dest: &mut std::fs::File,
) -> anyhow::Result<()> {
    // Start download. We are using streaming here to extract the file from the
    // tarball or zip instead of loading into memory for Cursor/Seek.
    let resp = Client::new().get(uri.parse()?).await?;
    let buf = body::aggregate(resp.into_body()).await?;
    let mut reader = buf.reader();

    // Extract from tarball or zip. We use sync IO here because the file
    // compression libraries use sync IO and it isn't worth using the Tokio sync
    // bridge at this time. This should not block a thread for _too_ long.
    if uri.ends_with(".tar.gz") {
        for entry in tar::Archive::new(GzDecoder::new(reader)).entries()? {
            let mut entry = entry?;
            if entry.path()? == file_to_extract {
                std::io::copy(&mut entry, dest)?;
                return Ok(());
            }
        }
        return Err(anyhow!("Unable to find file in tarball"));
    } else if uri.ends_with(".zip") {
        loop {
            // This is the way to stream a zip file without creating an archive
            // that requires Seek.
            if let Some(mut file) = read_zipfile_from_stream(&mut reader)? {
                // If this is the file we're expecting, extract it
                if file.enclosed_name() == Some(file_to_extract) {
                    std::io::copy(&mut file, dest)?;
                    return Ok(());
                }
            } else {
                return Err(anyhow!("Unable to find file in zip"));
            }
        }
    } else {
        return Err(anyhow!("URI not .tar.gz or .zip"));
    }
}
