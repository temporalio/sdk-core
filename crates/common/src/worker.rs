//! Contains types that are needed by both the client and the sdk when configuring / interacting
//! with workers.

use crate::protos::{coresdk, temporal, temporal::api::enums::v1::VersioningBehavior};
use std::{
    fs::File,
    io::{self, BufReader, Read},
    str::FromStr,
    sync::OnceLock,
};

/// Specifies which task types a worker will poll for.
///
/// Workers can be configured to handle any combination of workflows, activities, and nexus operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WorkerTaskTypes {
    /// Whether workflow tasks are enabled.
    pub enable_workflows: bool,
    /// Whether local activity tasks are enabled.
    pub enable_local_activities: bool,
    /// Whether remote activity tasks are enabled.
    pub enable_remote_activities: bool,
    /// Whether nexus tasks are enabled.
    pub enable_nexus: bool,
}

impl WorkerTaskTypes {
    /// Check if no task types are enabled
    pub fn is_empty(&self) -> bool {
        !self.enable_workflows
            && !self.enable_local_activities
            && !self.enable_remote_activities
            && !self.enable_nexus
    }

    /// Create a config with all task types enabled
    pub fn all() -> WorkerTaskTypes {
        WorkerTaskTypes {
            enable_workflows: true,
            enable_local_activities: true,
            enable_remote_activities: true,
            enable_nexus: true,
        }
    }

    /// Create a config with only workflow tasks enabled
    pub fn workflow_only() -> WorkerTaskTypes {
        WorkerTaskTypes {
            enable_workflows: true,
            enable_local_activities: false,
            enable_remote_activities: false,
            enable_nexus: false,
        }
    }

    /// Create a config with only activity tasks enabled
    pub fn activity_only() -> WorkerTaskTypes {
        WorkerTaskTypes {
            enable_workflows: false,
            enable_local_activities: false,
            enable_remote_activities: true,
            enable_nexus: false,
        }
    }

    /// Create a config with only nexus tasks enabled
    pub fn nexus_only() -> WorkerTaskTypes {
        WorkerTaskTypes {
            enable_workflows: false,
            enable_local_activities: false,
            enable_remote_activities: false,
            enable_nexus: true,
        }
    }

    /// Returns true if any task type is enabled in both configs.
    pub fn overlaps_with(&self, other: &WorkerTaskTypes) -> bool {
        (self.enable_workflows && other.enable_workflows)
            || (self.enable_local_activities && other.enable_local_activities)
            || (self.enable_remote_activities && other.enable_remote_activities)
            || (self.enable_nexus && other.enable_nexus)
    }
}

/// Configuration for worker deployment versioning.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct WorkerDeploymentOptions {
    /// The deployment version of this worker.
    pub version: WorkerDeploymentVersion,
    /// If set, opts in to the Worker Deployment Versioning feature, meaning this worker will only
    /// receive tasks for workflows it claims to be compatible with.
    pub use_worker_versioning: bool,
    /// The default versioning behavior to use for workflows that do not pass one to Core.
    /// It is a startup-time error to specify `Some(Unspecified)` here.
    pub default_versioning_behavior: Option<VersioningBehavior>,
}

impl WorkerDeploymentOptions {
    /// Create deployment options from just a build ID, without opting into worker versioning.
    pub fn from_build_id(build_id: String) -> Self {
        Self {
            version: WorkerDeploymentVersion {
                deployment_name: "".to_owned(),
                build_id,
            },
            use_worker_versioning: false,
            default_versioning_behavior: None,
        }
    }
}

/// Identifies a specific version of a worker deployment.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct WorkerDeploymentVersion {
    /// Name of the deployment
    pub deployment_name: String,
    /// Build ID for the worker.
    pub build_id: String,
}

impl WorkerDeploymentVersion {
    /// Returns true if both the deployment name and build ID are empty.
    pub fn is_empty(&self) -> bool {
        self.deployment_name.is_empty() && self.build_id.is_empty()
    }
}

impl FromStr for WorkerDeploymentVersion {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.split_once('.') {
            Some((name, build_id)) => Ok(WorkerDeploymentVersion {
                deployment_name: name.to_owned(),
                build_id: build_id.to_owned(),
            }),
            _ => Err(()),
        }
    }
}

impl From<WorkerDeploymentVersion> for coresdk::common::WorkerDeploymentVersion {
    fn from(v: WorkerDeploymentVersion) -> coresdk::common::WorkerDeploymentVersion {
        coresdk::common::WorkerDeploymentVersion {
            deployment_name: v.deployment_name,
            build_id: v.build_id,
        }
    }
}

impl From<coresdk::common::WorkerDeploymentVersion> for WorkerDeploymentVersion {
    fn from(v: coresdk::common::WorkerDeploymentVersion) -> WorkerDeploymentVersion {
        WorkerDeploymentVersion {
            deployment_name: v.deployment_name,
            build_id: v.build_id,
        }
    }
}

impl From<temporal::api::deployment::v1::WorkerDeploymentVersion> for WorkerDeploymentVersion {
    fn from(v: temporal::api::deployment::v1::WorkerDeploymentVersion) -> Self {
        Self {
            deployment_name: v.deployment_name,
            build_id: v.build_id,
        }
    }
}

static CACHED_BUILD_ID: OnceLock<String> = OnceLock::new();

/// Build ID derived from hashing the on-disk bytes of the current executable.
/// Deterministic across machines for the same binary. Cached per-process.
pub fn build_id_from_current_exe() -> &'static str {
    CACHED_BUILD_ID
        .get_or_init(|| compute_crc32_exe_id().unwrap_or_else(|_| "undetermined".to_owned()))
}

fn compute_crc32_exe_id() -> io::Result<String> {
    let exe_path = std::env::current_exe()?;
    let file = File::open(exe_path)?;
    let mut reader = BufReader::new(file);

    let mut hasher = crc32fast::Hasher::new();
    let mut buf = [0u8; 128 * 1024];

    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    let crc = hasher.finalize();

    Ok(format!("{:08x}", crc))
}
