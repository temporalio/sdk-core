//! Contains types that are needed by both the client and the sdk when configuring / interacting
//! with workers.

use crate::protos::{coresdk, temporal, temporal::api::enums::v1::VersioningBehavior};
use std::str::FromStr;

/// Specifies which task types a worker will poll for.
///
/// Workers can be configured to handle any combination of workflows, activities, and nexus operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct WorkerTaskTypes {
    pub enable_workflows: bool,
    pub enable_local_activities: bool,
    pub enable_remote_activities: bool,
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

    pub fn overlaps_with(&self, other: &WorkerTaskTypes) -> bool {
        (self.enable_workflows && other.enable_workflows)
            || (self.enable_local_activities && other.enable_local_activities)
            || (self.enable_remote_activities && other.enable_remote_activities)
            || (self.enable_nexus && other.enable_nexus)
    }
}

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

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct WorkerDeploymentVersion {
    /// Name of the deployment
    pub deployment_name: String,
    /// Build ID for the worker.
    pub build_id: String,
}

impl WorkerDeploymentVersion {
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
