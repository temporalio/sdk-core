//! Management of workflow tasks

use crate::worker::workflow::machines::WFMachinesError;
use std::{fmt::Debug, time::Instant};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{
            remove_from_cache::EvictionReason, QueryWorkflow, WorkflowActivation,
        },
        workflow_commands::QueryResult,
        workflow_completion::Failure,
    },
    temporal::api::{command::v1::Command as ProtoCommand, enums::v1::WorkflowTaskFailedCause},
    TaskToken,
};

/// What percentage of a WFT timeout we are willing to wait before sending a WFT heartbeat when
/// necessary.
const WFT_HEARTBEAT_TIMEOUT_FRACTION: f32 = 0.8;

#[derive(Clone, Debug)]
pub(crate) struct OutstandingTask {
    pub info: WorkflowTaskInfo,
    /// Set if the outstanding task has quer(ies) which must be fulfilled upon finishing replay
    pub pending_queries: Vec<QueryWorkflow>,
    pub start_time: Instant,
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum OutstandingActivation {
    /// A normal activation with a joblist
    Normal {
        /// True if there is an eviction in the joblist
        contains_eviction: bool,
        /// Number of jobs in the activation
        num_jobs: usize,
    },
    /// An activation for a legacy query
    LegacyQuery,
}

impl OutstandingActivation {
    pub(crate) const fn has_only_eviction(self) -> bool {
        matches!(
            self,
            OutstandingActivation::Normal {
                contains_eviction: true,
                num_jobs: nj
            }
        if nj == 1)
    }
    pub(crate) const fn has_eviction(self) -> bool {
        matches!(
            self,
            OutstandingActivation::Normal {
                contains_eviction: true,
                ..
            }
        )
    }
}

/// Contains important information about a given workflow task that we need to memorize while
/// lang handles it.
#[derive(Clone, Debug)]
pub struct WorkflowTaskInfo {
    pub task_token: TaskToken,
    pub attempt: u32,
}

#[derive(Debug, derive_more::From)]
pub(crate) enum NewWfTaskOutcome {
    /// A new activation for the workflow should be issued to lang
    IssueActivation(WorkflowActivation),
    /// The poll loop should be restarted, there is nothing to do
    TaskBuffered,
    /// The workflow task should be auto-completed with an empty command list, as it must be replied
    /// to but there is no meaningful work for lang to do.
    Autocomplete,
    /// The workflow task ran into problems while being applied and we must now evict the workflow
    Evict(WorkflowUpdateError),
    /// No action should be taken. Possibly we are waiting for local activities to complete
    LocalActsOutstanding,
}

#[derive(Debug)]
pub enum FailedActivationOutcome {
    NoReport,
    Report(TaskToken, WorkflowTaskFailedCause, Failure),
    ReportLegacyQueryFailure(TaskToken, Failure),
}

#[derive(Debug)]
pub(crate) struct ServerCommandsWithWorkflowInfo {
    pub task_token: TaskToken,
    pub action: ActivationAction,
}

#[derive(Debug)]
pub(crate) enum ActivationAction {
    /// We should respond that the workflow task is complete
    WftComplete {
        commands: Vec<ProtoCommand>,
        query_responses: Vec<QueryResult>,
        force_new_wft: bool,
    },
    /// We should respond to a legacy query request
    RespondLegacyQuery { result: QueryResult },
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub(crate) enum EvictionRequestResult {
    EvictionRequested(Option<u32>),
    NotFound,
    EvictionAlreadyRequested(Option<u32>),
}

#[derive(Debug)]
pub(crate) struct WorkflowUpdateError {
    /// Underlying workflow error
    pub source: WFMachinesError,
    /// The run id of the erring workflow
    #[allow(dead_code)] // Useful in debug output
    pub run_id: String,
}

impl WorkflowUpdateError {
    pub fn evict_reason(&self) -> EvictionReason {
        self.source.evict_reason()
    }
}

impl From<WorkflowMissingError> for WorkflowUpdateError {
    fn from(wme: WorkflowMissingError) -> Self {
        Self {
            source: WFMachinesError::Fatal("Workflow machines missing".to_string()),
            run_id: wme.run_id,
        }
    }
}

/// The workflow machines were expected to be in the cache but were not
#[derive(Debug)]
pub(crate) struct WorkflowMissingError {
    /// The run id of the erring workflow
    pub run_id: String,
}
