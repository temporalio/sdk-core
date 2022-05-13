//! Management of workflow tasks

use crate::worker::workflow::FailRunUpdateResponse;
use std::{fmt::Debug, time::Instant};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{QueryWorkflow, WorkflowActivation},
        workflow_commands::QueryResult,
        workflow_completion::Failure,
    },
    temporal::api::{command::v1::Command as ProtoCommand, enums::v1::WorkflowTaskFailedCause},
    TaskToken,
};
use tokio::sync::OwnedSemaphorePermit;

/// What percentage of a WFT timeout we are willing to wait before sending a WFT heartbeat when
/// necessary.
pub(crate) const WFT_HEARTBEAT_TIMEOUT_FRACTION: f32 = 0.8;

#[derive(Debug)]
pub(crate) struct OutstandingTask {
    pub info: WorkflowTaskInfo,
    pub hit_cache: bool,
    /// Set if the outstanding task has quer(ies) which must be fulfilled upon finishing replay
    pub pending_queries: Vec<QueryWorkflow>,
    pub start_time: Instant,
    /// The WFT permit owned by this task, ensures we don't exceed max concurrent WFT, and makes
    /// sure the permit is automatically freed when we delete the task.
    pub _permit: OwnedSemaphorePermit,
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
    /// A fake activation which is never sent to lang, but used internally
    Autocomplete,
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
pub(crate) enum RunUpdateOutcome {
    /// A new activation for the workflow should be issued to lang
    IssueActivation(WorkflowActivation),
    /// The workflow task should be auto-completed with an empty command list, as it must be replied
    /// to but there is no meaningful work for lang to do. For example, we are waiting on a signal
    /// or we are WFT heartbeating.
    Autocomplete { run_id: String },
    /// The run instance ran into problems while being applied and we must now evict the workflow,
    /// and possibly fail the workflow task.
    Failure(FailRunUpdateResponse),
    /// Nothing needs to be done as a result of the update
    DoNothing,
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
