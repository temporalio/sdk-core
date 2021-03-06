mod bridge;
mod concurrency_manager;
mod driven_workflow;
mod history_update;

pub(crate) use bridge::WorkflowBridge;
pub(crate) use concurrency_manager::WorkflowConcurrencyManager;
pub(crate) use driven_workflow::{ActivationListener, DrivenWorkflow, WorkflowFetcher};
pub(crate) use history_update::HistoryUpdate;

use crate::{
    machines::{ProtoCommand, WFCommand, WFMachinesError, WorkflowMachines},
    protos::{
        coresdk::workflow_activation::WfActivation, temporal::api::common::v1::WorkflowExecution,
    },
};
use std::sync::mpsc::{SendError, Sender};

pub(crate) const LEGACY_QUERY_ID: &str = "legacy_query";
type Result<T, E = WorkflowError> = std::result::Result<T, E>;

/// Errors relating to workflow management and machine logic. These are going to be passed up and
/// out to the lang SDK where it will need to handle them. Generally that will usually mean
/// showing an error to the user and/or invalidating the workflow cache.
#[derive(thiserror::Error, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum WorkflowError {
    /// The workflow machines associated with `run_id` were not found in memory
    #[error("Workflow machines associated with `{run_id}` not found")]
    MissingMachine { run_id: String },
    /// Underlying error in state machines
    #[error("Underlying error in state machines: {0:?}")]
    UnderlyingMachinesError(#[from] WFMachinesError),
    /// Error buffering commands coming in from the lang side. This shouldn't happen unless we've
    /// run out of memory or there is a logic bug. Considered fatal.
    #[error("Internal error buffering workflow commands")]
    CommandBufferingError(#[from] SendError<Vec<WFCommand>>),
    /// We tried to instantiate a workflow instance, but the provided history resulted in nothing
    /// for lang to do.
    #[error("Machine created with no activations for run_id {run_id}")]
    MachineWasCreatedWithNoJobs { run_id: String },
    /// Lang responded with other commands in addition to a legacy query response, which is not
    /// allowed.
    #[error("Lang responded with other commands in addition to legacy query response")]
    LegacyQueryResponseIncludedOtherCommands,
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub(crate) enum CommandID {
    Timer(String),
    Activity(String),
}

/// Manages an instance of a [WorkflowMachines], which is not thread-safe, as well as other data
/// associated with that specific workflow run.
pub(crate) struct WorkflowManager {
    machines: WorkflowMachines,
    /// Is always `Some` in normal operation. Optional to allow for unit testing with the test
    /// workflow driver, which does not need to complete activations the normal way.
    command_sink: Option<Sender<Vec<WFCommand>>>,
}

impl WorkflowManager {
    /// Create a new workflow manager given workflow history and execution info as would be found
    /// in [PollWorkflowTaskQueueResponse]
    pub fn new(history: HistoryUpdate, workflow_execution: WorkflowExecution) -> Self {
        let (wfb, cmd_sink) = WorkflowBridge::new();
        let state_machines = WorkflowMachines::new(
            workflow_execution.workflow_id,
            workflow_execution.run_id,
            history,
            Box::new(wfb).into(),
        );
        Self {
            machines: state_machines,
            command_sink: Some(cmd_sink),
        }
    }

    #[cfg(test)]
    pub fn new_from_machines(workflow_machines: WorkflowMachines) -> Self {
        Self {
            machines: workflow_machines,
            command_sink: None,
        }
    }
}

#[derive(Debug)]
pub struct OutgoingServerCommands {
    pub commands: Vec<ProtoCommand>,
    pub replaying: bool,
}

impl WorkflowManager {
    /// Given history that was just obtained from the server, pipe it into this workflow's machines.
    ///
    /// Should only be called when a workflow has caught up on replay (or is just beginning). It
    /// will return a workflow activation if one is needed, as well as a bool indicating if there
    /// are more workflow tasks that need to be performed to replay the remaining history.
    pub fn feed_history_from_server(&mut self, update: HistoryUpdate) -> Result<WfActivation> {
        self.machines.new_history_from_server(update)?;
        Ok(self.machines.get_wf_activation())
    }

    /// Fetch the next workflow activation for this workflow if one is required. Doing so will apply
    /// the next unapplied workflow task if such a sequence exists in history we already know about.
    /// Callers may also need to call [get_server_commands] after this to issue any pending commands
    /// to the server.
    pub fn get_next_activation(&mut self) -> Result<WfActivation> {
        // First check if there are already some pending jobs, which can be a result of replay.
        let activation = self.machines.get_wf_activation();
        if !activation.jobs.is_empty() {
            return Ok(activation);
        }

        self.machines.apply_next_wft_from_history()?;
        Ok(self.machines.get_wf_activation())
    }

    /// Typically called after [get_next_activation], use this to retrieve commands to be sent to
    /// the server which been generated by the machines since it was last called.
    pub fn get_server_commands(&mut self) -> OutgoingServerCommands {
        OutgoingServerCommands {
            commands: self.machines.get_commands(),
            replaying: self.machines.replaying,
        }
    }

    /// During testing it can be useful to run through all activations to simulate replay easily.
    /// Returns the last produced activation with jobs in it, or an activation with no jobs if
    /// the first call had no jobs.
    ///
    /// This is meant to be used with the TestWorkflowDriver which can automatically produce
    /// commands.
    #[cfg(test)]
    pub fn process_all_activations(&mut self) -> Result<WfActivation> {
        let mut last_act = self.get_next_activation()?;
        let mut next_act = self.get_next_activation()?;
        while !next_act.jobs.is_empty() {
            last_act = next_act;
            next_act = self.get_next_activation()?;
        }
        Ok(last_act)
    }

    /// Feed the workflow machines new commands issued by the executing workflow code, and iterate
    /// the machines.
    pub fn push_commands(&mut self, cmds: Vec<WFCommand>) -> Result<()> {
        if let Some(cs) = self.command_sink.as_mut() {
            cs.send(cmds)?;
        }
        self.machines.iterate_machines()?;
        Ok(())
    }
}

/// Determines when workflows are kept in the cache or evicted
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum WorkflowCachingPolicy {
    /// Workflows are cached until evicted explicitly or the cache size limit is reached, in which
    /// case they are evicted by least-recently-used ordering.
    Sticky {
        /// The maximum number of workflows that will be kept in the cache
        max_cached_workflows: usize,
    },
    /// Workflows are evicted after each workflow task completion. Note that this is *not* after
    /// each workflow activation - there are often multiple activations per workflow task.
    NonSticky,

    /// Not a real mode, but good for imitating crashes. Evict workflows after *every* reply,
    /// even if there are pending activations
    #[cfg(test)]
    AfterEveryReply,
}
