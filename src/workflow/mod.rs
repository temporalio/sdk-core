mod bridge;
mod concurrency_manager;
mod driven_workflow;

pub(crate) use bridge::WorkflowBridge;
pub(crate) use concurrency_manager::WorkflowConcurrencyManager;
pub(crate) use driven_workflow::{ActivationListener, DrivenWorkflow, WorkflowFetcher};

use crate::{
    machines::{ProtoCommand, WFCommand, WFMachinesError, WorkflowMachines},
    protos::{
        coresdk::workflow_activation::WfActivation,
        temporal::api::{common::v1::WorkflowExecution, history::v1::History},
    },
    protosext::{HistoryInfo, HistoryInfoError},
};
use std::sync::mpsc::{SendError, Sender};

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
    /// There was an error in the history associated with the workflow: {0:?}
    #[error("There was an error in the history associated with the workflow: {0:?}")]
    HistoryError(#[from] HistoryInfoError),
    /// Error buffering commands coming in from the lang side. This shouldn't happen unless we've
    /// run out of memory or there is a logic bug. Considered fatal.
    #[error("Internal error buffering workflow commands")]
    CommandBufferingError(#[from] SendError<Vec<WFCommand>>),
    /// We tried to instantiate a workflow instance, but the provided history resulted in no
    /// new activations. There is nothing to do.
    #[error("Machine created with no activations for run_id {run_id}")]
    MachineWasCreatedWithNoActivations { run_id: String },
}

/// Manages an instance of a [WorkflowMachines], which is not thread-safe, as well as other data
/// associated with that specific workflow run.
pub(crate) struct WorkflowManager {
    pub machines: WorkflowMachines,
    command_sink: Sender<Vec<WFCommand>>,
    /// The last recorded history we received from the server for this workflow run. This must be
    /// kept because the lang side polls & completes for every workflow task, but we do not need
    /// to poll the server that often during replay.
    last_history_from_server: History,
    last_history_task_count: usize,
    /// The current workflow task number this run is on. Starts at one and monotonically increases.
    current_wf_task_num: usize,
}

impl WorkflowManager {
    /// Create a new workflow manager given workflow history and execution info as would be found
    /// in [PollWorkflowTaskQueueResponse]
    pub fn new(
        history: History,
        workflow_execution: WorkflowExecution,
    ) -> Result<Self, HistoryInfoError> {
        let (wfb, cmd_sink) = WorkflowBridge::new();
        let state_machines = WorkflowMachines::new(
            workflow_execution.workflow_id,
            workflow_execution.run_id,
            Box::new(wfb).into(),
        );
        Ok(Self {
            machines: state_machines,
            command_sink: cmd_sink,
            last_history_task_count: history.get_workflow_task_count(None)?,
            last_history_from_server: history,
            current_wf_task_num: 1,
        })
    }
}

#[derive(Debug)]
pub(crate) struct NextWfActivation {
    /// Keep this private, so we can ensure task tokens are attached via [Self::finalize]
    activation: WfActivation,
    pub more_activations_needed: bool,
}

impl NextWfActivation {
    /// Attach a task token to the activation so it can be sent out to the lang sdk
    pub(crate) fn finalize(self, task_token: Vec<u8>) -> WfActivation {
        let mut a = self.activation;
        a.task_token = task_token;
        a
    }

    pub(crate) fn get_run_id(&self) -> &str {
        &self.activation.run_id
    }
}

#[derive(Debug)]
pub(crate) struct PushCommandsResult {
    pub server_commands: Vec<ProtoCommand>,
    pub has_new_lang_jobs: bool,
}

impl WorkflowManager {
    /// Given history that was just obtained from the server, pipe it into this workflow's machines.
    ///
    /// Should only be called when a workflow has caught up on replay (or is just beginning). It
    /// will return a workflow activation if one is needed, as well as a bool indicating if there
    /// are more workflow tasks that need to be performed to replay the remaining history.
    pub fn feed_history_from_server(&mut self, hist: History) -> Result<Option<NextWfActivation>> {
        let task_hist = HistoryInfo::new_from_history(&hist, Some(self.current_wf_task_num))?;
        let task_ct = hist.get_workflow_task_count(None)?;
        self.last_history_task_count = task_ct;
        self.last_history_from_server = hist;
        self.machines.apply_history_events(&task_hist)?;
        let activation = self.machines.get_wf_activation();
        let more_activations_needed = task_ct > self.current_wf_task_num;

        if more_activations_needed {
            debug!("More activations needed");
        }

        self.current_wf_task_num += 1;

        Ok(activation.map(|activation| NextWfActivation {
            activation,
            more_activations_needed,
        }))
    }

    pub fn get_next_activation(&mut self) -> Result<Option<NextWfActivation>> {
        let hist = &self.last_history_from_server;
        let task_hist = HistoryInfo::new_from_history(hist, Some(self.current_wf_task_num))?;
        self.machines.apply_history_events(&task_hist)?;
        let activation = self.machines.get_wf_activation();

        self.current_wf_task_num += 1;
        let more_activations_needed = self.current_wf_task_num <= self.last_history_task_count;
        if more_activations_needed {
            debug!("More activations needed");
        }

        Ok(activation.map(|activation| NextWfActivation {
            activation,
            more_activations_needed,
        }))
    }

    /// Feed the workflow machines new commands issued by the executing workflow code, iterate the
    /// workflow machines, and spit out the commands which are ready to be sent off to the server
    pub fn push_commands(&mut self, cmds: Vec<WFCommand>) -> Result<PushCommandsResult> {
        self.command_sink.send(cmds)?;
        let has_new_lang_jobs = self.machines.iterate_machines()?;
        Ok(PushCommandsResult {
            server_commands: self.machines.get_commands(),
            has_new_lang_jobs,
        })
    }
}
