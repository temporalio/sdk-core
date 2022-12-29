#[cfg(test)]
mod managed_wf_test;

use crate::{
    worker::{
        workflow::{
            machines::WorkflowMachines, HistoryUpdate, LocalResolution, OutgoingServerCommands,
            WFCommand, WorkflowBridge,
        },
        LocalActRequest,
    },
    MetricsContext,
};
use std::sync::mpsc::Sender;
use temporal_sdk_core_api::errors::WFMachinesError;
use temporal_sdk_core_protos::coresdk::workflow_activation::WorkflowActivation;
use tokio::sync::oneshot;

use crate::worker::workflow::ActivationCompleteResult;

#[cfg(test)]
pub(crate) use managed_wf_test::ManagedWFFunc;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

#[derive(derive_more::DebugCustom)]
#[debug(fmt = "RunUpdateErr({:?})", source)]
pub(super) struct RunUpdateErr {
    pub(super) source: WFMachinesError,
    pub(super) complete_resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
}

impl From<WFMachinesError> for RunUpdateErr {
    fn from(e: WFMachinesError) -> Self {
        RunUpdateErr {
            source: e,
            complete_resp_chan: None,
        }
    }
}

/// Manages an instance of a [WorkflowMachines], which is not thread-safe, as well as other data
/// associated with that specific workflow run.
pub(crate) struct WorkflowManager {
    pub(super) machines: WorkflowMachines,
    /// Is always `Some` in normal operation. Optional to allow for unit testing with the test
    /// workflow driver, which does not need to complete activations the normal way.
    pub(super) command_sink: Option<Sender<Vec<WFCommand>>>,
}

impl WorkflowManager {
    /// Create a new workflow manager given workflow history and execution info as would be found
    /// in [PollWorkflowTaskQueueResponse]
    pub fn new(
        history: HistoryUpdate,
        namespace: String,
        workflow_id: String,
        workflow_type: String,
        run_id: String,
        metrics: MetricsContext,
    ) -> Self {
        let (wfb, cmd_sink) = WorkflowBridge::new();
        let state_machines = WorkflowMachines::new(
            namespace,
            workflow_id,
            workflow_type,
            run_id,
            history,
            Box::new(wfb).into(),
            metrics,
        );
        Self {
            machines: state_machines,
            command_sink: Some(cmd_sink),
        }
    }

    #[cfg(test)]
    pub const fn new_from_machines(workflow_machines: WorkflowMachines) -> Self {
        Self {
            machines: workflow_machines,
            command_sink: None,
        }
    }

    /// Given history that was just obtained from the server, pipe it into this workflow's machines.
    ///
    /// Should only be called when a workflow has caught up on replay (or is just beginning). It
    /// will return a workflow activation if one is needed.
    pub(super) fn feed_history_from_server(
        &mut self,
        update: HistoryUpdate,
    ) -> Result<WorkflowActivation> {
        self.machines.new_history_from_server(update)?;
        self.get_next_activation()
    }

    /// Let this workflow know that something we've been waiting locally on has resolved, like a
    /// local activity or side effect
    ///
    /// Returns true if the resolution did anything. EX: If the activity is already canceled and
    /// used the TryCancel or Abandon modes, the resolution is uninteresting.
    pub(super) fn notify_of_local_result(&mut self, resolved: LocalResolution) -> Result<bool> {
        self.machines.local_resolution(resolved)
    }

    /// Fetch the next workflow activation for this workflow if one is required. Doing so will apply
    /// the next unapplied workflow task if such a sequence exists in history we already know about.
    ///
    /// Callers may also need to call [get_server_commands] after this to issue any pending commands
    /// to the server.
    pub(super) fn get_next_activation(&mut self) -> Result<WorkflowActivation> {
        // First check if there are already some pending jobs, which can be a result of replay.
        let activation = self.machines.get_wf_activation();
        if !activation.jobs.is_empty() {
            return Ok(activation);
        }

        self.machines.apply_next_wft_from_history()?;
        Ok(self.machines.get_wf_activation())
    }

    /// If there are no pending jobs for the workflow, apply the next workflow task and check
    /// again if there are any jobs. Importantly, does not *drain* jobs.
    ///
    /// Returns true if there are jobs (before or after applying the next WFT).
    pub(super) fn apply_next_task_if_ready(&mut self) -> Result<bool> {
        if self.machines.has_pending_jobs() {
            return Ok(true);
        }
        loop {
            let consumed_events = self.machines.apply_next_wft_from_history()?;

            if consumed_events == 0 || !self.machines.replaying || self.machines.has_pending_jobs()
            {
                // Keep applying tasks while there are events, we are still replaying, and there are
                // no jobs
                break;
            }
        }
        Ok(self.machines.has_pending_jobs())
    }

    /// Typically called after [get_next_activation], use this to retrieve commands to be sent to
    /// the server which have been generated by the machines. Does *not* drain those commands.
    /// See [WorkflowMachines::get_commands].
    pub(super) fn get_server_commands(&self) -> OutgoingServerCommands {
        OutgoingServerCommands {
            commands: self.machines.get_commands(),
            replaying: self.machines.replaying,
        }
    }

    /// Remove and return all queued local activities. Once this is called, they need to be
    /// dispatched for execution.
    pub(super) fn drain_queued_local_activities(&mut self) -> Vec<LocalActRequest> {
        self.machines.drain_queued_local_activities()
    }

    /// Feed the workflow machines new commands issued by the executing workflow code, and iterate
    /// the machines.
    pub(super) fn push_commands_and_iterate(&mut self, cmds: Vec<WFCommand>) -> Result<()> {
        if let Some(cs) = self.command_sink.as_mut() {
            cs.send(cmds).map_err(|_| {
                WFMachinesError::Fatal("Internal error buffering workflow commands".to_string())
            })?;
        }
        self.machines.iterate_machines()?;
        Ok(())
    }
}
