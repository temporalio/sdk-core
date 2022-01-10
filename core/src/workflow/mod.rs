pub(crate) mod workflow_tasks;

mod bridge;
mod driven_workflow;
mod history_update;
mod machines;

pub(crate) use bridge::WorkflowBridge;
pub(crate) use driven_workflow::{DrivenWorkflow, WorkflowFetcher};
pub(crate) use history_update::{HistoryPaginator, HistoryUpdate};
pub(crate) use machines::WFMachinesError;

use crate::{
    telemetry::metrics::MetricsContext,
    worker::{LocalActRequest, LocalActivityResolution},
};
use machines::WorkflowMachines;
use std::{result, sync::mpsc::Sender};
use temporal_sdk_core_protos::{
    coresdk::{workflow_activation::WorkflowActivation, workflow_commands::*},
    temporal::api::command::v1::Command as ProtoCommand,
};

pub(crate) const LEGACY_QUERY_ID: &str = "legacy_query";

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

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
}

#[derive(Debug)]
pub struct OutgoingServerCommands {
    pub commands: Vec<ProtoCommand>,
    pub replaying: bool,
}

#[derive(Debug)]
pub(crate) enum LocalResolution {
    LocalActivity(LocalActivityResolution),
}

impl WorkflowManager {
    /// Given history that was just obtained from the server, pipe it into this workflow's machines.
    ///
    /// Should only be called when a workflow has caught up on replay (or is just beginning). It
    /// will return a workflow activation if one is needed.
    pub async fn feed_history_from_server(
        &mut self,
        update: HistoryUpdate,
    ) -> Result<WorkflowActivation> {
        self.machines.new_history_from_server(update).await?;
        self.get_next_activation().await
    }

    /// Let this workflow know that something we've been waiting locally on has resolved, like a
    /// local activity or side effect
    pub fn notify_of_local_result(&mut self, resolved: LocalResolution) -> Result<()> {
        self.machines.local_resolution(resolved)
    }

    /// Fetch the next workflow activation for this workflow if one is required. Doing so will apply
    /// the next unapplied workflow task if such a sequence exists in history we already know about.
    ///
    /// Callers may also need to call [get_server_commands] after this to issue any pending commands
    /// to the server.
    pub async fn get_next_activation(&mut self) -> Result<WorkflowActivation> {
        // First check if there are already some pending jobs, which can be a result of replay.
        let activation = self.machines.get_wf_activation();
        if !activation.jobs.is_empty() {
            return Ok(activation);
        }

        self.machines.apply_next_wft_from_history().await?;
        Ok(self.machines.get_wf_activation())
    }

    /// If there are no pending jobs for the workflow, apply the next workflow task and check
    /// again if there are any jobs. Importantly, does not *drain* jobs.
    ///
    /// Returns true if there are jobs (before or after applying the next WFT).
    pub async fn apply_next_task_if_ready(&mut self) -> Result<bool> {
        if self.machines.has_pending_jobs() {
            return Ok(true);
        }
        loop {
            let consumed_events = self.machines.apply_next_wft_from_history().await?;

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
    pub fn get_server_commands(&mut self) -> OutgoingServerCommands {
        OutgoingServerCommands {
            commands: self.machines.get_commands(),
            replaying: self.machines.replaying,
        }
    }

    /// Remove and return all queued local activities. Once this is called, they need to be
    /// dispatched for execution.
    pub fn drain_queued_local_activities(&mut self) -> Vec<LocalActRequest> {
        self.machines.drain_queued_local_activities()
    }

    /// Feed the workflow machines new commands issued by the executing workflow code, and iterate
    /// the machines.
    pub async fn push_commands(&mut self, cmds: Vec<WFCommand>) -> Result<()> {
        if let Some(cs) = self.command_sink.as_mut() {
            cs.send(cmds).map_err(|_| {
                WFMachinesError::Fatal("Internal error buffering workflow commands".to_string())
            })?;
        }
        self.machines.iterate_machines().await?;
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

#[derive(thiserror::Error, Debug, derive_more::From)]
#[error("Lang provided workflow command with empty variant")]
pub struct EmptyWorkflowCommandErr;

/// [DrivenWorkflow]s respond with these when called, to indicate what they want to do next.
/// EX: Create a new timer, complete the workflow, etc.
#[derive(Debug, derive_more::From, derive_more::Display)]
#[allow(clippy::large_enum_variant)]
pub enum WFCommand {
    /// Returned when we need to wait for the lang sdk to send us something
    NoCommandsFromLang,
    AddActivity(ScheduleActivity),
    AddLocalActivity(ScheduleLocalActivity),
    RequestCancelActivity(RequestCancelActivity),
    RequestCancelLocalActivity(RequestCancelLocalActivity),
    AddTimer(StartTimer),
    CancelTimer(CancelTimer),
    CompleteWorkflow(CompleteWorkflowExecution),
    FailWorkflow(FailWorkflowExecution),
    QueryResponse(QueryResult),
    ContinueAsNew(ContinueAsNewWorkflowExecution),
    CancelWorkflow(CancelWorkflowExecution),
    SetPatchMarker(SetPatchMarker),
    AddChildWorkflow(StartChildWorkflowExecution),
    CancelUnstartedChild(CancelUnstartedChildWorkflowExecution),
    RequestCancelExternalWorkflow(RequestCancelExternalWorkflowExecution),
    SignalExternalWorkflow(SignalExternalWorkflowExecution),
    CancelSignalWorkflow(CancelSignalWorkflow),
}

impl TryFrom<WorkflowCommand> for WFCommand {
    type Error = EmptyWorkflowCommandErr;

    fn try_from(c: WorkflowCommand) -> result::Result<Self, Self::Error> {
        match c.variant.ok_or(EmptyWorkflowCommandErr)? {
            workflow_command::Variant::StartTimer(s) => Ok(Self::AddTimer(s)),
            workflow_command::Variant::CancelTimer(s) => Ok(Self::CancelTimer(s)),
            workflow_command::Variant::ScheduleActivity(s) => Ok(Self::AddActivity(s)),
            workflow_command::Variant::RequestCancelActivity(s) => {
                Ok(Self::RequestCancelActivity(s))
            }
            workflow_command::Variant::CompleteWorkflowExecution(c) => {
                Ok(Self::CompleteWorkflow(c))
            }
            workflow_command::Variant::FailWorkflowExecution(s) => Ok(Self::FailWorkflow(s)),
            workflow_command::Variant::RespondToQuery(s) => Ok(Self::QueryResponse(s)),
            workflow_command::Variant::ContinueAsNewWorkflowExecution(s) => {
                Ok(Self::ContinueAsNew(s))
            }
            workflow_command::Variant::CancelWorkflowExecution(s) => Ok(Self::CancelWorkflow(s)),
            workflow_command::Variant::SetPatchMarker(s) => Ok(Self::SetPatchMarker(s)),
            workflow_command::Variant::StartChildWorkflowExecution(s) => {
                Ok(Self::AddChildWorkflow(s))
            }
            workflow_command::Variant::RequestCancelExternalWorkflowExecution(s) => {
                Ok(Self::RequestCancelExternalWorkflow(s))
            }
            workflow_command::Variant::SignalExternalWorkflowExecution(s) => {
                Ok(Self::SignalExternalWorkflow(s))
            }
            workflow_command::Variant::CancelSignalWorkflow(s) => Ok(Self::CancelSignalWorkflow(s)),
            workflow_command::Variant::CancelUnstartedChildWorkflowExecution(s) => {
                Ok(Self::CancelUnstartedChild(s))
            }
            workflow_command::Variant::ScheduleLocalActivity(s) => Ok(Self::AddLocalActivity(s)),
            workflow_command::Variant::RequestCancelLocalActivity(s) => {
                Ok(Self::RequestCancelLocalActivity(s))
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy)]
enum CommandID {
    Timer(u32),
    Activity(u32),
    LocalActivity(u32),
    ChildWorkflowStart(u32),
    SignalExternal(u32),
    CancelExternal(u32),
}

#[cfg(test)]
pub mod managed_wf {
    use super::*;
    use crate::{
        test_help::{TestHistoryBuilder, TEST_Q},
        workflow::{history_update::tests::TestHBExt, WFCommand, WorkflowFetcher},
    };
    use std::{convert::TryInto, time::Duration};
    use temporal_sdk::{WorkflowFunction, WorkflowResult};
    use temporal_sdk_core_protos::coresdk::{
        activity_result::ActivityExecutionResult,
        common::Payload,
        workflow_activation::create_evict_activation,
        workflow_completion::{
            workflow_activation_completion::Status, WorkflowActivationCompletion,
        },
    };
    use tokio::{
        sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        task::JoinHandle,
    };

    pub(crate) struct WFFutureDriver {
        completions_rx: UnboundedReceiver<WorkflowActivationCompletion>,
    }

    #[async_trait::async_trait]
    impl WorkflowFetcher for WFFutureDriver {
        async fn fetch_workflow_iteration_output(&mut self) -> Vec<WFCommand> {
            if let Some(completion) = self.completions_rx.recv().await {
                debug!("Managed wf completion: {}", completion);
                completion
                    .status
                    .map(|s| match s {
                        Status::Successful(s) => s
                            .commands
                            .into_iter()
                            .map(|cmd| cmd.try_into().unwrap())
                            .collect(),
                        Status::Failed(_) => panic!("Ahh failed"),
                    })
                    .unwrap_or_default()
            } else {
                // Sender went away so nothing to do here. End of wf/test.
                vec![]
            }
        }
    }

    #[must_use]
    pub struct ManagedWFFunc {
        mgr: WorkflowManager,
        activation_tx: UnboundedSender<WorkflowActivation>,
        future_handle: Option<JoinHandle<WorkflowResult<()>>>,
        was_shutdown: bool,
    }

    impl ManagedWFFunc {
        pub fn new(hist: TestHistoryBuilder, func: WorkflowFunction, args: Vec<Payload>) -> Self {
            Self::new_from_update(hist.as_history_update(), func, args)
        }

        pub fn new_from_update(
            hist: HistoryUpdate,
            func: WorkflowFunction,
            args: Vec<Payload>,
        ) -> Self {
            let (completions_tx, completions_rx) = unbounded_channel();
            let (wff, activations) = func.start_workflow(
                "testnamespace".to_string(),
                TEST_Q.to_string(),
                args,
                completions_tx,
            );
            let spawned = tokio::spawn(wff);
            let driver = WFFutureDriver { completions_rx };
            let state_machines = WorkflowMachines::new(
                "test_namespace".to_string(),
                "wfid".to_string(),
                "wftype".to_string(),
                "runid".to_string(),
                hist,
                Box::new(driver).into(),
                Default::default(),
            );
            let mgr = WorkflowManager::new_from_machines(state_machines);
            Self {
                mgr,
                activation_tx: activations,
                future_handle: Some(spawned),
                was_shutdown: false,
            }
        }

        #[instrument(level = "debug", skip(self))]
        pub(crate) async fn get_next_activation(&mut self) -> Result<WorkflowActivation> {
            let res = self.mgr.get_next_activation().await?;
            debug!("Managed wf next activation: {}", &res);
            self.push_activation_to_wf(&res).await?;
            Ok(res)
        }

        /// Return outgoing server commands as of the last iteration
        pub(crate) fn get_server_commands(&mut self) -> OutgoingServerCommands {
            self.mgr.get_server_commands()
        }

        pub(crate) fn drain_queued_local_activities(&mut self) -> Vec<LocalActRequest> {
            self.mgr.drain_queued_local_activities()
        }

        /// Feed new history, as if received a new poll result. Returns new activation
        #[instrument(level = "debug", skip(self, update))]
        pub(crate) async fn new_history(
            &mut self,
            update: HistoryUpdate,
        ) -> Result<WorkflowActivation> {
            let res = self.mgr.feed_history_from_server(update).await?;
            self.push_activation_to_wf(&res).await?;
            Ok(res)
        }

        /// Say a local activity completed (they always take 1 second in these tests)
        pub(crate) fn complete_local_activity(
            &mut self,
            seq_num: u32,
            result: ActivityExecutionResult,
        ) -> Result<()> {
            self.mgr
                .notify_of_local_result(LocalResolution::LocalActivity(LocalActivityResolution {
                    seq: seq_num,
                    // We accept normal execution results and do this conversion because there
                    // are more helpers for constructing them.
                    result: result
                        .status
                        .expect("LA result must have a status")
                        .try_into()
                        .expect("LA execution result must be a valid LA result"),
                    runtime: Duration::from_secs(1),
                    attempt: 1,
                    backoff: None,
                    // Tests at this level don't use the LA dispatcher, so this is irrelevant
                    original_schedule_time: None,
                }))
        }

        /// During testing it can be useful to run through all activations to simulate replay
        /// easily. Returns the last produced activation with jobs in it, or an activation with no
        /// jobs if the first call had no jobs.
        pub(crate) async fn process_all_activations(&mut self) -> Result<WorkflowActivation> {
            let mut last_act = self.get_next_activation().await?;
            let mut next_act = self.get_next_activation().await?;
            while !next_act.jobs.is_empty() {
                last_act = next_act;
                next_act = self.get_next_activation().await?;
            }
            Ok(last_act)
        }

        pub async fn shutdown(&mut self) -> WorkflowResult<()> {
            self.was_shutdown = true;
            // Send an eviction to ensure wf exits if it has not finished (ex: feeding partial hist)
            let _ = self.activation_tx.send(create_evict_activation(
                "not actually important".to_string(),
                "force shutdown".to_string(),
            ));
            self.future_handle.take().unwrap().await.unwrap()
        }

        #[instrument(level = "debug", skip(self, res))]
        async fn push_activation_to_wf(&mut self, res: &WorkflowActivation) -> Result<()> {
            if res.jobs.is_empty() {
                // Nothing to do here
                return Ok(());
            }
            self.activation_tx
                .send(res.clone())
                .expect("Workflow should not be dropped if we are still sending activations");
            self.mgr.machines.iterate_machines().await?;
            Ok(())
        }
    }

    impl Drop for ManagedWFFunc {
        fn drop(&mut self) {
            // Double panics cause a SIGILL
            if !self.was_shutdown && !std::thread::panicking() {
                panic!("You must call `shutdown` to properly use ManagedWFFunc in tests")
            }
        }
    }
}
