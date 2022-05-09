#[cfg(test)]
mod managed_wf_test;

use crate::{
    worker::{
        workflow::{
            machines::WorkflowMachines,
            workflow_tasks::{ActivationAction, ServerCommandsWithWorkflowInfo},
            ActivationCompleteOutcome, HistoryUpdate, LocalResolution, NewIncomingWFT,
            OutgoingServerCommands, RequestEvictMsg, RunActions, RunActivationCompletion,
            RunUpdateResponse, RunUpdatedFromWft, WFCommand, WorkflowBridge,
        },
        LocalActRequest,
    },
    MetricsContext,
};
use futures::StreamExt;
use std::{sync::mpsc::Sender, time::Duration};
use temporal_sdk_core_api::errors::WFMachinesError;
use temporal_sdk_core_protos::coresdk::workflow_activation::{RemoveFromCache, WorkflowActivation};
use tokio::sync::{
    mpsc::{UnboundedReceiver, UnboundedSender},
    oneshot,
};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::worker::workflow::{FailRunUpdateResponse, GoodRunUpdateResponse};
#[cfg(test)]
pub(crate) use managed_wf_test::ManagedWFFunc;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

pub(super) struct ManagedRun {
    wfm: WorkflowManager,
    update_tx: UnboundedSender<RunUpdateResponse>,
    // Is set to true if the machines encounter an error and the only subsequent thing we should
    // do is be evicted.
    am_broken: bool,
}

impl ManagedRun {
    pub(super) fn new(wfm: WorkflowManager, update_tx: UnboundedSender<RunUpdateResponse>) -> Self {
        Self {
            wfm,
            update_tx,
            am_broken: false,
        }
    }

    pub(super) async fn run(self, run_actions_rx: UnboundedReceiver<RunActions>) {
        UnboundedReceiverStream::new(run_actions_rx)
            .fold(self, |mut me, action| async {
                let res = match action {
                    RunActions::NewIncomingWFT(wft) => me
                        .incoming_wft(wft)
                        .await
                        .map(|(act, wft_update)| (Some(act), Some(wft_update))),
                    RunActions::ActivationCompletion(completion) => {
                        me.completion(completion).await.map(|_| (None, None))
                    }
                    RunActions::CheckMoreWork { want_to_evict } => me
                        .check_more_work(want_to_evict)
                        .await
                        .map(|maybe_activation| (maybe_activation, None)),
                };
                match res {
                    Ok((maybe_act, maybe_wft_update)) => {
                        me.send_update_response(maybe_act, maybe_wft_update);
                    }
                    Err(e) => {
                        error!(error=?e, "Error in run machines");
                        me.am_broken = true;
                        me.update_tx
                            .send(RunUpdateResponse::Fail(FailRunUpdateResponse {
                                run_id: me.wfm.machines.run_id.clone(),
                                err: e.source,
                                completion_resp: e.complete_resp_chan,
                            }))
                            .expect("Machine can send update");
                    }
                }
                me
            })
            .await;
    }

    async fn incoming_wft(
        &mut self,
        wft: NewIncomingWFT,
    ) -> Result<(WorkflowActivation, RunUpdatedFromWft), RunUpdateErr> {
        debug!("Machine handling incoming wft");
        let activation = if let Some(h) = wft.history_update {
            self.wfm.feed_history_from_server(h).await?
        } else {
            // TODO: Keep machines created w/ no jobs check?
            let r = self.wfm.get_next_activation().await?;
            if r.jobs.is_empty() {
                return Err(RunUpdateErr {
                    source: WFMachinesError::Fatal(format!(
                        "Machines created for {} with no jobs",
                        self.wfm.machines.run_id
                    )),
                    complete_resp_chan: None,
                });
            }
            r
        };

        Ok((activation, RunUpdatedFromWft {}))
    }

    async fn completion(
        &mut self,
        mut completion: RunActivationCompletion,
    ) -> Result<(), RunUpdateErr> {
        debug!("Machine handling completion");
        let resp_chan = completion
            .resp_chan
            .take()
            .expect("Completion response channel must be populated");

        let resp = async move {
            // Send commands from lang into the machines then check if the workflow run
            // needs another activation and mark it if so
            self.wfm.push_commands(completion.commands).await?;
            // Don't bother applying the next task if we're evicting at the end of
            // this activation
            let are_pending = if !completion.activation_was_eviction {
                self.wfm.apply_next_task_if_ready().await?
            } else {
                false
            };
            // We want to fetch the outgoing commands only after a next WFT may have
            // been applied, as outgoing server commands may be affected.
            let outgoing_cmds = self.wfm.get_server_commands();
            let new_local_acts = self.wfm.drain_queued_local_activities();

            let wft_timeout: Duration = self
                .wfm
                .machines
                .get_started_info()
                .and_then(|attrs| attrs.workflow_task_timeout)
                .ok_or_else(|| {
                    WFMachinesError::Fatal(
                        "Workflow's start attribs were missing a well formed task timeout"
                            .to_string(),
                    )
                })?;

            // TODO: Local activity insanity

            let query_responses = completion.query_responses;
            let has_query_responses = !query_responses.is_empty();
            let is_query_playback = completion.has_pending_query && !has_query_responses;

            // We only actually want to send commands back to the server if there are no more
            // pending activations and we are caught up on replay. We don't want to complete a wft
            // if we already saw the final event in the workflow, or if we are playing back for the
            // express purpose of fulfilling a query. If the activation we sent was *only* an
            // eviction, and there were no commands produced during iteration, don't send that
            // either.
            let no_commands_and_evicting =
                outgoing_cmds.commands.is_empty() && completion.activation_was_only_eviction;
            let to_be_sent = ServerCommandsWithWorkflowInfo {
                task_token: completion.task_token,
                action: ActivationAction::WftComplete {
                    // TODO: Local activity insanity
                    // TODO: Don't force if also sending complete execution cmd
                    force_new_wft: false,
                    commands: outgoing_cmds.commands,
                    query_responses,
                },
            };

            if are_pending {
                // TODO: Wait on LAs somehow
            }

            let should_respond = !(are_pending
                || outgoing_cmds.replaying
                || is_query_playback
                || no_commands_and_evicting);

            Ok::<_, WFMachinesError>(if should_respond || has_query_responses {
                ActivationCompleteOutcome::ReportWFTSuccess(to_be_sent)
            } else {
                ActivationCompleteOutcome::DoNothing
            })
        }
        .await;

        match resp {
            Ok(resp) => {
                resp_chan
                    .send(resp)
                    .expect("Activation response channel not dropped");
                Ok(())
            }
            Err(e) => Err(RunUpdateErr {
                source: e,
                complete_resp_chan: Some(resp_chan),
            }),
        }
    }

    async fn check_more_work(
        &mut self,
        want_to_evict: Option<RequestEvictMsg>,
    ) -> Result<Option<WorkflowActivation>, RunUpdateErr> {
        info!(want_to_evict=%want_to_evict.is_some(), "Checking more work");
        // TODO:
        // We cannot always immediately send activation, b/c we may need to wait on LAs. If
        // there are none, though, we can immediately dispatch a new activation.
        // if new_local_acts.is_empty()
        //     && self.wfm.machines.outstanding_local_activity_count() == 0
        // {}

        if self.wfm.machines.has_pending_jobs() && !self.am_broken {
            Ok(Some(self.wfm.get_next_activation().await?))
        } else {
            if let Some(wte) = want_to_evict {
                let mut act = self.wfm.machines.get_wf_activation();
                if self.am_broken {
                    act.jobs = vec![];
                }
                act.append_evict_job(RemoveFromCache {
                    message: wte.message,
                    reason: wte.reason as i32,
                });
                Ok(Some(act))
            } else {
                Ok(None)
            }
        }
    }

    fn send_update_response(
        &self,
        outgoing_activation: Option<WorkflowActivation>,
        from_wft: Option<RunUpdatedFromWft>,
    ) {
        self.update_tx
            .send(RunUpdateResponse::Good(GoodRunUpdateResponse {
                run_id: self.wfm.machines.run_id.clone(),
                outgoing_activation,
                have_seen_terminal_event: self.wfm.machines.have_seen_terminal_event,
                more_pending_work: self.wfm.machines.has_pending_jobs(),
                current_outstanding_local_act_count: self
                    .wfm
                    .machines
                    .outstanding_local_activity_count(),
                in_response_to_wft: from_wft,
            }))
            .expect("Machine can send update");
    }
}

#[derive(Debug)]
struct RunUpdateErr {
    source: WFMachinesError,
    complete_resp_chan: Option<oneshot::Sender<ActivationCompleteOutcome>>,
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

    /// Given history that was just obtained from the server, pipe it into this workflow's machines.
    ///
    /// Should only be called when a workflow has caught up on replay (or is just beginning). It
    /// will return a workflow activation if one is needed.
    async fn feed_history_from_server(
        &mut self,
        update: HistoryUpdate,
    ) -> Result<WorkflowActivation> {
        self.machines.new_history_from_server(update).await?;
        self.get_next_activation().await
    }

    /// Let this workflow know that something we've been waiting locally on has resolved, like a
    /// local activity or side effect
    ///
    /// Returns true if the resolution did anything. EX: If the activity is already canceled and
    /// used the TryCancel or Abandon modes, the resolution is uninteresting.
    fn notify_of_local_result(&mut self, resolved: LocalResolution) -> Result<bool> {
        self.machines.local_resolution(resolved)
    }

    /// Fetch the next workflow activation for this workflow if one is required. Doing so will apply
    /// the next unapplied workflow task if such a sequence exists in history we already know about.
    ///
    /// Callers may also need to call [get_server_commands] after this to issue any pending commands
    /// to the server.
    async fn get_next_activation(&mut self) -> Result<WorkflowActivation> {
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
    async fn apply_next_task_if_ready(&mut self) -> Result<bool> {
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
    fn get_server_commands(&mut self) -> OutgoingServerCommands {
        OutgoingServerCommands {
            commands: self.machines.get_commands(),
            replaying: self.machines.replaying,
        }
    }

    /// Remove and return all queued local activities. Once this is called, they need to be
    /// dispatched for execution.
    fn drain_queued_local_activities(&mut self) -> Vec<LocalActRequest> {
        self.machines.drain_queued_local_activities()
    }

    /// Feed the workflow machines new commands issued by the executing workflow code, and iterate
    /// the machines.
    async fn push_commands(&mut self, cmds: Vec<WFCommand>) -> Result<()> {
        if let Some(cs) = self.command_sink.as_mut() {
            cs.send(cmds).map_err(|_| {
                WFMachinesError::Fatal("Internal error buffering workflow commands".to_string())
            })?;
        }
        self.machines.iterate_machines().await?;
        Ok(())
    }
}
