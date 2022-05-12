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
use futures::{future, stream, StreamExt};
use std::{
    ops::Add,
    sync::mpsc::Sender,
    time::{Duration, Instant},
};
use temporal_sdk_core_api::errors::WFMachinesError;
use temporal_sdk_core_protos::coresdk::{
    workflow_activation::{RemoveFromCache, WorkflowActivation},
    workflow_commands::QueryResult,
};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    task,
    task::JoinHandle,
};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::worker::workflow::{
    workflow_tasks::WFT_HEARTBEAT_TIMEOUT_FRACTION, ActivationCompleteResult, ActivationOrAuto,
    FailRunUpdateResponse, GoodRunUpdateResponse, LocalActivityRequestSink,
};
use temporal_sdk_core_protos::TaskToken;

#[cfg(test)]
pub(crate) use managed_wf_test::ManagedWFFunc;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;

pub(super) struct ManagedRun {
    wfm: WorkflowManager,
    update_tx: UnboundedSender<RunUpdateResponse>,
    local_activity_request_sink: LocalActivityRequestSink,
    waiting_on_la: Option<WaitingOnLAs>,
    // Is set to true if the machines encounter an error and the only subsequent thing we should
    // do is be evicted.
    am_broken: bool,
}

/// If an activation completion needed to wait on LA completions (or heartbeat timeout) we use
/// this struct to store the data we need to finish the completion once that has happened
struct WaitingOnLAs {
    wft_timeout: Duration,
    /// If set, we are waiting for LAs to complete as part of a just-finished workflow activation.
    /// If unset, we already had a heartbeat timeout and got a new WFT without any new work while
    /// there are still incomplete LAs.
    completion_dat: Option<(
        CompletionDataForWFT,
        oneshot::Sender<ActivationCompleteResult>,
    )>,
    hb_chan: UnboundedSender<()>,
    heartbeat_timeout_task: JoinHandle<()>,
}

struct CompletionDataForWFT {
    task_token: TaskToken,
    query_responses: Vec<QueryResult>,
    has_pending_query: bool,
    activation_was_only_eviction: bool,
}

impl ManagedRun {
    pub(super) fn new(
        wfm: WorkflowManager,
        update_tx: UnboundedSender<RunUpdateResponse>,
        local_activity_request_sink: LocalActivityRequestSink,
    ) -> Self {
        Self {
            wfm,
            update_tx,
            local_activity_request_sink,
            waiting_on_la: None,
            am_broken: false,
        }
    }

    pub(super) async fn run(self, run_actions_rx: UnboundedReceiver<RunActions>) {
        let (heartbeat_tx, heartbeat_rx) = unbounded_channel();
        stream::select_all([
            future::Either::Left(UnboundedReceiverStream::new(run_actions_rx)),
            future::Either::Right(
                UnboundedReceiverStream::new(heartbeat_rx).map(|_| RunActions::HeartbeatTimeout),
            ),
        ])
        .fold(self, |mut me, action| async {
            let res = match action {
                RunActions::NewIncomingWFT(wft) => me
                    .incoming_wft(wft)
                    .await
                    .map(|(act, wft_update)| (act, Some(wft_update))),
                RunActions::ActivationCompletion(completion) => me
                    .completion(completion, &heartbeat_tx)
                    .await
                    .map(|_| (None, None)),
                RunActions::CheckMoreWork {
                    want_to_evict,
                    has_pending_queries,
                } => me
                    .check_more_work(want_to_evict, has_pending_queries)
                    .await
                    .map(|maybe_activation| (maybe_activation, None)),
                RunActions::LocalResolution(r) => {
                    me.local_resolution(r).await.map(|_| (None, None))
                }
                RunActions::HeartbeatTimeout => me.heartbeat_timeout().map(|autocomplete| {
                    if autocomplete {
                        (
                            Some(ActivationOrAuto::Autocomplete {
                                run_id: me.wfm.machines.run_id.clone(),
                                is_wft_heartbeat: true,
                            }),
                            None,
                        )
                    } else {
                        (None, None)
                    }
                }),
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
    ) -> Result<(Option<ActivationOrAuto>, RunUpdatedFromWft), RunUpdateErr> {
        debug!("Machine handling incoming wft");
        let activation = if let Some(h) = wft.history_update {
            self.wfm.feed_history_from_server(h).await?
        } else {
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

        if activation.jobs.is_empty() {
            if self.wfm.machines.outstanding_local_activity_count() > 0 {
                // If the activation has no jobs but there are outstanding LAs, we need to restart the
                // WFT heartbeat.
                if let Some(ref mut lawait) = self.waiting_on_la {
                    info!("Incoming wft no jobs but acts");
                    if lawait.completion_dat.is_some() {
                        panic!("Should not have completion dat when getting new wft & empty jobs")
                    }
                    lawait.heartbeat_timeout_task.abort();
                    lawait.heartbeat_timeout_task = start_heartbeat_timeout_task(
                        lawait.hb_chan.clone(),
                        wft.start_time,
                        lawait.wft_timeout,
                    );
                    // No activation needs to be sent to lang. We just need to wait for another
                    // heartbeat timeout or LAs to resolve
                    return Ok((None, RunUpdatedFromWft {}));
                } else {
                    panic!(
                        "Got a new WFT while there are outstanding local activities, but there \
                     was no waiting on LA info."
                    )
                }
            } else {
                return Ok((
                    Some(ActivationOrAuto::Autocomplete {
                        run_id: self.wfm.machines.run_id.clone(),
                        is_wft_heartbeat: false,
                    }),
                    RunUpdatedFromWft {},
                ));
            }
        }

        Ok((
            Some(ActivationOrAuto::LangActivation(activation)),
            RunUpdatedFromWft {},
        ))
    }

    async fn completion(
        &mut self,
        mut completion: RunActivationCompletion,
        heartbeat_tx: &UnboundedSender<()>,
    ) -> Result<(), RunUpdateErr> {
        debug!("Machine handling completion");
        let resp_chan = completion
            .resp_chan
            .take()
            .expect("Completion response channel must be populated");

        let outcome = async move {
            // Send commands from lang into the machines then check if the workflow run
            // needs another activation and mark it if so
            self.wfm.push_commands(completion.commands).await?;
            // Don't bother applying the next task if we're evicting at the end of
            // this activation
            if !completion.activation_was_eviction {
                self.wfm.apply_next_task_if_ready().await?;
            }
            let new_local_acts = self.wfm.drain_queued_local_activities();

            let immediate_resolutions = (self.local_activity_request_sink)(new_local_acts);
            for resolution in immediate_resolutions {
                self.wfm
                    .notify_of_local_result(LocalResolution::LocalActivity(resolution))?;
            }

            let data = CompletionDataForWFT {
                task_token: completion.task_token,
                query_responses: completion.query_responses,
                has_pending_query: completion.has_pending_query,
                activation_was_only_eviction: completion.activation_was_only_eviction,
            };
            if self.wfm.machines.outstanding_local_activity_count() <= 0 {
                Ok((None, data, self))
            } else {
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
                let heartbeat_tx = heartbeat_tx.clone();
                Ok((
                    Some((heartbeat_tx, completion.start_time, wft_timeout)),
                    data,
                    self,
                ))
            }
        }
        .await;

        match outcome {
            Ok((None, data, me)) => {
                me.finish_completion(resp_chan, data, false);
                Ok(())
            }
            Ok((Some((chan, start_t, wft_timeout)), data, me)) => {
                me.waiting_on_la = Some(WaitingOnLAs {
                    wft_timeout,
                    completion_dat: Some((data, resp_chan)),
                    hb_chan: chan.clone(),
                    heartbeat_timeout_task: start_heartbeat_timeout_task(
                        chan,
                        start_t,
                        wft_timeout,
                    ),
                });
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
        has_pending_queries: bool,
    ) -> Result<Option<ActivationOrAuto>, RunUpdateErr> {
        info!(want_to_evict=%want_to_evict.is_some(), has_pending_queries, "Checking more work");

        if self.wfm.machines.has_pending_jobs() && !self.am_broken {
            Ok(Some(ActivationOrAuto::LangActivation(
                self.wfm.get_next_activation().await?,
            )))
        } else {
            if has_pending_queries && !self.am_broken {
                return Ok(Some(ActivationOrAuto::ReadyForQueries(
                    self.wfm.machines.get_wf_activation(),
                )));
            }
            if let Some(wte) = want_to_evict {
                let mut act = self.wfm.machines.get_wf_activation();
                // No other jobs make any sense to send if we encountered an error.
                if self.am_broken {
                    act.jobs = vec![];
                }
                act.append_evict_job(RemoveFromCache {
                    message: wte.message,
                    reason: wte.reason as i32,
                });
                Ok(Some(ActivationOrAuto::LangActivation(act)))
            } else {
                Ok(None)
            }
        }
    }

    fn finish_completion(
        &mut self,
        resp_chan: oneshot::Sender<ActivationCompleteResult>,
        data: CompletionDataForWFT,
        due_to_heartbeat_timeout: bool,
    ) {
        let outgoing_cmds = self.wfm.get_server_commands();
        let query_responses = data.query_responses;
        let has_query_responses = !query_responses.is_empty();
        let is_query_playback = data.has_pending_query && !has_query_responses;

        // We only actually want to send commands back to the server if there are no more
        // pending activations and we are caught up on replay. We don't want to complete a wft
        // if we already saw the final event in the workflow, or if we are playing back for the
        // express purpose of fulfilling a query. If the activation we sent was *only* an
        // eviction, and there were no commands produced during iteration, don't send that
        // either.
        let no_commands_and_evicting =
            outgoing_cmds.commands.is_empty() && data.activation_was_only_eviction;
        let to_be_sent = ServerCommandsWithWorkflowInfo {
            task_token: data.task_token,
            action: ActivationAction::WftComplete {
                force_new_wft: due_to_heartbeat_timeout,
                commands: outgoing_cmds.commands,
                query_responses,
            },
        };

        let should_respond = !(self.wfm.machines.has_pending_jobs()
            || outgoing_cmds.replaying
            || is_query_playback
            || no_commands_and_evicting);
        let outcome = if should_respond || has_query_responses {
            ActivationCompleteOutcome::ReportWFTSuccess(to_be_sent)
        } else {
            ActivationCompleteOutcome::DoNothing
        };
        resp_chan
            .send(ActivationCompleteResult {
                most_recently_processed_event: self.wfm.machines.last_processed_event as usize,
                outcome,
            })
            .expect("Activation response channel not dropped");
    }

    async fn local_resolution(&mut self, res: LocalResolution) -> Result<(), RunUpdateErr> {
        info!(resolution=?res, "Applying local resolution");
        self.wfm.notify_of_local_result(res)?;
        if self.wfm.machines.outstanding_local_activity_count() == 0 {
            if let Some(mut wait_dat) = self.waiting_on_la.take() {
                // Cancel the heartbeat timeout
                wait_dat.heartbeat_timeout_task.abort();
                if let Some((completion_dat, resp_chan)) = wait_dat.completion_dat.take() {
                    self.finish_completion(resp_chan, completion_dat, false);
                }
            }
        }
        Ok(())
    }

    /// Returns true if autocompletion should be issued
    fn heartbeat_timeout(&mut self) -> Result<bool, RunUpdateErr> {
        info!("Hearbeat timeout");
        if let Some(ref mut wait_dat) = self.waiting_on_la {
            // Cancel the heartbeat timeout
            wait_dat.heartbeat_timeout_task.abort();
            if let Some((completion_dat, resp_chan)) = wait_dat.completion_dat.take() {
                self.finish_completion(resp_chan, completion_dat, true);
            } else {
                // Auto-reply WFT complete
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn send_update_response(
        &self,
        outgoing_activation: Option<ActivationOrAuto>,
        from_wft: Option<RunUpdatedFromWft>,
    ) {
        self.update_tx
            .send(RunUpdateResponse::Good(GoodRunUpdateResponse {
                run_id: self.wfm.machines.run_id.clone(),
                outgoing_activation,
                have_seen_terminal_event: self.wfm.machines.have_seen_terminal_event,
                more_pending_work: self.wfm.machines.has_pending_jobs(),
                most_recently_processed_event_number: self.wfm.machines.last_processed_event
                    as usize,
                in_response_to_wft: from_wft,
            }))
            .expect("Machine can send update");
    }
}

fn start_heartbeat_timeout_task(
    chan: UnboundedSender<()>,
    wft_start_time: Instant,
    wft_timeout: Duration,
) -> JoinHandle<()> {
    // The heartbeat deadline is 80% of the WFT timeout
    let wft_heartbeat_deadline =
        wft_start_time.add(wft_timeout.mul_f32(WFT_HEARTBEAT_TIMEOUT_FRACTION));
    task::spawn(async move {
        tokio::time::sleep_until(wft_heartbeat_deadline.into()).await;
        let _ = chan.send(());
    })
}

#[derive(Debug)]
struct RunUpdateErr {
    source: WFMachinesError,
    complete_resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
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
    fn get_server_commands(&self) -> OutgoingServerCommands {
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
