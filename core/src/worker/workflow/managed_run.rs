#[cfg(test)]
mod managed_wf_test;

use crate::{
    worker::{
        workflow::{
            machines::WorkflowMachines, ActivationAction, ActivationCompleteOutcome, HistoryUpdate,
            LocalResolution, NewIncomingWFT, OutgoingServerCommands, RequestEvictMsg, RunActions,
            RunActivationCompletion, RunUpdateResponse, ServerCommandsWithWorkflowInfo, WFCommand,
            WorkflowBridge,
        },
        LocalActRequest,
    },
    MetricsContext,
};
use futures::{stream, StreamExt};
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
use tracing::Span;
use tracing_futures::Instrument;

use crate::worker::workflow::{
    ActivationCompleteResult, ActivationOrAuto, FailRunUpdate, FulfillableActivationComplete,
    GoodRunUpdate, LocalActivityRequestSink, RunAction, RunUpdateResponseKind,
};
use temporal_sdk_core_protos::TaskToken;

use crate::abstractions::dbg_panic;
#[cfg(test)]
pub(crate) use managed_wf_test::ManagedWFFunc;

type Result<T, E = WFMachinesError> = std::result::Result<T, E>;
/// What percentage of a WFT timeout we are willing to wait before sending a WFT heartbeat when
/// necessary.
const WFT_HEARTBEAT_TIMEOUT_FRACTION: f32 = 0.8;

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
    hb_chan: UnboundedSender<Span>,
    heartbeat_timeout_task: JoinHandle<()>,
}

#[derive(Debug)]
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

    pub(super) async fn run(self, run_actions_rx: UnboundedReceiver<RunAction>) {
        let (heartbeat_tx, heartbeat_rx) = unbounded_channel();
        stream::select(
            UnboundedReceiverStream::new(run_actions_rx),
            UnboundedReceiverStream::new(heartbeat_rx).map(|trace_span| RunAction {
                action: RunActions::HeartbeatTimeout,
                trace_span,
            }),
        )
        .fold((self, heartbeat_tx), |(mut me, heartbeat_tx), action| {
            let span = action.trace_span;
            let action = action.action;
            let mut no_wft = false;
            async move {
                let res = match action {
                    RunActions::NewIncomingWFT(wft) => me
                        .incoming_wft(wft)
                        .await
                        .map(RunActionOutcome::AfterNewWFT),
                    RunActions::ActivationCompletion(completion) => me
                        .completion(completion, &heartbeat_tx)
                        .await
                        .map(RunActionOutcome::AfterCompletion),
                    RunActions::CheckMoreWork {
                        want_to_evict,
                        has_pending_queries,
                        has_wft,
                    } => {
                        if !has_wft {
                            no_wft = true;
                        }
                        me.check_more_work(want_to_evict, has_pending_queries, has_wft)
                            .await
                            .map(RunActionOutcome::AfterCheckWork)
                    }
                    RunActions::LocalResolution(r) => me
                        .local_resolution(r)
                        .await
                        .map(RunActionOutcome::AfterLocalResolution),
                    RunActions::HeartbeatTimeout => {
                        let maybe_act = if me.heartbeat_timeout() {
                            Some(ActivationOrAuto::Autocomplete {
                                run_id: me.wfm.machines.run_id.clone(),
                            })
                        } else {
                            None
                        };
                        Ok(RunActionOutcome::AfterHeartbeatTimeout(maybe_act))
                    }
                };
                match res {
                    Ok(outcome) => {
                        me.send_update_response(outcome, no_wft);
                    }
                    Err(e) => {
                        error!(error=?e, "Error in run machines");
                        me.am_broken = true;
                        me.update_tx
                            .send(RunUpdateResponse {
                                kind: RunUpdateResponseKind::Fail(FailRunUpdate {
                                    run_id: me.wfm.machines.run_id.clone(),
                                    err: e.source,
                                    completion_resp: e.complete_resp_chan,
                                }),
                                span: Span::current(),
                            })
                            .expect("Machine can send update");
                    }
                }
                (me, heartbeat_tx)
            }
            .instrument(span)
        })
        .await;
    }

    async fn incoming_wft(
        &mut self,
        wft: NewIncomingWFT,
    ) -> Result<Option<ActivationOrAuto>, RunUpdateErr> {
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
                    return Ok(None);
                } else {
                    panic!(
                        "Got a new WFT while there are outstanding local activities, but there \
                     was no waiting on LA info."
                    )
                }
            } else {
                return Ok(Some(ActivationOrAuto::Autocomplete {
                    run_id: self.wfm.machines.run_id.clone(),
                }));
            }
        }

        Ok(Some(ActivationOrAuto::LangActivation(activation)))
    }

    async fn completion(
        &mut self,
        mut completion: RunActivationCompletion,
        heartbeat_tx: &UnboundedSender<Span>,
    ) -> Result<Option<FulfillableActivationComplete>, RunUpdateErr> {
        let resp_chan = completion
            .resp_chan
            .take()
            .expect("Completion response channel must be populated");

        let data = CompletionDataForWFT {
            task_token: completion.task_token,
            query_responses: completion.query_responses,
            has_pending_query: completion.has_pending_query,
            activation_was_only_eviction: completion.activation_was_only_eviction,
        };

        // If this is just bookkeeping after a reply to an only-eviction activation, we can bypass
        // everything, since there is no reason to continue trying to update machines.
        if completion.activation_was_only_eviction {
            return Ok(Some(self.prepare_complete_resp(resp_chan, data, false)));
        }

        let outcome = async move {
            // Send commands from lang into the machines then check if the workflow run
            // needs another activation and mark it if so
            self.wfm
                .push_commands_and_iterate(completion.commands)
                .await?;
            // Don't bother applying the next task if we're evicting at the end of
            // this activation
            if !completion.activation_was_eviction {
                self.wfm.apply_next_task_if_ready().await?;
            }
            let new_local_acts = self.wfm.drain_queued_local_activities();
            self.sink_la_requests(new_local_acts)?;

            if self.wfm.machines.outstanding_local_activity_count() == 0 {
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
            Ok((None, data, me)) => Ok(Some(me.prepare_complete_resp(resp_chan, data, false))),
            Ok((Some((chan, start_t, wft_timeout)), data, me)) => {
                if let Some(wola) = me.waiting_on_la.as_mut() {
                    wola.heartbeat_timeout_task.abort();
                }
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
                Ok(None)
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
        has_wft: bool,
    ) -> Result<Option<ActivationOrAuto>, RunUpdateErr> {
        // In the event it's time to evict this run, cancel any outstanding LAs
        if want_to_evict.is_some() {
            self.sink_la_requests(vec![LocalActRequest::CancelAllInRun(
                self.wfm.machines.run_id.clone(),
            )])?;
        }

        if !has_wft {
            // It doesn't make sense to do workflow work unless we have a WFT
            return Ok(None);
        }

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

    fn prepare_complete_resp(
        &mut self,
        resp_chan: oneshot::Sender<ActivationCompleteResult>,
        data: CompletionDataForWFT,
        due_to_heartbeat_timeout: bool,
    ) -> FulfillableActivationComplete {
        let outgoing_cmds = self.wfm.get_server_commands();
        if data.activation_was_only_eviction && !outgoing_cmds.commands.is_empty() {
            dbg_panic!(
                "There should not be any outgoing commands when preparing a completion response \
                 if the activation was only an eviction. This is an SDK bug."
            );
        }

        let query_responses = data.query_responses;
        let has_query_responses = !query_responses.is_empty();
        let is_query_playback = data.has_pending_query && !has_query_responses;
        let mut force_new_wft = due_to_heartbeat_timeout;

        // We only actually want to send commands back to the server if there are no more pending
        // activations and we are caught up on replay. We don't want to complete a wft if we already
        // saw the final event in the workflow, or if we are playing back for the express purpose of
        // fulfilling a query. If the activation we sent was *only* an eviction, don't send that
        // either.
        let should_respond = !(self.wfm.machines.has_pending_jobs()
            || outgoing_cmds.replaying
            || is_query_playback
            || data.activation_was_only_eviction);
        // If there are pending LA resolutions, and we're responding to a query here,
        // we want to make sure to force a new task, as otherwise once we tell lang about
        // the LA resolution there wouldn't be any task to reply to with the result of iterating
        // the workflow.
        if has_query_responses && self.wfm.machines.has_pending_la_resolutions() {
            force_new_wft = true;
        }

        let outcome = if should_respond || has_query_responses {
            ActivationCompleteOutcome::ReportWFTSuccess(ServerCommandsWithWorkflowInfo {
                task_token: data.task_token,
                action: ActivationAction::WftComplete {
                    force_new_wft,
                    commands: outgoing_cmds.commands,
                    query_responses,
                },
            })
        } else {
            ActivationCompleteOutcome::DoNothing
        };
        FulfillableActivationComplete {
            result: ActivationCompleteResult {
                most_recently_processed_event: self.wfm.machines.last_processed_event as usize,
                outcome,
            },
            resp_chan,
        }
    }

    async fn local_resolution(
        &mut self,
        res: LocalResolution,
    ) -> Result<Option<FulfillableActivationComplete>, RunUpdateErr> {
        debug!(resolution=?res, "Applying local resolution");
        self.wfm.notify_of_local_result(res)?;
        if self.wfm.machines.outstanding_local_activity_count() == 0 {
            if let Some(mut wait_dat) = self.waiting_on_la.take() {
                // Cancel the heartbeat timeout
                wait_dat.heartbeat_timeout_task.abort();
                if let Some((completion_dat, resp_chan)) = wait_dat.completion_dat.take() {
                    return Ok(Some(self.prepare_complete_resp(
                        resp_chan,
                        completion_dat,
                        false,
                    )));
                }
            }
        }
        Ok(None)
    }

    /// Pump some local activity requests into the sink, applying any immediate results to the
    /// workflow machines.
    fn sink_la_requests(
        &mut self,
        new_local_acts: Vec<LocalActRequest>,
    ) -> Result<(), WFMachinesError> {
        let immediate_resolutions = (self.local_activity_request_sink)(new_local_acts);
        for resolution in immediate_resolutions {
            self.wfm
                .notify_of_local_result(LocalResolution::LocalActivity(resolution))?;
        }
        Ok(())
    }

    /// Returns `true` if autocompletion should be issued, which will actually cause us to end up
    /// in [completion] again, at which point we'll start a new heartbeat timeout, which will
    /// immediately trigger and thus finish the completion, forcing a new task as it should.
    fn heartbeat_timeout(&mut self) -> bool {
        if let Some(ref mut wait_dat) = self.waiting_on_la {
            // Cancel the heartbeat timeout
            wait_dat.heartbeat_timeout_task.abort();
            if let Some((completion_dat, resp_chan)) = wait_dat.completion_dat.take() {
                let compl = self.prepare_complete_resp(resp_chan, completion_dat, true);
                // Immediately fulfill the completion since the run update will already have
                // been replied to
                compl.fulfill();
            } else {
                // Auto-reply WFT complete
                return true;
            }
        } else {
            // If a heartbeat timeout happened, we should always have been waiting on LAs
            dbg_panic!("WFT heartbeat timeout fired but we were not waiting on any LAs");
        }
        false
    }

    fn send_update_response(&self, outcome: RunActionOutcome, no_wft: bool) {
        let mut in_response_to_wft = false;
        let (outgoing_activation, fulfillable_complete) = match outcome {
            RunActionOutcome::AfterNewWFT(a) => {
                in_response_to_wft = true;
                (a, None)
            }
            RunActionOutcome::AfterCheckWork(a) => (a, None),
            RunActionOutcome::AfterLocalResolution(f) => (None, f),
            RunActionOutcome::AfterCompletion(f) => (None, f),
            RunActionOutcome::AfterHeartbeatTimeout(a) => (a, None),
        };
        let mut more_pending_work = self.wfm.machines.has_pending_jobs();
        // We don't want to consider there to be more local-only work to be done if there is no
        // workflow task associated with the run right now. This can happen if, ex, we complete
        // a local activity while waiting for server to send us the next WFT. Activating lang would
        // be harmful at this stage, as there might be work returned in that next WFT which should
        // be part of the next activation.
        if no_wft {
            more_pending_work = false;
        }
        self.update_tx
            .send(RunUpdateResponse {
                kind: RunUpdateResponseKind::Good(GoodRunUpdate {
                    run_id: self.wfm.machines.run_id.clone(),
                    outgoing_activation,
                    fulfillable_complete,
                    have_seen_terminal_event: self.wfm.machines.have_seen_terminal_event,
                    more_pending_work,
                    most_recently_processed_event_number: self.wfm.machines.last_processed_event
                        as usize,
                    in_response_to_wft,
                }),
                span: Span::current(),
            })
            .expect("Machine can send update");
    }
}

fn start_heartbeat_timeout_task(
    chan: UnboundedSender<Span>,
    wft_start_time: Instant,
    wft_timeout: Duration,
) -> JoinHandle<()> {
    // The heartbeat deadline is 80% of the WFT timeout
    let wft_heartbeat_deadline =
        wft_start_time.add(wft_timeout.mul_f32(WFT_HEARTBEAT_TIMEOUT_FRACTION));
    task::spawn(async move {
        tokio::time::sleep_until(wft_heartbeat_deadline.into()).await;
        let _ = chan.send(Span::current());
    })
}

enum RunActionOutcome {
    AfterNewWFT(Option<ActivationOrAuto>),
    AfterCheckWork(Option<ActivationOrAuto>),
    AfterLocalResolution(Option<FulfillableActivationComplete>),
    AfterCompletion(Option<FulfillableActivationComplete>),
    AfterHeartbeatTimeout(Option<ActivationOrAuto>),
}

#[derive(derive_more::DebugCustom)]
#[debug(fmt = "RunUpdateErr({:?})", source)]
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
    async fn push_commands_and_iterate(&mut self, cmds: Vec<WFCommand>) -> Result<()> {
        if let Some(cs) = self.command_sink.as_mut() {
            cs.send(cmds).map_err(|_| {
                WFMachinesError::Fatal("Internal error buffering workflow commands".to_string())
            })?;
        }
        self.machines.iterate_machines().await?;
        Ok(())
    }
}
