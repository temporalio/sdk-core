use crate::{
    abstractions::{stream_when_allowed, MeteredSemaphore},
    protosext::ValidPollWFTQResponse,
    telemetry::metrics::workflow_worker_type,
    worker::{
        workflow::{
            history_update::NextPageToken,
            run_cache::RunCache,
            workflow_tasks::{
                ActivationAction, EvictionRequestResult, FailedActivationOutcome,
                OutstandingActivation, OutstandingTask, RunUpdateOutcome,
                ServerCommandsWithWorkflowInfo, WorkflowTaskInfo,
            },
            ActivationCompleteOutcome, ActivationCompleteResult, ActivationOrAuto,
            EmptyWorkflowCommandErr, FailRunUpdateResponse, GetStateInfoMsg, HistoryPaginator,
            HistoryUpdate, LocalResolutionMsg, PostActivationMsg, RequestEvictMsg,
            RunActivationCompletion, RunUpdateResponse, RunUpdatedFromWft, ValidatedCompletion,
            WFActCompleteMsg, WFCommand, WorkflowStateInfo,
        },
        LocalActRequest, LocalActivityResolution, LEGACY_QUERY_ID,
    },
    MetricsContext, WorkerClientBag,
};
use futures::{stream, stream::PollNext, Stream, StreamExt};
use std::{collections::VecDeque, fmt::Debug, future, sync::Arc, time::Instant};
use temporal_sdk_core_api::errors::{CompleteWfError, PollWfError, WFMachinesError};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{
            create_evict_activation, query_to_job, remove_from_cache::EvictionReason,
            workflow_activation_job,
        },
        workflow_completion::{
            workflow_activation_completion, Failure, WorkflowActivationCompletion,
        },
    },
    temporal::api::{enums::v1::WorkflowTaskFailedCause, failure::v1::Failure as TFailure},
};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver},
    oneshot,
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

pub(crate) struct WFStream {
    runs: RunCache,
    /// Buffered polls for new runs which need a cache slot to open up before we can handle them
    buffered_polls_need_cache_slot: VecDeque<ValidPollWFTQResponse>,

    /// Client for accessing server for history pagination etc.
    client: Arc<WorkerClientBag>,

    /// Ensures we stay at or below this worker's maximum concurrent workflow task limit
    wft_semaphore: MeteredSemaphore,
    shutdown_token: CancellationToken,

    metrics: MetricsContext,
}
enum WFActStreamInput {
    NewWft(ValidPollWFTQResponse),
    Completion(WFActCompleteMsg),
    LocalResolution(LocalResolutionMsg),
    PostActivation(PostActivationMsg),
    RunUpdateResponse(RunUpdateResponse),
    RequestEviction(RequestEvictMsg),
    GetStateInfo(GetStateInfoMsg),
    // The stream given to us which represents the poller (or a mock) terminated.
    PollerDead,
}
/// Everything that _isn't_ a poll which may affect workflow state. Always higher priority than
/// new polls.
#[derive(Debug, derive_more::From)]
pub(super) enum LocalInputs {
    Completion(WFActCompleteMsg),
    LocalResolution(LocalResolutionMsg),
    PostActivation(PostActivationMsg),
    RunUpdateResponse(RunUpdateResponse),
    RequestEviction(RequestEvictMsg),
    GetStateInfo(GetStateInfoMsg),
}
impl From<LocalInputs> for WFActStreamInput {
    fn from(l: LocalInputs) -> Self {
        match l {
            LocalInputs::Completion(c) => WFActStreamInput::Completion(c),
            LocalInputs::LocalResolution(r) => WFActStreamInput::LocalResolution(r),
            LocalInputs::PostActivation(p) => WFActStreamInput::PostActivation(p),
            LocalInputs::RunUpdateResponse(r) => WFActStreamInput::RunUpdateResponse(r),
            LocalInputs::RequestEviction(e) => WFActStreamInput::RequestEviction(e),
            LocalInputs::GetStateInfo(g) => WFActStreamInput::GetStateInfo(g),
        }
    }
}
#[derive(Debug, derive_more::From)]
#[allow(clippy::large_enum_variant)] // PollerDead only ever gets used once, so not important.
enum ExternalPollerInputs {
    NewWft(ValidPollWFTQResponse),
    PollerDead,
}
impl From<ExternalPollerInputs> for WFActStreamInput {
    fn from(l: ExternalPollerInputs) -> Self {
        match l {
            ExternalPollerInputs::NewWft(n) => WFActStreamInput::NewWft(n),
            ExternalPollerInputs::PollerDead => WFActStreamInput::PollerDead,
        }
    }
}

/// Non generic args for [WFStream::build]
pub(super) struct WFStreamBasics {
    pub max_cached_workflows: usize,
    pub max_outstanding_wfts: usize,
    pub shutdown_token: CancellationToken,
    pub metrics: MetricsContext,
}
impl WFStream {
    pub(super) fn build(
        basics: WFStreamBasics,
        external_wfts: impl Stream<Item = ValidPollWFTQResponse> + Send + 'static,
        wfts_from_complete: UnboundedReceiver<ValidPollWFTQResponse>,
        local_rx: impl Stream<Item = LocalInputs> + Send + 'static,
        client: Arc<WorkerClientBag>,
        local_activity_request_sink: impl Fn(Vec<LocalActRequest>) -> Vec<LocalActivityResolution>
            + Send
            + Sync
            + 'static,
    ) -> impl Stream<Item = Result<ActivationOrAuto, PollWfError>> {
        let (external_wft_allow_handle, poller_wfts) = stream_when_allowed(external_wfts);
        let new_wft_rx = stream::select(
            poller_wfts
                .map(ExternalPollerInputs::NewWft)
                .chain(stream::once(async { ExternalPollerInputs::PollerDead })),
            UnboundedReceiverStream::new(wfts_from_complete).map(ExternalPollerInputs::NewWft),
        );
        let (run_update_tx, run_update_rx) = unbounded_channel();
        let local_rx = stream::select(
            local_rx.map(Into::into),
            UnboundedReceiverStream::new(run_update_rx).map(WFActStreamInput::RunUpdateResponse),
        );
        let low_pri_streams = stream::select_all([new_wft_rx.map(Into::into).boxed()]);
        let all_inputs = stream::select_with_strategy(
            local_rx,
            low_pri_streams,
            // Priority always goes to the local stream
            |_: &mut ()| PollNext::Left,
        );
        let mut state = WFStream {
            buffered_polls_need_cache_slot: Default::default(),
            runs: RunCache::new(
                basics.max_cached_workflows,
                client.namespace().to_string(),
                run_update_tx,
                Arc::new(local_activity_request_sink),
                basics.metrics.clone(),
            ),
            client,
            wft_semaphore: MeteredSemaphore::new(
                basics.max_outstanding_wfts,
                basics.metrics.with_new_attrs([workflow_worker_type()]),
                MetricsContext::available_task_slots,
            ),
            shutdown_token: basics.shutdown_token,
            metrics: basics.metrics,
        };
        all_inputs
            .map(move |action| {
                let maybe_activation = match action {
                    WFActStreamInput::NewWft(wft) => {
                        debug!(run_id=%wft.workflow_execution.run_id, "New WFT");
                        state.instantiate_or_update(wft);
                        None
                    }
                    WFActStreamInput::RunUpdateResponse(resp) => {
                        match state.process_run_update_response(resp) {
                            RunUpdateOutcome::IssueActivation(act) => {
                                Some(ActivationOrAuto::LangActivation(act))
                            }
                            RunUpdateOutcome::Autocomplete { run_id } => {
                                Some(ActivationOrAuto::Autocomplete { run_id })
                            }
                            RunUpdateOutcome::Failure(err) => {
                                if let Some(resp_chan) = err.completion_resp {
                                    // Automatically fail the workflow task in the event we couldn't
                                    // update machines
                                    let fail_cause =
                                        if matches!(&err.err, WFMachinesError::Nondeterminism(_)) {
                                            WorkflowTaskFailedCause::NonDeterministicError
                                        } else {
                                            WorkflowTaskFailedCause::Unspecified
                                        };
                                    let wft_fail_str = format!("{:?}", err.err);
                                    state.failed_completion(
                                        err.run_id,
                                        fail_cause,
                                        err.err.evict_reason(),
                                        TFailure::application_failure(wft_fail_str, false).into(),
                                        resp_chan,
                                    );
                                } else {
                                    // TODO: This should probably also fail workflow tasks, but that
                                    //  wasn't implemented pre-refactor either.
                                    warn!(error=?err.err, run_id=%err.run_id,
                                          "Error while updating workflow");
                                    state.request_eviction(RequestEvictMsg {
                                        run_id: err.run_id,
                                        message: format!(
                                            "Error while updating workflow: {:?}",
                                            err.err
                                        ),
                                        reason: err.err.evict_reason(),
                                    });
                                }
                                None
                            }
                            RunUpdateOutcome::DoNothing => None,
                        }
                    }
                    WFActStreamInput::Completion(completion) => {
                        state.process_completion(completion);
                        None
                    }
                    WFActStreamInput::PostActivation(report) => {
                        state.process_post_activation(report);
                        None
                    }
                    WFActStreamInput::LocalResolution(res) => {
                        state.local_resolution(res);
                        None
                    }
                    WFActStreamInput::RequestEviction(evict) => {
                        state.request_eviction(evict);
                        None
                    }
                    WFActStreamInput::GetStateInfo(gsi) => {
                        let _ = gsi.response_tx.send(WorkflowStateInfo {
                            cached_workflows: state.runs.len(),
                            outstanding_wft: state.outstanding_wfts(),
                            available_wft_permits: state.wft_semaphore.sem.available_permits(),
                        });
                        None
                    }
                    WFActStreamInput::PollerDead => {
                        error!("Poller died");
                        state.shutdown_token.cancel();
                        None
                    }
                };

                if state.should_allow_poll() {
                    external_wft_allow_handle.allow_one();
                }

                if let Some(ref act) = maybe_activation {
                    if let Some(run_handle) = state.runs.get_mut(act.run_id()) {
                        run_handle.insert_outstanding_activation(act);
                    } else {
                        // TODO: Don't panic
                        panic!("Tried to insert activation for missing run!");
                    }
                }
                if state.shutdown_done() {
                    return Err(PollWfError::ShutDown);
                }

                Ok(maybe_activation)
            })
            .filter_map(|o| {
                future::ready(match o {
                    Ok(None) => None,
                    Ok(Some(v)) => Some(Ok(v)),
                    Err(e) => {
                        if !matches!(e, PollWfError::ShutDown) {
                            error!(
                            "Workflow processing encountered fatal error and must shut down {:?}",
                            e
                            );
                        }
                        Some(Err(e))
                    }
                })
            })
            // Stop the stream once we have shut down
            .take_while(|o| future::ready(!matches!(o, Err(PollWfError::ShutDown))))
    }

    fn process_run_update_response(&mut self, resp: RunUpdateResponse) -> RunUpdateOutcome {
        debug!("Processing run update response from machines, {:?}", resp);
        match resp {
            RunUpdateResponse::Good(resp) => {
                if let Some(r) = self.runs.get_mut(&resp.run_id) {
                    r.have_seen_terminal_event = resp.have_seen_terminal_event;
                    r.more_pending_work = resp.more_pending_work;
                    r.last_action_acked = true;
                    r.most_recently_processed_event_number =
                        resp.most_recently_processed_event_number;
                }
                let run_handle = self
                    .runs
                    .get_mut(&resp.run_id)
                    .expect("Workflow must exist, it just sent us an update response");

                let r = match resp.outgoing_activation {
                    Some(ActivationOrAuto::LangActivation(mut activation)) => {
                        if let Some(RunUpdatedFromWft {}) = resp.in_response_to_wft {
                            let wft = run_handle
                                .wft
                                .as_mut()
                                .expect("WFT must exist for run just updated with one");
                            // If there are in-poll queries, insert jobs for those queries into the
                            // activation, but only if we hit the cache. If we didn't, those queries
                            // will need to be dealt with once replay is over
                            // TODO: Probably should be *and replay is finished*??
                            if !wft.pending_queries.is_empty() && wft.hit_cache {
                                let query_jobs = wft.pending_queries.drain(..).map(|q| {
                                    workflow_activation_job::Variant::QueryWorkflow(q).into()
                                });
                                activation.jobs.extend(query_jobs);
                            }
                        }

                        if activation.jobs.is_empty() {
                            panic!("Should not send lang activation with no jobs");
                        } else {
                            RunUpdateOutcome::IssueActivation(activation)
                        }
                    }
                    Some(ActivationOrAuto::ReadyForQueries(mut act)) => {
                        if let Some(wft) = run_handle.wft.as_mut() {
                            info!("Queries? {:?}", wft.pending_queries);
                            let query_jobs = wft
                                .pending_queries
                                .drain(..)
                                .map(|q| workflow_activation_job::Variant::QueryWorkflow(q).into());
                            act.jobs.extend(query_jobs);
                            RunUpdateOutcome::IssueActivation(act)
                        } else {
                            panic!("Ready for queries but no WFT!");
                        }
                    }
                    Some(ActivationOrAuto::Autocomplete { run_id }) => {
                        RunUpdateOutcome::Autocomplete { run_id }
                    }
                    None => {
                        // If the response indicates there is no activation to send yet but there
                        // is more pending work, we should check again.
                        if resp.more_pending_work {
                            run_handle.check_more_activations();
                            return RunUpdateOutcome::DoNothing;
                        }

                        // If a run update came back and had nothing to do, but we're trying to
                        // evict, just do that now as long as there's no other outstanding work.
                        if let Some(reason) = run_handle.trying_to_evict.as_ref() {
                            if run_handle.activation.is_none() && !run_handle.more_pending_work {
                                let evict_act = create_evict_activation(
                                    resp.run_id,
                                    reason.message.clone(),
                                    reason.reason,
                                );
                                return RunUpdateOutcome::IssueActivation(evict_act);
                            }
                        }
                        RunUpdateOutcome::DoNothing
                    }
                };
                // After each run update, check if it's ready to handle any buffered poll
                if matches!(
                    &r,
                    RunUpdateOutcome::Autocomplete { .. } | RunUpdateOutcome::DoNothing
                ) && !run_handle.has_any_pending_work(false, true)
                {
                    if let Some(bufft) = run_handle.buffered_resp.take() {
                        self.instantiate_or_update(bufft);
                    }
                }
                r
            }
            RunUpdateResponse::Fail(fail) => {
                if let Some(r) = self.runs.get_mut(&fail.run_id) {
                    r.last_action_acked = true;
                }
                RunUpdateOutcome::Failure(FailRunUpdateResponse {
                    err: fail.err,
                    run_id: fail.run_id,
                    completion_resp: fail.completion_resp,
                })
            }
        }
    }

    #[instrument(level = "debug", skip(self, work))]
    fn instantiate_or_update(&mut self, work: ValidPollWFTQResponse) {
        let mut work = if let Some(w) = self.buffer_resp_if_outstanding_work(work) {
            w
        } else {
            return;
        };

        let run_id = work.workflow_execution.run_id.clone();
        // If our cache is full and this WFT is for an unseen run we must first evict a run before
        // we can deal with this task. So, buffer the task in that case.
        if !self.runs.has_run(&run_id) && self.runs.is_full() {
            self.buffer_resp_on_full_cache(work);
            return;
        }

        let start_event_id = work.history.events.first().map(|e| e.event_id);
        debug!(
            run_id = %run_id,
            task_token = %&work.task_token,
            history_length = %work.history.events.len(),
            start_event_id = ?start_event_id,
            has_legacy_query = %work.legacy_query.is_some(),
            attempt = %work.attempt,
            "Applying new workflow task from server"
        );

        let wft_info = WorkflowTaskInfo {
            attempt: work.attempt,
            task_token: work.task_token,
        };
        let poll_resp_is_incremental = work
            .history
            .events
            .get(0)
            .map(|ev| ev.event_id > 1)
            .unwrap_or_default();
        let poll_resp_is_incremental = poll_resp_is_incremental || work.history.events.is_empty();

        let mut did_miss_cache = !poll_resp_is_incremental;

        let page_token = if !self.runs.has_run(&run_id) && poll_resp_is_incremental {
            debug!(run_id=?run_id, "Workflow task has partial history, but workflow is not in \
                   cache. Will fetch history");
            self.metrics.sticky_cache_miss();
            did_miss_cache = true;
            NextPageToken::FetchFromStart
        } else {
            work.next_page_token.into()
        };
        let history_update = HistoryUpdate::new(
            HistoryPaginator::new(
                work.history,
                work.workflow_execution.workflow_id.clone(),
                run_id.clone(),
                page_token,
                self.client.clone(),
            ),
            work.previous_started_event_id,
        );
        let legacy_query_from_poll = work
            .legacy_query
            .take()
            .map(|q| query_to_job(LEGACY_QUERY_ID.to_string(), q));

        let mut pending_queries = work.query_requests.into_iter().collect::<Vec<_>>();
        if !pending_queries.is_empty() && legacy_query_from_poll.is_some() {
            error!(
                "Server issued both normal and legacy queries. This should not happen. Please \
                 file a bug report."
            );
            self.request_eviction(RequestEvictMsg {
                run_id,
                message: "Server issued both normal and legacy query".to_string(),
                reason: EvictionReason::Fatal,
            });
            return;
        }
        if let Some(lq) = legacy_query_from_poll {
            pending_queries.push(lq);
        }

        let start_time = Instant::now();
        let run_handle = self.runs.instantiate_or_update(
            &run_id,
            &work.workflow_execution.workflow_id,
            &work.workflow_type,
            history_update,
            start_time,
        );
        run_handle.wft = Some(OutstandingTask {
            info: wft_info,
            hit_cache: !did_miss_cache,
            pending_queries,
            start_time,
            // TODO: Probably shouldn't/can't be expect
            _permit: self
                .wft_semaphore
                .try_acquire_owned()
                .expect("WFT Permit is available if we allowed poll"),
        })
    }

    #[instrument(level = "debug", skip(self, complete))]
    fn process_completion(&mut self, complete: WFActCompleteMsg) {
        match self.validate_completion(complete.completion) {
            Ok(ValidatedCompletion::Success { run_id, commands }) => {
                self.successful_completion(run_id, commands, complete.response_tx);
            }
            Ok(ValidatedCompletion::Fail { run_id, failure }) => {
                self.failed_completion(
                    run_id,
                    WorkflowTaskFailedCause::Unspecified,
                    EvictionReason::LangFail,
                    failure,
                    complete.response_tx,
                );
            }
            Err(_) => {
                todo!("Immediately reply on complete chan w/ err")
            }
        }
        // Always queue evictions after completion when we have a zero-size cache
        if self.runs.cache_capacity() == 0 {
            self.request_eviction_of_lru_run();
        }
    }

    fn successful_completion(
        &mut self,
        run_id: String,
        mut commands: Vec<WFCommand>,
        resp_chan: oneshot::Sender<ActivationCompleteResult>,
    ) {
        let activation_was_only_eviction = self.activation_has_only_eviction(&run_id);
        let (task_token, has_pending_query, start_time) =
            if let Some(entry) = self.get_task(&run_id) {
                (
                    entry.info.task_token.clone(),
                    !entry.pending_queries.is_empty(),
                    entry.start_time,
                )
            } else {
                if !activation_was_only_eviction {
                    // Don't bother warning if this was an eviction, since it's normal to issue
                    // eviction activations without an associated workflow task in that case.
                    warn!(
                        run_id=%run_id,
                        "Attempted to complete activation for run without associated workflow task"
                    );
                }
                self.reply_to_complete(&run_id, ActivationCompleteOutcome::DoNothing, resp_chan);
                return;
            };

        // If the only command from the activation is a legacy query response, that means we need
        // to respond differently than a typical activation.
        if matches!(&commands.as_slice(),
                    &[WFCommand::QueryResponse(qr)] if qr.query_id == LEGACY_QUERY_ID)
        {
            let qr = match commands.remove(0) {
                WFCommand::QueryResponse(qr) => qr,
                _ => unreachable!("We just verified this is the only command"),
            };
            self.reply_to_complete(
                &run_id,
                ActivationCompleteOutcome::ReportWFTSuccess(ServerCommandsWithWorkflowInfo {
                    task_token,
                    action: ActivationAction::RespondLegacyQuery { result: qr },
                }),
                resp_chan,
            );
        } else {
            // First strip out query responses from other commands that actually affect machines
            // Would be prettier with `drain_filter`
            let mut i = 0;
            let mut query_responses = vec![];
            while i < commands.len() {
                if matches!(commands[i], WFCommand::QueryResponse(_)) {
                    if let WFCommand::QueryResponse(qr) = commands.remove(i) {
                        query_responses.push(qr);
                    }
                } else {
                    i += 1;
                }
            }

            let activation_was_eviction = self.activation_has_eviction(&run_id);
            self.runs
                .get_mut(&run_id)
                .expect("TODO: no expect here")
                .send_completion(RunActivationCompletion {
                    task_token,
                    start_time,
                    commands,
                    activation_was_eviction,
                    activation_was_only_eviction,
                    has_pending_query,
                    query_responses,
                    resp_chan: Some(resp_chan),
                });
        };
    }

    fn failed_completion(
        &mut self,
        run_id: String,
        cause: WorkflowTaskFailedCause,
        reason: EvictionReason,
        failure: Failure,
        resp_chan: oneshot::Sender<ActivationCompleteResult>,
    ) {
        let tt = if let Some(tt) = self.get_task(&run_id).map(|t| t.info.task_token.clone()) {
            tt
        } else {
            warn!(
                "No workflow task for run id {} found when trying to fail activation",
                run_id
            );
            self.reply_to_complete(
                &run_id,
                ActivationCompleteOutcome::ReportWFTFail(FailedActivationOutcome::NoReport),
                resp_chan,
            );
            return;
        };

        if let Some(m) = self.run_metrics(&run_id) {
            m.wf_task_failed();
        }
        let message = format!("Workflow activation completion failed: {:?}", &failure);
        // If the outstanding activation is a legacy query task, report that we need to fail it
        let outcome = if let Some(OutstandingActivation::LegacyQuery) = self.get_activation(&run_id)
        {
            FailedActivationOutcome::ReportLegacyQueryFailure(tt, failure)
        } else {
            // Blow up any cached data associated with the workflow
            let should_report = match self.request_eviction(RequestEvictMsg {
                run_id: run_id.clone(),
                message,
                reason,
            }) {
                EvictionRequestResult::EvictionRequested(Some(attempt))
                | EvictionRequestResult::EvictionAlreadyRequested(Some(attempt)) => attempt <= 1,
                _ => false,
            };
            if should_report {
                FailedActivationOutcome::Report(tt, cause, failure)
            } else {
                FailedActivationOutcome::NoReport
            }
        };
        self.reply_to_complete(
            &run_id,
            ActivationCompleteOutcome::ReportWFTFail(outcome),
            resp_chan,
        );
    }

    fn process_post_activation(&mut self, report: PostActivationMsg) {
        let run_id = &report.run_id;

        if self
            .get_activation(run_id)
            .map(|a| a.has_eviction())
            .unwrap_or_default()
        {
            self.evict_run(run_id);
        };

        // If we reported to server, we always want to mark it complete.
        self.complete_wft(run_id, report.reported_wft_to_server);
        if let Some(rh) = self.runs.get_mut(run_id) {
            // Delete the activation
            rh.activation.take();
            // Attempt to produce the next activation if needed
            rh.check_more_activations();
        }
    }

    fn local_resolution(&mut self, msg: LocalResolutionMsg) {
        let run_id = msg.run_id;
        if let Some(rh) = self.runs.get_mut(&run_id) {
            rh.send_local_resolution(msg.res)
        } else {
            // It isn't an explicit error if the machine is missing when a local activity resolves.
            // This can happen if an activity reports a timeout after we stopped caring about it.
            debug!(run_id = %run_id,
                   "Tried to resolve a local activity for a run we are no longer tracking");
        }
    }

    fn validate_completion(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<ValidatedCompletion, CompleteWfError> {
        match completion.status {
            Some(workflow_activation_completion::Status::Successful(success)) => {
                // Convert to wf commands
                let commands = success
                    .commands
                    .into_iter()
                    .map(|c| c.try_into())
                    .collect::<Result<Vec<_>, EmptyWorkflowCommandErr>>()
                    .map_err(|_| CompleteWfError::MalformedWorkflowCompletion {
                        reason: "At least one workflow command in the completion contained \
                                 an empty variant"
                            .to_owned(),
                        run_id: completion.run_id.clone(),
                    })?;

                if commands.len() > 1 && commands.iter().any(|c| {
                    matches!(c, WFCommand::QueryResponse(q) if q.query_id == LEGACY_QUERY_ID)
                }) {
                    return Err(CompleteWfError::MalformedWorkflowCompletion {
                        reason: "Workflow completion had a legacy query response along with other \
                                commands. This is not allowed and constitutes an error in the \
                                lang SDK".to_owned(),
                        run_id: completion.run_id,
                    });
                }

                Ok(ValidatedCompletion::Success {
                    run_id: completion.run_id,
                    commands,
                })
            }
            Some(workflow_activation_completion::Status::Failed(failure)) => {
                Ok(ValidatedCompletion::Fail {
                    run_id: completion.run_id,
                    failure,
                })
            }
            None => Err(CompleteWfError::MalformedWorkflowCompletion {
                reason: "Workflow completion had empty status field".to_owned(),
                run_id: completion.run_id,
            }),
        }
    }

    /// Request a workflow eviction. This will (eventually, after replay is done) queue up an
    /// activation to evict the workflow from the lang side. Workflow will not *actually* be evicted
    /// until lang replies to that activation
    fn request_eviction(&mut self, info: RequestEvictMsg) -> EvictionRequestResult {
        let activation_has_eviction = self.activation_has_eviction(&info.run_id);
        if let Some(rh) = self.runs.get_mut(&info.run_id) {
            let attempts = rh.wft.as_ref().map(|wt| wt.info.attempt);
            if !activation_has_eviction && rh.trying_to_evict.is_none() {
                debug!(run_id=%info.run_id, message=%info.message, "Eviction requested");
                rh.trying_to_evict = Some(info);
                rh.check_more_activations();
                EvictionRequestResult::EvictionRequested(attempts)
            } else {
                EvictionRequestResult::EvictionAlreadyRequested(attempts)
            }
        } else {
            warn!(run_id=%info.run_id, "Eviction requested for unknown run");
            EvictionRequestResult::NotFound
        }
    }

    fn request_eviction_of_lru_run(&mut self) -> EvictionRequestResult {
        if let Some(lru_run_id) = self.runs.current_lru_run() {
            let run_id = lru_run_id.to_string();
            self.request_eviction(RequestEvictMsg {
                run_id,
                message: "Workflow cache full".to_string(),
                reason: EvictionReason::CacheFull,
            })
        } else {
            // This branch shouldn't really be possible
            EvictionRequestResult::NotFound
        }
    }

    /// Evict a workflow from the cache by its run id. Any existing pending activations will be
    /// destroyed, and any outstanding activations invalidated.
    fn evict_run(&mut self, run_id: &str) {
        debug!(run_id=%run_id, "Evicting run");

        let mut did_take_buff = false;
        // Now it can safely be deleted, it'll get recreated once the un-buffered poll is handled if
        // there was one.
        if let Some(mut rh) = self.runs.remove(run_id) {
            rh.handle.abort();

            if let Some(buff) = rh.buffered_resp.take() {
                self.instantiate_or_update(buff);
                did_take_buff = true;
            }
        }

        if !did_take_buff {
            // If there wasn't a buffered poll, there might be one for a different run which needs
            // a free cache slot, and now there is.
            if let Some(buff) = self.buffered_polls_need_cache_slot.pop_front() {
                self.instantiate_or_update(buff);
            }
        }
    }

    fn complete_wft(
        &mut self,
        run_id: &str,
        reported_wft_to_server: bool,
    ) -> Option<OutstandingTask> {
        // If the WFT completion wasn't sent to the server, but we did see the final event, we still
        // want to clear the workflow task. This can really only happen in replay testing, where we
        // will generate poll responses with complete history but no attached query, and such a WFT
        // would never really exist. The server wouldn't send a workflow task with nothing to do,
        // but they are very useful for testing complete replay.
        let saw_final = self
            .runs
            .get(run_id)
            .map(|r| r.have_seen_terminal_event)
            .unwrap_or_default();
        if !saw_final && !reported_wft_to_server {
            return None;
        }

        if let Some(rh) = self.runs.get_mut(run_id) {
            // TODO: Not right I think
            // Can't mark the WFT complete if there are pending queries, as doing so would destroy
            // them.
            if rh
                .wft
                .as_ref()
                .map(|wft| !wft.pending_queries.is_empty())
                .unwrap_or_default()
            {
                return None;
            }

            debug!("Marking WFT completed");
            let retme = rh.wft.take();
            if let Some(ot) = &retme {
                if let Some(m) = self.run_metrics(run_id) {
                    m.wf_task_latency(ot.start_time.elapsed());
                }
            }
            retme
        } else {
            None
        }
    }

    /// Stores some work if there is any outstanding WFT or activation for the run. If there was
    /// not, returns the work back out inside the option.
    fn buffer_resp_if_outstanding_work(
        &mut self,
        work: ValidPollWFTQResponse,
    ) -> Option<ValidPollWFTQResponse> {
        let run_id = &work.workflow_execution.run_id;
        if let Some(mut run) = self.runs.get_mut(run_id) {
            let about_to_issue_evict = run.trying_to_evict.is_some() && !run.last_action_acked;
            let has_wft = run.wft.is_some();
            let has_activation = run.activation.is_some();
            if has_wft
                || has_activation
                || about_to_issue_evict
                || run.more_pending_work
                || !run.last_action_acked
            {
                debug!(run_id = %run_id, run = ?run,
                       "Got new WFT for a run with outstanding work");
                run.buffered_resp = Some(work);
                None
            } else {
                Some(work)
            }
        } else {
            Some(work)
        }
    }

    fn buffer_resp_on_full_cache(&mut self, work: ValidPollWFTQResponse) {
        debug!(run_id=%work.workflow_execution.run_id, "Buffering WFT because cache is full");
        // If there's already a buffered poll for the run, replace it.
        if let Some(rh) = self
            .buffered_polls_need_cache_slot
            .iter_mut()
            .find(|w| w.workflow_execution.run_id == work.workflow_execution.run_id)
        {
            *rh = work;
        } else {
            // Otherwise push it to the back
            self.buffered_polls_need_cache_slot.push_back(work);
        }

        // We must ensure that there are at least as many pending evictions as there are tasks
        // that we might need to un-buffer (skipping runs which already have buffered tasks for
        // themselves)
        let mut num_in_buff = self.buffered_polls_need_cache_slot.len();
        let mut evict_these = vec![];
        for (rid, handle) in self.runs.runs_lru_order() {
            if num_in_buff == 0 {
                break;
            }
            if handle.buffered_resp.is_none() {
                num_in_buff -= 1;
                evict_these.push(rid.to_string());
            }
        }
        for run_id in evict_these {
            self.request_eviction(RequestEvictMsg {
                run_id,
                message: "Workflow cache full".to_string(),
                reason: EvictionReason::CacheFull,
            });
        }
    }

    fn reply_to_complete(
        &self,
        run_id: &str,
        outcome: ActivationCompleteOutcome,
        chan: oneshot::Sender<ActivationCompleteResult>,
    ) {
        let most_recently_processed_event = self
            .runs
            .peek(run_id)
            .map(|rh| rh.most_recently_processed_event_number)
            .unwrap_or_default();
        chan.send(ActivationCompleteResult {
            most_recently_processed_event,
            outcome,
        })
        .expect("Rcv half of activation reply not dropped");
    }

    fn shutdown_done(&self) -> bool {
        let all_runs_ready = self
            .runs
            .handles()
            .all(|r| !r.has_any_pending_work(true, false));
        if self.shutdown_token.is_cancelled() && all_runs_ready {
            info!("Workflow shutdown is done");
            true
        } else {
            false
        }
    }

    fn get_task(&mut self, run_id: &str) -> Option<&OutstandingTask> {
        self.runs.get(run_id).and_then(|rh| rh.wft.as_ref())
    }

    fn get_activation(&mut self, run_id: &str) -> Option<&OutstandingActivation> {
        self.runs.get(run_id).and_then(|rh| rh.activation.as_ref())
    }

    fn run_metrics(&mut self, run_id: &str) -> Option<&MetricsContext> {
        self.runs.get(run_id).map(|r| &r.metrics)
    }

    fn activation_has_only_eviction(&mut self, run_id: &str) -> bool {
        self.runs
            .get(run_id)
            .and_then(|rh| rh.activation)
            .map(OutstandingActivation::has_only_eviction)
            .unwrap_or_default()
    }

    fn activation_has_eviction(&mut self, run_id: &str) -> bool {
        self.runs
            .get(run_id)
            .and_then(|rh| rh.activation)
            .map(OutstandingActivation::has_eviction)
            .unwrap_or_default()
    }

    fn outstanding_wfts(&self) -> usize {
        self.runs.handles().filter(|r| r.wft.is_some()).count()
    }

    fn should_allow_poll(&self) -> bool {
        self.runs.can_accept_new() && self.wft_semaphore.sem.available_permits() > 0
    }

    // Useful when debugging
    #[allow(dead_code)]
    fn info_dump(&self, run_id: &str) {
        if let Some(r) = self.runs.peek(run_id) {
            info!(run_id, wft=?r.wft, activation=?r.activation, buffered=r.buffered_resp.is_some(),
                  trying_to_evict=r.trying_to_evict.is_some(), more_work=r.more_pending_work,
                  last_action_acked=r.last_action_acked);
        } else {
            info!(run_id, "Run not found");
        }
    }
}
