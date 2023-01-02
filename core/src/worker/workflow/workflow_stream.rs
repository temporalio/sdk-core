use crate::{
    abstractions::{dbg_panic, stream_when_allowed, MeteredSemaphore},
    protosext::ValidPollWFTQResponse,
    telemetry::metrics::workflow_worker_type,
    worker::{
        workflow::{history_update::NextPageToken, run_cache::RunCache, *},
        LocalActRequest, LocalActivityResolution, LEGACY_QUERY_ID,
    },
    MetricsContext,
};
use futures::{stream, stream::PollNext, Stream, StreamExt};
use std::{collections::VecDeque, fmt::Debug, future, sync::Arc, time::Instant};
use temporal_sdk_core_api::errors::{PollWfError, WFMachinesError};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{
            create_evict_activation, query_to_job, remove_from_cache::EvictionReason,
            workflow_activation_job,
        },
        workflow_completion::Failure,
    },
    temporal::api::{enums::v1::WorkflowTaskFailedCause, failure::v1::Failure as TFailure},
};
use tokio::sync::{mpsc::unbounded_channel, oneshot};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{Level, Span};

/// This struct holds all the state needed for tracking what workflow runs are currently cached
/// and how WFTs should be dispatched to them, etc.
///
/// See [WFStream::build] for more
pub(crate) struct WFStream {
    runs: RunCache,
    /// Buffered polls for new runs which need a cache slot to open up before we can handle them
    buffered_polls_need_cache_slot: VecDeque<PermittedWFT>,

    /// Client for accessing server for history pagination etc.
    client: Arc<dyn WorkerClient>,

    /// Ensures we stay at or below this worker's maximum concurrent workflow task limit
    wft_semaphore: MeteredSemaphore,
    shutdown_token: CancellationToken,
    ignore_evicts_on_shutdown: bool,

    metrics: MetricsContext,
}
impl WFStream {
    fn record_span_fields(&mut self, run_id: &str, span: &Span) {
        if let Some(run_handle) = self.runs.get_mut(run_id) {
            if let Some(spid) = span.id() {
                if run_handle.recorded_span_ids.contains(&spid) {
                    return;
                }
                run_handle.recorded_span_ids.insert(spid);

                if let Some(wid) = run_handle.wft.as_ref().map(|wft| &wft.info.wf_id) {
                    span.record("workflow_id", wid.as_str());
                }
            }
        }
    }
}

/// All possible inputs to the [WFStream]
#[derive(derive_more::From, Debug)]
enum WFStreamInput {
    NewWft(PermittedWFT),
    Local(LocalInput),
    /// The stream given to us which represents the poller (or a mock) terminated.
    PollerDead,
    /// The stream given to us which represents the poller (or a mock) encountered a non-retryable
    /// error while polling
    PollerError(tonic::Status),
}
impl From<RunUpdateResponse> for WFStreamInput {
    fn from(r: RunUpdateResponse) -> Self {
        WFStreamInput::Local(LocalInput {
            input: LocalInputs::RunUpdateResponse(r.kind),
            span: r.span,
        })
    }
}
/// A non-poller-received input to the [WFStream]
#[derive(derive_more::DebugCustom)]
#[debug(fmt = "LocalInput {{ {:?} }}", input)]
pub(super) struct LocalInput {
    pub input: LocalInputs,
    pub span: Span,
}
/// Everything that _isn't_ a poll which may affect workflow state. Always higher priority than
/// new polls.
#[derive(Debug, derive_more::From)]
pub(super) enum LocalInputs {
    Completion(WFActCompleteMsg),
    LocalResolution(LocalResolutionMsg),
    PostActivation(PostActivationMsg),
    RunUpdateResponse(RunUpdateResponseKind),
    RequestEviction(RequestEvictMsg),
    GetStateInfo(GetStateInfoMsg),
}
impl LocalInputs {
    fn run_id(&self) -> Option<&str> {
        Some(match self {
            LocalInputs::Completion(c) => c.completion.run_id(),
            LocalInputs::LocalResolution(lr) => &lr.run_id,
            LocalInputs::PostActivation(pa) => &pa.run_id,
            LocalInputs::RunUpdateResponse(rur) => rur.run_id(),
            LocalInputs::RequestEviction(re) => &re.run_id,
            LocalInputs::GetStateInfo(_) => return None,
        })
    }
}
#[derive(Debug, derive_more::From)]
#[allow(clippy::large_enum_variant)] // PollerDead only ever gets used once, so not important.
enum ExternalPollerInputs {
    NewWft(PermittedWFT),
    PollerDead,
    PollerError(tonic::Status),
}
impl From<ExternalPollerInputs> for WFStreamInput {
    fn from(l: ExternalPollerInputs) -> Self {
        match l {
            ExternalPollerInputs::NewWft(v) => WFStreamInput::NewWft(v),
            ExternalPollerInputs::PollerDead => WFStreamInput::PollerDead,
            ExternalPollerInputs::PollerError(e) => WFStreamInput::PollerError(e),
        }
    }
}

impl WFStream {
    /// Constructs workflow state management and returns a stream which outputs activations.
    ///
    /// * `external_wfts` is a stream of validated poll responses as returned by a poller (or mock)
    /// * `wfts_from_complete` is the recv side of a channel that new WFTs from completions should
    ///   come down.
    /// * `local_rx` is a stream of actions that workflow state needs to see. Things like
    ///   completions, local activities finishing, etc. See [LocalInputs].
    ///
    /// These inputs are combined, along with an internal feedback channel for run-specific updates,
    /// to form the inputs to a stream of [WFActStreamInput]s. The stream processor then takes
    /// action on those inputs, and then may yield activations.
    ///
    /// Updating runs may need to do async work like fetching additional history. In order to
    /// facilitate this, each run lives in its own task which is communicated with by sending
    /// [RunAction]s and receiving [RunUpdateResponse]s via its [ManagedRunHandle].
    pub(super) fn build(
        basics: WorkflowBasics,
        external_wfts: impl Stream<Item = Result<ValidPollWFTQResponse, tonic::Status>> + Send + 'static,
        local_rx: impl Stream<Item = LocalInput> + Send + 'static,
        client: Arc<dyn WorkerClient>,
        local_activity_request_sink: impl Fn(Vec<LocalActRequest>) -> Vec<LocalActivityResolution>
            + Send
            + Sync
            + 'static,
    ) -> impl Stream<Item = Result<ActivationOrAuto, PollWfError>> {
        let wft_semaphore = MeteredSemaphore::new(
            basics.max_outstanding_wfts,
            basics.metrics.with_new_attrs([workflow_worker_type()]),
            MetricsContext::available_task_slots,
        );
        let wft_sem_clone = wft_semaphore.clone();
        let proceeder = stream::unfold(wft_sem_clone, |sem| async move {
            Some((sem.acquire_owned().await.unwrap(), sem))
        });
        let poller_wfts = stream_when_allowed(external_wfts, proceeder);
        let (run_update_tx, run_update_rx) = unbounded_channel();
        let local_rx = stream::select(
            local_rx.map(Into::into),
            UnboundedReceiverStream::new(run_update_rx).map(Into::into),
        );
        let all_inputs = stream::select_with_strategy(
            local_rx,
            poller_wfts
                .map(|(wft, permit)| match wft {
                    Ok(wft) => ExternalPollerInputs::NewWft(PermittedWFT { wft, permit }),
                    Err(e) => ExternalPollerInputs::PollerError(e),
                })
                .chain(stream::once(async { ExternalPollerInputs::PollerDead }))
                .map(Into::into)
                .boxed(),
            // Priority always goes to the local stream
            |_: &mut ()| PollNext::Left,
        );
        let mut state = WFStream {
            buffered_polls_need_cache_slot: Default::default(),
            runs: RunCache::new(
                basics.max_cached_workflows,
                basics.namespace.clone(),
                run_update_tx,
                Arc::new(local_activity_request_sink),
                basics.metrics.clone(),
            ),
            client,
            wft_semaphore,
            shutdown_token: basics.shutdown_token,
            ignore_evicts_on_shutdown: basics.ignore_evicts_on_shutdown,
            metrics: basics.metrics,
        };
        all_inputs
            .map(move |action| {
                let span = span!(Level::DEBUG, "new_stream_input", action=?action);
                let _span_g = span.enter();

                let maybe_activation = match action {
                    WFStreamInput::NewWft(pwft) => {
                        debug!(run_id=%pwft.wft.workflow_execution.run_id, "New WFT");
                        state.instantiate_or_update(pwft);
                        None
                    }
                    WFStreamInput::Local(local_input) => {
                        let _span_g = local_input.span.enter();
                        if let Some(rid) = local_input.input.run_id() {
                            state.record_span_fields(rid, &local_input.span);
                        }
                        match local_input.input {
                            LocalInputs::RunUpdateResponse(resp) => {
                                state.process_run_update_response(resp)
                            }
                            LocalInputs::Completion(completion) => {
                                state.process_completion(completion);
                                None
                            }
                            LocalInputs::PostActivation(report) => {
                                state.process_post_activation(report);
                                None
                            }
                            LocalInputs::LocalResolution(res) => {
                                state.local_resolution(res);
                                None
                            }
                            LocalInputs::RequestEviction(evict) => {
                                state.request_eviction(evict);
                                None
                            }
                            LocalInputs::GetStateInfo(gsi) => {
                                let _ = gsi.response_tx.send(WorkflowStateInfo {
                                    cached_workflows: state.runs.len(),
                                    outstanding_wft: state.outstanding_wfts(),
                                    available_wft_permits: state.wft_semaphore.available_permits(),
                                });
                                None
                            }
                        }
                    }
                    WFStreamInput::PollerDead => {
                        debug!("WFT poller died, shutting down");
                        state.shutdown_token.cancel();
                        None
                    }
                    WFStreamInput::PollerError(e) => {
                        warn!("WFT poller errored, shutting down");
                        return Err(PollWfError::TonicError(e));
                    }
                };

                if let Some(ref act) = maybe_activation {
                    if let Some(run_handle) = state.runs.get_mut(act.run_id()) {
                        run_handle.insert_outstanding_activation(act);
                    } else {
                        dbg_panic!("Tried to insert activation for missing run!");
                    }
                }
                state.reconcile_buffered();
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

    fn process_run_update_response(
        &mut self,
        resp: RunUpdateResponseKind,
    ) -> Option<ActivationOrAuto> {
        debug!(resp=%resp, "Processing run update response from machines");
        match resp {
            RunUpdateResponseKind::Good(mut resp) => {
                let run_handle = self
                    .runs
                    .get_mut(&resp.run_id)
                    .expect("Workflow must exist, it just sent us an update response");
                run_handle.have_seen_terminal_event = resp.have_seen_terminal_event;
                run_handle.more_pending_work = resp.more_pending_work;
                run_handle.last_action_acked = true;
                run_handle.most_recently_processed_event_number =
                    resp.most_recently_processed_event_number;

                let r = match resp.outgoing_activation {
                    Some(ActivationOrAuto::LangActivation(mut activation)) => {
                        if resp.in_response_to_wft {
                            let wft = run_handle
                                .wft
                                .as_mut()
                                .expect("WFT must exist for run just updated with one");
                            // If there are in-poll queries, insert jobs for those queries into the
                            // activation, but only if we hit the cache. If we didn't, those queries
                            // will need to be dealt with once replay is over
                            if wft.hit_cache {
                                put_queries_in_act(&mut activation, wft);
                            }
                        }

                        if activation.jobs.is_empty() {
                            dbg_panic!("Should not send lang activation with no jobs");
                        }
                        Some(ActivationOrAuto::LangActivation(activation))
                    }
                    Some(ActivationOrAuto::ReadyForQueries(mut act)) => {
                        if let Some(wft) = run_handle.wft.as_mut() {
                            put_queries_in_act(&mut act, wft);
                            Some(ActivationOrAuto::LangActivation(act))
                        } else {
                            dbg_panic!("Ready for queries but no WFT!");
                            None
                        }
                    }
                    a @ Some(ActivationOrAuto::Autocomplete { .. }) => a,
                    None => {
                        // If the response indicates there is no activation to send yet but there
                        // is more pending work, we should check again.
                        if run_handle.more_pending_work {
                            run_handle.check_more_activations();
                            None
                        } else if let Some(reason) = run_handle.trying_to_evict.as_ref() {
                            // If a run update came back and had nothing to do, but we're trying to
                            // evict, just do that now as long as there's no other outstanding work.
                            if run_handle.activation.is_none() && !run_handle.more_pending_work {
                                let mut evict_act = create_evict_activation(
                                    resp.run_id,
                                    reason.message.clone(),
                                    reason.reason,
                                );
                                evict_act.history_length =
                                    run_handle.most_recently_processed_event_number as u32;
                                Some(ActivationOrAuto::LangActivation(evict_act))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                };
                if let Some(f) = resp.fulfillable_complete.take() {
                    f.fulfill();
                }

                // After each run update, check if it's ready to handle any buffered poll
                if matches!(&r, Some(ActivationOrAuto::Autocomplete { .. }) | None)
                    && !run_handle.has_any_pending_work(false, true)
                {
                    if let Some(bufft) = run_handle.buffered_resp.take() {
                        self.instantiate_or_update(bufft);
                    }
                }
                r
            }
            RunUpdateResponseKind::Fail(fail) => {
                if let Some(r) = self.runs.get_mut(&fail.run_id) {
                    r.last_action_acked = true;
                }

                if let Some(resp_chan) = fail.completion_resp {
                    // Automatically fail the workflow task in the event we couldn't update machines
                    let fail_cause = if matches!(&fail.err, WFMachinesError::Nondeterminism(_)) {
                        WorkflowTaskFailedCause::NonDeterministicError
                    } else {
                        WorkflowTaskFailedCause::Unspecified
                    };
                    let wft_fail_str = format!("{:?}", fail.err);
                    self.failed_completion(
                        fail.run_id,
                        fail_cause,
                        fail.err.evict_reason(),
                        TFailure::application_failure(wft_fail_str, false).into(),
                        resp_chan,
                    );
                } else {
                    // TODO: This should probably also fail workflow tasks, but that wasn't
                    //  implemented pre-refactor either.
                    warn!(error=?fail.err, run_id=%fail.run_id, "Error while updating workflow");
                    self.request_eviction(RequestEvictMsg {
                        run_id: fail.run_id,
                        message: format!("Error while updating workflow: {:?}", fail.err),
                        reason: fail.err.evict_reason(),
                    });
                }
                None
            }
        }
    }

    #[instrument(skip(self, pwft),
                 fields(run_id=%pwft.wft.workflow_execution.run_id,
                        workflow_id=%pwft.wft.workflow_execution.workflow_id))]
    fn instantiate_or_update(&mut self, pwft: PermittedWFT) {
        let (mut work, permit) = if let Some(w) = self.buffer_resp_if_outstanding_work(pwft) {
            (w.wft, w.permit)
        } else {
            return;
        };

        let run_id = work.workflow_execution.run_id.clone();
        // If our cache is full and this WFT is for an unseen run we must first evict a run before
        // we can deal with this task. So, buffer the task in that case.
        if !self.runs.has_run(&run_id) && self.runs.is_full() {
            self.buffer_resp_on_full_cache(PermittedWFT { wft: work, permit });
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
            wf_id: work.workflow_execution.workflow_id.clone(),
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
            permit,
        })
    }

    fn process_completion(&mut self, complete: WFActCompleteMsg) {
        match complete.completion {
            ValidatedCompletion::Success { run_id, commands } => {
                self.successful_completion(run_id, commands, complete.response_tx);
            }
            ValidatedCompletion::Fail { run_id, failure } => {
                self.failed_completion(
                    run_id,
                    WorkflowTaskFailedCause::Unspecified,
                    EvictionReason::LangFail,
                    failure,
                    complete.response_tx,
                );
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
                    // Not an error if this was an eviction, since it's normal to issue eviction
                    // activations without an associated workflow task in that case.
                    dbg_panic!(
                    "Attempted to complete activation for run {} without associated workflow task",
                    run_id
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
                    action: ActivationAction::RespondLegacyQuery {
                        result: Box::new(qr),
                    },
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

            if activation_was_only_eviction && !commands.is_empty() {
                dbg_panic!("Reply to an eviction only containing an eviction included commands");
            }

            let activation_was_eviction = self.activation_has_eviction(&run_id);
            if let Some(rh) = self.runs.get_mut(&run_id) {
                rh.send_completion(RunActivationCompletion {
                    task_token,
                    start_time,
                    commands,
                    activation_was_eviction,
                    activation_was_only_eviction,
                    has_pending_query,
                    query_responses,
                    resp_chan: Some(resp_chan),
                });
            } else {
                dbg_panic!("Run {} missing during completion", run_id);
            }
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
            dbg_panic!(
                "No workflow task for run id {} found when trying to fail activation",
                run_id
            );
            self.reply_to_complete(&run_id, ActivationCompleteOutcome::DoNothing, resp_chan);
            return;
        };

        if let Some(m) = self.run_metrics(&run_id) {
            m.wf_task_failed();
        }
        let message = format!("Workflow activation completion failed: {:?}", &failure);
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
        // If the outstanding WFT is a legacy query task, report that we need to fail it
        let outcome = if self
            .runs
            .get(&run_id)
            .map(|rh| rh.pending_work_is_legacy_query())
            .unwrap_or_default()
        {
            ActivationCompleteOutcome::ReportWFTFail(
                FailedActivationWFTReport::ReportLegacyQueryFailure(tt, failure),
            )
        } else if should_report {
            ActivationCompleteOutcome::ReportWFTFail(FailedActivationWFTReport::Report(
                tt, cause, failure,
            ))
        } else {
            ActivationCompleteOutcome::WFTFailedDontReport
        };
        self.reply_to_complete(&run_id, outcome, resp_chan);
    }

    fn process_post_activation(&mut self, report: PostActivationMsg) {
        let run_id = &report.run_id;

        // If we reported to server, we always want to mark it complete.
        let maybe_t = self.complete_wft(run_id, report.wft_report_status);

        if self
            .get_activation(run_id)
            .map(|a| a.has_eviction())
            .unwrap_or_default()
        {
            self.evict_run(run_id);
        };

        if let Some(wft) = report.wft_from_complete {
            debug!(run_id=%wft.workflow_execution.run_id, "New WFT from completion");
            if let Some(t) = maybe_t {
                self.instantiate_or_update(PermittedWFT {
                    wft,
                    permit: t.permit,
                })
            }
        }

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

    /// Request a workflow eviction. This will (eventually, after replay is done) queue up an
    /// activation to evict the workflow from the lang side. Workflow will not *actually* be evicted
    /// until lang replies to that activation
    fn request_eviction(&mut self, info: RequestEvictMsg) -> EvictionRequestResult {
        let activation_has_eviction = self.activation_has_eviction(&info.run_id);
        if let Some(rh) = self.runs.get_mut(&info.run_id) {
            let attempts = rh.wft.as_ref().map(|wt| wt.info.attempt);
            if !activation_has_eviction && rh.trying_to_evict.is_none() {
                debug!(run_id=%info.run_id, reason=%info.message, "Eviction requested");
                rh.trying_to_evict = Some(info);
                rh.check_more_activations();
                EvictionRequestResult::EvictionRequested(attempts)
            } else {
                EvictionRequestResult::EvictionAlreadyRequested(attempts)
            }
        } else {
            debug!(run_id=%info.run_id, "Eviction requested for unknown run");
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
        wft_report_status: WFTReportStatus,
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
        if !saw_final && matches!(wft_report_status, WFTReportStatus::NotReported) {
            return None;
        }

        if let Some(rh) = self.runs.get_mut(run_id) {
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

            // Only record latency metrics if we genuinely reported to server
            if matches!(wft_report_status, WFTReportStatus::Reported) {
                if let Some(ot) = &retme {
                    if let Some(m) = self.run_metrics(run_id) {
                        m.wf_task_latency(ot.start_time.elapsed());
                    }
                }
            }

            retme
        } else {
            None
        }
    }

    /// Stores some work if there is any outstanding WFT or activation for the run. If there was
    /// not, returns the work back out inside the option.
    fn buffer_resp_if_outstanding_work(&mut self, work: PermittedWFT) -> Option<PermittedWFT> {
        let run_id = &work.wft.workflow_execution.run_id;
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
                       "Got new WFT for a run with outstanding work, buffering it");
                run.buffered_resp = Some(work);
                None
            } else {
                Some(work)
            }
        } else {
            Some(work)
        }
    }

    fn buffer_resp_on_full_cache(&mut self, work: PermittedWFT) {
        debug!(run_id=%work.wft.workflow_execution.run_id, "Buffering WFT because cache is full");
        // If there's already a buffered poll for the run, replace it.
        if let Some(rh) = self
            .buffered_polls_need_cache_slot
            .iter_mut()
            .find(|w| w.wft.workflow_execution.run_id == work.wft.workflow_execution.run_id)
        {
            *rh = work;
        } else {
            // Otherwise push it to the back
            self.buffered_polls_need_cache_slot.push_back(work);
        }
    }

    /// Makes sure we have enough pending evictions to fulfill the needs of buffered WFTs who are
    /// waiting on a cache slot
    fn reconcile_buffered(&mut self) {
        // We must ensure that there are at least as many pending evictions as there are tasks
        // that we might need to un-buffer (skipping runs which already have buffered tasks for
        // themselves)
        let num_in_buff = self.buffered_polls_need_cache_slot.len();
        let mut evict_these = vec![];
        let num_existing_evictions = self
            .runs
            .runs_lru_order()
            .filter(|(_, h)| h.trying_to_evict.is_some())
            .count();
        let mut num_evicts_needed = num_in_buff.saturating_sub(num_existing_evictions);
        for (rid, handle) in self.runs.runs_lru_order() {
            if num_evicts_needed == 0 {
                break;
            }
            if handle.buffered_resp.is_none() {
                num_evicts_needed -= 1;
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
            .all(|r| !r.has_any_pending_work(self.ignore_evicts_on_shutdown, false));
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

/// Drains pending queries from the workflow task and appends them to the activation's jobs
fn put_queries_in_act(act: &mut WorkflowActivation, wft: &mut OutstandingTask) {
    // Nothing to do if there are no pending queries
    if wft.pending_queries.is_empty() {
        return;
    }

    let has_legacy = wft.has_pending_legacy_query();
    // Cannot dispatch legacy query if there are any other jobs - which can happen if, ex, a local
    // activity resolves while we've gotten a legacy query after heartbeating.
    if has_legacy && !act.jobs.is_empty() {
        return;
    }

    debug!(queries=?wft.pending_queries, "Dispatching queries");
    let query_jobs = wft
        .pending_queries
        .drain(..)
        .map(|q| workflow_activation_job::Variant::QueryWorkflow(q).into());
    act.jobs.extend(query_jobs);
}
