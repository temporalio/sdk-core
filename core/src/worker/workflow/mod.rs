pub(crate) mod workflow_tasks;

mod bridge;
mod driven_workflow;
mod history_update;
mod machines;
mod managed_run;
mod run_cache;
pub(crate) mod wft_poller;

pub(crate) use bridge::WorkflowBridge;
pub(crate) use driven_workflow::{DrivenWorkflow, WorkflowFetcher};
pub(crate) use history_update::{HistoryPaginator, HistoryUpdate};
pub(crate) use machines::WFMachinesError;
#[cfg(test)]
pub(crate) use managed_run::ManagedWFFunc;

use crate::{
    abstractions::{stream_when_allowed, MeteredSemaphore, StreamAllowHandle},
    protosext::{ValidPollWFTQResponse, WorkflowActivationExt},
    telemetry::metrics::workflow_worker_type,
    worker::{
        workflow::{
            history_update::NextPageToken,
            managed_run::{ManagedRun, WorkflowManager},
            run_cache::RunCache,
            workflow_tasks::{
                ActivationAction, EvictionRequestResult, FailedActivationOutcome,
                OutstandingActivation, OutstandingTask, RunUpdateOutcome,
                ServerCommandsWithWorkflowInfo, WorkflowTaskInfo, WorkflowUpdateError,
            },
        },
        LocalActRequest, LocalActivityResolution,
    },
    MetricsContext, WorkerClientBag,
};
use futures::{
    stream,
    stream::{BoxStream, PollNext},
    Stream, StreamExt,
};
use std::{
    collections::VecDeque,
    fmt::Debug,
    future,
    future::Future,
    ops::DerefMut,
    result,
    sync::Arc,
    time::{Duration, Instant},
};
use temporal_sdk_core_api::errors::{CompleteWfError, PollWfError};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{
            create_evict_activation, query_to_job, remove_from_cache::EvictionReason,
            workflow_activation_job, WorkflowActivation,
        },
        workflow_commands::*,
        workflow_completion::{
            workflow_activation_completion, Failure, WorkflowActivationCompletion,
        },
    },
    temporal::api::{
        command::v1::Command as ProtoCommand, enums::v1::WorkflowTaskFailedCause,
        failure::v1::Failure as TFailure,
    },
    TaskToken,
};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        oneshot,
    },
    task,
    task::{JoinError, JoinHandle},
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

pub(crate) const LEGACY_QUERY_ID: &str = "legacy_query";

type Result<T, E = WFMachinesError> = result::Result<T, E>;

/// Centralizes all state related to workflows and workflow tasks
pub(crate) struct Workflows {
    new_wft_tx: UnboundedSender<ValidPollWFTQResponse>,
    local_tx: UnboundedSender<LocalInputs>,
    processing_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    activation_stream: tokio::sync::Mutex<(
        BoxStream<'static, Result<ActivationOrAuto, PollWfError>>,
        // Used to indicate polling may begin
        Option<oneshot::Sender<()>>,
    )>,
}

#[derive(Debug)]
pub(crate) enum ActivationOrAuto {
    LangActivation(WorkflowActivation),
    /// This type should only be filled with an empty activation which is ready to have queries
    /// inserted into the joblist
    ReadyForQueries(WorkflowActivation),
    Autocomplete {
        // TODO: Can I delete this?
        run_id: String,
        is_wft_heartbeat: bool,
    },
}
impl ActivationOrAuto {
    pub fn run_id(&self) -> &str {
        match self {
            ActivationOrAuto::LangActivation(act) => &act.run_id,
            ActivationOrAuto::Autocomplete { run_id, .. } => &run_id,
            ActivationOrAuto::ReadyForQueries(act) => &act.run_id,
        }
    }
}

impl Workflows {
    pub fn new(
        max_cached_workflows: usize,
        max_outstanding_wfts: usize,
        client: Arc<WorkerClientBag>,
        wft_stream: impl Stream<Item = ValidPollWFTQResponse> + Send + 'static,
        local_activity_request_sink: impl Fn(Vec<LocalActRequest>) -> Vec<LocalActivityResolution>
            + Send
            + Sync
            + 'static,
        shutdown_token: CancellationToken,
        metrics: MetricsContext,
    ) -> Self {
        let (new_wft_tx, new_wft_rx) = unbounded_channel();
        let (local_tx, local_rx) = unbounded_channel();
        let (allow_handle, poller_wfts) = stream_when_allowed(wft_stream);
        let new_wft_rx = stream::select_all([
            poller_wfts
                .map(ExternalPollerInputs::NewWft)
                .chain(stream::once(async { ExternalPollerInputs::PollerDead }))
                .boxed(),
            UnboundedReceiverStream::new(new_wft_rx)
                .map(ExternalPollerInputs::NewWft)
                .boxed(),
        ]);
        let shutdown_tok = shutdown_token.clone();
        let mut stream = WFActivationStream::new(
            max_cached_workflows,
            max_outstanding_wfts,
            new_wft_rx,
            allow_handle,
            UnboundedReceiverStream::new(local_rx),
            client,
            local_activity_request_sink,
            shutdown_token,
            metrics.clone(),
        );
        let (activation_tx, activation_rx) = unbounded_channel();
        let (start_polling_tx, start_polling_rx) = oneshot::channel();
        // We must spawn a task to constantly poll the activation stream, because otherwise
        // activation completions would not cause anything to happen until the next poll.
        let processing_task = task::spawn(async move {
            // However, we want to avoid plowing ahead until we've been asked to poll at least once.
            // This supports activity-only workers.
            let do_poll = tokio::select! {
                sp = start_polling_rx => {
                    sp.is_ok()
                }
                _ = shutdown_tok.cancelled() => {
                    false
                }
            };
            if !do_poll {
                return;
            }
            while let Some(act) = stream.next().await {
                activation_tx
                    .send(act)
                    .expect("Activation processor channel not dropped");
            }
        });
        Self {
            new_wft_tx,
            local_tx,
            processing_task: tokio::sync::Mutex::new(Some(processing_task)),
            activation_stream: tokio::sync::Mutex::new((
                UnboundedReceiverStream::new(activation_rx).boxed(),
                Some(start_polling_tx),
            )),
        }
    }

    pub async fn next_workflow_activation(&self) -> Result<ActivationOrAuto, PollWfError> {
        let mut lock = self.activation_stream.lock().await;
        let (ref mut stream, ref mut beginner) = lock.deref_mut();
        if let Some(beginner) = beginner.take() {
            let _ = beginner.send(());
        }
        stream.next().await.unwrap_or(Err(PollWfError::ShutDown))
    }

    /// Queue a new WFT received from server for processing (ex: when a new WFT is obtained in
    /// response to a completion). Most tasks should just come from the stream passed in during
    /// construction.
    pub fn new_wft(&self, wft: ValidPollWFTQResponse) {
        self.new_wft_tx
            .send(wft)
            .expect("Rcv half of new WFT channel not closed");
    }

    /// Queue an activation completion for processing, returning a future that will resolve with
    /// the outcome of that completion. See [ActivationCompletedOutcome].
    pub fn activation_completed(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> impl Future<Output = ActivationCompleteOutcome> {
        let (tx, rx) = oneshot::channel();
        self.send_local(WFActCompleteMsg {
            completion,
            response_tx: tx,
        });
        async move {
            rx.await
                .expect("Send half of activation complete response not dropped")
        }
    }

    /// Must be called after every activation completion has finished
    pub fn post_activation(&self, msg: PostActivationMsg) {
        self.send_local(msg);
    }

    /// Tell workflow that a local activity has finished with the provided result
    pub fn notify_of_local_result(&self, run_id: impl Into<String>, resolved: LocalResolution) {
        self.send_local(LocalResolutionMsg {
            run_id: run_id.into(),
            res: resolved,
        });
    }

    /// Request eviction of a workflow
    pub fn request_eviction(
        &self,
        run_id: impl Into<String>,
        message: impl Into<String>,
        reason: EvictionReason,
    ) {
        self.send_local(RequestEvictMsg {
            run_id: run_id.into(),
            message: message.into(),
            reason,
        });
    }

    /// Query the state of workflow management. Can return `None` if workflow state is shut down.
    pub fn get_state_info(&self) -> impl Future<Output = Option<WorkflowStateInfo>> {
        let (tx, rx) = oneshot::channel();
        self.send_local(GetStateInfoMsg { response_tx: tx });
        async move { rx.await.ok() }
    }

    pub async fn shutdown(&self) -> Result<(), JoinError> {
        let maybe_jh = self.processing_task.lock().await.take();
        if let Some(jh) = maybe_jh {
            // This acts as a final wake up in case the stream is still alive and wouldn't otherwise
            // receive another message. It allows it to shut itself down.
            let _ = self.get_state_info();
            jh.await
        } else {
            Ok(())
        }
    }

    fn send_local(&self, msg: impl Into<LocalInputs>) {
        let msg = msg.into();
        let print_err = !matches!(msg, LocalInputs::GetStateInfo(_));
        if let Err(e) = self.local_tx.send(msg) {
            if print_err {
                error!(
                "Tried to interact with workflow state after it shut down. This is not allowed. \
                 A worker cannot be interacted with after shutdown has finished. When sending {:?}",
                e.0
                )
            }
        }
    }
}
#[derive(Debug)]
pub(crate) struct WorkflowStateInfo {
    pub(crate) cached_workflows: usize,
    pub(crate) outstanding_wft: usize,
    pub(crate) available_wft_permits: usize,
}

#[derive(Debug)]
struct WFActCompleteMsg {
    completion: WorkflowActivationCompletion,
    response_tx: oneshot::Sender<ActivationCompleteOutcome>,
}
#[derive(Debug)]
struct LocalResolutionMsg {
    run_id: String,
    res: LocalResolution,
}
#[derive(Debug)]
pub(crate) struct PostActivationMsg {
    pub(crate) run_id: String,
    pub(crate) reported_wft_to_server: bool,
}
#[derive(Debug, Clone)]
struct RequestEvictMsg {
    run_id: String,
    message: String,
    reason: EvictionReason,
}
#[derive(Debug)]
struct GetStateInfoMsg {
    response_tx: oneshot::Sender<WorkflowStateInfo>,
}

/// What needs to be done after calling [Workflows::activation_completed]
#[derive(Debug)]
pub(crate) enum ActivationCompleteOutcome {
    /// The WFT must be reported as successful to the server using the contained information.
    ReportWFTSuccess(ServerCommandsWithWorkflowInfo),
    /// The WFT must be reported as failed to the server using the contained information.
    ReportWFTFail(FailedActivationOutcome),
    /// There's nothing to do right now. EX: The workflow needs to keep replaying.
    DoNothing,
}

pub(crate) struct WFActivationStream {
    runs: RunCache,
    /// Buffered polls for new runs which need a cache slot to open up before we can handle them
    buffered_polls_need_cache_slot: VecDeque<ValidPollWFTQResponse>,

    /// Client for accessing server for history pagination etc. TODO: Smaller type w/ only that?
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
enum LocalInputs {
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

impl WFActivationStream {
    fn new(
        max_cached_workflows: usize,
        max_outstanding_wfts: usize,
        new_wft_rx: impl Stream<Item = ExternalPollerInputs> + Send + 'static,
        external_wft_allow_handle: StreamAllowHandle,
        local_rx: impl Stream<Item = LocalInputs> + Send + 'static,
        client: Arc<WorkerClientBag>,
        local_activity_request_sink: impl Fn(Vec<LocalActRequest>) -> Vec<LocalActivityResolution>
            + Send
            + Sync
            + 'static,
        shutdown_token: CancellationToken,
        metrics: MetricsContext,
    ) -> impl Stream<Item = Result<ActivationOrAuto, PollWfError>> {
        let (run_update_tx, run_update_rx) = unbounded_channel();
        let local_rx = stream::select_all([
            local_rx.map(Into::into).boxed(),
            UnboundedReceiverStream::new(run_update_rx)
                .map(WFActStreamInput::RunUpdateResponse)
                .boxed(),
        ]);
        let low_pri_streams = stream::select_all([new_wft_rx.map(Into::into).boxed()]);
        let all_inputs = stream::select_with_strategy(
            local_rx,
            low_pri_streams,
            // Priority always goes to the local stream
            |_: &mut ()| PollNext::Left,
        );
        let mut state = WFActivationStream {
            buffered_polls_need_cache_slot: Default::default(),
            runs: RunCache::new(
                max_cached_workflows,
                client.namespace().to_string(),
                run_update_tx,
                Arc::new(local_activity_request_sink),
                metrics.clone(),
            ),
            client,
            wft_semaphore: MeteredSemaphore::new(
                max_outstanding_wfts,
                metrics.with_new_attrs([workflow_worker_type()]),
                MetricsContext::available_task_slots,
            ),
            shutdown_token,
            metrics,
        };
        all_inputs
            .map(move |action| {
                let maybe_activation = match action {
                    WFActStreamInput::NewWft(wft) => {
                        state.instantiate_or_update(wft);
                        None
                    }
                    WFActStreamInput::RunUpdateResponse(resp) => {
                        match state.process_run_update_response(resp) {
                            RunUpdateOutcome::IssueActivation(act) => {
                                Some(ActivationOrAuto::LangActivation(act))
                            }
                            RunUpdateOutcome::Autocomplete {
                                run_id,
                                is_wft_heartbeat,
                            } => Some(ActivationOrAuto::Autocomplete {
                                run_id,
                                is_wft_heartbeat,
                            }),
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
                                        TFailure::application_failure(wft_fail_str.clone(), false)
                                            .into(),
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
                        run_handle.insert_outstanding_activation(&act);
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
                        error!("Ahhhhh {:?}", e);
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
                            if !wft.pending_queries.is_empty() {
                                if wft.hit_cache {
                                    warn!("Draining queries!!!");
                                    let query_jobs = wft.pending_queries.drain(..).map(|q| {
                                        workflow_activation_job::Variant::QueryWorkflow(q).into()
                                    });
                                    activation.jobs.extend(query_jobs);
                                }
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
                    Some(ActivationOrAuto::Autocomplete {
                        run_id,
                        is_wft_heartbeat,
                    }) => {
                        warn!("Autocomplete run response");
                        RunUpdateOutcome::Autocomplete {
                            run_id,
                            is_wft_heartbeat,
                        }
                    }
                    None => {
                        warn!("No activation in process run update resp");
                        // If the response indicates there are outstanding local activities,
                        // we should check again since we might be in the process of completing them
                        // TODO: This spins too much right now
                        if resp.current_outstanding_local_act_count > 0 {
                            run_handle.check_more_activations();
                            return RunUpdateOutcome::DoNothing;
                        }
                        // If a run update came back and had nothing to do, but we're trying to
                        // evict, just do that now as long as there's no other outstanding work.
                        if let Some(reason) = run_handle.trying_to_evict.as_ref() {
                            if run_handle.activation.is_none() && !run_handle.more_pending_work {
                                warn!("Making evict activation since no other work");
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
        self.info_dump(&work.workflow_execution.run_id);
        let mut work = if let Some(w) = self.buffer_resp_if_outstanding_work(work) {
            w
        } else {
            return;
        };

        let run_id = work.workflow_execution.run_id.clone();
        // If our cache is full and this WFT is for an unseen run we must first evict a run before
        // we can deal with this task. So, buffer the task in that case.
        if !self.runs.has_run(&run_id) && self.runs.is_full() {
            debug!("Buffering WFT because cache is full");
            self.buffered_polls_need_cache_slot.push_back(work);
            self.request_eviction_of_lru_run();
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
            run_id,
            work.workflow_execution.workflow_id,
            work.workflow_type,
            history_update,
            start_time,
        );
        run_handle.wft = Some(OutstandingTask {
            info: wft_info,
            hit_cache: !did_miss_cache,
            pending_queries,
            start_time,
            // TODO: Probably shouldn't/can't be expect
            permit: self
                .wft_semaphore
                .try_acquire_owned()
                .expect("WFT Permit is available if we allowed poll"),
        })
    }

    #[instrument(level = "debug", skip(self, complete))]
    fn process_completion(&mut self, complete: WFActCompleteMsg) {
        debug!("Processing WF activation completion");
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
        resp_chan: oneshot::Sender<ActivationCompleteOutcome>,
    ) {
        debug!("Processing successful completion");
        let run_id = run_id.as_str();
        let activation_was_only_eviction = self.activation_has_only_eviction(run_id);
        let (task_token, has_pending_query, start_time) = if let Some(entry) = self.get_task(run_id)
        {
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
                    run_id,
                    "Attempted to complete activation for run without associated workflow task"
                );
            }
            resp_chan
                .send(ActivationCompleteOutcome::DoNothing)
                .expect("Rcv half of activation reply not dropped");
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
            resp_chan
                .send(ActivationCompleteOutcome::ReportWFTSuccess(
                    ServerCommandsWithWorkflowInfo {
                        task_token,
                        action: ActivationAction::RespondLegacyQuery { result: qr },
                    },
                ))
                .expect("Rcv half of activation reply not dropped");
        } else {
            // First strip out query responses from other commands that actually affect machines
            // Would be prettier with `drain_filter`
            let mut i = 0;
            let mut query_responses = vec![];
            while i < commands.len() {
                if matches!(commands[i], WFCommand::QueryResponse(_)) {
                    if let WFCommand::QueryResponse(qr) = commands.remove(i) {
                        if qr.query_id == LEGACY_QUERY_ID {
                            let update_err = WorkflowUpdateError {
                                source: WFMachinesError::Fatal(
                                    "Legacy query activation response included other commands, \
                                     this is not allowed and constitutes an error in the lang SDK"
                                        .to_string(),
                                ),
                                run_id: run_id.to_string(),
                            };
                            self.auto_fail(run_id.to_string(), update_err);
                            return;
                        }
                        query_responses.push(qr);
                    }
                } else {
                    i += 1;
                }
            }

            let activation_was_eviction = self.activation_has_eviction(run_id);
            self.runs
                .get_mut(run_id)
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
        resp_chan: oneshot::Sender<ActivationCompleteOutcome>,
    ) {
        debug!("Processing failed completion");
        let tt = if let Some(tt) = self.get_task(&run_id).map(|t| t.info.task_token.clone()) {
            tt
        } else {
            warn!(
                "No workflow task for run id {} found when trying to fail activation",
                run_id
            );
            resp_chan
                .send(ActivationCompleteOutcome::ReportWFTFail(
                    FailedActivationOutcome::NoReport,
                ))
                .expect("Rcv half of activation reply not dropped");
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
                run_id,
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
        resp_chan
            .send(ActivationCompleteOutcome::ReportWFTFail(outcome))
            .expect("Rcv half of activation reply not dropped");
    }

    fn process_post_activation(&mut self, report: PostActivationMsg) {
        debug!("Processing post activation");
        let run_id = &report.run_id;
        let mut just_evicted = false;

        if self
            .get_activation(run_id)
            .map(|a| a.has_eviction())
            .unwrap_or_default()
        {
            self.evict_run(run_id);
            just_evicted = true;
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
            todo!("fail run due to local resolution w/ missing run")
        }
        // error!(
        //     "Problem with local resolution on run {}: {:?} -- will evict the \
        //      workflow",
        //     run_id, e
        // );
        // self.request_wf_eviction(
        //     run_id,
        //     "Issue while processing local resolution",
        //     e.evict_reason(),
        // );
    }

    /// Fail a workflow task because of some internal error, rather than a reported activation
    /// failure from lang
    fn auto_fail(&mut self, _run_id: String, _err: WorkflowUpdateError) {
        unimplemented!()
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
                        completion: None,
                    })?;
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
                completion: None,
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
            self.request_eviction(RequestEvictMsg {
                run_id: lru_run_id.to_string(),
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

        // Remove buffered poll if it exists
        let maybe_buff = self
            .runs
            .get_mut(run_id)
            .and_then(|rh| rh.buffered_resp.take());

        // Now it can safely be deleted, it'll get recreated once the un-buffered poll is handled if
        // there was one.
        self.runs.remove(run_id);

        if let Some(buff) = maybe_buff {
            self.instantiate_or_update(buff);
        } else {
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
                debug!(run_id = %run_id, has_wft, has_activation, about_to_issue_evict,
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

    fn shutdown_done(&self) -> bool {
        // let no_buffered_poll = self.buffered_polls.is_empty();
        let all_runs_ready = self
            .runs
            .handles()
            .all(|r| !r.has_any_pending_work(true, false));
        if self.shutdown_token.is_cancelled() && /*no_buffered_poll &&*/ all_runs_ready {
            info!("Workflow shutdown is done");
            true
        } else {
            false
        }
    }

    fn get_task_mut(&mut self, run_id: &str) -> Option<&mut OutstandingTask> {
        self.runs.get_mut(run_id).and_then(|rh| rh.wft.as_mut())
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

enum ValidatedCompletion {
    Success {
        run_id: String,
        commands: Vec<WFCommand>,
    },
    Fail {
        run_id: String,
        failure: Failure,
    },
}

#[derive(Debug)]
struct ManagedRunHandle {
    wft: Option<OutstandingTask>,
    activation: Option<OutstandingActivation>,
    /// If set, it indicates there is a buffered poll response from the server that applies to this
    /// run. This can happen when lang takes too long to complete a task and the task times out, for
    /// example. Upon next completion, the buffered response will be removed and can be made ready
    /// to be returned from polling
    buffered_resp: Option<ValidPollWFTQResponse>,
    have_seen_terminal_event: bool,
    /// Is set true when the machines indicate that there is additional known work to be processed
    more_pending_work: bool,
    /// Is set if an eviction has been requested for this run
    trying_to_evict: Option<RequestEvictMsg>,
    /// Set to true if the last action we tried to take to this run has been processed (ie: the
    /// [RunUpdateResponse] for it has been seen.
    last_action_acked: bool,

    // TODO: Also should be joined
    handle: JoinHandle<()>,
    run_actions_tx: UnboundedSender<RunActions>,
    metrics: MetricsContext,
}
impl ManagedRunHandle {
    fn new(
        wfm: WorkflowManager,
        activations_tx: UnboundedSender<RunUpdateResponse>,
        local_activity_request_sink: LocalActivityRequestSink,
        metrics: MetricsContext,
    ) -> Self {
        let (run_actions_tx, run_actions_rx) = unbounded_channel();
        let managed = ManagedRun::new(wfm, activations_tx, local_activity_request_sink);
        let handle = tokio::task::spawn(managed.run(run_actions_rx));
        Self {
            wft: None,
            activation: None,
            buffered_resp: None,
            have_seen_terminal_event: false,
            more_pending_work: false,
            trying_to_evict: None,
            last_action_acked: true,
            handle,
            metrics,
            run_actions_tx,
        }
    }

    fn incoming_wft(&mut self, wft: NewIncomingWFT) {
        if self.wft.is_some() {
            error!("Trying to send a new WFT for a run which already has one!");
        }
        info!(last_acked=%self.last_action_acked, "Sending incoming wft");
        self.send_run_action(RunActions::NewIncomingWFT(wft));
    }
    fn check_more_activations(&mut self) {
        // No point in checking for more activations if we have not acked the last update, or
        // if there's already an outstanding activation.
        if self.last_action_acked && self.activation.is_none() {
            info!(last_acked=%self.last_action_acked, "Sending check more work");
            self.send_run_action(RunActions::CheckMoreWork {
                want_to_evict: self.trying_to_evict.clone(),
                has_pending_queries: self
                    .wft
                    .as_ref()
                    .map(|wft| !wft.pending_queries.is_empty())
                    .unwrap_or_default(),
            });
        }
    }
    fn send_completion(&mut self, c: RunActivationCompletion) {
        info!(last_acked=%self.last_action_acked, "Sending completion");
        self.send_run_action(RunActions::ActivationCompletion(c));
    }
    fn send_local_resolution(&mut self, r: LocalResolution) {
        info!(last_acked=%self.last_action_acked, "Sending local resolution");
        self.send_run_action(RunActions::LocalResolution(r));
    }

    fn insert_outstanding_activation(&mut self, act: &ActivationOrAuto) {
        let act_type = match &act {
            ActivationOrAuto::LangActivation(act) | ActivationOrAuto::ReadyForQueries(act) => {
                if act.is_legacy_query() {
                    OutstandingActivation::LegacyQuery
                } else {
                    OutstandingActivation::Normal {
                        contains_eviction: act.eviction_index().is_some(),
                        num_jobs: act.jobs.len(),
                    }
                }
            }
            ActivationOrAuto::Autocomplete { .. } => OutstandingActivation::Autocomplete,
        };
        if let Some(old_act) = self.activation {
            // This is a panic because we have screwed up core logic if this is violated. It must be
            // upheld.
            panic!(
                "Attempted to insert a new outstanding activation {:?}, but there already was \
                 one outstanding: {:?}",
                act, old_act
            );
        }
        self.activation = Some(act_type);
    }

    fn send_run_action(&mut self, ra: RunActions) {
        self.last_action_acked = false;
        self.run_actions_tx
            .send(ra)
            .expect("Receive half of run actions not dropped");
    }

    /// Returns true if the managed run has any form of pending work
    /// If `ignore_evicts` is true, pending evictions do not count as pending work.
    fn has_any_pending_work(&self, ignore_evicts: bool, ignore_buffered: bool) -> bool {
        let evict_work = if ignore_evicts {
            false
        } else {
            self.trying_to_evict.is_some()
        };
        let act_work = if ignore_evicts {
            if let Some(ref act) = self.activation {
                !act.has_only_eviction()
            } else {
                false
            }
        } else {
            self.activation.is_some()
        };
        let buffered = if ignore_buffered {
            false
        } else {
            self.buffered_resp.is_some()
        };
        self.wft.is_some()
            || buffered
            || !self.last_action_acked
            || self.more_pending_work
            || act_work
            || evict_work
    }
}

// TODO: Attach trace context to these somehow?
#[derive(Debug)]
enum RunActions {
    NewIncomingWFT(NewIncomingWFT),
    ActivationCompletion(RunActivationCompletion),
    CheckMoreWork {
        want_to_evict: Option<RequestEvictMsg>,
        has_pending_queries: bool,
    },
    LocalResolution(LocalResolution),
    HeartbeatTimeout,
}
#[derive(Debug)]
struct NewIncomingWFT {
    /// This field is only populated if the machines already exist. Otherwise the machines
    /// are instantiated with the workflow history.
    history_update: Option<HistoryUpdate>,
    /// Wft start time
    start_time: Instant,
}
#[derive(Debug)]
struct RunActivationCompletion {
    task_token: TaskToken,
    start_time: Instant,
    commands: Vec<WFCommand>,
    activation_was_eviction: bool,
    activation_was_only_eviction: bool,
    has_pending_query: bool,
    query_responses: Vec<QueryResult>,
    /// Used to notify the worker when the completion is done processing and the completion can
    /// unblock. Must always be `Some` when initialized.
    resp_chan: Option<oneshot::Sender<ActivationCompleteOutcome>>,
}

#[derive(Debug)]
enum RunUpdateResponse {
    Good(GoodRunUpdateResponse),
    Fail(FailRunUpdateResponse),
}

#[derive(Debug)]
struct GoodRunUpdateResponse {
    run_id: String,
    outgoing_activation: Option<ActivationOrAuto>,
    have_seen_terminal_event: bool,
    /// Is true if there are more jobs that need to be sent to lang
    more_pending_work: bool,
    current_outstanding_local_act_count: usize,
    in_response_to_wft: Option<RunUpdatedFromWft>,
}
#[derive(Debug)]
pub(crate) struct FailRunUpdateResponse {
    run_id: String,
    err: WFMachinesError,
    /// This is populated if the run update failed while processing a completion - and thus we
    /// must respond down it when handling the failure.
    completion_resp: Option<oneshot::Sender<ActivationCompleteOutcome>>,
}
#[derive(Debug)]
struct RunUpdatedFromWft {}
#[derive(Debug)]
pub struct OutgoingServerCommands {
    pub commands: Vec<ProtoCommand>,
    pub replaying: bool,
}

#[derive(Debug)]
pub(crate) enum LocalResolution {
    LocalActivity(LocalActivityResolution),
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
    UpsertSearchAttributes(UpsertWorkflowSearchAttributes),
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
            workflow_command::Variant::UpsertWorkflowSearchAttributesCommandAttributes(s) => {
                Ok(Self::UpsertSearchAttributes(s))
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

/// Details remembered from the workflow execution started event that we may need to recall later.
/// Is a subset of `WorkflowExecutionStartedEventAttributes`, but avoids holding on to huge fields.
#[derive(Debug, Clone)]
pub struct WorkflowStartedInfo {
    workflow_task_timeout: Option<Duration>,
    workflow_execution_timeout: Option<Duration>,
}

type LocalActivityRequestSink =
    Arc<dyn Fn(Vec<LocalActRequest>) -> Vec<LocalActivityResolution> + Send + Sync>;
