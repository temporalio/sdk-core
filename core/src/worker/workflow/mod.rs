pub(crate) mod workflow_tasks;

mod bridge;
mod driven_workflow;
mod history_update;
mod machines;
mod managed_run;

pub(crate) use bridge::WorkflowBridge;
pub(crate) use driven_workflow::{DrivenWorkflow, WorkflowFetcher};
pub(crate) use history_update::{HistoryPaginator, HistoryUpdate};
pub(crate) use machines::WFMachinesError;
#[cfg(test)]
pub(crate) use managed_run::ManagedWFFunc;

use crate::{
    abstractions::MeteredSemaphore,
    pollers::BoxedWFPoller,
    protosext::{ValidPollWFTQResponse, WorkflowActivationExt},
    telemetry::metrics::{workflow_type, workflow_worker_type},
    worker::{
        workflow::{
            history_update::NextPageToken,
            managed_run::WorkflowManager,
            workflow_tasks::{
                NewWfTaskOutcome, OutstandingActivation, OutstandingTask,
                ServerCommandsWithWorkflowInfo, WorkflowTaskInfo, WorkflowUpdateError,
            },
        },
        LocalActivityResolution,
    },
    MetricsContext, WorkerClientBag,
};
use futures::{
    stream,
    stream::{BoxStream, PollNext},
    Stream, StreamExt,
};
use std::{
    collections::HashMap,
    fmt::Debug,
    future,
    future::Future,
    result,
    sync::Arc,
    time::{Duration, Instant},
};
use temporal_sdk_core_api::errors::{CompleteWfError, PollWfError};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{
            create_evict_activation, create_query_activation, query_to_job,
            remove_from_cache::EvictionReason, workflow_activation_job, QueryWorkflow,
            WorkflowActivation,
        },
        workflow_commands::*,
        workflow_completion::{
            workflow_activation_completion, Failure, WorkflowActivationCompletion,
        },
    },
    temporal::api::{
        command::v1::Command as ProtoCommand, enums::v1::WorkflowTaskFailedCause,
        workflowservice::v1::PollWorkflowTaskQueueResponse,
    },
    TaskToken,
};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedSender},
        oneshot, Semaphore,
    },
    task,
    task::JoinHandle,
};
use tokio_stream::wrappers::UnboundedReceiverStream;

#[cfg(test)]
use crate::test_help::NO_MORE_WORK_ERROR_MSG;
use crate::worker::workflow::{
    managed_run::ManagedRun,
    workflow_tasks::{EvictionRequestResult, FailedActivationOutcome},
};

pub(crate) const LEGACY_QUERY_ID: &str = "legacy_query";

type Result<T, E = WFMachinesError> = result::Result<T, E>;

/// Centralizes all state related to workflows and workflow tasks
pub(crate) struct Workflows {
    new_wft_tx: UnboundedSender<ValidPollWFTQResponse>,
    local_tx: UnboundedSender<LocalInputs>,
    processing_task: JoinHandle<()>,
    activation_stream:
        tokio::sync::Mutex<BoxStream<'static, Result<WorkflowActivation, PollWfError>>>,
}

impl Workflows {
    pub fn new(
        max_cached_workflows: usize,
        max_outstanding_wfts: usize,
        client: Arc<WorkerClientBag>,
        poller: BoxedWFPoller,
        metrics: MetricsContext,
    ) -> Self {
        let (new_wft_tx, new_wft_rx) = unbounded_channel();
        let (local_tx, local_rx) = unbounded_channel();
        let polls_requested = Arc::new(Semaphore::new(1));
        let mut stream = WFActivationStream::new(
            max_cached_workflows,
            max_outstanding_wfts,
            new_wft_tx.clone(),
            UnboundedReceiverStream::new(new_wft_rx),
            UnboundedReceiverStream::new(local_rx),
            client,
            polls_requested.clone(),
            metrics.clone(),
        );
        // TODO: Probably belongs somewhere better
        let poller_wft_tx = new_wft_tx.clone();
        task::spawn(async move {
            while let Ok(perm) = polls_requested.acquire().await {
                // warn!("Pollin'");
                match poller.poll().await {
                    Some(Ok(wft)) => {
                        if wft == PollWorkflowTaskQueueResponse::default() {
                            // We get the default proto in the event that the long poll times out.
                            debug!("Poll wft timeout");
                            metrics.wf_tq_poll_empty();
                            continue;
                        }
                        if let Some(dur) = wft.sched_to_start() {
                            metrics.wf_task_sched_to_start_latency(dur);
                        }
                        let work: ValidPollWFTQResponse = wft.try_into().map_err(|resp| {
                            tonic::Status::new(
                                tonic::Code::DataLoss,
                                format!(
                                    "Server returned a poll WFT response we couldn't interpret: {:?}",
                                    resp
                                ),
                            )
                        })?;
                        perm.forget();
                        poller_wft_tx
                            .send(work)
                            .expect("Rcv half of new wft channel not dropped");
                    }
                    Some(Err(e)) => {
                        // When using mocks we don't want to hammer it over and over when there's
                        // nothing to do, so drop the permit in that case as well.
                        #[cfg(test)]
                        if e.code() == tonic::Code::Cancelled
                            && e.message() == NO_MORE_WORK_ERROR_MSG
                        {
                            perm.forget();
                        }
                    }
                    None => {
                        unimplemented!("Handle shutdown");
                    }
                }
                // TODO: Error/panic handling, wait for request
            }
            Ok::<_, tonic::Status>(())
        });
        let (activation_tx, activation_rx) = unbounded_channel();
        let processing_task = task::spawn(async move {
            while let Some(act) = stream.next().await {
                activation_tx
                    .send(act)
                    .expect("Activation processor channel not dropped");
            }
        });
        Self {
            new_wft_tx,
            local_tx,
            processing_task,
            activation_stream: tokio::sync::Mutex::new(
                UnboundedReceiverStream::new(activation_rx).boxed(),
            ),
        }
    }

    pub async fn next_workflow_activation(&self) -> Result<WorkflowActivation, PollWfError> {
        self.activation_stream
            .lock()
            .await
            .next()
            .await
            .unwrap_or(Err(PollWfError::ShutDown))
    }

    /// Queue a new WFT received from server for processing
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

    /// Query the state of workflow management
    pub fn get_state_info(&self) -> impl Future<Output = WorkflowStateInfo> {
        let (tx, rx) = oneshot::channel();
        self.send_local(GetStateInfoMsg { response_tx: tx });
        async move {
            rx.await
                .expect("Send half of get state info response not dropped")
        }
    }

    fn send_local(&self, msg: impl Into<LocalInputs>) {
        self.local_tx
            .send(msg.into())
            .expect("Local inputs channel not dropped")
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
    activations_tx: UnboundedSender<RunUpdateResponse>,
    /// Can be used to re-feed ourselves a poll. EX: It was buffered but is now ready.
    new_wft_tx: UnboundedSender<ValidPollWFTQResponse>,

    /// Client for accessing server for history pagination etc. TODO: Smaller type w/ only that?
    client: Arc<WorkerClientBag>,

    /// Maps run id -> data about and machines for that run
    runs: HashMap<String, ManagedRunHandle>,
    /// Ensures we stay at or below this worker's maximum concurrent workflow task limit
    wft_semaphore: MeteredSemaphore,

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

impl WFActivationStream {
    fn new(
        max_cached_workflows: usize,
        max_outstanding_wfts: usize,
        new_wft_tx: UnboundedSender<ValidPollWFTQResponse>,
        new_wft_rx: impl Stream<Item = ValidPollWFTQResponse> + Send + 'static,
        local_rx: impl Stream<Item = LocalInputs> + Send + 'static,
        client: Arc<WorkerClientBag>,
        poll_requester: Arc<Semaphore>,
        metrics: MetricsContext,
    ) -> impl Stream<Item = Result<WorkflowActivation, PollWfError>> {
        let (activations_tx, activations_rx) = unbounded_channel();
        let local_rx = stream::select_all([
            local_rx.map(Into::into).boxed(),
            UnboundedReceiverStream::new(activations_rx)
                .map(WFActStreamInput::RunUpdateResponse)
                .boxed(),
        ]);
        let all_inputs = stream::select_with_strategy(
            local_rx,
            new_wft_rx.map(WFActStreamInput::NewWft).boxed(),
            // Priority always goes to the local stream
            |_: &mut ()| PollNext::Left,
        );
        let mut state = WFActivationStream {
            activations_tx,
            new_wft_tx,
            client,
            runs: Default::default(),
            wft_semaphore: MeteredSemaphore::new(
                max_outstanding_wfts,
                metrics.with_new_attrs([workflow_worker_type()]),
                MetricsContext::available_task_slots,
            ),
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
                            NewWfTaskOutcome::IssueActivation(act) => Some(act),
                            NewWfTaskOutcome::TaskBuffered => None,
                            NewWfTaskOutcome::Autocomplete => None,
                            NewWfTaskOutcome::Evict(_) => {
                                unimplemented!()
                            }
                            NewWfTaskOutcome::LocalActsOutstanding => None,
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
                    WFActStreamInput::LocalResolution(_) => {
                        unimplemented!()
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
                    WFActStreamInput::RequestEviction(evict) => {
                        state.request_eviction(evict);
                        None
                    }
                    WFActStreamInput::GetStateInfo(gsi) => {
                        let _ = gsi.response_tx.send(WorkflowStateInfo {
                            cached_workflows: state.runs.len(),
                            outstanding_wft: state.outstanding_wfts(),
                            // TODO: Actually use permits
                            available_wft_permits: 0,
                        });
                        None
                    }
                };

                // TODO: Only request a new poll if cache slots are available -- probably
                //   need to wrap WFT stream with manual impl which only adds permit when poll
                //   called on it.
                // warn!("Adding poll permit");
                poll_requester.add_permits(1);

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
    }

    fn process_run_update_response(&mut self, resp: RunUpdateResponse) -> NewWfTaskOutcome {
        debug!("Processing run update response from machines, {:?}", resp);
        if let Some(r) = self.runs.get_mut(&resp.run_id) {
            r.have_seen_terminal_event = resp.have_seen_terminal_event;
            r.more_pending_work = resp.more_pending_work;
            r.last_action_acked = true;
        }
        let run_handle = self
            .runs
            .get_mut(&resp.run_id)
            .expect("Workflow must exist, it just sent us an update response");

        if let Some(mut activation) = resp.outgoing_activation {
            if let Some(RunUpdatedFromWft {
                wf_info,
                mut pending_queries,
                legacy_query,
            }) = resp.in_response_to_wft
            {
                if !pending_queries.is_empty() && legacy_query.is_some() {
                    error!(
                    "Server issued both normal and legacy queries. This should not happen. Please \
                 file a bug report."
                );
                    return NewWfTaskOutcome::Evict(WorkflowUpdateError {
                        source: WFMachinesError::Fatal(
                            "Server issued both normal and legacy query".to_string(),
                        ),
                        run_id: activation.run_id,
                    });
                }

                // Immediately dispatch query activation if no other jobs
                if let Some(lq) = legacy_query {
                    if activation.jobs.is_empty() {
                        debug!("Dispatching legacy query {}", &lq);
                        activation
                            .jobs
                            .push(workflow_activation_job::Variant::QueryWorkflow(lq).into());
                    } else {
                        pending_queries.push(lq);
                    }
                }

                run_handle.wft = Some(OutstandingTask {
                    info: wf_info,
                    pending_queries,
                    start_time: Instant::now(),
                });
            }

            if activation.jobs.is_empty() {
                // TODO: Probably must be sent in outgoing act
                if resp.current_outstanding_local_act_count > 0 {
                    // If there are outstanding local activities, we don't want to autocomplete the
                    // workflow task. We want to give them a chance to complete. If they take longer
                    // than the WFT timeout, we will force a new WFT just before the timeout.
                    NewWfTaskOutcome::LocalActsOutstanding
                } else {
                    NewWfTaskOutcome::Autocomplete
                }
            } else {
                run_handle.insert_outstanding_activation(&activation);
                NewWfTaskOutcome::IssueActivation(activation)
            }
        } else {
            warn!("No activation in process run update resp");
            // If a run update came back and had nothing to do, but we're trying to evict, just do
            // that now as long as there's no other outstanding work.
            if let Some(reason) = run_handle.trying_to_evict.as_ref() {
                if run_handle.activation.is_none() {
                    let evict_act =
                        create_evict_activation(resp.run_id, reason.message.clone(), reason.reason);
                    run_handle.insert_outstanding_activation(&evict_act);
                    return NewWfTaskOutcome::IssueActivation(evict_act);
                }
            }

            // TODO: Do nothing??
            NewWfTaskOutcome::Autocomplete
        }
    }

    // TODO: Instrument
    #[instrument(level = "debug", skip(self, work))]
    fn instantiate_or_update(&mut self, work: ValidPollWFTQResponse) {
        debug!("Processing new WFT");
        let mut work = if let Some(w) = self.buffer_resp_if_outstanding_work(work) {
            w
        } else {
            // return NewWfTaskOutcome::TaskBuffered;
            return;
        };

        let start_event_id = work.history.events.first().map(|e| e.event_id);
        debug!(
            task_token = %&work.task_token,
            history_length = %work.history.events.len(),
            start_event_id = ?start_event_id,
            attempt = %work.attempt,
            run_id = %work.workflow_execution.run_id,
            "Applying new workflow task from server"
        );

        let run_id = work.workflow_execution.run_id.clone();

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

        let page_token = if !self.runs.contains_key(&run_id) && poll_resp_is_incremental {
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

        let mut wft = NewIncomingWFT {
            // We only populate this field if the machines already exist
            history_update: None,
            wft_info,
            query_requests: work.query_requests,
            did_miss_cache,
            legacy_query_from_poll,
        };

        if let Some(run_handle) = self.runs.get_mut(&run_id) {
            run_handle.metrics.sticky_cache_hit();
            wft.history_update = Some(history_update);
            run_handle.incoming_wft(wft);
        } else {
            // Create a new workflow machines instance for this workflow, initialize it, and
            // track it.
            let metrics = self
                .metrics
                .with_new_attrs([workflow_type(work.workflow_type.clone())]);
            let wfm = WorkflowManager::new(
                history_update,
                self.client.namespace().to_owned(),
                work.workflow_execution.workflow_id,
                work.workflow_type,
                run_id.clone(),
                metrics.clone(),
            );
            let mut mrh = ManagedRunHandle::new(wfm, self.activations_tx.clone(), metrics);
            mrh.incoming_wft(wft);
            self.runs.insert(run_id, mrh);
        }
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
        let ret = if matches!(&commands.as_slice(),
                    &[WFCommand::QueryResponse(qr)] if qr.query_id == LEGACY_QUERY_ID)
        {
            let qr = match commands.remove(0) {
                WFCommand::QueryResponse(qr) => qr,
                _ => unreachable!("We just verified this is the only command"),
            };
            todo!("Tell worker to reply to legacy query")
            // Some(ServerCommandsWithWorkflowInfo {
            //     task_token,
            //     action: ActivationAction::RespondLegacyQuery { result: qr },
            // })
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
                    commands,
                    activation_was_eviction,
                    activation_was_only_eviction,
                    has_pending_query,
                    query_responses,
                    resp_chan,
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
                "No info for workflow with run id {} found when trying to fail activation",
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
        let more_pending_work = self
            .runs
            .get(run_id)
            .map(|r| r.more_pending_work)
            .unwrap_or_default();

        // Workflows with no more pending activations (IE: They have completed a WFT) must be
        // removed from the outstanding tasks map
        if !more_pending_work && !just_evicted {
            warn!("No more pending work");
            if let Some(ot) = self.get_task_mut(run_id) {
                // Check if there was a pending query which must be fulfilled, and if there is
                // create a new pending activation for it.
                if !ot.pending_queries.is_empty() {
                    for query in ot.pending_queries.drain(..) {
                        let na = create_query_activation(run_id.to_string(), [query]);
                        // TODO: Handle pending query
                        // self.pending_queries.push(na);
                    }
                    // self.pending_activations_notifier.notify_waiters();
                }
            }

            // Evict run id if cache is full. Non-sticky will always evict.
            // let maybe_evicted = self.cache_manager.lock().insert(run_id);
            // if let Some(evicted_run_id) = maybe_evicted {
            //     self.request_eviction(
            //         &evicted_run_id,
            //         "Workflow cache full",
            //         EvictionReason::CacheFull,
            //     );
            // }

            // If there was a buffered poll response from the server, it is now ready to
            // be handled.
            self.make_buffered_poll_ready_if_exists(run_id);
        }

        // If we reported to server, we always want to mark it complete.
        self.complete_wft(run_id, report.reported_wft_to_server);
        if let Some(rh) = self.runs.get_mut(run_id) {
            // Delete the activation
            rh.activation.take();
            // Attempt to produce the next activation if needed
            rh.check_more_activations();
        }
    }

    /// Fail a workflow task because of some internal error, rather than a reported activation
    /// failure from lang
    fn auto_fail(&mut self, run_id: String, err: WorkflowUpdateError) {
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

    /// Evict a workflow from the cache by its run id. Any existing pending activations will be
    /// destroyed, and any outstanding activations invalidated.
    fn evict_run(&mut self, run_id: &str) {
        debug!(run_id=%run_id, "Evicting run");
        // If we just evicted something and there was a buffered poll response for the workflow,
        // it is now ready to be produced by the next poll. (Not immediate next, since, ignoring
        // other workflows, the next poll will be the eviction we just produced. Buffered polls
        // always are popped after pending activations)
        self.make_buffered_poll_ready_if_exists(run_id);
        // Now it can safely be deleted, it'll get recreated once the un-buffered poll is handled.
        self.runs.remove(run_id);
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

        let retme = self.runs.get_mut(run_id).and_then(|r| r.wft.take());
        if let Some(ot) = &retme {
            if let Some(m) = self.run_metrics(run_id) {
                m.wf_task_latency(ot.start_time.elapsed());
            }
        }
        retme
    }

    /// Stores some work if there is any outstanding WFT or activation for the run. If there was
    /// not, returns the work back out inside the option.
    fn buffer_resp_if_outstanding_work(
        &mut self,
        work: ValidPollWFTQResponse,
    ) -> Option<ValidPollWFTQResponse> {
        let run_id = &work.workflow_execution.run_id;
        if let Some(mut run) = self.runs.get_mut(run_id) {
            if run.wft.is_some() || run.activation.is_some() {
                debug!(run_id = %run_id, "Got new WFT for a run with outstanding work");
                run.buffered_resp = Some(work);
                None
            } else {
                Some(work)
            }
        } else {
            Some(work)
        }
    }

    fn make_buffered_poll_ready_if_exists(&mut self, run_id: &str) {
        if let Some(buf) = self
            .runs
            .get_mut(run_id)
            .and_then(|r| r.buffered_resp.take())
        {
            self.new_wft_tx
                .send(buf)
                .expect("New poll receive half not dropped");
        }
    }

    fn get_task_mut(&mut self, run_id: &str) -> Option<&mut OutstandingTask> {
        self.runs.get_mut(run_id).and_then(|rh| rh.wft.as_mut())
    }

    fn get_task(&self, run_id: &str) -> Option<&OutstandingTask> {
        self.runs.get(run_id).and_then(|rh| rh.wft.as_ref())
    }

    fn get_activation(&self, run_id: &str) -> Option<&OutstandingActivation> {
        self.runs.get(run_id).and_then(|rh| rh.activation.as_ref())
    }

    fn run_metrics(&self, run_id: &str) -> Option<&MetricsContext> {
        self.runs.get(run_id).map(|r| &r.metrics)
    }

    fn activation_has_only_eviction(&self, run_id: &str) -> bool {
        self.runs
            .get(run_id)
            .and_then(|rh| rh.activation)
            .map(OutstandingActivation::has_only_eviction)
            .unwrap_or_default()
    }

    fn activation_has_eviction(&self, run_id: &str) -> bool {
        self.runs
            .get(run_id)
            .and_then(|rh| rh.activation)
            .map(OutstandingActivation::has_eviction)
            .unwrap_or_default()
    }

    fn outstanding_wfts(&self) -> usize {
        self.runs.values().filter(|r| r.wft.is_some()).count()
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

struct ManagedRunHandle {
    wft: Option<OutstandingTask>,
    activation: Option<OutstandingActivation>,
    /// If set, it indicates there is a buffered poll response from the server that applies to this
    /// run. This can happen when lang takes too long to complete a task and the task times out, for
    /// example. Upon next completion, the buffered response will be removed and can be made ready
    /// to be returned from polling
    buffered_resp: Option<ValidPollWFTQResponse>,
    have_seen_terminal_event: bool,
    more_pending_work: bool,
    /// Is set if an eviction has been requested for this run
    trying_to_evict: Option<RequestEvictMsg>,
    /// Set to true if the last action we tried to take to this run has been processed (ie: the
    /// [RunUpdateResponse] for it has been seen.
    last_action_acked: bool,

    handle: JoinHandle<()>,
    run_actions_tx: UnboundedSender<RunActions>,
    metrics: MetricsContext,
}
impl ManagedRunHandle {
    fn new(
        wfm: WorkflowManager,
        activations_tx: UnboundedSender<RunUpdateResponse>,
        metrics: MetricsContext,
    ) -> Self {
        let (run_actions_tx, run_actions_rx) = unbounded_channel();
        let managed = ManagedRun::new(wfm, activations_tx);
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
            });
        }
    }
    fn send_completion(&mut self, c: RunActivationCompletion) {
        info!(last_acked=%self.last_action_acked, "Sending completion");
        self.send_run_action(RunActions::ActivationCompletion(c));
    }

    fn insert_outstanding_activation(&mut self, act: &WorkflowActivation) {
        let act_type = if act.is_legacy_query() {
            OutstandingActivation::LegacyQuery
        } else {
            OutstandingActivation::Normal {
                contains_eviction: act.eviction_index().is_some(),
                num_jobs: act.jobs.len(),
            }
        };
        if let Some(old_act) = self.activation {
            // This is a panic because we have screwed up core logic if this is violated. It must be
            // upheld.
            panic!(
                "Attempted to insert a new outstanding activation {}, but there already was \
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
}

// TODO: Attach trace context to these somehow?
#[derive(Debug)]
enum RunActions {
    NewIncomingWFT(NewIncomingWFT),
    ActivationCompletion(RunActivationCompletion),
    CheckMoreWork {
        want_to_evict: Option<RequestEvictMsg>,
    },
}
#[derive(Debug)]
struct NewIncomingWFT {
    history_update: Option<HistoryUpdate>,
    wft_info: WorkflowTaskInfo,
    query_requests: Vec<QueryWorkflow>,
    did_miss_cache: bool,
    legacy_query_from_poll: Option<QueryWorkflow>,
}
#[derive(Debug)]
struct RunActivationCompletion {
    task_token: TaskToken,
    commands: Vec<WFCommand>,
    activation_was_eviction: bool,
    activation_was_only_eviction: bool,
    has_pending_query: bool,
    query_responses: Vec<QueryResult>,
    resp_chan: oneshot::Sender<ActivationCompleteOutcome>,
}

#[derive(Debug)]
struct RunUpdateResponse {
    run_id: String,
    outgoing_activation: Option<WorkflowActivation>,
    have_seen_terminal_event: bool,
    /// Is true if there are more jobs that need to be sent to lang
    more_pending_work: bool,
    current_outstanding_local_act_count: usize,
    in_response_to_wft: Option<RunUpdatedFromWft>,
}
#[derive(Debug)]
struct RunUpdatedFromWft {
    wf_info: WorkflowTaskInfo,
    pending_queries: Vec<QueryWorkflow>,
    // TODO: Can probably be eliminated and handled inside run entirely
    legacy_query: Option<QueryWorkflow>,
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
