//! This module and its submodules implement Core's logic for managing workflows (which is the
//! lion's share of the complexity in Core). See the `ARCHITECTURE.md` file in the repo root for
//! a diagram of the internals.

mod bridge;
mod driven_workflow;
mod history_update;
mod machines;
mod managed_run;
mod run_cache;
mod wft_extraction;
pub(crate) mod wft_poller;
mod workflow_stream;

pub(crate) use bridge::WorkflowBridge;
pub(crate) use driven_workflow::{DrivenWorkflow, WorkflowFetcher};
pub(crate) use history_update::HistoryUpdate;
pub(crate) use machines::WFMachinesError;
#[cfg(test)]
pub(crate) use managed_run::ManagedWFFunc;

use crate::{
    abstractions::{dbg_panic, stream_when_allowed, MeteredSemaphore, OwnedMeteredSemPermit},
    protosext::{legacy_query_failure, ValidPollWFTQResponse, WorkflowActivationExt},
    telemetry::{metrics::workflow_worker_type, VecDisplayer},
    worker::{
        activities::{ActivitiesFromWFTsHandle, PermittedTqResp},
        client::{WorkerClient, WorkflowTaskCompletion},
        workflow::{
            history_update::HistoryPaginator,
            managed_run::{RunUpdateErr, WorkflowManager},
            wft_extraction::WFTExtractor,
            wft_poller::validate_wft,
            workflow_stream::{LocalInput, LocalInputs, WFStream},
        },
        LocalActRequest, LocalActivityExecutionResult, LocalActivityResolution,
    },
    MetricsContext,
};
use crossbeam::queue::SegQueue;
use futures::{stream::BoxStream, Stream, StreamExt};
use futures_util::stream;
use std::{
    collections::{HashSet, VecDeque},
    fmt::{Debug, Display, Formatter},
    future::Future,
    ops::{Add, DerefMut},
    result,
    sync::Arc,
    time::{Duration, Instant},
};
use temporal_sdk_core_api::errors::{CompleteWfError, PollWfError};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{
            remove_from_cache::EvictionReason, workflow_activation_job, QueryWorkflow,
            RemoveFromCache, WorkflowActivation, WorkflowActivationJob,
        },
        workflow_commands::*,
        workflow_completion,
        workflow_completion::{
            workflow_activation_completion, Failure, WorkflowActivationCompletion,
        },
    },
    temporal::api::{
        command::v1::{command::Attributes, Command as ProtoCommand, Command},
        common::v1::{Memo, RetryPolicy, SearchAttributes, WorkflowExecution},
        enums::v1::WorkflowTaskFailedCause,
        query::v1::WorkflowQuery,
        taskqueue::v1::StickyExecutionAttributes,
        workflowservice::v1::PollActivityTaskQueueResponse,
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
use tracing::Span;

pub(crate) const LEGACY_QUERY_ID: &str = "legacy_query";
/// What percentage of a WFT timeout we are willing to wait before sending a WFT heartbeat when
/// necessary.
const WFT_HEARTBEAT_TIMEOUT_FRACTION: f32 = 0.8;
const MAX_EAGER_ACTIVITY_RESERVATIONS_PER_WORKFLOW_TASK: usize = 3;

type Result<T, E = WFMachinesError> = result::Result<T, E>;
type BoxedActivationStream = BoxStream<'static, Result<WFStreamOutput, PollWfError>>;

/// Centralizes all state related to workflows and workflow tasks
pub(crate) struct Workflows {
    task_queue: String,
    local_tx: UnboundedSender<LocalInput>,
    fetch_tx: UnboundedSender<HistoryFetchReq>,
    processing_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    /// Because inputs may trigger activations for more than one run (ex: An activation for the
    /// run the input is associated with, and an activation to evict a different run because the
    /// cache is full), there is a need to buffer activations sometimes. They are pulled from
    /// first before
    buffered_activations: SegQueue<ActivationOrAuto>,
    activation_stream: tokio::sync::Mutex<(
        BoxedActivationStream,
        // Used to indicate polling may begin
        Option<oneshot::Sender<()>>,
    )>,
    client: Arc<dyn WorkerClient>,
    /// Will be populated when this worker is using a cache and should complete WFTs with a sticky
    /// queue.
    sticky_attrs: Option<StickyExecutionAttributes>,
    /// If set, can be used to reserve activity task slots for eager-return of new activity tasks.
    activity_tasks_handle: Option<ActivitiesFromWFTsHandle>,
    /// Ensures we stay at or below this worker's maximum concurrent workflow task limit
    wft_semaphore: MeteredSemaphore,
}

pub(super) struct WorkflowBasics {
    pub max_cached_workflows: usize,
    pub max_outstanding_wfts: usize,
    pub shutdown_token: CancellationToken,
    pub metrics: MetricsContext,
    pub namespace: String,
    pub task_queue: String,
    pub ignore_evicts_on_shutdown: bool,
}

impl Workflows {
    pub(super) fn new(
        basics: WorkflowBasics,
        sticky_attrs: Option<StickyExecutionAttributes>,
        client: Arc<dyn WorkerClient>,
        wft_stream: impl Stream<Item = Result<ValidPollWFTQResponse, tonic::Status>> + Send + 'static,
        local_activity_request_sink: impl Fn(Vec<LocalActRequest>) -> Vec<LocalActivityResolution>
            + Send
            + Sync
            + 'static,
        activity_tasks_handle: Option<ActivitiesFromWFTsHandle>,
    ) -> Self {
        let (local_tx, local_rx) = unbounded_channel();
        let (fetch_tx, fetch_rx) = unbounded_channel();
        let shutdown_tok = basics.shutdown_token.clone();
        let task_queue = basics.task_queue.clone();
        let wft_semaphore = MeteredSemaphore::new(
            basics.max_outstanding_wfts,
            basics.metrics.with_new_attrs([workflow_worker_type()]),
            MetricsContext::available_task_slots,
        );
        // Only allow polling of the new WFT stream if there are available task slots
        let proceeder = stream::unfold(wft_semaphore.clone(), |sem| async move {
            Some((sem.acquire_owned().await.unwrap(), sem))
        });
        let wft_stream = stream_when_allowed(wft_stream, proceeder);
        let extracted_wft_stream = WFTExtractor::new(
            client.clone(),
            wft_stream,
            UnboundedReceiverStream::new(fetch_rx),
        );
        let mut stream = WFStream::build(
            basics,
            extracted_wft_stream,
            UnboundedReceiverStream::new(local_rx),
            local_activity_request_sink,
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
            task_queue,
            local_tx,
            fetch_tx,
            processing_task: tokio::sync::Mutex::new(Some(processing_task)),
            buffered_activations: Default::default(),
            activation_stream: tokio::sync::Mutex::new((
                UnboundedReceiverStream::new(activation_rx).boxed(),
                Some(start_polling_tx),
            )),
            client,
            sticky_attrs,
            activity_tasks_handle,
            wft_semaphore,
        }
    }

    pub async fn next_workflow_activation(&self) -> Result<WorkflowActivation, PollWfError> {
        let activation = loop {
            if let Some(act) = self.buffered_activations.pop() {
                Span::current().record("run_id", act.run_id());
                match act {
                    ActivationOrAuto::LangActivation(act)
                    | ActivationOrAuto::ReadyForQueries(act) => {
                        break act;
                    }
                    ActivationOrAuto::Autocomplete { run_id } => {
                        self.activation_completed(WorkflowActivationCompletion {
                            run_id,
                            status: Some(
                                workflow_completion::Success::from_variants(vec![]).into(),
                            ),
                        })
                        .await?;
                        continue;
                    }
                }
            }
            let r = {
                let mut lock = self.activation_stream.lock().await;
                let (ref mut stream, ref mut beginner) = lock.deref_mut();
                if let Some(beginner) = beginner.take() {
                    let _ = beginner.send(());
                }
                stream.next().await.unwrap_or(Err(PollWfError::ShutDown))?
            };
            // TODO: Extract this probably
            if !r.fetch_histories.is_empty() {
                for fetchreq in r.fetch_histories {
                    if self.fetch_tx.send(fetchreq).is_err() {
                        error!("Some history fetch requests were dropped while shutting down");
                    }
                }
            }
            for act in r.activations {
                self.buffered_activations.push(act);
            }
        };
        debug!(activation=%activation, "Sending activation to lang");
        Ok(activation)
    }

    /// Queue an activation completion for processing, returning a future that will resolve with
    /// the outcome of that completion. See [ActivationCompletedOutcome].
    ///
    /// Returns the most-recently-processed event number for the run
    pub async fn activation_completed(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<usize, CompleteWfError> {
        let is_empty_completion = completion.is_empty();
        let completion = validate_completion(completion)?;
        let run_id = completion.run_id().to_string();
        let (tx, rx) = oneshot::channel();
        let was_sent = self.send_local(WFActCompleteMsg {
            completion,
            response_tx: tx,
        });
        if !was_sent {
            if is_empty_completion {
                // Empty complete which is likely an evict reply, we can just ignore.
                return Ok(0);
            }
            panic!(
                "A non-empty completion was not processed. Workflow processing may have \
                 terminated unexpectedly. This is a bug."
            );
        }

        let completion_outcome = rx
            .await
            .expect("Send half of activation complete response not dropped");
        let mut wft_from_complete = None;
        let wft_report_status = match completion_outcome.outcome {
            ActivationCompleteOutcome::ReportWFTSuccess(report) => match report {
                ServerCommandsWithWorkflowInfo {
                    task_token,
                    action:
                        ActivationAction::WftComplete {
                            mut commands,
                            query_responses,
                            force_new_wft,
                        },
                } => {
                    let reserved_act_permits =
                        self.reserve_activity_slots_for_outgoing_commands(commands.as_mut_slice());
                    debug!(commands=%commands.display(), query_responses=%query_responses.display(),
                           force_new_wft, "Sending responses to server");
                    let mut completion = WorkflowTaskCompletion {
                        task_token,
                        commands,
                        query_responses,
                        sticky_attributes: None,
                        return_new_workflow_task: true,
                        force_create_new_workflow_task: force_new_wft,
                    };
                    let sticky_attrs = self.sticky_attrs.clone();
                    // Do not return new WFT if we would not cache, because returned new WFTs are
                    // always partial.
                    if sticky_attrs.is_none() {
                        completion.return_new_workflow_task = false;
                    }
                    completion.sticky_attributes = sticky_attrs;

                    self.handle_wft_reporting_errs(&run_id, || async {
                        let maybe_wft = self.client.complete_workflow_task(completion).await?;
                        if let Some(wft) = maybe_wft.workflow_task {
                            wft_from_complete = Some(validate_wft(wft)?);
                        }
                        self.handle_eager_activities(
                            reserved_act_permits,
                            maybe_wft.activity_tasks,
                        );
                        Ok(())
                    })
                    .await;
                    WFTReportStatus::Reported
                }
                ServerCommandsWithWorkflowInfo {
                    task_token,
                    action: ActivationAction::RespondLegacyQuery { result },
                } => {
                    self.respond_legacy_query(task_token, *result).await;
                    WFTReportStatus::Reported
                }
            },
            ActivationCompleteOutcome::ReportWFTFail(outcome) => match outcome {
                FailedActivationWFTReport::Report(tt, cause, failure) => {
                    warn!(run_id=%run_id, failure=?failure, "Failing workflow task");
                    self.handle_wft_reporting_errs(&run_id, || async {
                        self.client
                            .fail_workflow_task(tt, cause, failure.failure.map(Into::into))
                            .await
                    })
                    .await;
                    WFTReportStatus::Reported
                }
                FailedActivationWFTReport::ReportLegacyQueryFailure(task_token, failure) => {
                    warn!(run_id=%run_id, failure=?failure, "Failing legacy query request");
                    self.respond_legacy_query(task_token, legacy_query_failure(failure))
                        .await;
                    WFTReportStatus::Reported
                }
            },
            ActivationCompleteOutcome::WFTFailedDontReport => WFTReportStatus::DropWft,
            ActivationCompleteOutcome::DoNothing => WFTReportStatus::NotReported,
        };

        // TODO: Combine with other place paginator is created, probably lift that into here, test.
        let maybe_pwft = if let Some(wft) = wft_from_complete {
            let (paginator, pwft) = HistoryPaginator::from_poll(wft, self.client.clone(), 0)
                .await
                .expect("TODO: Handle error");
            Some(pwft)
        } else {
            None
        };

        self.post_activation(PostActivationMsg {
            run_id,
            wft_report_status,
            wft_from_complete: maybe_pwft,
        });

        Ok(completion_outcome.most_recently_processed_event)
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

    pub fn available_wft_permits(&self) -> usize {
        self.wft_semaphore.available_permits()
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

    /// Must be called after every activation completion has finished
    fn post_activation(&self, msg: PostActivationMsg) {
        self.send_local(msg);
    }

    /// Handle server errors from either completing or failing a workflow task. Un-handleable errors
    /// trigger a workflow eviction and are logged.
    async fn handle_wft_reporting_errs<T, Fut>(&self, run_id: &str, completer: impl FnOnce() -> Fut)
    where
        Fut: Future<Output = Result<T, tonic::Status>>,
    {
        let mut should_evict = None;
        if let Err(err) = completer().await {
            match err.code() {
                // Silence unhandled command errors since the lang SDK cannot do anything
                // about them besides poll again, which it will do anyway.
                tonic::Code::InvalidArgument if err.message() == "UnhandledCommand" => {
                    debug!(error = %err, run_id, "Unhandled command response when completing");
                    should_evict = Some(EvictionReason::UnhandledCommand);
                }
                tonic::Code::NotFound => {
                    warn!(error = %err, run_id, "Task not found when completing");
                    should_evict = Some(EvictionReason::TaskNotFound);
                }
                _ => {
                    warn!(error= %err, "Network error while completing workflow activation");
                    should_evict = Some(EvictionReason::Fatal);
                }
            }
        }
        if let Some(reason) = should_evict {
            self.request_eviction(run_id, "Error reporting WFT to server", reason);
        }
    }

    /// Sends a message to the workflow processing stream. Returns true if the message was sent
    /// successfully.
    fn send_local(&self, msg: impl Into<LocalInputs>) -> bool {
        let msg = msg.into();
        let print_err = match &msg {
            LocalInputs::GetStateInfo(_) => false,
            LocalInputs::LocalResolution(lr) if lr.res.is_la_cancel_confirmation() => false,
            _ => true,
        };
        if let Err(e) = self.local_tx.send(LocalInput {
            input: msg,
            span: Span::current(),
        }) {
            if print_err {
                warn!(
                    "Tried to interact with workflow state after it shut down. This may be benign \
                     when processing evictions during shutdown. When sending {:?}",
                    e.0.input
                )
            }
            false
        } else {
            true
        }
    }

    /// Process eagerly returned activities from WFT completion
    fn handle_eager_activities(
        &self,
        reserved_act_permits: Vec<OwnedMeteredSemPermit>,
        eager_acts: Vec<PollActivityTaskQueueResponse>,
    ) {
        if let Some(at_handle) = self.activity_tasks_handle.as_ref() {
            let excess_reserved = reserved_act_permits.len().saturating_sub(eager_acts.len());
            if excess_reserved > 0 {
                debug!(
                    "Server returned {excess_reserved} fewer activities for \
                     eager execution than we requested"
                );
            } else if eager_acts.len() > reserved_act_permits.len() {
                // If we somehow got more activities from server than we asked for, server did
                // something wrong.
                error!(
                    "Server sent more activities for eager execution than we requested! They will \
                     be dropped and eventually time out. Please report this, as it is a server bug."
                )
            }
            let with_permits = reserved_act_permits
                .into_iter()
                .zip(eager_acts.into_iter())
                .map(|(permit, resp)| PermittedTqResp { permit, resp });
            if with_permits.len() > 0 {
                debug!(
                    "Adding {} activity tasks received from WFT complete",
                    with_permits.len()
                );
                at_handle.add_tasks(with_permits);
            }
        } else if !eager_acts.is_empty() {
            panic!(
                "Requested eager activity execution but this worker has no activity task \
                 manager! This is an internal bug, Core should not have asked for tasks."
            )
        }
    }

    /// Attempt to reserve activity slots for activities we could eagerly execute on
    /// this worker.
    ///
    /// Returns the number of activity slots that were reserved
    fn reserve_activity_slots_for_outgoing_commands(
        &self,
        commands: &mut [Command],
    ) -> Vec<OwnedMeteredSemPermit> {
        let mut reserved = vec![];
        for cmd in commands {
            if let Some(Attributes::ScheduleActivityTaskCommandAttributes(attrs)) =
                cmd.attributes.as_mut()
            {
                // If request_eager_execution was already false, that means lang explicitly
                // told us it didn't want to eagerly execute for some reason. So, we only
                // ever turn *off* eager execution if a slot is not available or the activity
                // is scheduled on a different task queue.
                if attrs.request_eager_execution {
                    let same_task_queue = attrs
                        .task_queue
                        .as_ref()
                        .map(|q| q.name == self.task_queue)
                        .unwrap_or_default();
                    if same_task_queue
                        && reserved.len() < MAX_EAGER_ACTIVITY_RESERVATIONS_PER_WORKFLOW_TASK
                    {
                        if let Some(p) = self
                            .activity_tasks_handle
                            .as_ref()
                            .and_then(|h| h.reserve_slot())
                        {
                            reserved.push(p);
                        } else {
                            attrs.request_eager_execution = false;
                        }
                    } else {
                        attrs.request_eager_execution = false;
                    }
                }
            }
        }
        reserved
    }

    /// Wraps responding to legacy queries. Handles ignore-able failures.
    async fn respond_legacy_query(&self, tt: TaskToken, res: QueryResult) {
        match self.client.respond_legacy_query(tt, res).await {
            Ok(_) => {}
            Err(e) if e.code() == tonic::Code::NotFound => {
                warn!(error=?e, "Query not found when attempting to respond to it");
            }
            Err(e) => {
                warn!(error= %e, "Network error while responding to legacy query");
            }
        }
    }
}

// TODO: Rename to just managedrun after combining -- put in own module & fix visibility
/// Manages access to a specific workflow run, and contains various bookkeeping information that the
/// [WFStream] may need to access quickly.
#[derive(derive_more::DebugCustom)]
#[debug(
    fmt = "ManagedRunHandle {{ wft: {:?}, activation: {:?}, buffered_resp: {:?} \
           have_seen_terminal_event: {}, most_recently_processed_event: {}, more_pending_work: {}, \
           trying_to_evict: {} }}",
    wft,
    activation,
    buffered_resp,
    have_seen_terminal_event,
    most_recently_processed_event_number,
    more_pending_work,
    "trying_to_evict.is_some()"
)]
struct ManagedRunHandle {
    // TODO: Eliminate now duped fields
    wfm: WorkflowManager,
    local_activity_request_sink: LocalActivityRequestSink,
    waiting_on_la: Option<WaitingOnLAs>,
    // Is set to true if the machines encounter an error and the only subsequent thing we should
    // do is be evicted.
    am_broken: bool,
    /// If set, the WFT this run is currently/will be processing.
    wft: Option<OutstandingTask>,
    /// An outstanding activation to lang
    activation: Option<OutstandingActivation>,
    /// If set, it indicates there is a buffered poll response from the server that applies to this
    /// run. This can happen when lang takes too long to complete a task and the task times out, for
    /// example. Upon next completion, the buffered response will be removed and can be made ready
    /// to be returned from polling
    buffered_resp: Option<PermittedWFT>,
    /// True if this machine has seen an event which ends the execution
    have_seen_terminal_event: bool,
    /// The most recently processed event id this machine has seen. 0 means it has seen nothing.
    most_recently_processed_event_number: usize,
    /// Is set true when the machines indicate that there is additional known work to be processed
    more_pending_work: bool,
    /// Is set if an eviction has been requested for this run
    trying_to_evict: Option<RequestEvictMsg>,
    // Used to feed heartbeat timeout events back into the workflow inputs.
    // TODO: Ideally, we wouldn't spawn tasks inside here now that this is tokio-free-ish.
    //   Instead, `Workflows` can spawn them itself if there are any LAs.
    heartbeat_tx: UnboundedSender<HeartbeatTimeoutMsg>,

    /// We track if we have recorded useful debugging values onto a certain span yet, to overcome
    /// duplicating field values. Remove this once https://github.com/tokio-rs/tracing/issues/2334
    /// is fixed.
    recorded_span_ids: HashSet<tracing::Id>,
    metrics: MetricsContext,
}
impl ManagedRunHandle {
    fn new(
        wfm: WorkflowManager,
        local_activity_request_sink: LocalActivityRequestSink,
        heartbeat_tx: UnboundedSender<HeartbeatTimeoutMsg>,
        metrics: MetricsContext,
    ) -> Self {
        Self {
            wfm,
            local_activity_request_sink,
            waiting_on_la: None,
            am_broken: false,
            wft: None,
            activation: None,
            buffered_resp: None,
            have_seen_terminal_event: false,
            most_recently_processed_event_number: 0,
            more_pending_work: false,
            trying_to_evict: None,
            heartbeat_tx,
            recorded_span_ids: Default::default(),
            metrics,
        }
    }

    // TODO: Collapse all these wrapper funcs
    fn incoming_wft(&mut self, wft: NewIncomingWFT) -> RunUpdateResponseKind {
        let res = self._incoming_wft(wft);
        self.run_update_to_response(res.map(Into::into), true)
    }

    fn _incoming_wft(
        &mut self,
        wft: NewIncomingWFT,
    ) -> Result<Option<ActivationOrAuto>, RunUpdateErr> {
        if self.wft.is_some() {
            error!("Trying to send a new WFT for a run which already has one!");
        }
        let activation = if let Some(h) = wft.history_update {
            self.wfm.feed_history_from_server(h)?
        } else {
            let r = self.wfm.get_next_activation()?;
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
                // If the activation has no jobs but there are outstanding LAs, we need to restart
                // the WFT heartbeat.
                if let Some(ref mut lawait) = self.waiting_on_la {
                    if lawait.completion_dat.is_some() {
                        panic!("Should not have completion dat when getting new wft & empty jobs")
                    }
                    lawait.heartbeat_timeout_task.abort();
                    lawait.heartbeat_timeout_task = start_heartbeat_timeout_task(
                        self.wfm.machines.run_id.clone(),
                        self.heartbeat_tx.clone(),
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

    // TODO: Collapse all these wrapper funcs
    fn check_more_activations(&mut self) -> RunUpdateResponseKind {
        let res = self._check_more_activations();
        self.run_update_to_response(res.map(Into::into), false)
    }

    fn _check_more_activations(&mut self) -> Result<Option<ActivationOrAuto>, RunUpdateErr> {
        // No point in checking for more activations if there's already an outstanding activation.
        if self.activation.is_some() {
            return Ok(None);
        }
        let has_pending_queries = self
            .wft
            .as_ref()
            .map(|wft| !wft.pending_queries.is_empty())
            .unwrap_or_default();
        // In the event it's time to evict this run, cancel any outstanding LAs
        if self.trying_to_evict.is_some() {
            self.sink_la_requests(vec![LocalActRequest::CancelAllInRun(
                self.wfm.machines.run_id.clone(),
            )])?;
        }

        if !self.wft.is_some() {
            // It doesn't make sense to do workflow work unless we have a WFT
            return Ok(None);
        }

        if self.wfm.machines.has_pending_jobs() && !self.am_broken {
            Ok(Some(ActivationOrAuto::LangActivation(
                self.wfm.get_next_activation()?,
            )))
        } else {
            if has_pending_queries && !self.am_broken {
                return Ok(Some(ActivationOrAuto::ReadyForQueries(
                    self.wfm.machines.get_wf_activation(),
                )));
            }
            if let Some(wte) = self.trying_to_evict.clone() {
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

    // TODO: Collapse all these wrapper funcs
    fn send_completion(&mut self, completion: RunActivationCompletion) -> RunUpdateResponseKind {
        let res = self._send_completion(completion);
        self.run_update_to_response(res.map(Into::into), false)
    }

    fn _send_completion(
        &mut self,
        mut completion: RunActivationCompletion,
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

        // TODO: Probably func-ify this rather than inline closure
        let outcome = (|| {
            // Send commands from lang into the machines then check if the workflow run
            // needs another activation and mark it if so
            self.wfm.push_commands_and_iterate(completion.commands)?;
            // Don't bother applying the next task if we're evicting at the end of
            // this activation
            if !completion.activation_was_eviction {
                self.wfm.apply_next_task_if_ready()?;
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
                Ok((Some((completion.start_time, wft_timeout)), data, self))
            }
        })();

        match outcome {
            Ok((None, data, me)) => Ok(Some(me.prepare_complete_resp(resp_chan, data, false))),
            Ok((Some((start_t, wft_timeout)), data, me)) => {
                if let Some(wola) = me.waiting_on_la.as_mut() {
                    wola.heartbeat_timeout_task.abort();
                }
                me.waiting_on_la = Some(WaitingOnLAs {
                    wft_timeout,
                    completion_dat: Some((data, resp_chan)),
                    heartbeat_timeout_task: start_heartbeat_timeout_task(
                        me.wfm.machines.run_id.clone(),
                        me.heartbeat_tx.clone(),
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

    // TODO: Collapse all these wrapper funcs
    fn local_resolution(&mut self, res: LocalResolution) -> RunUpdateResponseKind {
        let res = self._local_resolution(res);
        self.run_update_to_response(res.map(Into::into), false)
    }

    fn _local_resolution(
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

    fn heartbeat_timeout(&mut self) -> RunUpdateResponseKind {
        let maybe_act = if self._heartbeat_timeout() {
            Some(ActivationOrAuto::Autocomplete {
                run_id: self.wfm.machines.run_id.clone(),
            })
        } else {
            None
        };
        self.run_update_to_response(Ok(maybe_act).map(Into::into), false)
    }
    /// Returns `true` if autocompletion should be issued, which will actually cause us to end up
    /// in [completion] again, at which point we'll start a new heartbeat timeout, which will
    /// immediately trigger and thus finish the completion, forcing a new task as it should.
    fn _heartbeat_timeout(&mut self) -> bool {
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

    /// Returns true if the managed run has any form of pending work
    /// If `ignore_evicts` is true, pending evictions do not count as pending work.
    /// If `ignore_buffered` is true, buffered workflow tasks do not count as pending work.
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
        trace!(wft=self.wft.is_some(), buffered=?buffered, more_work=?self.more_pending_work,
               act_work, evict_work, "Does run have pending work?");
        self.wft.is_some() || buffered || self.more_pending_work || act_work || evict_work
    }

    /// Returns true if the handle is currently processing a WFT which contains a legacy query.
    fn pending_work_is_legacy_query(&self) -> bool {
        // Either we know because there is a pending legacy query, or it's already been drained and
        // sent as an activation.
        matches!(self.activation, Some(OutstandingActivation::LegacyQuery))
            || self
                .wft
                .as_ref()
                .map(|t| t.has_pending_legacy_query())
                .unwrap_or_default()
    }

    // TODO: Collapse a bunch of this stuff after seeing it work
    fn run_update_to_response(
        &mut self,
        outcome: Result<ActOrFulfill, RunUpdateErr>,
        in_response_to_wft: bool,
    ) -> RunUpdateResponseKind {
        match outcome {
            Err(e) => self.fail_resp(e),
            Ok(act_or_fulfull) => {
                let (mut outgoing_activation, fulfillable_complete) = match act_or_fulfull {
                    ActOrFulfill::OutgoingAct(a) => (a, None),
                    ActOrFulfill::FulfillableComplete(c) => (None, c),
                };
                let mut more_pending_work = self.wfm.machines.has_pending_jobs();
                // We don't want to consider there to be more local-only work to be done if there is
                // no workflow task associated with the run right now. This can happen if, ex, we
                // complete a local activity while waiting for server to send us the next WFT.
                // Activating lang would be harmful at this stage, as there might be work returned
                // in that next WFT which should be part of the next activation.
                if self.wft.is_none() {
                    more_pending_work = false;
                }
                // If there's no activation but is pending work, check and possibly assign one
                if more_pending_work && outgoing_activation.is_none() {
                    match self._check_more_activations() {
                        Ok(oa) => outgoing_activation = oa,
                        Err(e) => return self.fail_resp(e),
                    }
                }
                RunUpdateResponseKind::Good(GoodRunUpdate {
                    run_id: self.wfm.machines.run_id.clone(),
                    outgoing_activation,
                    fulfillable_complete,
                    have_seen_terminal_event: self.wfm.machines.have_seen_terminal_event,
                    more_pending_work,
                    most_recently_processed_event_number: self.wfm.machines.last_processed_event
                        as usize,
                    in_response_to_wft,
                })
            }
        }
    }
    fn fail_resp(&mut self, e: RunUpdateErr) -> RunUpdateResponseKind {
        self.am_broken = true;
        RunUpdateResponseKind::Fail(FailRunUpdate {
            run_id: self.wfm.machines.run_id.clone(),
            err: e.source,
            completion_resp: e.complete_resp_chan,
        })
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
}

fn start_heartbeat_timeout_task(
    run_id: String,
    chan: UnboundedSender<HeartbeatTimeoutMsg>,
    wft_start_time: Instant,
    wft_timeout: Duration,
) -> JoinHandle<()> {
    // The heartbeat deadline is 80% of the WFT timeout
    let wft_heartbeat_deadline =
        wft_start_time.add(wft_timeout.mul_f32(WFT_HEARTBEAT_TIMEOUT_FRACTION));
    task::spawn(async move {
        tokio::time::sleep_until(wft_heartbeat_deadline.into()).await;
        let _ = chan.send(HeartbeatTimeoutMsg { run_id });
    })
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
    heartbeat_timeout_task: JoinHandle<()>,
}
#[derive(Debug)]
struct CompletionDataForWFT {
    task_token: TaskToken,
    query_responses: Vec<QueryResult>,
    has_pending_query: bool,
    activation_was_only_eviction: bool,
}

// Bubbled up from calls to update workflow state if history needs fetching for a workflow
#[derive(Debug, derive_more::Display)]
#[display(
    fmt = "HistoryFetchReq(run_id: {})",
    "original_wft.work.execution.run_id"
)]
#[must_use]
struct HistoryFetchReq {
    original_wft: PermittedWFT,
}

#[derive(Debug)]
struct WFStreamOutput {
    activations: VecDeque<ActivationOrAuto>,
    fetch_histories: VecDeque<HistoryFetchReq>,
}

#[derive(Debug, derive_more::Display)]
enum ActivationOrAuto {
    LangActivation(WorkflowActivation),
    /// This type should only be filled with an empty activation which is ready to have queries
    /// inserted into the joblist
    ReadyForQueries(WorkflowActivation),
    #[display(fmt = "Autocomplete(run_id={})", run_id)]
    Autocomplete {
        run_id: String,
    },
}
impl ActivationOrAuto {
    pub fn run_id(&self) -> &str {
        match self {
            ActivationOrAuto::LangActivation(act) => &act.run_id,
            ActivationOrAuto::Autocomplete { run_id, .. } => run_id,
            ActivationOrAuto::ReadyForQueries(act) => &act.run_id,
        }
    }
}

/// A processed WFT which has been validated and had a history update extracted from it
#[derive(derive_more::DebugCustom)]
#[debug(fmt = "PermittedWft({:?})", work)]
pub(crate) struct PermittedWFT {
    work: PreparedWFT,
    permit: OwnedMeteredSemPermit,
}
#[derive(Debug)]
struct PreparedWFT {
    task_token: TaskToken,
    attempt: u32,
    execution: WorkflowExecution,
    workflow_type: String,
    legacy_query: Option<WorkflowQuery>,
    query_requests: Vec<QueryWorkflow>,
    // TODO: make either leg query or update?
    update: HistoryUpdate,
}

#[derive(Debug)]
pub(crate) struct OutstandingTask {
    pub info: WorkflowTaskInfo,
    pub hit_cache: bool,
    /// Set if the outstanding task has quer(ies) which must be fulfilled upon finishing replay
    pub pending_queries: Vec<QueryWorkflow>,
    pub start_time: Instant,
    /// The WFT permit owned by this task, ensures we don't exceed max concurrent WFT, and makes
    /// sure the permit is automatically freed when we delete the task.
    pub permit: OwnedMeteredSemPermit,
}

impl OutstandingTask {
    pub fn has_pending_legacy_query(&self) -> bool {
        self.pending_queries
            .iter()
            .any(|q| q.query_id == LEGACY_QUERY_ID)
    }
}

#[derive(Copy, Clone, Debug)]
pub(crate) enum OutstandingActivation {
    /// A normal activation with a joblist
    Normal {
        /// True if there is an eviction in the joblist
        contains_eviction: bool,
        /// Number of jobs in the activation
        num_jobs: usize,
    },
    /// An activation for a legacy query
    LegacyQuery,
    /// A fake activation which is never sent to lang, but used internally
    Autocomplete,
}

impl OutstandingActivation {
    pub(crate) const fn has_only_eviction(self) -> bool {
        matches!(
            self,
            OutstandingActivation::Normal {
                contains_eviction: true,
                num_jobs: nj
            }
        if nj == 1)
    }
    pub(crate) const fn has_eviction(self) -> bool {
        matches!(
            self,
            OutstandingActivation::Normal {
                contains_eviction: true,
                ..
            }
        )
    }
}

/// Contains important information about a given workflow task that we need to memorize while
/// lang handles it.
#[derive(Clone, Debug)]
pub struct WorkflowTaskInfo {
    pub task_token: TaskToken,
    pub attempt: u32,
    /// Exists to allow easy tagging of spans with workflow ids. Is duplicative of info inside the
    /// run machines themselves, but that can't be accessed easily. Would be nice to somehow have a
    /// shared repository, or refcounts, or whatever, for strings like these that get duped all
    /// sorts of places.
    pub wf_id: String,
}

#[derive(Debug)]
pub enum FailedActivationWFTReport {
    Report(TaskToken, WorkflowTaskFailedCause, Failure),
    ReportLegacyQueryFailure(TaskToken, Failure),
}

#[derive(Debug)]
pub(crate) struct ServerCommandsWithWorkflowInfo {
    pub task_token: TaskToken,
    pub action: ActivationAction,
}

#[derive(Debug)]
pub(crate) enum ActivationAction {
    /// We should respond that the workflow task is complete
    WftComplete {
        commands: Vec<ProtoCommand>,
        query_responses: Vec<QueryResult>,
        force_new_wft: bool,
    },
    /// We should respond to a legacy query request
    RespondLegacyQuery { result: Box<QueryResult> },
}

#[derive(Debug)]
enum EvictionRequestResult {
    EvictionRequested(Option<u32>, RunUpdateResponseKind),
    NotFound,
    EvictionAlreadyRequested(Option<u32>),
}
impl EvictionRequestResult {
    fn into_run_update_resp(self) -> Option<RunUpdateResponseKind> {
        match self {
            EvictionRequestResult::EvictionRequested(_, resp) => Some(resp),
            EvictionRequestResult::NotFound
            | EvictionRequestResult::EvictionAlreadyRequested(_) => None,
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)] // Not always used in non-test
pub(crate) struct WorkflowStateInfo {
    pub cached_workflows: usize,
    pub outstanding_wft: usize,
}

#[derive(Debug)]
struct WFActCompleteMsg {
    completion: ValidatedCompletion,
    response_tx: oneshot::Sender<ActivationCompleteResult>,
}
#[derive(Debug)]
struct LocalResolutionMsg {
    run_id: String,
    res: LocalResolution,
}
#[derive(Debug)]
struct PostActivationMsg {
    run_id: String,
    wft_report_status: WFTReportStatus,
    wft_from_complete: Option<PreparedWFT>,
}
#[derive(Debug, Clone)]
struct RequestEvictMsg {
    run_id: String,
    message: String,
    reason: EvictionReason,
}
#[derive(Debug)]
struct HeartbeatTimeoutMsg {
    run_id: String,
}
#[derive(Debug)]
struct GetStateInfoMsg {
    response_tx: oneshot::Sender<WorkflowStateInfo>,
}

/// Each activation completion produces one of these
#[derive(Debug)]
struct ActivationCompleteResult {
    most_recently_processed_event: usize,
    outcome: ActivationCompleteOutcome,
}
/// What needs to be done after calling [Workflows::activation_completed]
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
enum ActivationCompleteOutcome {
    /// The WFT must be reported as successful to the server using the contained information.
    ReportWFTSuccess(ServerCommandsWithWorkflowInfo),
    /// The WFT must be reported as failed to the server using the contained information.
    ReportWFTFail(FailedActivationWFTReport),
    /// There's nothing to do right now. EX: The workflow needs to keep replaying.
    DoNothing,
    /// The workflow task failed, but we shouldn't report it. EX: We have failed 2 or more attempts
    /// in a row.
    WFTFailedDontReport,
}
/// Did we report, or not, completion of a WFT to server?
#[derive(Debug)]
enum WFTReportStatus {
    Reported,
    /// The WFT completion was not reported when finishing the activation, because there's still
    /// work to be done. EX: Running LAs.
    NotReported,
    /// We didn't report, but we want to clear the outstanding workflow task anyway. See
    /// [ActivationCompleteOutcome::WFTFailedDontReport]
    DropWft,
}
#[derive(Debug)]
struct FulfillableActivationComplete {
    result: ActivationCompleteResult,
    resp_chan: oneshot::Sender<ActivationCompleteResult>,
}
impl FulfillableActivationComplete {
    fn fulfill(self) {
        let _ = self.resp_chan.send(self.result);
    }
}

fn validate_completion(
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

            if commands.len() > 1
                && commands.iter().any(
                    |c| matches!(c, WFCommand::QueryResponse(q) if q.query_id == LEGACY_QUERY_ID),
                )
            {
                return Err(CompleteWfError::MalformedWorkflowCompletion {
                    reason: format!(
                        "Workflow completion had a legacy query response along with other \
                         commands. This is not allowed and constitutes an error in the \
                         lang SDK. Commands: {:?}",
                        commands
                    ),
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

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
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

impl ValidatedCompletion {
    pub fn run_id(&self) -> &str {
        match self {
            ValidatedCompletion::Success { run_id, .. } => run_id,
            ValidatedCompletion::Fail { run_id, .. } => run_id,
        }
    }
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
    resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
}
#[derive(Debug, derive_more::From)]
enum ActOrFulfill {
    OutgoingAct(Option<ActivationOrAuto>),
    FulfillableComplete(Option<FulfillableActivationComplete>),
}

#[derive(Debug, derive_more::Display)]
#[must_use]
#[allow(clippy::large_enum_variant)]
enum RunUpdateResponseKind {
    Good(GoodRunUpdate),
    Fail(FailRunUpdate),
}

#[derive(Debug)]
struct GoodRunUpdate {
    run_id: String,
    outgoing_activation: Option<ActivationOrAuto>,
    fulfillable_complete: Option<FulfillableActivationComplete>,
    have_seen_terminal_event: bool,
    /// Is true if there are more jobs that need to be sent to lang
    more_pending_work: bool,
    most_recently_processed_event_number: usize,
    /// Is true if this update was in response to a new WFT
    in_response_to_wft: bool,
}
impl Display for GoodRunUpdate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GoodRunUpdate(run_id: {}, outgoing_activation: {}, more_pending_work: {})",
            self.run_id,
            if let Some(og) = self.outgoing_activation.as_ref() {
                format!("{}", og)
            } else {
                "None".to_string()
            },
            self.more_pending_work
        )
    }
}
#[derive(Debug)]
pub(crate) struct FailRunUpdate {
    run_id: String,
    err: WFMachinesError,
    /// This is populated if the run update failed while processing a completion - and thus we
    /// must respond down it when handling the failure.
    completion_resp: Option<oneshot::Sender<ActivationCompleteResult>>,
}
impl Display for FailRunUpdate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FailRunUpdate(run_id: {}, error: {:?})",
            self.run_id, self.err
        )
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
impl LocalResolution {
    pub fn is_la_cancel_confirmation(&self) -> bool {
        match self {
            LocalResolution::LocalActivity(lar) => {
                matches!(lar.result, LocalActivityExecutionResult::Cancelled(_))
            }
        }
    }
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
    CancelChild(CancelChildWorkflowExecution),
    RequestCancelExternalWorkflow(RequestCancelExternalWorkflowExecution),
    SignalExternalWorkflow(SignalExternalWorkflowExecution),
    CancelSignalWorkflow(CancelSignalWorkflow),
    UpsertSearchAttributes(UpsertWorkflowSearchAttributes),
    ModifyWorkflowProperties(ModifyWorkflowProperties),
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
            workflow_command::Variant::CancelChildWorkflowExecution(s) => Ok(Self::CancelChild(s)),
            workflow_command::Variant::ScheduleLocalActivity(s) => Ok(Self::AddLocalActivity(s)),
            workflow_command::Variant::RequestCancelLocalActivity(s) => {
                Ok(Self::RequestCancelLocalActivity(s))
            }
            workflow_command::Variant::UpsertWorkflowSearchAttributes(s) => {
                Ok(Self::UpsertSearchAttributes(s))
            }
            workflow_command::Variant::ModifyWorkflowProperties(s) => {
                Ok(Self::ModifyWorkflowProperties(s))
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
    memo: Option<Memo>,
    search_attrs: Option<SearchAttributes>,
    retry_policy: Option<RetryPolicy>,
}

type LocalActivityRequestSink =
    Arc<dyn Fn(Vec<LocalActRequest>) -> Vec<LocalActivityResolution> + Send + Sync>;

/// Wraps outgoing activation job protos with some internal details core might care about
#[derive(Debug, derive_more::Display)]
#[display(fmt = "{}", variant)]
struct OutgoingJob {
    variant: workflow_activation_job::Variant,
    /// Since LA resolutions are not distinguished from non-LA resolutions as far as lang is
    /// concerned, but core cares about that sometimes, attach that info here.
    is_la_resolution: bool,
}
impl<WA: Into<workflow_activation_job::Variant>> From<WA> for OutgoingJob {
    fn from(wa: WA) -> Self {
        Self {
            variant: wa.into(),
            is_la_resolution: false,
        }
    }
}
impl From<OutgoingJob> for WorkflowActivationJob {
    fn from(og: OutgoingJob) -> Self {
        Self {
            variant: Some(og.variant),
        }
    }
}
