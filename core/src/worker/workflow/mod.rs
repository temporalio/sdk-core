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

#[cfg(feature = "save_wf_inputs")]
pub use workflow_stream::replay_wf_state_inputs;

pub(crate) use bridge::WorkflowBridge;
pub(crate) use driven_workflow::{DrivenWorkflow, WorkflowFetcher};
pub(crate) use history_update::HistoryUpdate;
#[cfg(test)]
pub(crate) use managed_run::ManagedWFFunc;

use crate::{
    abstractions::{
        dbg_panic, take_cell::TakeCell, MeteredSemaphore, TrackedOwnedMeteredSemPermit,
        UsedMeteredSemPermit,
    },
    internal_flags::InternalFlags,
    protosext::legacy_query_failure,
    telemetry::{set_trace_subscriber_for_current_thread, TelemetryInstance, VecDisplayer},
    worker::{
        activities::{ActivitiesFromWFTsHandle, LocalActivityManager, TrackedPermittedTqResp},
        client::{WorkerClient, WorkflowTaskCompletion},
        workflow::{
            history_update::HistoryPaginator,
            managed_run::RunUpdateAct,
            wft_extraction::{HistoryFetchReq, WFTExtractor, WFTStreamIn},
            wft_poller::validate_wft,
            workflow_stream::{LocalInput, LocalInputs, WFStream},
        },
        LocalActRequest, LocalActivityExecutionResult, LocalActivityResolution,
        PostActivateHookData,
    },
    MetricsContext,
};
use anyhow::anyhow;
use futures::{stream::BoxStream, Stream, StreamExt};
use futures_util::{future::abortable, stream};
use prost_types::TimestampError;
use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::VecDeque,
    fmt::Debug,
    future::Future,
    mem::discriminant,
    ops::DerefMut,
    rc::Rc,
    result,
    sync::{atomic, atomic::AtomicBool, Arc},
    thread,
    time::{Duration, Instant},
};
use temporal_sdk_core_api::errors::{CompleteWfError, PollWfError};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{
            remove_from_cache::EvictionReason, workflow_activation_job, QueryWorkflow,
            WorkflowActivation, WorkflowActivationJob,
        },
        workflow_commands::*,
        workflow_completion,
        workflow_completion::{
            workflow_activation_completion, Failure, WorkflowActivationCompletion,
        },
    },
    temporal::api::{
        command::v1::{command::Attributes, Command as ProtoCommand, Command},
        common::v1::{Memo, MeteringMetadata, RetryPolicy, SearchAttributes, WorkflowExecution},
        enums::v1::WorkflowTaskFailedCause,
        query::v1::WorkflowQuery,
        sdk::v1::WorkflowTaskCompletedMetadata,
        taskqueue::v1::StickyExecutionAttributes,
        workflowservice::v1::{get_system_info_response, PollActivityTaskQueueResponse},
    },
    TaskToken,
};
use tokio::{
    sync::{
        mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    task::{spawn_blocking, LocalSet},
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
type BoxedActivationStream = BoxStream<'static, Result<ActivationOrAuto, PollWfError>>;
type InternalFlagsRef = Rc<RefCell<InternalFlags>>;

/// Centralizes all state related to workflows and workflow tasks
pub(crate) struct Workflows {
    task_queue: String,
    local_tx: UnboundedSender<LocalInput>,
    processing_task: TakeCell<thread::JoinHandle<()>>,
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
    wft_semaphore: Arc<MeteredSemaphore>,
    local_act_mgr: Arc<LocalActivityManager>,
    ever_polled: AtomicBool,
}

pub(crate) struct WorkflowBasics {
    pub max_cached_workflows: usize,
    pub shutdown_token: CancellationToken,
    pub metrics: MetricsContext,
    pub namespace: String,
    pub task_queue: String,
    pub ignore_evicts_on_shutdown: bool,
    pub fetching_concurrency: usize,
    pub server_capabilities: get_system_info_response::Capabilities,
    #[cfg(feature = "save_wf_inputs")]
    pub wf_state_inputs: Option<UnboundedSender<Vec<u8>>>,
}

pub(crate) struct RunBasics<'a> {
    pub namespace: String,
    pub workflow_id: String,
    pub workflow_type: String,
    pub run_id: String,
    pub task_queue: String,
    pub history: HistoryUpdate,
    pub metrics: MetricsContext,
    pub capabilities: &'a get_system_info_response::Capabilities,
}

impl Workflows {
    #[allow(clippy::too_many_arguments)] // Not much worth combining here
    pub(super) fn new(
        basics: WorkflowBasics,
        sticky_attrs: Option<StickyExecutionAttributes>,
        client: Arc<dyn WorkerClient>,
        wft_semaphore: Arc<MeteredSemaphore>,
        wft_stream: impl Stream<Item = WFTStreamIn> + Send + 'static,
        local_activity_request_sink: impl LocalActivityRequestSink,
        local_act_mgr: Arc<LocalActivityManager>,
        heartbeat_timeout_rx: UnboundedReceiver<HeartbeatTimeoutMsg>,
        activity_tasks_handle: Option<ActivitiesFromWFTsHandle>,
        telem_instance: Option<&TelemetryInstance>,
    ) -> Self {
        let (local_tx, local_rx) = unbounded_channel();
        let (fetch_tx, fetch_rx) = unbounded_channel();
        let shutdown_tok = basics.shutdown_token.clone();
        let task_queue = basics.task_queue.clone();
        let extracted_wft_stream = WFTExtractor::build(
            client.clone(),
            basics.fetching_concurrency,
            wft_stream,
            UnboundedReceiverStream::new(fetch_rx),
        );
        let locals_stream = stream::select(
            UnboundedReceiverStream::new(local_rx),
            UnboundedReceiverStream::new(heartbeat_timeout_rx).map(Into::into),
        );
        let (activation_tx, activation_rx) = unbounded_channel();
        let (start_polling_tx, start_polling_rx) = oneshot::channel();
        // We must spawn a task to constantly poll the activation stream, because otherwise
        // activation completions would not cause anything to happen until the next poll.
        let tracing_sub = telem_instance.map(|ti| ti.trace_subscriber());
        let processing_task = thread::spawn(move || {
            if let Some(ts) = tracing_sub {
                set_trace_subscriber_for_current_thread(ts);
            }
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .thread_name("workflow-processing")
                .build()
                .unwrap();
            let local = LocalSet::new();
            local.block_on(&rt, async move {
                let mut stream = WFStream::build(
                    basics,
                    extracted_wft_stream,
                    locals_stream,
                    local_activity_request_sink,
                );

                // However, we want to avoid plowing ahead until we've been asked to poll at least
                // once. This supports activity-only workers.
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
                while let Some(output) = stream.next().await {
                    match output {
                        Ok(o) => {
                            for fetchreq in o.fetch_histories {
                                fetch_tx
                                    .send(fetchreq)
                                    .expect("Fetch channel must not be dropped");
                            }
                            for act in o.activations {
                                activation_tx
                                    .send(Ok(act))
                                    .expect("Activation processor channel not dropped");
                            }
                        }
                        Err(e) => activation_tx
                            .send(Err(e))
                            .expect("Activation processor channel not dropped"),
                    }
                }
            });
        });
        Self {
            task_queue,
            local_tx,
            processing_task: TakeCell::new(processing_task),
            activation_stream: tokio::sync::Mutex::new((
                UnboundedReceiverStream::new(activation_rx).boxed(),
                Some(start_polling_tx),
            )),
            client,
            sticky_attrs,
            activity_tasks_handle,
            wft_semaphore,
            local_act_mgr,
            ever_polled: AtomicBool::new(false),
        }
    }

    pub(super) async fn next_workflow_activation(&self) -> Result<WorkflowActivation, PollWfError> {
        self.ever_polled.store(true, atomic::Ordering::Release);
        loop {
            let al = {
                let mut lock = self.activation_stream.lock().await;
                let (ref mut stream, ref mut beginner) = lock.deref_mut();
                if let Some(beginner) = beginner.take() {
                    let _ = beginner.send(());
                }
                stream.next().await.unwrap_or(Err(PollWfError::ShutDown))?
            };
            Span::current().record("run_id", al.run_id());
            match al {
                ActivationOrAuto::LangActivation(mut act)
                | ActivationOrAuto::ReadyForQueries(mut act) => {
                    sort_act_jobs(&mut act);
                    debug!(activation=%act, "Sending activation to lang");
                    break Ok(act);
                }
                ActivationOrAuto::Autocomplete { run_id } => {
                    self.activation_completed(
                        WorkflowActivationCompletion {
                            run_id,
                            status: Some(
                                workflow_completion::Success::from_variants(vec![]).into(),
                            ),
                        },
                        true,
                        // We need to say a type, but the type is irrelevant, so imagine some
                        // boxed function we'll never call.
                        Option::<Box<dyn Fn(PostActivateHookData) + Send>>::None,
                    )
                    .await?;
                }
                ActivationOrAuto::AutoFail {
                    run_id,
                    machines_err,
                } => {
                    self.activation_completed(
                        WorkflowActivationCompletion {
                            run_id,
                            status: Some(auto_fail_to_complete_status(machines_err)),
                        },
                        true,
                        Option::<Box<dyn Fn(PostActivateHookData) + Send>>::None,
                    )
                    .await?;
                }
            }
        }
    }

    /// Queue an activation completion for processing, returning a future that will resolve with
    /// the outcome of that completion. See [ActivationCompletedOutcome].
    ///
    /// Returns the most-recently-processed event number for the run.
    pub(super) async fn activation_completed(
        &self,
        completion: WorkflowActivationCompletion,
        is_autocomplete: bool,
        post_activate_hook: Option<impl Fn(PostActivateHookData)>,
    ) -> Result<(), CompleteWfError> {
        let is_empty_completion = completion.is_empty();
        let completion = validate_completion(completion)?;
        let run_id = completion.run_id().to_string();
        let (tx, rx) = oneshot::channel();
        let was_sent = self.send_local(WFActCompleteMsg {
            completion,
            response_tx: Some(tx),
        });
        if !was_sent {
            if is_empty_completion {
                // Empty complete which is likely an evict reply, we can just ignore.
                return Ok(());
            }
            panic!(
                "A non-empty completion was not processed. Workflow processing may have \
                 terminated unexpectedly. This is a bug."
            );
        }

        let completion_outcome = if let Ok(c) = rx.await {
            c
        } else {
            dbg_panic!("Send half of activation complete response channel went missing");
            self.request_eviction(
                run_id,
                "Send half of activation complete response channel went missing",
                EvictionReason::Fatal,
            );
            return Ok(());
        };

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
                            sdk_metadata,
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
                        sdk_metadata,
                        metering_metadata: MeteringMetadata {
                            nonfirst_local_activity_execution_attempts: self
                                .local_act_mgr
                                .get_nonfirst_attempt_count(&run_id)
                                as u32,
                        },
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

        let maybe_pwft = if let Some(wft) = wft_from_complete {
            match HistoryPaginator::from_poll(wft, self.client.clone()).await {
                Ok((paginator, pwft)) => Some((pwft, paginator)),
                Err(e) => {
                    self.request_eviction(
                        &run_id,
                        format!("Failed to paginate workflow task from completion: {e:?}"),
                        EvictionReason::Fatal,
                    );
                    None
                }
            }
        } else {
            None
        };

        if let Some(h) = post_activate_hook {
            h(PostActivateHookData {
                run_id: &run_id,
                most_recent_event: completion_outcome.most_recently_processed_event,
                replaying: completion_outcome.replaying,
            });
        }

        self.post_activation(PostActivationMsg {
            run_id,
            wft_report_status,
            wft_from_complete: maybe_pwft,
            is_autocomplete,
        });

        Ok(())
    }

    /// Tell workflow that a local activity has finished with the provided result
    pub(super) fn notify_of_local_result(
        &self,
        run_id: impl Into<String>,
        resolved: LocalResolution,
    ) {
        self.send_local(LocalResolutionMsg {
            run_id: run_id.into(),
            res: resolved,
        });
    }

    /// Request eviction of a workflow
    pub(super) fn request_eviction(
        &self,
        run_id: impl Into<String>,
        message: impl Into<String>,
        reason: EvictionReason,
    ) {
        self.send_local(RequestEvictMsg {
            run_id: run_id.into(),
            message: message.into(),
            reason,
            auto_reply_fail_tt: None,
        });
    }

    /// Query the state of workflow management. Can return `None` if workflow state is shut down.
    pub(super) fn get_state_info(&self) -> impl Future<Output = Option<WorkflowStateInfo>> {
        let (tx, rx) = oneshot::channel();
        self.send_local(GetStateInfoMsg { response_tx: tx });
        async move { rx.await.ok() }
    }

    pub(super) fn available_wft_permits(&self) -> usize {
        self.wft_semaphore.available_permits()
    }
    #[cfg(test)]
    pub(super) fn unused_wft_permits(&self) -> usize {
        self.wft_semaphore.unused_permits()
    }

    pub(super) async fn shutdown(&self) -> Result<(), anyhow::Error> {
        if let Some(jh) = self.processing_task.take_once() {
            // This serves to drive the stream if it is still alive and wouldn't otherwise receive
            // another message. It allows it to shut itself down.
            let (waker, stop_waker) = abortable(async {
                let mut interval = tokio::time::interval(Duration::from_millis(10));
                loop {
                    interval.tick().await;
                    let _ = self.get_state_info().await;
                }
            });
            let (_, jh_res) = tokio::join!(
                waker,
                spawn_blocking(move || {
                    let r = jh.join();
                    stop_waker.abort();
                    r
                })
            );
            jh_res?.map_err(|e| anyhow!("Error joining workflow processing thread: {e:?}"))?;
        }
        Ok(())
    }

    pub(super) fn ever_polled(&self) -> bool {
        self.ever_polled.load(atomic::Ordering::Acquire)
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
        reserved_act_permits: Vec<TrackedOwnedMeteredSemPermit>,
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
                .map(|(permit, resp)| TrackedPermittedTqResp { permit, resp });
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
    ) -> Vec<TrackedOwnedMeteredSemPermit> {
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

/// Returned when a cache miss happens and we need to fetch history from the beginning to
/// replay a run
#[derive(Debug, derive_more::Display)]
#[display(
    fmt = "CacheMissFetchReq(run_id: {})",
    "original_wft.work.execution.run_id"
)]
#[must_use]
struct CacheMissFetchReq {
    original_wft: PermittedWFT,
}
/// Bubbled up from inside workflow state if we're trying to apply the next workflow task but it
/// isn't in memory
#[derive(Debug)]
#[must_use]
struct NextPageReq {
    paginator: HistoryPaginator,
    span: Span,
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
    #[display(fmt = "Autocomplete(run_id={run_id})")]
    Autocomplete {
        run_id: String,
    },
    #[display(fmt = "AutoFail(run_id={run_id})")]
    AutoFail {
        run_id: String,
        machines_err: WFMachinesError,
    },
}
impl ActivationOrAuto {
    pub fn run_id(&self) -> &str {
        match self {
            ActivationOrAuto::LangActivation(act) => &act.run_id,
            ActivationOrAuto::Autocomplete { run_id, .. } => run_id,
            ActivationOrAuto::ReadyForQueries(act) => &act.run_id,
            ActivationOrAuto::AutoFail { run_id, .. } => run_id,
        }
    }
}

/// A processed WFT which has been validated and had a history update extracted from it
#[derive(derive_more::DebugCustom)]
#[cfg_attr(
    feature = "save_wf_inputs",
    derive(serde::Serialize, serde::Deserialize)
)]
#[debug(fmt = "PermittedWft({work:?})")]
pub(crate) struct PermittedWFT {
    work: PreparedWFT,
    #[cfg_attr(
        feature = "save_wf_inputs",
        serde(skip, default = "UsedMeteredSemPermit::fake_deserialized")
    )]
    permit: UsedMeteredSemPermit,
    #[cfg_attr(
        feature = "save_wf_inputs",
        serde(skip, default = "HistoryPaginator::fake_deserialized")
    )]
    paginator: HistoryPaginator,
}
#[derive(Debug)]
#[cfg_attr(
    feature = "save_wf_inputs",
    derive(serde::Serialize, serde::Deserialize)
)]
struct PreparedWFT {
    task_token: TaskToken,
    attempt: u32,
    execution: WorkflowExecution,
    workflow_type: String,
    legacy_query: Option<WorkflowQuery>,
    query_requests: Vec<QueryWorkflow>,
    update: HistoryUpdate,
}
impl PreparedWFT {
    /// Returns true if the contained history update is incremental (IE: expects to hit a cached
    /// workflow)
    pub fn is_incremental(&self) -> bool {
        let start_event_id = self.update.first_event_id();
        let poll_resp_is_incremental = start_event_id.map(|eid| eid > 1).unwrap_or_default();
        poll_resp_is_incremental || start_event_id.is_none()
    }
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
    pub permit: UsedMeteredSemPermit,
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
        sdk_metadata: WorkflowTaskCompletedMetadata,
    },
    /// We should respond to a legacy query request
    RespondLegacyQuery { result: Box<QueryResult> },
}

#[derive(Debug)]
enum EvictionRequestResult {
    EvictionRequested(Option<u32>, RunUpdateAct),
    NotFound,
    EvictionAlreadyRequested(Option<u32>),
}
impl EvictionRequestResult {
    fn into_run_update_resp(self) -> RunUpdateAct {
        match self {
            EvictionRequestResult::EvictionRequested(_, resp) => resp,
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
#[cfg_attr(
    feature = "save_wf_inputs",
    derive(serde::Serialize, serde::Deserialize)
)]
struct WFActCompleteMsg {
    completion: ValidatedCompletion,
    #[cfg_attr(feature = "save_wf_inputs", serde(skip))]
    response_tx: Option<oneshot::Sender<ActivationCompleteResult>>,
}
#[derive(Debug)]
#[cfg_attr(
    feature = "save_wf_inputs",
    derive(serde::Serialize, serde::Deserialize)
)]
struct LocalResolutionMsg {
    run_id: String,
    res: LocalResolution,
}
#[derive(Debug)]
#[cfg_attr(
    feature = "save_wf_inputs",
    derive(serde::Serialize, serde::Deserialize)
)]
struct PostActivationMsg {
    run_id: String,
    wft_report_status: WFTReportStatus,
    wft_from_complete: Option<(PreparedWFT, HistoryPaginator)>,
    is_autocomplete: bool,
}
#[derive(Debug, Clone)]
#[cfg_attr(
    feature = "save_wf_inputs",
    derive(serde::Serialize, serde::Deserialize)
)]
struct RequestEvictMsg {
    run_id: String,
    message: String,
    reason: EvictionReason,
    /// If set, we requested eviction because something went wrong processing a brand new poll task,
    /// which means we won't have stored the WFT and we need to track the task token separately so
    /// we can reply with a failure to server after the evict goes through.
    auto_reply_fail_tt: Option<TaskToken>,
}
#[derive(Debug)]
pub(crate) struct HeartbeatTimeoutMsg {
    pub(crate) run_id: String,
    pub(crate) span: Span,
}
#[derive(Debug)]
struct GetStateInfoMsg {
    response_tx: oneshot::Sender<WorkflowStateInfo>,
}

/// Each activation completion produces one of these
#[derive(Debug)]
struct ActivationCompleteResult {
    most_recently_processed_event: usize,
    replaying: bool,
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
#[derive(Debug, Copy, Clone)]
#[cfg_attr(
    feature = "save_wf_inputs",
    derive(serde::Serialize, serde::Deserialize)
)]
enum WFTReportStatus {
    Reported,
    /// The WFT completion was not reported when finishing the activation, because there's still
    /// work to be done. EX: Running LAs.
    NotReported,
    /// We didn't report, but we want to clear the outstanding workflow task anyway. See
    /// [ActivationCompleteOutcome::WFTFailedDontReport]
    DropWft,
}

fn validate_completion(
    completion: WorkflowActivationCompletion,
) -> Result<ValidatedCompletion, CompleteWfError> {
    match completion.status {
        Some(workflow_activation_completion::Status::Successful(success)) => {
            // Convert to wf commands
            let mut commands = success
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
                         lang SDK. Commands: {commands:?}"
                    ),
                    run_id: completion.run_id,
                });
            }

            // Any non-query-response commands after a terminal command should be ignored
            if let Some(term_cmd_pos) = commands.iter().position(|c| c.is_terminal()) {
                // Query responses are just fine, so keep them.
                let queries = commands
                    .split_off(term_cmd_pos + 1)
                    .into_iter()
                    .filter(|c| matches!(c, WFCommand::QueryResponse(_)));
                commands.extend(queries);
            }

            Ok(ValidatedCompletion::Success {
                run_id: completion.run_id,
                commands,
                used_flags: success.used_internal_flags,
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
#[cfg_attr(
    feature = "save_wf_inputs",
    derive(serde::Serialize, serde::Deserialize)
)]
#[allow(clippy::large_enum_variant)]
enum ValidatedCompletion {
    Success {
        run_id: String,
        commands: Vec<WFCommand>,
        used_flags: Vec<u32>,
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
pub struct OutgoingServerCommands {
    pub commands: Vec<ProtoCommand>,
    pub replaying: bool,
}

#[derive(Debug)]
#[cfg_attr(
    feature = "save_wf_inputs",
    derive(serde::Serialize, serde::Deserialize)
)]
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
#[cfg_attr(
    feature = "save_wf_inputs",
    derive(serde::Serialize, serde::Deserialize)
)]
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

impl WFCommand {
    /// Returns true if the command is one which ends the workflow:
    /// * Completed
    /// * Failed
    /// * Cancelled
    /// * Continue-as-new
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            WFCommand::CompleteWorkflow(_)
                | WFCommand::FailWorkflow(_)
                | WFCommand::CancelWorkflow(_)
                | WFCommand::ContinueAsNew(_)
        )
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
    memo: Option<Memo>,
    search_attrs: Option<SearchAttributes>,
    retry_policy: Option<RetryPolicy>,
}

/// Wraps outgoing activation job protos with some internal details core might care about
#[derive(Debug, derive_more::Display)]
#[display(fmt = "{variant}")]
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

/// Errors thrown inside of workflow machines
#[derive(thiserror::Error, Debug)]
pub(crate) enum WFMachinesError {
    #[error("Nondeterminism error: {0}")]
    Nondeterminism(String),
    #[error("Fatal error in workflow machines: {0}")]
    Fatal(String),
}

impl WFMachinesError {
    pub fn evict_reason(&self) -> EvictionReason {
        match self {
            WFMachinesError::Nondeterminism(_) => EvictionReason::Nondeterminism,
            WFMachinesError::Fatal(_) => EvictionReason::Fatal,
        }
    }
}

impl From<TimestampError> for WFMachinesError {
    fn from(_: TimestampError) -> Self {
        Self::Fatal("Could not decode timestamp".to_string())
    }
}

impl From<anyhow::Error> for WFMachinesError {
    fn from(value: anyhow::Error) -> Self {
        WFMachinesError::Fatal(value.to_string())
    }
}

fn auto_fail_to_complete_status(err: WFMachinesError) -> workflow_activation_completion::Status {
    workflow_activation_completion::Status::Failed(Failure {
        failure: Some(
            temporal_sdk_core_protos::temporal::api::failure::v1::Failure {
                message: "Error while processing workflow task".to_string(),
                source: err.to_string(),
                stack_trace: "".to_string(),
                encoded_attributes: None,
                cause: None,
                failure_info: None,
            },
        ),
        force_cause: WorkflowTaskFailedCause::from(err.evict_reason()) as i32,
    })
}

pub(crate) trait LocalActivityRequestSink: Send + Sync + 'static {
    fn sink_reqs(&self, reqs: Vec<LocalActRequest>) -> Vec<LocalActivityResolution>;
}

#[derive(derive_more::Constructor)]
pub(super) struct LAReqSink {
    lam: Arc<LocalActivityManager>,
    /// If we're recording WF inputs, we also need to store immediate resolutions so they're
    /// available on replay.
    #[allow(dead_code)] // sometimes appears unused due to feature flagging
    recorder: Option<UnboundedSender<Vec<u8>>>,
}

impl LocalActivityRequestSink for LAReqSink {
    fn sink_reqs(&self, reqs: Vec<LocalActRequest>) -> Vec<LocalActivityResolution> {
        if reqs.is_empty() {
            return vec![];
        }

        #[allow(clippy::let_and_return)] // When feature is off clippy doesn't like this
        let res = self.lam.enqueue(reqs);

        // We always save when there are any reqs, even if the response might be empty, so that
        // calls/responses are 1:1
        #[cfg(feature = "save_wf_inputs")]
        self.write_req(&res);

        res
    }
}

/// Sorts jobs in an activation to be in the order lang expects:
/// `patches -> signals -> other -> queries`
fn sort_act_jobs(wfa: &mut WorkflowActivation) {
    wfa.jobs.sort_by(|j1, j2| {
        // Unwrapping is fine here since we'll never issue empty variants
        let j1v = j1.variant.as_ref().unwrap();
        let j2v = j2.variant.as_ref().unwrap();
        if discriminant(j1v) == discriminant(j2v) {
            return Ordering::Equal;
        }
        fn variant_ordinal(v: &workflow_activation_job::Variant) -> u8 {
            match v {
                workflow_activation_job::Variant::NotifyHasPatch(_) => 1,
                workflow_activation_job::Variant::SignalWorkflow(_) => 2,
                workflow_activation_job::Variant::QueryWorkflow(_) => 4,
                _ => 3,
            }
        }
        variant_ordinal(j1v).cmp(&variant_ordinal(j2v))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use itertools::Itertools;

    #[test]
    fn jobs_sort() {
        let mut act = WorkflowActivation {
            jobs: vec![
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::SignalWorkflow(
                        Default::default(),
                    )),
                },
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::NotifyHasPatch(
                        Default::default(),
                    )),
                },
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::QueryWorkflow(
                        Default::default(),
                    )),
                },
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::FireTimer(
                        Default::default(),
                    )),
                },
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(
                        Default::default(),
                    )),
                },
            ],
            ..Default::default()
        };
        sort_act_jobs(&mut act);
        let variants = act
            .jobs
            .into_iter()
            .map(|j| j.variant.unwrap())
            .collect_vec();
        assert_matches!(
            variants.as_slice(),
            &[
                workflow_activation_job::Variant::NotifyHasPatch(_),
                workflow_activation_job::Variant::SignalWorkflow(_),
                workflow_activation_job::Variant::FireTimer(_),
                workflow_activation_job::Variant::ResolveActivity(_),
                workflow_activation_job::Variant::QueryWorkflow(_)
            ]
        )
    }
}
