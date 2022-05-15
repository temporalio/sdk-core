pub(crate) mod workflow_tasks;

mod bridge;
mod driven_workflow;
mod history_update;
mod machines;
mod managed_run;
mod run_cache;
pub(crate) mod wft_poller;
mod workflow_stream;

pub(crate) use bridge::WorkflowBridge;
pub(crate) use driven_workflow::{DrivenWorkflow, WorkflowFetcher};
pub(crate) use history_update::{HistoryPaginator, HistoryUpdate};
pub(crate) use machines::WFMachinesError;
#[cfg(test)]
pub(crate) use managed_run::ManagedWFFunc;

use crate::{
    protosext::{legacy_query_failure, ValidPollWFTQResponse, WorkflowActivationExt},
    telemetry::VecDisplayer,
    worker::{
        workflow::{
            managed_run::{ManagedRun, WorkflowManager},
            wft_poller::validate_wft,
            workflow_stream::{LocalInputs, WFStream, WFStreamBasics},
            workflow_tasks::{
                ActivationAction, FailedActivationOutcome, OutstandingActivation, OutstandingTask,
                ServerCommandsWithWorkflowInfo,
            },
        },
        LocalActRequest, LocalActivityResolution,
    },
    MetricsContext, WorkerClientBag,
};
use futures::{stream::BoxStream, Stream, StreamExt};
use std::{
    fmt::Debug,
    future::Future,
    ops::DerefMut,
    result,
    sync::Arc,
    time::{Duration, Instant},
};
use temporal_client::WorkflowTaskCompletion;
use temporal_sdk_core_api::errors::{CompleteWfError, PollWfError};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{remove_from_cache::EvictionReason, WorkflowActivation},
        workflow_commands::*,
        workflow_completion,
        workflow_completion::{Failure, WorkflowActivationCompletion},
    },
    temporal::api::{
        command::v1::Command as ProtoCommand, taskqueue::v1::StickyExecutionAttributes,
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
use tracing::Instrument;

pub(crate) const LEGACY_QUERY_ID: &str = "legacy_query";

type Result<T, E = WFMachinesError> = result::Result<T, E>;
type BoxedActivationStream = BoxStream<'static, Result<ActivationOrAuto, PollWfError>>;

/// Centralizes all state related to workflows and workflow tasks
pub(crate) struct Workflows {
    new_wft_tx: UnboundedSender<ValidPollWFTQResponse>,
    local_tx: UnboundedSender<LocalInputs>,
    processing_task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    activation_stream: tokio::sync::Mutex<(
        BoxedActivationStream,
        // Used to indicate polling may begin
        Option<oneshot::Sender<()>>,
    )>,
    client: Arc<WorkerClientBag>,
    /// Will be populated when this worker is using a cache and should complete WFTs with a sticky
    /// queue.
    sticky_attrs: Option<StickyExecutionAttributes>,
}

impl Workflows {
    pub fn new(
        max_cached_workflows: usize,
        max_outstanding_wfts: usize,
        sticky_attrs: Option<StickyExecutionAttributes>,
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
        let shutdown_tok = shutdown_token.clone();
        let basics = WFStreamBasics {
            max_cached_workflows,
            max_outstanding_wfts,
            shutdown_token,
            metrics,
        };
        let mut stream = WFStream::build(
            basics,
            wft_stream,
            new_wft_rx,
            UnboundedReceiverStream::new(local_rx),
            client.clone(),
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
            new_wft_tx,
            local_tx,
            processing_task: tokio::sync::Mutex::new(Some(processing_task)),
            activation_stream: tokio::sync::Mutex::new((
                UnboundedReceiverStream::new(activation_rx).boxed(),
                Some(start_polling_tx),
            )),
            client,
            sticky_attrs,
        }
    }

    pub async fn next_workflow_activation(&self) -> Result<WorkflowActivation, PollWfError> {
        loop {
            let r = {
                let mut lock = self.activation_stream.lock().await;
                let (ref mut stream, ref mut beginner) = lock.deref_mut();
                if let Some(beginner) = beginner.take() {
                    let _ = beginner.send(());
                }
                stream.next().await.unwrap_or(Err(PollWfError::ShutDown))?
            };
            match r {
                ActivationOrAuto::LangActivation(act) | ActivationOrAuto::ReadyForQueries(act) => {
                    debug!(activation=%act, "Sending activation to lang");
                    break Ok(act);
                }
                ActivationOrAuto::Autocomplete { run_id } => {
                    self.activation_completed(WorkflowActivationCompletion {
                        run_id,
                        status: Some(workflow_completion::Success::from_variants(vec![]).into()),
                    })
                    .await?;
                }
            }
        }
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
    ///
    /// Returns the most-recently-processed event number for the run
    pub async fn activation_completed(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<usize, CompleteWfError> {
        let run_id = completion.run_id.clone();
        let (tx, rx) = oneshot::channel();
        self.send_local(WFActCompleteMsg {
            completion,
            response_tx: tx,
        });
        let completion_outcome = rx
            .await
            .expect("Send half of activation complete response not dropped");
        let reported_wft_to_server = match completion_outcome.outcome {
            ActivationCompleteOutcome::ReportWFTSuccess(report) => match report {
                ServerCommandsWithWorkflowInfo {
                    task_token,
                    action:
                        ActivationAction::WftComplete {
                            commands,
                            query_responses,
                            force_new_wft,
                        },
                } => {
                    debug!(commands=%commands.display(), query_responses=%query_responses.display(),
                           "Sending responses to server");
                    let mut completion = WorkflowTaskCompletion {
                        task_token,
                        commands,
                        query_responses,
                        sticky_attributes: None,
                        return_new_workflow_task: true,
                        force_create_new_workflow_task: dbg!(force_new_wft),
                    };
                    let sticky_attrs = self.sticky_attrs.clone();
                    // Do not return new WFT if we would not cache, because returned new WFTs are always
                    // partial.
                    if sticky_attrs.is_none() {
                        completion.return_new_workflow_task = false;
                    }
                    completion.sticky_attributes = sticky_attrs;

                    self.handle_wft_reporting_errs(&run_id, || async {
                        let maybe_wft = self
                            .client
                            .complete_workflow_task(completion)
                            .instrument(span!(tracing::Level::DEBUG, "Complete WFT call"))
                            .await?;
                        if let Some(wft) = maybe_wft.workflow_task {
                            self.new_wft(validate_wft(wft)?);
                        }
                        Ok(())
                    })
                    .await?;
                    true
                }
                ServerCommandsWithWorkflowInfo {
                    task_token,
                    action: ActivationAction::RespondLegacyQuery { result },
                } => {
                    self.client.respond_legacy_query(task_token, result).await?;
                    true
                }
            },
            ActivationCompleteOutcome::ReportWFTFail(outcome) => match outcome {
                FailedActivationOutcome::Report(tt, cause, failure) => {
                    warn!(run_id=%run_id, failure=?failure, "Failing workflow task");
                    self.handle_wft_reporting_errs(&run_id, || async {
                        self.client
                            .fail_workflow_task(tt, cause, failure.failure.map(Into::into))
                            .await
                    })
                    .await?;
                    true
                }
                FailedActivationOutcome::ReportLegacyQueryFailure(task_token, failure) => {
                    warn!(run_id=%run_id, failure=?failure, "Failing legacy query request");
                    self.client
                        .respond_legacy_query(task_token, legacy_query_failure(failure))
                        .await?;
                    true
                }
                FailedActivationOutcome::NoReport => false,
            },
            ActivationCompleteOutcome::DoNothing => false,
        };

        self.post_activation(PostActivationMsg {
            run_id,
            reported_wft_to_server,
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

    /// Handle server errors from either completing or failing a workflow task. Returns any errors
    /// that can't be automatically handled.
    async fn handle_wft_reporting_errs<T, Fut>(
        &self,
        run_id: &str,
        completer: impl FnOnce() -> Fut,
    ) -> Result<(), CompleteWfError>
    where
        Fut: Future<Output = Result<T, tonic::Status>>,
    {
        let mut should_evict = None;
        let res = match completer().await {
            Err(err) => {
                match err.code() {
                    // Silence unhandled command errors since the lang SDK cannot do anything about
                    // them besides poll again, which it will do anyway.
                    tonic::Code::InvalidArgument if err.message() == "UnhandledCommand" => {
                        debug!(error = %err, run_id, "Unhandled command response when completing");
                        should_evict = Some(EvictionReason::UnhandledCommand);
                        Ok(())
                    }
                    tonic::Code::NotFound => {
                        warn!(error = %err, run_id, "Task not found when completing");
                        should_evict = Some(EvictionReason::TaskNotFound);
                        Ok(())
                    }
                    _ => Err(err),
                }
            }
            _ => Ok(()),
        };
        if let Some(reason) = should_evict {
            self.request_eviction(run_id, "Error reporting WFT to server", reason);
        }
        res.map_err(Into::into)
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
enum ActivationOrAuto {
    LangActivation(WorkflowActivation),
    /// This type should only be filled with an empty activation which is ready to have queries
    /// inserted into the joblist
    ReadyForQueries(WorkflowActivation),
    Autocomplete {
        // TODO: Can I delete this?
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

#[derive(Debug)]
#[allow(dead_code)] // Not always used in non-test
pub(crate) struct WorkflowStateInfo {
    pub cached_workflows: usize,
    pub outstanding_wft: usize,
    pub available_wft_permits: usize,
}

#[derive(Debug)]
struct WFActCompleteMsg {
    completion: WorkflowActivationCompletion,
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
    reported_wft_to_server: bool,
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

/// Each activation completion produces one of these
#[derive(Debug)]
struct ActivationCompleteResult {
    most_recently_processed_event: usize,
    outcome: ActivationCompleteOutcome,
}

/// What needs to be done after calling [Workflows::activation_completed]
#[derive(Debug)]
enum ActivationCompleteOutcome {
    /// The WFT must be reported as successful to the server using the contained information.
    ReportWFTSuccess(ServerCommandsWithWorkflowInfo),
    /// The WFT must be reported as failed to the server using the contained information.
    ReportWFTFail(FailedActivationOutcome),
    /// There's nothing to do right now. EX: The workflow needs to keep replaying.
    DoNothing,
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
    /// If set, the WFT this run is currently/will be processing.
    wft: Option<OutstandingTask>,
    /// An outstanding activation to lang
    activation: Option<OutstandingActivation>,
    /// If set, it indicates there is a buffered poll response from the server that applies to this
    /// run. This can happen when lang takes too long to complete a task and the task times out, for
    /// example. Upon next completion, the buffered response will be removed and can be made ready
    /// to be returned from polling
    buffered_resp: Option<ValidPollWFTQResponse>,
    /// True if this machine has seen an event which ends the execution
    have_seen_terminal_event: bool,
    /// The most recently processed event id this machine has seen. 0 means it has seen nothing.
    most_recently_processed_event_number: usize,
    /// Is set true when the machines indicate that there is additional known work to be processed
    more_pending_work: bool,
    /// Is set if an eviction has been requested for this run
    trying_to_evict: Option<RequestEvictMsg>,
    /// Set to true if the last action we tried to take to this run has been processed (ie: the
    /// [RunUpdateResponse] for it has been seen.
    last_action_acked: bool,
    /// For sending work to the machines
    run_actions_tx: UnboundedSender<RunActions>,
    /// Handle to the task where the actual machines live
    handle: JoinHandle<()>,
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
            most_recently_processed_event_number: 0,
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
        self.send_run_action(RunActions::NewIncomingWFT(wft));
    }
    fn check_more_activations(&mut self) {
        // No point in checking for more activations if we have not acked the last update, or
        // if there's already an outstanding activation.
        if self.last_action_acked && self.activation.is_none() {
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
        self.send_run_action(RunActions::ActivationCompletion(c));
    }
    fn send_local_resolution(&mut self, r: LocalResolution) {
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
    resp_chan: Option<oneshot::Sender<ActivationCompleteResult>>,
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
    most_recently_processed_event_number: usize,
    in_response_to_wft: Option<RunUpdatedFromWft>,
}
#[derive(Debug)]
pub(crate) struct FailRunUpdateResponse {
    run_id: String,
    err: WFMachinesError,
    /// This is populated if the run update failed while processing a completion - and thus we
    /// must respond down it when handling the failure.
    completion_resp: Option<oneshot::Sender<ActivationCompleteResult>>,
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
