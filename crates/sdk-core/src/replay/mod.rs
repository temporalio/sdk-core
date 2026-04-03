//! This module implements support for creating special core instances and workers which can be used
//! to replay canned histories. It should be used by Lang SDKs to provide replay capabilities to
//! users during testing.

#[cfg(any(feature = "test-utilities", test))]
use crate::worker::client::mocks::{MockManualWorkerClient, mock_manual_worker_client};
use crate::{
    Worker, WorkerConfig, WorkflowTaskCompletion,
    worker::{
        PollerBehavior, PostActivateHookData,
        client::{
            LegacyQueryResult, PollActivityOptions, PollOptions, PollWorkflowOptions, WorkerClient,
            mocks::{DEFAULT_TEST_CAPABILITIES, DEFAULT_WORKERS_REGISTRY},
        },
    },
};
#[cfg(any(feature = "test-utilities", test))]
use futures_util::FutureExt;
use futures_util::{Stream, StreamExt};
use parking_lot::Mutex;
use std::{
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
    time::SystemTime,
};
use temporalio_client::{Connection, worker::ClientWorkerSet};
pub use temporalio_common::protos::{
    DEFAULT_WORKFLOW_TYPE, HistoryInfo, TestHistoryBuilder, default_wes_attribs,
};
use temporalio_common::{
    protos::{
        TaskToken,
        coresdk::workflow_activation::remove_from_cache::EvictionReason,
        temporal::api::{
            common::v1::{Payloads, WorkflowExecution},
            failure::v1::Failure,
            history::v1::History,
            nexus::v1::NexusTaskFailure,
            worker::v1::WorkerHeartbeat,
            workflowservice::v1::{
                DescribeNamespaceResponse, GetWorkflowExecutionHistoryResponse,
                PollActivityTaskQueueResponse, PollNexusTaskQueueResponse,
                PollWorkflowTaskQueueResponse, RecordActivityTaskHeartbeatResponse,
                RecordWorkerHeartbeatResponse, RespondActivityTaskCanceledResponse,
                RespondActivityTaskCompletedResponse, RespondActivityTaskFailedResponse,
                RespondNexusTaskCompletedResponse, RespondNexusTaskFailedResponse,
                RespondQueryTaskCompletedResponse, RespondWorkflowTaskCompletedResponse,
                RespondWorkflowTaskFailedResponse, ShutdownWorkerResponse,
                get_system_info_response::Capabilities,
            },
        },
    },
    worker::WorkerTaskTypes,
};
use tokio::sync::{Mutex as TokioMutex, mpsc, mpsc::UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Contains inputs to a replay worker
pub struct ReplayWorkerInput<I> {
    /// The worker's configuration. Some portions will be overridden to required values once the
    /// worker is instantiated.
    pub config: WorkerConfig,
    history_stream: I,
    /// If specified use this as the basis for the internal mocked client
    #[cfg(any(feature = "test-utilities", test))]
    pub(crate) client_override: Option<MockManualWorkerClient>,
}

impl<I> ReplayWorkerInput<I>
where
    I: Stream<Item = HistoryForReplay> + Send + 'static,
{
    /// Build a replay worker
    pub fn new(config: WorkerConfig, history_stream: I) -> Self {
        Self {
            config,
            history_stream,
            #[cfg(any(feature = "test-utilities", test))]
            client_override: None,
        }
    }

    pub(crate) fn into_core_worker(mut self) -> Result<Worker, anyhow::Error> {
        self.config.max_cached_workflows = 1;
        self.config.workflow_task_poller_behavior = PollerBehavior::SimpleMaximum(1);
        self.config.task_types = WorkerTaskTypes::workflow_only();
        self.config.skip_client_worker_set_check = true;
        let historator = Historator::new(self.history_stream);
        let post_activate = historator.get_post_activate_hook();
        let shutdown_tok = historator.get_shutdown_setter();
        let hist_allow_tx = historator.replay_done_tx.clone();
        let historator = Arc::new(TokioMutex::new(historator));

        #[cfg(any(feature = "test-utilities", test))]
        let client: Arc<dyn WorkerClient> = {
            // Create a mock client which can be used by a replay worker to serve up canned
            // histories. It will return the entire history in one workflow task. If a workflow
            // task failure is sent to the mock, it will send the complete response again.
            //
            // Once it runs out of histories to return, it will serve up default responses after a
            // 10s delay.
            let mut client = if let Some(c) = self.client_override {
                c
            } else {
                mock_manual_worker_client()
            };

            // TODO: Should use `new_with_pollers` and avoid re-doing mocking stuff
            client.expect_poll_workflow_task().returning(move |_, _| {
                let historator = historator.clone();
                async move {
                    let mut hlock = historator.lock().await;
                    // Always wait for permission before dispatching the next task
                    let _ = hlock.allow_stream.next().await;

                    if let Some(history) = hlock.next().await {
                        let hist_info = HistoryInfo::new_from_history(&history.hist, None).unwrap();
                        let mut resp = hist_info.as_poll_wft_response();
                        resp.workflow_execution = Some(WorkflowExecution {
                            workflow_id: history.workflow_id,
                            run_id: hist_info.orig_run_id().to_string(),
                        });
                        Ok(resp)
                    } else {
                        if let Some(wc) = hlock.worker_closer.get() {
                            wc.cancel();
                        }
                        Ok(Default::default())
                    }
                }
                .boxed()
            });

            client.expect_complete_workflow_task().returning(move |_a| {
                async move { Ok(RespondWorkflowTaskCompletedResponse::default()) }.boxed()
            });
            client
                .expect_fail_workflow_task()
                .returning(move |_, _, _| {
                    hist_allow_tx.send("Failed".to_string()).unwrap();
                    async move { Ok(RespondWorkflowTaskFailedResponse::default()) }.boxed()
                });

            Arc::new(client)
        };

        #[cfg(not(any(feature = "test-utilities", test)))]
        let client: Arc<dyn WorkerClient> =
            Arc::new(ReplayWorkerClient::new(historator.clone(), hist_allow_tx));

        let mut worker = Worker::new(self.config, None, client, None, None)?;
        worker.set_post_activate_hook(post_activate);
        shutdown_tok(worker.shutdown_token());
        Ok(worker)
    }
}

#[cfg(not(any(feature = "test-utilities", test)))]
struct ReplayWorkerClient {
    historator: Arc<TokioMutex<Historator>>,
    replay_done_tx: UnboundedSender<String>,
    worker_grouping_key: Uuid,
    worker_instance_key: Uuid,
}

#[cfg(not(any(feature = "test-utilities", test)))]
impl ReplayWorkerClient {
    fn new(
        historator: Arc<TokioMutex<Historator>>,
        replay_done_tx: UnboundedSender<String>,
    ) -> Self {
        Self {
            historator,
            replay_done_tx,
            worker_grouping_key: Uuid::new_v4(),
            worker_instance_key: Uuid::new_v4(),
        }
    }

    async fn next_workflow_task(
        &self,
    ) -> std::result::Result<PollWorkflowTaskQueueResponse, tonic::Status> {
        let mut hlock = self.historator.lock().await;
        let _ = hlock.allow_stream.next().await;

        if let Some(history) = hlock.next().await {
            let hist_info = HistoryInfo::new_from_history(&history.hist, None).unwrap();
            let mut resp = hist_info.as_poll_wft_response();
            resp.workflow_execution = Some(WorkflowExecution {
                workflow_id: history.workflow_id,
                run_id: hist_info.orig_run_id().to_string(),
            });
            Ok(resp)
        } else {
            if let Some(wc) = hlock.worker_closer.get() {
                wc.cancel();
            }
            Ok(Default::default())
        }
    }

    fn replay_only_status(method: &str) -> tonic::Status {
        tonic::Status::unimplemented(format!("replay worker does not implement {method}"))
    }
}

#[cfg(not(any(feature = "test-utilities", test)))]
#[async_trait::async_trait]
impl WorkerClient for ReplayWorkerClient {
    async fn poll_workflow_task(
        &self,
        _poll_options: PollOptions,
        _wf_options: PollWorkflowOptions,
    ) -> std::result::Result<PollWorkflowTaskQueueResponse, tonic::Status> {
        self.next_workflow_task().await
    }

    async fn poll_activity_task(
        &self,
        _poll_options: PollOptions,
        _act_options: PollActivityOptions,
    ) -> std::result::Result<PollActivityTaskQueueResponse, tonic::Status> {
        Err(Self::replay_only_status("activity polling"))
    }

    async fn poll_nexus_task(
        &self,
        _poll_options: PollOptions,
        _send_heartbeat: bool,
    ) -> std::result::Result<PollNexusTaskQueueResponse, tonic::Status> {
        Err(Self::replay_only_status("nexus polling"))
    }

    async fn complete_workflow_task(
        &self,
        _request: WorkflowTaskCompletion,
    ) -> std::result::Result<RespondWorkflowTaskCompletedResponse, tonic::Status> {
        Ok(Default::default())
    }

    async fn complete_activity_task(
        &self,
        _task_token: TaskToken,
        _result: Option<Payloads>,
    ) -> std::result::Result<RespondActivityTaskCompletedResponse, tonic::Status> {
        Err(Self::replay_only_status("activity completion"))
    }

    async fn complete_nexus_task(
        &self,
        _task_token: TaskToken,
        _response: temporalio_common::protos::temporal::api::nexus::v1::Response,
    ) -> std::result::Result<RespondNexusTaskCompletedResponse, tonic::Status> {
        Err(Self::replay_only_status("nexus completion"))
    }

    async fn record_activity_heartbeat(
        &self,
        _task_token: TaskToken,
        _details: Option<Payloads>,
    ) -> std::result::Result<RecordActivityTaskHeartbeatResponse, tonic::Status> {
        Err(Self::replay_only_status("activity heartbeat"))
    }

    async fn cancel_activity_task(
        &self,
        _task_token: TaskToken,
        _details: Option<Payloads>,
    ) -> std::result::Result<RespondActivityTaskCanceledResponse, tonic::Status> {
        Err(Self::replay_only_status("activity cancel"))
    }

    async fn fail_activity_task(
        &self,
        _task_token: TaskToken,
        _failure: Option<Failure>,
    ) -> std::result::Result<RespondActivityTaskFailedResponse, tonic::Status> {
        Err(Self::replay_only_status("activity failure"))
    }

    async fn fail_workflow_task(
        &self,
        _task_token: TaskToken,
        _cause: temporalio_common::protos::temporal::api::enums::v1::WorkflowTaskFailedCause,
        _failure: Option<Failure>,
    ) -> std::result::Result<RespondWorkflowTaskFailedResponse, tonic::Status> {
        self.replay_done_tx.send("Failed".to_string()).unwrap();
        Ok(Default::default())
    }

    async fn fail_nexus_task(
        &self,
        _task_token: TaskToken,
        _error: NexusTaskFailure,
    ) -> std::result::Result<RespondNexusTaskFailedResponse, tonic::Status> {
        Err(Self::replay_only_status("nexus failure"))
    }

    async fn get_workflow_execution_history(
        &self,
        _workflow_id: String,
        _run_id: Option<String>,
        _page_token: Vec<u8>,
    ) -> std::result::Result<GetWorkflowExecutionHistoryResponse, tonic::Status> {
        Err(Self::replay_only_status("history fetch"))
    }

    async fn respond_legacy_query(
        &self,
        _task_token: TaskToken,
        _query_result: LegacyQueryResult,
    ) -> std::result::Result<RespondQueryTaskCompletedResponse, tonic::Status> {
        Err(Self::replay_only_status("legacy query"))
    }

    async fn describe_namespace(
        &self,
    ) -> std::result::Result<DescribeNamespaceResponse, tonic::Status> {
        Ok(Default::default())
    }

    async fn shutdown_worker(
        &self,
        _sticky_task_queue: String,
        _task_queue: String,
        _task_queue_types: Vec<temporalio_common::protos::temporal::api::enums::v1::TaskQueueType>,
        _final_heartbeat: Option<WorkerHeartbeat>,
    ) -> std::result::Result<ShutdownWorkerResponse, tonic::Status> {
        Ok(Default::default())
    }

    async fn record_worker_heartbeat(
        &self,
        _namespace: String,
        _worker_heartbeat: Vec<WorkerHeartbeat>,
    ) -> std::result::Result<RecordWorkerHeartbeatResponse, tonic::Status> {
        Ok(Default::default())
    }

    fn replace_connection(&self, _new_client: Connection) {}

    fn capabilities(&self) -> Option<Capabilities> {
        Some(*DEFAULT_TEST_CAPABILITIES)
    }

    fn workers(&self) -> Arc<ClientWorkerSet> {
        DEFAULT_WORKERS_REGISTRY.clone()
    }

    fn is_mock(&self) -> bool {
        true
    }

    fn sdk_name_and_version(&self) -> (String, String) {
        ("replay-core".to_owned(), "0.0.0".to_owned())
    }

    fn identity(&self) -> String {
        "replay-identity".to_owned()
    }

    fn worker_grouping_key(&self) -> Uuid {
        self.worker_grouping_key
    }

    fn worker_instance_key(&self) -> Uuid {
        self.worker_instance_key
    }

    fn set_heartbeat_client_fields(&self, heartbeat: &mut WorkerHeartbeat) {
        heartbeat.sdk_name = "replay-core".to_owned();
        heartbeat.sdk_version = "0.0.0".to_owned();
        heartbeat.worker_identity = "replay-identity".to_owned();
        heartbeat.heartbeat_time = Some(SystemTime::now().into());
    }
}

/// A history which will be used during replay verification. Since histories do not include the
/// workflow id, it must be manually attached.
#[derive(Debug, Clone)]
pub struct HistoryForReplay {
    hist: History,
    workflow_id: String,
}
impl HistoryForReplay {
    /// Create a new history from replay from something that looks like a history and a workflow id.
    pub fn new(history: impl Into<History>, workflow_id: impl Into<String>) -> Self {
        Self {
            hist: history.into(),
            workflow_id: workflow_id.into(),
        }
    }
}
impl From<TestHistoryBuilder> for HistoryForReplay {
    fn from(thb: TestHistoryBuilder) -> Self {
        thb.get_full_history_info().unwrap().into()
    }
}
impl From<HistoryInfo> for HistoryForReplay {
    fn from(histinfo: HistoryInfo) -> Self {
        HistoryForReplay::new(histinfo, "fake".to_owned())
    }
}

/// Allows lang to feed histories into the replayer one at a time. Simply drop the feeder to signal
/// to the worker that you're done and it should initiate shutdown.
pub struct HistoryFeeder {
    tx: mpsc::Sender<HistoryForReplay>,
}
/// The stream half of a [HistoryFeeder]
pub struct HistoryFeederStream {
    rcvr: mpsc::Receiver<HistoryForReplay>,
}

impl HistoryFeeder {
    /// Make a new history feeder, which will store at most `buffer_size` histories before `feed`
    /// blocks.
    ///
    /// Returns a feeder which will be used to feed in histories, and a stream you can pass to
    /// one of the replay worker init functions.
    pub fn new(buffer_size: usize) -> (Self, HistoryFeederStream) {
        let (tx, rcvr) = mpsc::channel(buffer_size);
        (Self { tx }, HistoryFeederStream { rcvr })
    }
    /// Feed a new history into the replayer, blocking if there is not room to accept another
    /// history.
    pub async fn feed(&self, history: HistoryForReplay) -> anyhow::Result<()> {
        self.tx.send(history).await?;
        Ok(())
    }
}

impl Stream for HistoryFeederStream {
    type Item = HistoryForReplay;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rcvr.poll_recv(cx)
    }
}

pub(crate) struct Historator {
    iter: Pin<Box<dyn Stream<Item = HistoryForReplay> + Send>>,
    allow_stream: UnboundedReceiverStream<String>,
    worker_closer: Arc<OnceLock<CancellationToken>>,
    dat: Arc<Mutex<HistoratorDat>>,
    replay_done_tx: UnboundedSender<String>,
}
impl Historator {
    pub(crate) fn new(histories: impl Stream<Item = HistoryForReplay> + Send + 'static) -> Self {
        let dat = Arc::new(Mutex::new(HistoratorDat::default()));
        let (replay_done_tx, replay_done_rx) = mpsc::unbounded_channel();
        // Need to allow the first history item
        replay_done_tx.send("fake".to_string()).unwrap();
        Self {
            iter: Box::pin(histories.fuse()),
            allow_stream: UnboundedReceiverStream::new(replay_done_rx),
            worker_closer: Arc::new(OnceLock::new()),
            dat,
            replay_done_tx,
        }
    }

    /// Returns a callback that can be used as the post-activation hook for a worker to indicate
    /// we're ready to replay the next history, or whatever else.
    pub(crate) fn get_post_activate_hook(
        &self,
    ) -> impl Fn(&Worker, PostActivateHookData) + Send + Sync + use<> {
        let done_tx = self.replay_done_tx.clone();
        move |worker, data| {
            if !data.replaying {
                worker.request_wf_eviction(
                    data.run_id,
                    "Always evict workflows after replay",
                    EvictionReason::LangRequested,
                );
                done_tx.send(data.run_id.to_string()).unwrap();
            }
        }
    }

    pub(crate) fn get_shutdown_setter(&self) -> impl FnOnce(CancellationToken) + 'static {
        let wc = self.worker_closer.clone();
        move |ct| {
            wc.set(ct).expect("Shutdown token must only be set once");
        }
    }
}

impl Stream for Historator {
    type Item = HistoryForReplay;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.iter.poll_next_unpin(cx) {
            Poll::Ready(Some(history)) => {
                history
                    .hist
                    .extract_run_id_from_start()
                    .expect(
                        "Histories provided for replay must contain run ids in their workflow \
                                 execution started events",
                    )
                    .to_string();
                Poll::Ready(Some(history))
            }
            Poll::Ready(None) => {
                self.dat.lock().all_dispatched = true;
                Poll::Ready(None)
            }
            o => o,
        }
    }
}

#[derive(Default)]
struct HistoratorDat {
    all_dispatched: bool,
}
