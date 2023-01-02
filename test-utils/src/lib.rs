//! This crate contains testing functionality that can be useful when building SDKs against Core,
//! or even when testing workflows written in SDKs that use Core.

#[macro_use]
extern crate tracing;

pub mod canned_histories;

use crate::stream::{Stream, TryStreamExt};
use futures::{future, stream, stream::FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use prost::Message;
use rand::{distributions::Standard, Rng};
use std::{
    convert::TryFrom, env, future::Future, net::SocketAddr, path::PathBuf, sync::Arc,
    time::Duration,
};
use temporal_client::{
    Client, RetryClient, WorkflowClientTrait, WorkflowExecutionInfo, WorkflowOptions,
};
use temporal_sdk::{
    interceptors::{FailOnNondeterminismInterceptor, WorkerInterceptor},
    IntoActivityFunc, Worker, WorkflowFunction,
};
use temporal_sdk_core::{
    ephemeral_server::{EphemeralExe, EphemeralExeVersion},
    init_replay_worker, init_worker,
    replay::HistoryForReplay,
    ClientOptions, ClientOptionsBuilder, CoreRuntime, WorkerConfig, WorkerConfigBuilder,
};
use temporal_sdk_core_api::{
    telemetry::{
        Logger, MetricsExporter, OtelCollectorOptions, TelemetryOptions, TelemetryOptionsBuilder,
        TraceExportConfig, TraceExporter,
    },
    Worker as CoreWorker,
};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_commands::{
            workflow_command, ActivityCancellationType, CompleteWorkflowExecution,
            ScheduleActivity, ScheduleLocalActivity, StartTimer,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{common::v1::Payload, history::v1::History},
};
use tokio::sync::OnceCell;
use url::Url;

pub const NAMESPACE: &str = "default";
pub const TEST_Q: &str = "q";
/// The env var used to specify where the integ tests should point
pub const INTEG_SERVER_TARGET_ENV_VAR: &str = "TEMPORAL_SERVICE_ADDRESS";
/// This env var is set (to any value) if temporalite is in use
pub const INTEG_TEMPORALITE_USED_ENV_VAR: &str = "INTEG_TEMPORALITE_ON";
/// This env var is set (to any value) if the test server is in use
pub const INTEG_TEST_SERVER_USED_ENV_VAR: &str = "INTEG_TEST_SERVER_ON";

/// If set, turn export traces and metrics to the OTel collector at the given URL
const OTEL_URL_ENV_VAR: &str = "TEMPORAL_INTEG_OTEL_URL";
/// If set, enable direct scraping of prom metrics on the specified port
const PROM_ENABLE_ENV_VAR: &str = "TEMPORAL_INTEG_PROM_PORT";
/// Create a worker instance which will use the provided test name to base the task queue and wf id
/// upon. Returns the instance and the task queue name (which is also the workflow id).
pub async fn init_core_and_create_wf(test_name: &str) -> CoreWfStarter {
    let mut starter = CoreWfStarter::new(test_name);
    let _ = starter.get_worker().await;
    starter.start_wf().await;
    starter
}

/// Create a worker replay instance preloaded with provided histories. Returns the worker impl.
pub fn init_core_replay_preloaded<I>(test_name: &str, histories: I) -> Arc<dyn CoreWorker>
where
    I: IntoIterator<Item = HistoryForReplay> + 'static,
    <I as IntoIterator>::IntoIter: Send,
{
    init_core_replay_stream(test_name, stream::iter(histories))
}
pub fn init_core_replay_stream<I>(test_name: &str, histories: I) -> Arc<dyn CoreWorker>
where
    I: Stream<Item = HistoryForReplay> + Send + 'static,
{
    init_integ_telem();
    let worker_cfg = WorkerConfigBuilder::default()
        .namespace(NAMESPACE)
        .task_queue(test_name)
        .worker_build_id("test_bin_id")
        .build()
        .expect("Configuration options construct properly");
    let worker =
        init_replay_worker(worker_cfg, histories).expect("Replay worker must init properly");
    Arc::new(worker)
}
pub fn replay_sdk_worker<I>(histories: I) -> Worker
where
    I: IntoIterator<Item = HistoryForReplay> + 'static,
    <I as IntoIterator>::IntoIter: Send,
{
    replay_sdk_worker_stream(stream::iter(histories))
}
pub fn replay_sdk_worker_stream<I>(histories: I) -> Worker
where
    I: Stream<Item = HistoryForReplay> + Send + 'static,
{
    let core = init_core_replay_stream("replay_worker_test", histories);
    let mut worker = Worker::new_from_core(core, "replay_q".to_string());
    worker.set_worker_interceptor(Box::new(FailOnNondeterminismInterceptor {}));
    worker
}

/// Load history from a file containing the protobuf serialization of it
pub async fn history_from_proto_binary(path_from_root: &str) -> Result<History, anyhow::Error> {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("..");
    path.push(path_from_root);
    let bytes = tokio::fs::read(path).await?;
    Ok(History::decode(&*bytes)?)
}

static INTEG_TESTS_RT: once_cell::sync::OnceCell<CoreRuntime> = once_cell::sync::OnceCell::new();
pub fn init_integ_telem() {
    INTEG_TESTS_RT.get_or_init(|| {
        let telemetry_options = get_integ_telem_options();
        let rt =
            CoreRuntime::new_assume_tokio(telemetry_options).expect("Core runtime inits cleanly");
        let _ = tracing::subscriber::set_global_default(rt.trace_subscriber());
        rt
    });
}

/// Implements a builder pattern to help integ tests initialize core and create workflows
pub struct CoreWfStarter {
    /// Used for both the task queue and workflow id
    task_queue_name: String,
    pub worker_config: WorkerConfig,
    wft_timeout: Option<Duration>,
    initted_worker: OnceCell<InitializedWorker>,
}
struct InitializedWorker {
    worker: Arc<dyn CoreWorker>,
    client: Arc<RetryClient<Client>>,
}

impl CoreWfStarter {
    pub fn new(test_name: &str) -> Self {
        let rand_bytes: Vec<u8> = rand::thread_rng().sample_iter(&Standard).take(6).collect();
        let task_q_salt = base64::encode(rand_bytes);
        let task_queue = format!("{}_{}", test_name, task_q_salt);
        Self::new_tq_name(&task_queue)
    }

    pub fn new_tq_name(task_queue: &str) -> Self {
        init_integ_telem();
        Self {
            task_queue_name: task_queue.to_owned(),
            worker_config: WorkerConfigBuilder::default()
                .namespace(NAMESPACE)
                .task_queue(task_queue)
                .worker_build_id("test_build_id")
                .max_cached_workflows(1000_usize)
                .build()
                .unwrap(),
            wft_timeout: None,
            initted_worker: OnceCell::new(),
        }
    }

    pub async fn worker(&mut self) -> TestWorker {
        let mut w = TestWorker::new(
            self.get_worker().await,
            self.worker_config.task_queue.clone(),
        );
        w.client = Some(self.get_client().await);

        w
    }

    pub async fn shutdown(&mut self) {
        self.get_worker().await.shutdown().await;
    }

    pub async fn get_worker(&mut self) -> Arc<dyn CoreWorker> {
        self.get_or_init().await.worker.clone()
    }

    pub async fn get_client(&mut self) -> Arc<RetryClient<Client>> {
        self.get_or_init().await.client.clone()
    }

    /// Start the workflow defined by the builder and return run id
    pub async fn start_wf(&self) -> String {
        self.start_wf_with_id(self.task_queue_name.clone(), WorkflowOptions::default())
            .await
    }

    pub async fn start_wf_with_id(&self, workflow_id: String, mut opts: WorkflowOptions) -> String {
        opts.task_timeout = opts.task_timeout.or(self.wft_timeout);
        self.initted_worker
            .get()
            .expect(
                "Worker must be initted before starting a workflow.\
                             Tests must call `get_worker` first.",
            )
            .client
            .start_workflow(
                vec![],
                self.worker_config.task_queue.clone(),
                workflow_id,
                self.task_queue_name.clone(),
                None,
                opts,
            )
            .await
            .unwrap()
            .run_id
    }

    /// Fetch the history for the indicated workflow and replay it using the provided worker.
    /// Can be used after completing workflows normally to ensure replay works as well.
    pub async fn fetch_history_and_replay(
        &mut self,
        wf_id: impl Into<String>,
        run_id: impl Into<String>,
        worker: &mut Worker,
    ) -> Result<(), anyhow::Error> {
        let wf_id = wf_id.into();
        // Fetch history and replay it
        let history = self
            .get_client()
            .await
            .get_workflow_execution_history(wf_id.clone(), Some(run_id.into()), vec![])
            .await?
            .history
            .expect("history field must be populated");
        let with_id = HistoryForReplay::new(history, wf_id);
        let replay_worker = init_core_replay_preloaded(worker.task_queue(), [with_id]);
        worker.with_new_core_worker(replay_worker);
        worker.set_worker_interceptor(Box::new(FailOnNondeterminismInterceptor {}));
        worker.run().await.unwrap();
        Ok(())
    }

    pub fn get_task_queue(&self) -> &str {
        &self.worker_config.task_queue
    }

    pub fn get_wf_id(&self) -> &str {
        &self.task_queue_name
    }

    pub fn max_cached_workflows(&mut self, num: usize) -> &mut Self {
        self.worker_config.max_cached_workflows = num;
        self
    }

    pub fn max_wft(&mut self, max: usize) -> &mut Self {
        self.worker_config.max_outstanding_workflow_tasks = max;
        self
    }

    pub fn max_at(&mut self, max: usize) -> &mut Self {
        self.worker_config.max_outstanding_activities = max;
        self
    }

    pub fn max_local_at(&mut self, max: usize) -> &mut Self {
        self.worker_config.max_outstanding_local_activities = max;
        self
    }

    pub fn max_at_polls(&mut self, max: usize) -> &mut Self {
        self.worker_config.max_concurrent_at_polls = max;
        self
    }

    pub fn wft_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.wft_timeout = Some(timeout);
        self
    }

    async fn get_or_init(&mut self) -> &InitializedWorker {
        self.initted_worker
            .get_or_init(|| async {
                let client = Arc::new(
                    get_integ_server_options()
                        .connect(self.worker_config.namespace.clone(), None, None)
                        .await
                        .expect("Must connect"),
                );
                let worker = init_worker(
                    INTEG_TESTS_RT.get().unwrap(),
                    self.worker_config.clone(),
                    client.clone(),
                )
                .expect("Worker inits cleanly");
                InitializedWorker {
                    worker: Arc::new(worker),
                    client,
                }
            })
            .await
    }
}

/// Provides conveniences for running integ tests with the SDK (against real server or mocks)
pub struct TestWorker {
    inner: Worker,
    pub core_worker: Arc<dyn CoreWorker>,
    client: Option<Arc<RetryClient<Client>>>,
    pub started_workflows: Mutex<Vec<WorkflowExecutionInfo>>,
    /// If set true (default), and a client is available, we will fetch workflow results to
    /// determine when they have all completed.
    pub fetch_results: bool,
    iceptor: Option<TestWorkerCompletionIceptor>,
}
impl TestWorker {
    /// Create a new test worker
    pub fn new(core_worker: Arc<dyn CoreWorker>, task_queue: impl Into<String>) -> Self {
        let inner = Worker::new_from_core(core_worker.clone(), task_queue);
        let iceptor = TestWorkerCompletionIceptor::new(
            TestWorkerShutdownCond::NoAutoShutdown,
            Arc::new(inner.shutdown_handle()),
        );
        Self {
            inner,
            core_worker,
            client: None,
            started_workflows: Mutex::new(vec![]),
            fetch_results: true,
            iceptor: Some(iceptor),
        }
    }

    pub fn inner_mut(&mut self) -> &mut Worker {
        &mut self.inner
    }

    // TODO: Maybe trait-ify?
    pub fn register_wf<F: Into<WorkflowFunction>>(
        &mut self,
        workflow_type: impl Into<String>,
        wf_function: F,
    ) {
        self.inner.register_wf(workflow_type, wf_function)
    }

    pub fn register_activity<A, R, O>(
        &mut self,
        activity_type: impl Into<String>,
        act_function: impl IntoActivityFunc<A, R, O>,
    ) {
        self.inner.register_activity(activity_type, act_function)
    }

    /// Create a workflow, asking the server to start it with the provided workflow ID and using the
    /// provided workflow function.
    ///
    /// Increments the expected Workflow run count.
    ///
    /// Returns the run id of the started workflow
    pub async fn submit_wf(
        &self,
        workflow_id: impl Into<String>,
        workflow_type: impl Into<String>,
        input: Vec<Payload>,
        options: WorkflowOptions,
    ) -> Result<String, anyhow::Error> {
        if let Some(c) = self.client.as_ref() {
            let wfid = workflow_id.into();
            let res = c
                .start_workflow(
                    input,
                    self.inner.task_queue().to_string(),
                    wfid.clone(),
                    workflow_type.into(),
                    None,
                    options,
                )
                .await?;
            self.started_workflows.lock().push(WorkflowExecutionInfo {
                namespace: c.namespace().to_string(),
                workflow_id: wfid,
                run_id: Some(res.run_id.clone()),
            });
            Ok(res.run_id)
        } else {
            Ok("fake_run_id".to_string())
        }
    }

    /// Runs until all expected workflows have completed
    pub async fn run_until_done(&mut self) -> Result<(), anyhow::Error> {
        self.run_until_done_intercepted(Option::<TestWorkerCompletionIceptor>::None)
            .await
    }

    /// See [Self::run_until_done], but allows configuration of some low-level interception.
    pub async fn run_until_done_intercepted(
        &mut self,
        interceptor: Option<impl WorkerInterceptor + 'static>,
    ) -> Result<(), anyhow::Error> {
        let mut iceptor = self.iceptor.take().unwrap();
        // Automatically use results-based complete detection if we have a client
        if self.fetch_results {
            if let Some(c) = self.client.clone() {
                iceptor.condition = TestWorkerShutdownCond::GetResults(
                    std::mem::take(&mut self.started_workflows.lock()),
                    c,
                );
            }
        }
        iceptor.next = interceptor.map(|i| Box::new(i) as Box<dyn WorkerInterceptor>);
        let get_results_waiter = iceptor.wait_all_wfs();
        self.inner.set_worker_interceptor(Box::new(iceptor));
        tokio::try_join!(self.inner.run(), get_results_waiter)?;
        Ok(())
    }
}

pub type BoxDynActivationHook = Box<dyn Fn(&WorkflowActivationCompletion)>;

pub enum TestWorkerShutdownCond {
    GetResults(Vec<WorkflowExecutionInfo>, Arc<RetryClient<Client>>),
    NoAutoShutdown,
}
/// Implements calling the shutdown handle when the expected number of test workflows has completed
pub struct TestWorkerCompletionIceptor {
    condition: TestWorkerShutdownCond,
    shutdown_handle: Arc<dyn Fn()>,
    every_activation: Option<BoxDynActivationHook>,
    next: Option<Box<dyn WorkerInterceptor>>,
}
impl TestWorkerCompletionIceptor {
    pub fn new(condition: TestWorkerShutdownCond, shutdown_handle: Arc<dyn Fn()>) -> Self {
        Self {
            condition,
            shutdown_handle,
            every_activation: None,
            next: None,
        }
    }

    fn wait_all_wfs(&mut self) -> impl Future<Output = Result<(), anyhow::Error>> + 'static {
        if let TestWorkerShutdownCond::GetResults(ref mut wfs, ref client) = self.condition {
            let wfs = std::mem::take(wfs);
            let shutdown_h = self.shutdown_handle.clone();
            let client = (**client).clone();
            let stream = stream::iter(
                wfs.into_iter()
                    .map(move |info| info.bind_untyped(client.clone())),
            )
            .map(Ok);
            future::Either::Left(async move {
                stream
                    .try_for_each_concurrent(None, |wh| async move {
                        wh.get_workflow_result(Default::default()).await?;
                        Ok::<_, anyhow::Error>(())
                    })
                    .await?;
                shutdown_h();
                Ok(())
            })
        } else {
            future::Either::Right(future::ready(Ok(())))
        }
    }
}
#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for TestWorkerCompletionIceptor {
    async fn on_workflow_activation_completion(&self, completion: &WorkflowActivationCompletion) {
        if let Some(func) = self.every_activation.as_ref() {
            func(completion);
        }
        if completion.has_execution_ending() {
            info!("Workflow {} says it's finishing", &completion.run_id);
        }
        if let Some(n) = self.next.as_ref() {
            n.on_workflow_activation_completion(completion).await;
        }
    }

    fn on_shutdown(&self, sdk_worker: &Worker) {
        if let Some(n) = self.next.as_ref() {
            n.on_shutdown(sdk_worker);
        }
    }
}

/// Returns the client options used to connect to the server used for integration tests.
pub fn get_integ_server_options() -> ClientOptions {
    let temporal_server_address = match env::var(INTEG_SERVER_TARGET_ENV_VAR) {
        Ok(addr) => addr,
        Err(_) => "http://localhost:7233".to_owned(),
    };
    let url = Url::try_from(&*temporal_server_address).unwrap();
    ClientOptionsBuilder::default()
        .identity("integ_tester".to_string())
        .target_url(url)
        .client_name("temporal-core".to_string())
        .client_version("0.1.0".to_string())
        .build()
        .unwrap()
}

pub fn get_integ_telem_options() -> TelemetryOptions {
    let mut ob = TelemetryOptionsBuilder::default();
    let filter_string =
        env::var("RUST_LOG").unwrap_or_else(|_| "temporal_sdk_core=INFO".to_string());
    if let Some(url) = env::var(OTEL_URL_ENV_VAR)
        .ok()
        .map(|x| x.parse::<Url>().unwrap())
    {
        let opts = OtelCollectorOptions {
            url,
            headers: Default::default(),
            metric_periodicity: None,
        };
        ob.tracing(TraceExportConfig {
            filter: filter_string.clone(),
            exporter: TraceExporter::Otel(opts.clone()),
        });
        ob.metrics(MetricsExporter::Otel(opts));
    }
    if let Some(addr) = env::var(PROM_ENABLE_ENV_VAR)
        .ok()
        .map(|x| SocketAddr::new([127, 0, 0, 1].into(), x.parse().unwrap()))
    {
        ob.metrics(MetricsExporter::Prometheus(addr));
    }
    ob.logging(Logger::Console {
        filter: filter_string,
    })
    .build()
    .unwrap()
}

pub fn default_cached_download() -> EphemeralExe {
    EphemeralExe::CachedDownload {
        version: EphemeralExeVersion::Default {
            sdk_name: "sdk-rust".to_string(),
            sdk_version: "0.1.0".to_string(),
        },
        dest_dir: None,
    }
}

pub fn schedule_activity_cmd(
    seq: u32,
    task_q: &str,
    activity_id: &str,
    cancellation_type: ActivityCancellationType,
    activity_timeout: Duration,
    heartbeat_timeout: Duration,
) -> workflow_command::Variant {
    ScheduleActivity {
        seq,
        activity_id: activity_id.to_string(),
        activity_type: "test_activity".to_string(),
        task_queue: task_q.to_owned(),
        schedule_to_start_timeout: Some(activity_timeout.try_into().expect("duration fits")),
        start_to_close_timeout: Some(activity_timeout.try_into().expect("duration fits")),
        schedule_to_close_timeout: Some(activity_timeout.try_into().expect("duration fits")),
        heartbeat_timeout: Some(heartbeat_timeout.try_into().expect("duration fits")),
        cancellation_type: cancellation_type as i32,
        ..Default::default()
    }
    .into()
}

pub fn schedule_local_activity_cmd(
    seq: u32,
    activity_id: &str,
    cancellation_type: ActivityCancellationType,
    activity_timeout: Duration,
) -> workflow_command::Variant {
    ScheduleLocalActivity {
        seq,
        activity_id: activity_id.to_string(),
        activity_type: "test_activity".to_string(),
        schedule_to_start_timeout: Some(activity_timeout.try_into().expect("duration fits")),
        start_to_close_timeout: Some(activity_timeout.try_into().expect("duration fits")),
        schedule_to_close_timeout: Some(activity_timeout.try_into().expect("duration fits")),
        cancellation_type: cancellation_type as i32,
        ..Default::default()
    }
    .into()
}

pub fn start_timer_cmd(seq: u32, duration: Duration) -> workflow_command::Variant {
    StartTimer {
        seq,
        start_to_fire_timeout: Some(duration.try_into().expect("duration fits")),
    }
    .into()
}

/// Given a desired number of concurrent executions and a provided function that produces a future,
/// run that many instances of the future concurrently.
///
/// Annoyingly, because of a sorta-bug in the way async blocks work, the async block produced by
/// the closure must be `async move` if it uses the provided iteration number. On the plus side,
/// since you're usually just accessing core in the closure, if core is a reference everything just
/// works. See <https://github.com/rust-lang/rust/issues/81653>
pub async fn fanout_tasks<FutureMaker, Fut>(num: usize, fm: FutureMaker)
where
    FutureMaker: Fn(usize) -> Fut,
    Fut: Future,
{
    let mut tasks = FuturesUnordered::new();
    for i in 0..num {
        tasks.push(fm(i));
    }

    while tasks.next().await.is_some() {}
}

#[async_trait::async_trait]
pub trait WorkerTestHelpers {
    async fn complete_execution(&self, run_id: &str);
    async fn complete_timer(&self, run_id: &str, seq: u32, duration: Duration);
}

#[async_trait::async_trait]
impl<T> WorkerTestHelpers for T
where
    T: CoreWorker + ?Sized,
{
    async fn complete_execution(&self, run_id: &str) {
        self.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.to_string(),
            vec![CompleteWorkflowExecution { result: None }.into()],
        ))
        .await
        .unwrap();
    }

    async fn complete_timer(&self, run_id: &str, seq: u32, duration: Duration) {
        self.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            run_id.to_string(),
            vec![StartTimer {
                seq,
                start_to_fire_timeout: Some(duration.try_into().expect("duration fits")),
            }
            .into()],
        ))
        .await
        .unwrap();
    }
}
