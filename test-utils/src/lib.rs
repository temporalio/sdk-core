//! This crate contains testing functionality that can be useful when building SDKs against Core,
//! or even when testing workflows written in SDKs that use Core.

pub mod canned_histories;

use futures::{stream::FuturesUnordered, StreamExt};
use log::LevelFilter;
use prost::Message;
use rand::{distributions::Standard, Rng};
use std::{
    convert::TryFrom, env, future::Future, net::SocketAddr, path::PathBuf, sync::Arc,
    time::Duration,
};
use temporal_sdk::TestRustWorker;
use temporal_sdk_core::{
    init_replay_worker, init_worker, replay::mock_gateway_from_history, telemetry_init,
    ServerGatewayOptions, ServerGatewayOptionsBuilder, TelemetryOptions, TelemetryOptionsBuilder,
    WorkerConfig, WorkerConfigBuilder,
};
use temporal_sdk_core_api::Worker;
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_commands::{
            workflow_command, ActivityCancellationType, CompleteWorkflowExecution,
            ScheduleActivity, StartTimer,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::history::v1::History,
};
use url::Url;

pub const NAMESPACE: &str = "default";
pub const TEST_Q: &str = "q";
/// If set, turn export traces and metrics to the OTel collector at the given URL
const OTEL_URL_ENV_VAR: &str = "TEMPORAL_INTEG_OTEL_URL";
/// If set, enable direct scraping of prom metrics on the specified port
const PROM_ENABLE_ENV_VAR: &str = "TEMPORAL_INTEG_PROM_PORT";

/// Create a worker instance which will use the provided test name to base the task queue and wf id
/// upon. Returns the instance and the task queue name (which is also the workflow id).
pub async fn init_core_and_create_wf(test_name: &str) -> (Arc<dyn Worker>, String) {
    let mut starter = CoreWfStarter::new(test_name);
    let core = starter.get_worker().await;
    starter.start_wf().await;
    (core, starter.get_task_queue().to_string())
}

/// Create a worker replay instance preloaded with a provided history. Returns the worker impl
/// and the task queue name as in [init_core_and_create_wf].
pub fn init_core_replay_preloaded(test_name: &str, history: &History) -> (Arc<dyn Worker>, String) {
    let worker_cfg = WorkerConfigBuilder::default()
        .namespace(NAMESPACE)
        .task_queue(test_name)
        .build()
        .expect("Configuration options construct properly");
    let gateway = mock_gateway_from_history(history, test_name.to_string());
    let worker = init_replay_worker(worker_cfg, Arc::new(gateway), history)
        .expect("Replay worker must init properly");
    (Arc::new(worker), test_name.to_string())
}

/// Load history from a file containing the protobuf serialization of it
pub async fn history_from_proto_binary(path_from_root: &str) -> Result<History, anyhow::Error> {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push("..");
    path.push(path_from_root);
    let bytes = tokio::fs::read(path).await?;
    Ok(History::decode(&*bytes)?)
}

/// Implements a builder pattern to help integ tests initialize core and create workflows
pub struct CoreWfStarter {
    /// Used for both the task queue and workflow id
    task_queue_name: String,
    telemetry_options: TelemetryOptions,
    worker_config: WorkerConfig,
    wft_timeout: Option<Duration>,
    initted_worker: Option<Arc<dyn Worker>>,
}

impl CoreWfStarter {
    pub fn new(test_name: &str) -> Self {
        let rand_bytes: Vec<u8> = rand::thread_rng().sample_iter(&Standard).take(6).collect();
        let task_q_salt = base64::encode(rand_bytes);
        let task_queue = format!("{}_{}", test_name, task_q_salt);
        Self::new_tq_name(&task_queue)
    }

    pub fn new_tq_name(task_queue: &str) -> Self {
        Self {
            task_queue_name: task_queue.to_owned(),
            telemetry_options: get_integ_telem_options(),
            worker_config: WorkerConfigBuilder::default()
                .namespace(NAMESPACE)
                .task_queue(task_queue)
                .max_cached_workflows(1000_usize)
                .build()
                .unwrap(),
            wft_timeout: None,
            initted_worker: None,
        }
    }

    pub async fn worker(&mut self) -> TestRustWorker {
        TestRustWorker::new(
            self.get_worker().await,
            self.worker_config.task_queue.clone(),
            self.wft_timeout,
        )
    }

    pub async fn shutdown(&mut self) {
        self.get_worker().await.shutdown().await;
    }

    pub async fn get_worker(&mut self) -> Arc<dyn Worker> {
        if self.initted_worker.is_none() {
            telemetry_init(&self.telemetry_options).expect("Telemetry inits cleanly");
            let gateway = get_integ_server_options()
                .connect(None)
                .await
                .expect("Must connect");
            let worker = init_worker(self.worker_config.clone(), gateway);
            self.initted_worker = Some(Arc::new(worker));
        }
        self.initted_worker.as_ref().unwrap().clone()
    }

    /// Start the workflow defined by the builder and return run id
    pub async fn start_wf(&self) -> String {
        self.start_wf_with_id(self.task_queue_name.clone()).await
    }

    pub async fn start_wf_with_id(&self, workflow_id: String) -> String {
        self.initted_worker
            .as_ref()
            .expect(
                "Core must be initted before starting a workflow.\
                             Tests must call `get_core` first.",
            )
            .as_ref()
            .server_gateway()
            .start_workflow(
                vec![],
                self.worker_config.task_queue.clone(),
                workflow_id,
                self.task_queue_name.clone(),
                self.wft_timeout,
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
        // TODO: Need not be passed in
        worker: &mut TestRustWorker,
    ) -> Result<(), anyhow::Error> {
        // Fetch history and replay it
        let history = self
            .get_worker()
            .await
            .server_gateway()
            .get_workflow_execution_history(wf_id.into(), Some(run_id.into()), vec![])
            .await?
            .history
            .expect("history field must be populated");
        let (replay_worker, _) = init_core_replay_preloaded(worker.task_queue(), &history);
        worker.with_new_core_worker(replay_worker);
        worker.incr_expected_run_count(1);
        worker.run_until_done().await.unwrap();
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
}

pub fn get_integ_server_options() -> ServerGatewayOptions {
    let temporal_server_address = match env::var("TEMPORAL_SERVICE_ADDRESS") {
        Ok(addr) => addr,
        Err(_) => "http://localhost:7233".to_owned(),
    };
    let url = Url::try_from(&*temporal_server_address).unwrap();
    ServerGatewayOptionsBuilder::default()
        .namespace(NAMESPACE.to_string())
        .identity("integ_tester".to_string())
        .worker_binary_id("fakebinaryid".to_string())
        .target_url(url)
        .client_name("temporal-core".to_string())
        .client_version("0.1.0".to_string())
        .build()
        .unwrap()
}

pub fn get_integ_telem_options() -> TelemetryOptions {
    let mut ob = TelemetryOptionsBuilder::default();
    if let Some(url) = env::var(OTEL_URL_ENV_VAR)
        .ok()
        .map(|x| x.parse::<Url>().unwrap())
    {
        ob.otel_collector_url(url);
    }
    if let Some(addr) = env::var(PROM_ENABLE_ENV_VAR)
        .ok()
        .map(|x| SocketAddr::new([127, 0, 0, 1].into(), x.parse().unwrap()))
    {
        ob.prometheus_export_bind_address(addr);
    }
    ob.tracing_filter(env::var("RUST_LOG").unwrap_or_else(|_| "temporal_sdk_core=INFO".to_string()))
        .log_forwarding_level(LevelFilter::Off)
        .build()
        .unwrap()
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
        namespace: NAMESPACE.to_owned(),
        task_queue: task_q.to_owned(),
        schedule_to_start_timeout: Some(activity_timeout.into()),
        start_to_close_timeout: Some(activity_timeout.into()),
        schedule_to_close_timeout: Some(activity_timeout.into()),
        heartbeat_timeout: Some(heartbeat_timeout.into()),
        cancellation_type: cancellation_type as i32,
        ..Default::default()
    }
    .into()
}

pub fn start_timer_cmd(seq: u32, duration: Duration) -> workflow_command::Variant {
    StartTimer {
        seq,
        start_to_fire_timeout: Some(duration.into()),
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
    T: Worker + ?Sized,
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
                start_to_fire_timeout: Some(duration.into()),
            }
            .into()],
        ))
        .await
        .unwrap();
    }
}
