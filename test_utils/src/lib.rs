use futures::{stream::FuturesUnordered, StreamExt};
use rand::{distributions::Standard, Rng};
use std::{convert::TryFrom, env, future::Future, sync::Arc, time::Duration};
use temporal_sdk_core::{
    protos::coresdk::workflow_commands::{
        workflow_command, ActivityCancellationType, ScheduleActivity,
    },
    test_workflow_driver::TestRustWorker,
    Core, CoreInitOptions, CoreInitOptionsBuilder, ServerGatewayApis, ServerGatewayOptions,
    WorkerConfig, WorkerConfigBuilder,
};
use url::Url;

pub const NAMESPACE: &str = "default";
pub type GwApi = Arc<dyn ServerGatewayApis>;

/// Implements a builder pattern to help integ tests initialize core and create workflows
pub struct CoreWfStarter {
    test_name: String,
    core_options: CoreInitOptions,
    worker_config: WorkerConfig,
    wft_timeout: Option<Duration>,
    initted_core: Option<Arc<dyn Core>>,
}

impl CoreWfStarter {
    pub fn new(test_name: &str) -> Self {
        let rand_bytes: Vec<u8> = rand::thread_rng().sample_iter(&Standard).take(6).collect();
        let task_q_salt = base64::encode(rand_bytes);
        let task_queue = format!("{}_{}", test_name, task_q_salt);
        Self {
            test_name: test_name.to_owned(),
            core_options: CoreInitOptionsBuilder::default()
                .gateway_opts(get_integ_server_options())
                .max_cached_workflows(1000usize)
                .build()
                .unwrap(),
            worker_config: WorkerConfigBuilder::default()
                .task_queue(task_queue)
                .build()
                .unwrap(),
            wft_timeout: None,
            initted_core: None,
        }
    }

    pub async fn worker(&mut self) -> TestRustWorker {
        TestRustWorker::new(
            self.get_core().await,
            NAMESPACE.to_owned(),
            self.worker_config.task_queue.clone(),
        )
    }

    pub async fn get_core(&mut self) -> Arc<dyn Core> {
        self.get_or_init_core().await
    }

    /// Start the workflow defined by the builder and return run id
    pub async fn start_wf(&self) -> String {
        self.start_wf_with_id(self.test_name.clone()).await
    }

    pub async fn start_wf_with_id(&self, workflow_id: String) -> String {
        with_gw(
            self.initted_core
                .as_ref()
                .expect(
                    "Core must be initted before starting a workflow.\
                             Tests must call `get_core` first.",
                )
                .as_ref(),
            |gw: GwApi| async move {
                gw.start_workflow(
                    NAMESPACE.to_owned(),
                    self.worker_config.task_queue.clone(),
                    workflow_id,
                    self.test_name.clone(),
                    self.wft_timeout,
                )
                .await
                .unwrap()
                .run_id
            },
        )
        .await
    }

    pub fn get_task_queue(&self) -> &str {
        &self.worker_config.task_queue
    }

    pub fn get_wf_id(&self) -> &str {
        &self.test_name
    }

    pub fn max_cached_workflows(&mut self, num: usize) -> &mut Self {
        self.core_options.max_cached_workflows = num;
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

    pub fn wft_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.wft_timeout = Some(timeout);
        self
    }

    async fn get_or_init_core(&mut self) -> Arc<dyn Core> {
        let opts = self.core_options.clone();
        if self.initted_core.is_none() {
            let core = temporal_sdk_core::init(opts).await.unwrap();
            // Register a worker for the task queue
            core.register_worker(self.worker_config.clone()).await;
            self.initted_core = Some(Arc::new(core));
        }
        self.initted_core.as_ref().unwrap().clone()
    }
}

pub async fn init_core_and_create_wf(test_name: &str) -> (Arc<dyn Core>, String) {
    let mut starter = CoreWfStarter::new(test_name);
    let core = starter.get_core().await;
    starter.start_wf().await;
    (core, starter.get_task_queue().to_string())
}

pub async fn with_gw<F: FnOnce(GwApi) -> Fout, Fout: Future>(
    core: &dyn Core,
    fun: F,
) -> Fout::Output {
    let gw = core.server_gateway();
    fun(gw).await
}

pub fn get_integ_server_options() -> ServerGatewayOptions {
    let temporal_server_address = match env::var("TEMPORAL_SERVICE_ADDRESS") {
        Ok(addr) => addr,
        Err(_) => "http://localhost:7233".to_owned(),
    };
    let url = Url::try_from(&*temporal_server_address).unwrap();
    ServerGatewayOptions {
        namespace: NAMESPACE.to_string(),
        identity: "integ_tester".to_string(),
        worker_binary_id: "".to_string(),
        long_poll_timeout: Duration::from_secs(60),
        target_url: url,
        tls_cfg: None,
    }
}

pub fn schedule_activity_cmd(
    task_q: &str,
    activity_id: &str,
    cancellation_type: ActivityCancellationType,
    activity_timeout: Duration,
    heartbeat_timeout: Duration,
) -> workflow_command::Variant {
    ScheduleActivity {
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

/// Given a desired number of concurrent executions and a provided function that produces a future,
/// run that many instances of the future concurrently.
///
/// Annoyingly, because of a sorta-bug in the way async blocks work, the async block produced by
/// the closure must be `async move` if it uses the provided iteration number. On the plus side,
/// since you're usually just accessing core in the closure, if core is a reference everything just
/// works. See https://github.com/rust-lang/rust/issues/81653
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
