mod activities;
pub(crate) mod client;
pub(crate) mod heartbeat;
mod nexus;
mod slot_provider;
pub(crate) mod tuner;
mod workflow;

pub use temporal_sdk_core_api::worker::{WorkerConfig, WorkerConfigBuilder};
pub use tuner::{
    FixedSizeSlotSupplier, RealSysInfo, ResourceBasedSlotsOptions,
    ResourceBasedSlotsOptionsBuilder, ResourceBasedTuner, ResourceSlotOptions, SlotSupplierOptions,
    TunerBuilder, TunerHolder, TunerHolderOptions, TunerHolderOptionsBuilder,
};

pub(crate) use activities::{
    ExecutingLAId, LocalActRequest, LocalActivityExecutionResult, LocalActivityResolution,
    NewLocalAct,
};
pub(crate) use wft_poller::WFTPollerShared;
pub(crate) use workflow::LEGACY_QUERY_ID;

use crate::worker::heartbeat::WorkerHeartbeatData;
use crate::{
    ActivityHeartbeat, CompleteActivityError, PollError, WorkerTrait,
    abstractions::{MeteredPermitDealer, PermitDealerContextData, dbg_panic},
    errors::CompleteWfError,
    pollers::{BoxedActPoller, BoxedNexusPoller},
    protosext::validate_activity_completion,
    telemetry::{
        TelemetryInstance,
        metrics::{
            MetricsContext, activity_poller, activity_worker_type, local_activity_worker_type,
            nexus_poller, nexus_worker_type, workflow_worker_type,
        },
    },
    worker::{
        activities::{LACompleteAction, LocalActivityManager, NextPendingLAAction},
        client::WorkerClient,
        nexus::NexusManager,
        workflow::{
            LAReqSink, LocalResolution, WorkflowBasics, Workflows, wft_poller::make_wft_poller,
        },
    },
};
use crate::{
    pollers::{ActivityTaskOptions, LongPollBuffer},
    worker::workflow::wft_poller,
};
use activities::WorkerActivityTasks;
use futures_util::{StreamExt, stream};
use parking_lot::Mutex;
use slot_provider::SlotProvider;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicUsize;
use std::{
    convert::TryInto,
    future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use temporal_client::{ConfiguredClient, TemporalServiceClientWithMetrics, WorkerKey};
use temporal_sdk_core_api::errors::WorkflowErrorType;
use temporal_sdk_core_api::worker::{
    WorkerDeploymentVersion, WorkerTuner, WorkerVersioningStrategy,
};
use temporal_sdk_core_api::{
    errors::{CompleteNexusError, WorkerValidationError},
    worker::PollerBehavior,
};
use temporal_sdk_core_protos::{
    TaskToken,
    coresdk::{
        ActivityTaskCompletion,
        activity_result::activity_execution_result,
        activity_task::ActivityTask,
        nexus::{NexusTask, NexusTaskCompletion, nexus_task_completion},
        workflow_activation::{WorkflowActivation, remove_from_cache::EvictionReason},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        enums::v1::TaskQueueKind,
        taskqueue::v1::{StickyExecutionAttributes, TaskQueue},
    },
};
use tokio::sync::{OnceCell, mpsc::unbounded_channel, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
#[cfg(test)]
use {
    crate::{
        pollers::{BoxedPoller, MockPermittedPollBuffer},
        protosext::ValidPollWFTQResponse,
    },
    futures_util::stream::BoxStream,
    temporal_sdk_core_protos::temporal::api::workflowservice::v1::{
        PollActivityTaskQueueResponse, PollNexusTaskQueueResponse,
    },
};

/// A worker polls on a certain task queue
pub struct Worker {
    config: WorkerConfigInner,
    client: Arc<dyn WorkerClient>,
    /// Registration key to enable eager workflow start for this worker
    worker_key: Mutex<Option<WorkerKey>>,
    /// Manages all workflows and WFT processing
    workflows: Workflows,
    /// Manages activity tasks for this worker/task queue
    at_task_mgr: Option<WorkerActivityTasks>,
    /// Manages local activities
    local_act_mgr: Arc<LocalActivityManager>,
    /// Manages Nexus tasks
    nexus_mgr: NexusManager,
    /// Has shutdown been called?
    shutdown_token: CancellationToken,
    /// Will be called at the end of each activation completion
    #[allow(clippy::type_complexity)] // Sorry clippy, there's no simple way to re-use here.
    post_activate_hook: Option<Box<dyn Fn(&Self, PostActivateHookData) + Send + Sync>>,
    /// Set when non-local activities are complete and should stop being polled
    non_local_activities_complete: Arc<AtomicBool>,
    /// Set when local activities are complete and should stop being polled
    local_activities_complete: Arc<AtomicBool>,
    /// Used to track all permits have been released
    all_permits_tracker: tokio::sync::Mutex<AllPermitsTracker>,
    worker_heartbeat_data: Option<Arc<Mutex<WorkerHeartbeatData>>>,
    /// Used to remove this worker from the parent map used to track this worker for
    /// worker heartbeat
    worker_heartbeat_shutdown_callback: OnceCell<Arc<dyn Fn() + Send + Sync>>,
}

/// Mirrors `WorkerConfig`, but with atomic structs to allow Worker Commands to make config changes
/// from the server
#[derive(Clone)]
pub(crate) struct WorkerConfigInner {
    /// If set nonzero, workflows will be cached and sticky task queues will be used, meaning that
    /// history updates are applied incrementally to suspended instances of workflow execution.
    /// Workflows are evicted according to a least-recently-used policy one the cache maximum is
    /// reached. Workflows may also be explicitly evicted at any time, or as a result of errors
    /// or failures.
    pub(crate) max_cached_workflows: Arc<AtomicUsize>,

    // Everything else (unchanged)
    /// The Temporal service namespace this worker is bound to
    pub(crate) namespace: String,
    /// What task queue will this worker poll from? This task queue name will be used for both
    /// workflow and activity polling.
    pub(crate) task_queue: String,
    /// A human-readable string that can identify this worker. Using something like sdk version
    /// and host name is a good default. If set, overrides the identity set (if any) on the client
    /// used by this worker.
    pub(crate) client_identity_override: Option<String>,
    /// Set a [WorkerTuner] for this worker. Either this or at least one of the `max_outstanding_*`
    /// fields must be set.
    pub(crate) tuner: Option<Arc<dyn WorkerTuner + Send + Sync>>,
    /// Maximum number of concurrent poll workflow task requests we will perform at a time on this
    /// worker's task queue. See also [WorkerConfig::nonsticky_to_sticky_poll_ratio].
    /// If using SimpleMaximum, Must be at least 2 when `max_cached_workflows` > 0, or is an error.
    pub(crate) workflow_task_poller_behavior: PollerBehavior,
    /// Only applies when using [PollerBehavior::SimpleMaximum]
    ///
    /// (max workflow task polls * this number) = the number of max pollers that will be allowed for
    /// the nonsticky queue when sticky tasks are enabled. If both defaults are used, the sticky
    /// queue will allow 4 max pollers while the nonsticky queue will allow one. The minimum for
    /// either poller is 1, so if the maximum allowed is 1 and sticky queues are enabled, there will
    /// be 2 concurrent polls.
    pub(crate) nonsticky_to_sticky_poll_ratio: f32,
    /// Maximum number of concurrent poll activity task requests we will perform at a time on this
    /// worker's task queue
    pub(crate) activity_task_poller_behavior: PollerBehavior,
    /// Maximum number of concurrent poll nexus task requests we will perform at a time on this
    /// worker's task queue
    pub(crate) nexus_task_poller_behavior: PollerBehavior,
    /// If set to true this worker will only handle workflow tasks and local activities, it will not
    /// poll for activity tasks.
    pub(crate) no_remote_activities: bool,
    /// How long a workflow task is allowed to sit on the sticky queue before it is timed out
    /// and moved to the non-sticky queue where it may be picked up by any worker.
    pub(crate) sticky_queue_schedule_to_start_timeout: Duration,

    /// Longest interval for throttling activity heartbeats
    pub(crate) max_heartbeat_throttle_interval: Duration,

    /// Default interval for throttling activity heartbeats in case
    /// `ActivityOptions.heartbeat_timeout` is unset.
    /// When the timeout *is* set in the `ActivityOptions`, throttling is set to
    /// `heartbeat_timeout * 0.8`.
    pub(crate) default_heartbeat_throttle_interval: Duration,

    /// Sets the maximum number of activities per second the task queue will dispatch, controlled
    /// server-side. Note that this only takes effect upon an activity poll request. If multiple
    /// workers on the same queue have different values set, they will thrash with the last poller
    /// winning.
    ///
    /// Setting this to a nonzero value will also disable eager activity execution.
    pub(crate) max_task_queue_activities_per_second: Option<f64>,

    /// Limits the number of activities per second that this worker will process. The worker will
    /// not poll for new activities if by doing so it might receive and execute an activity which
    /// would cause it to exceed this limit. Negative, zero, or NaN values will cause building
    /// the options to fail.
    pub(crate) max_worker_activities_per_second: Option<f64>,

    /// If set false (default), shutdown will not finish until all pending evictions have been
    /// issued and replied to. If set true shutdown will be considered complete when the only
    /// remaining work is pending evictions.
    ///
    /// This flag is useful during tests to avoid needing to deal with lots of uninteresting
    /// evictions during shutdown. Alternatively, if a lang implementation finds it easy to clean
    /// up during shutdown, setting this true saves some back-and-forth.
    pub(crate) ignore_evicts_on_shutdown: bool,

    /// Maximum number of next page (or initial) history event listing requests we'll make
    /// concurrently. I don't this it's worth exposing this to users until we encounter a reason.
    pub(crate) fetching_concurrency: usize,

    /// If set, core will issue cancels for all outstanding activities and nexus operations after
    /// shutdown has been initiated and this amount of time has elapsed.
    pub(crate) graceful_shutdown_period: Option<Duration>,

    /// The amount of time core will wait before timing out activities using its own local timers
    /// after one of them elapses. This is to avoid racing with server's own tracking of the
    /// timeout.
    pub(crate) local_timeout_buffer_for_activities: Duration,

    /// Any error types listed here will cause any workflow being processed by this worker to fail,
    /// rather than simply failing the workflow task.
    pub(crate) workflow_failure_errors: HashSet<WorkflowErrorType>,

    /// Like [WorkerConfig::workflow_failure_errors], but specific to certain workflow types (the
    /// map key).
    pub(crate) workflow_types_to_failure_errors: HashMap<String, HashSet<WorkflowErrorType>>,

    /// The maximum allowed number of workflow tasks that will ever be given to this worker at one
    /// time. Note that one workflow task may require multiple activations - so the WFT counts as
    /// "outstanding" until all activations it requires have been completed. Must be at least 2 if
    /// `max_cached_workflows` is > 0, or is an error.
    ///
    /// Mutually exclusive with `tuner`
    pub(crate) max_outstanding_workflow_tasks: Option<usize>,
    /// The maximum number of activity tasks that will ever be given to this worker concurrently
    ///
    /// Mutually exclusive with `tuner`
    pub(crate) max_outstanding_activities: Option<usize>,
    /// The maximum number of local activity tasks that will ever be given to this worker
    /// concurrently
    ///
    /// Mutually exclusive with `tuner`
    pub(crate) max_outstanding_local_activities: Option<usize>,
    /// The maximum number of nexus tasks that will ever be given to this worker
    /// concurrently
    ///
    /// Mutually exclusive with `tuner`
    pub(crate) max_outstanding_nexus_tasks: Option<usize>,

    /// A versioning strategy for this worker.
    pub(crate) versioning_strategy: WorkerVersioningStrategy,
}

impl From<WorkerConfig> for WorkerConfigInner {
    fn from(config: WorkerConfig) -> Self {
        Self {
            max_cached_workflows: Arc::new(AtomicUsize::new(config.max_cached_workflows)),

            namespace: config.namespace,
            task_queue: config.task_queue,
            client_identity_override: config.client_identity_override,
            tuner: config.tuner,
            workflow_task_poller_behavior: config.workflow_task_poller_behavior,
            nonsticky_to_sticky_poll_ratio: config.nonsticky_to_sticky_poll_ratio,
            activity_task_poller_behavior: config.activity_task_poller_behavior,
            nexus_task_poller_behavior: config.nexus_task_poller_behavior,
            no_remote_activities: config.no_remote_activities,
            sticky_queue_schedule_to_start_timeout: config.sticky_queue_schedule_to_start_timeout,
            max_heartbeat_throttle_interval: config.max_heartbeat_throttle_interval,
            default_heartbeat_throttle_interval: config.default_heartbeat_throttle_interval,
            max_task_queue_activities_per_second: config.max_task_queue_activities_per_second,
            max_worker_activities_per_second: config.max_worker_activities_per_second,
            ignore_evicts_on_shutdown: config.ignore_evicts_on_shutdown,
            fetching_concurrency: config.fetching_concurrency,
            graceful_shutdown_period: config.graceful_shutdown_period,
            local_timeout_buffer_for_activities: config.local_timeout_buffer_for_activities,
            workflow_failure_errors: config.workflow_failure_errors,
            workflow_types_to_failure_errors: config.workflow_types_to_failure_errors,
            max_outstanding_workflow_tasks: config.max_outstanding_workflow_tasks,
            max_outstanding_activities: config.max_outstanding_activities,
            max_outstanding_local_activities: config.max_outstanding_local_activities,
            max_outstanding_nexus_tasks: config.max_outstanding_nexus_tasks,
            versioning_strategy: config.versioning_strategy,
        }
    }
}

impl WorkerConfigInner {
    /// Returns true if the configuration specifies we should fail a workflow on a certain error
    /// type rather than failing the workflow task.
    pub(crate) fn should_fail_workflow(
        &self,
        workflow_type: &str,
        error_type: &WorkflowErrorType,
    ) -> bool {
        self.workflow_failure_errors.contains(error_type)
            || self
                .workflow_types_to_failure_errors
                .get(workflow_type)
                .map(|s| s.contains(error_type))
                .unwrap_or(false)
    }

    pub(crate) fn computed_deployment_version(&self) -> Option<WorkerDeploymentVersion> {
        let wdv = match self.versioning_strategy {
            WorkerVersioningStrategy::None { ref build_id } => WorkerDeploymentVersion {
                deployment_name: "".to_owned(),
                build_id: build_id.clone(),
            },
            WorkerVersioningStrategy::WorkerDeploymentBased(ref opts) => opts.version.clone(),
            WorkerVersioningStrategy::LegacyBuildIdBased { ref build_id } => {
                WorkerDeploymentVersion {
                    deployment_name: "".to_owned(),
                    build_id: build_id.clone(),
                }
            }
        };
        if wdv.is_empty() { None } else { Some(wdv) }
    }
}

struct AllPermitsTracker {
    wft_permits: watch::Receiver<usize>,
    act_permits: watch::Receiver<usize>,
    la_permits: watch::Receiver<usize>,
}

impl AllPermitsTracker {
    async fn all_done(&mut self) {
        let _ = self.wft_permits.wait_for(|x| *x == 0).await;
        let _ = self.act_permits.wait_for(|x| *x == 0).await;
        let _ = self.la_permits.wait_for(|x| *x == 0).await;
    }
}

#[async_trait::async_trait]
impl WorkerTrait for Worker {
    async fn validate(&self) -> Result<(), WorkerValidationError> {
        self.verify_namespace_exists().await?;
        Ok(())
    }

    async fn poll_workflow_activation(&self) -> Result<WorkflowActivation, PollError> {
        self.next_workflow_activation().await
    }

    #[instrument(skip(self))]
    async fn poll_activity_task(&self) -> Result<ActivityTask, PollError> {
        loop {
            match self.activity_poll().await.transpose() {
                Some(r) => break r,
                None => {
                    tokio::task::yield_now().await;
                    continue;
                }
            }
        }
    }

    #[instrument(skip(self))]
    async fn poll_nexus_task(&self) -> Result<NexusTask, PollError> {
        self.nexus_mgr.next_nexus_task().await
    }

    async fn complete_workflow_activation(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<(), CompleteWfError> {
        self.complete_workflow_activation(completion).await
    }

    async fn complete_activity_task(
        &self,
        completion: ActivityTaskCompletion,
    ) -> Result<(), CompleteActivityError> {
        let task_token = TaskToken(completion.task_token);
        let status = if let Some(s) = completion.result.and_then(|r| r.status) {
            s
        } else {
            return Err(CompleteActivityError::MalformedActivityCompletion {
                reason: "Activity completion had empty result/status field".to_owned(),
                completion: None,
            });
        };

        self.complete_activity(task_token, status).await
    }

    async fn complete_nexus_task(
        &self,
        completion: NexusTaskCompletion,
    ) -> Result<(), CompleteNexusError> {
        let status = if let Some(s) = completion.status {
            s
        } else {
            return Err(CompleteNexusError::MalformedNexusCompletion {
                reason: "Nexus completion had empty status field".to_owned(),
            });
        };

        self.complete_nexus_task(TaskToken(completion.task_token), status)
            .await
    }

    fn record_activity_heartbeat(&self, details: ActivityHeartbeat) {
        self.record_heartbeat(details);
    }

    fn request_workflow_eviction(&self, run_id: &str) {
        self.request_wf_eviction(
            run_id,
            "Eviction explicitly requested by lang",
            EvictionReason::LangRequested,
        );
    }

    fn get_task_queue(&self) -> String {
        self.config.task_queue.clone()
    }

    fn get_namespace(&self) -> String {
        self.config.namespace.clone()
    }

    /// Begins the shutdown process, tells pollers they should stop. Is idempotent.
    fn initiate_shutdown(&self) {
        if !self.shutdown_token.is_cancelled() {
            info!(
                task_queue=%self.config.task_queue,
                namespace=%self.config.namespace,
                "Initiated shutdown",
            );
        }
        self.shutdown_token.cancel();
        // First, disable Eager Workflow Start
        if let Some(key) = *self.worker_key.lock() {
            self.client.workers().unregister(key);
        }
        // Second, we want to stop polling of both activity and workflow tasks
        if let Some(atm) = self.at_task_mgr.as_ref() {
            atm.initiate_shutdown();
        }
        // Let the manager know that shutdown has been initiated to try to unblock the local
        // activity poll in case this worker is an activity-only worker.
        self.local_act_mgr.shutdown_initiated();

        if !self.workflows.ever_polled() {
            self.local_act_mgr.workflows_have_shutdown();
        } else {
            // Bump the workflow stream with a pointless input, since if a client initiates shutdown
            // and then immediately blocks waiting on a workflow activation poll, it's possible that
            // there may not be any more inputs ever, and that poll will never resolve.
            self.workflows.send_get_state_info_msg();
        }
    }

    async fn shutdown(&self) {
        self.shutdown().await
    }

    async fn finalize_shutdown(self) {
        self.finalize_shutdown().await
    }
}

impl Worker {
    /// Creates a new [Worker] from a [WorkerClient] instance with real task pollers and optional telemetry.
    ///
    /// This is a convenience constructor that logs initialization and delegates to
    /// [Worker::new_with_pollers()] using [TaskPollers::Real].
    pub fn new(
        config: WorkerConfig,
        sticky_queue_name: Option<String>,
        client: Arc<dyn WorkerClient>,
        telem_instance: Option<&TelemetryInstance>,
        shared_namespace_worker: bool,
    ) -> Self {
        info!(task_queue=%config.task_queue, namespace=%config.namespace, "Initializing worker");

        Self::new_with_pollers(
            config.into(),
            sticky_queue_name,
            client,
            TaskPollers::Real,
            telem_instance,
            shared_namespace_worker,
        )
    }

    /// Replace client and return a new client. For eager workflow purposes, this new client will
    /// now apply to future eager start requests and the older client will not.
    pub fn replace_client(&self, new_client: ConfiguredClient<TemporalServiceClientWithMetrics>) {
        // Unregister worker from current client, register in new client at the end
        let mut worker_key = self.worker_key.lock();
        let slot_provider = (*worker_key).and_then(|k| self.client.workers().unregister(k));
        self.client.replace_client(super::init_worker_client(
            self.config.namespace.clone(),
            self.config.client_identity_override.clone(),
            new_client,
        ));
        *worker_key =
            slot_provider.and_then(|slot_provider| self.client.workers().register(slot_provider));
    }

    #[cfg(test)]
    pub(crate) fn new_test(config: WorkerConfig, client: impl WorkerClient + 'static) -> Self {
        let client = Arc::new(client);
        Self::new(config.into(), None, client, None, false)
    }

    pub(crate) fn new_with_pollers(
        config: WorkerConfigInner,
        sticky_queue_name: Option<String>,
        client: Arc<dyn WorkerClient>,
        task_pollers: TaskPollers,
        telem_instance: Option<&TelemetryInstance>,
        shared_namespace_worker: bool,
    ) -> Self {
        let (metrics, meter) = if let Some(ti) = telem_instance {
            (
                MetricsContext::top_level(config.namespace.clone(), config.task_queue.clone(), ti),
                ti.get_metric_meter(),
            )
        } else {
            (MetricsContext::no_op(), None)
        };
        let tuner = config
            .tuner
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Arc::new(TunerBuilder::from_config(&config).build()));

        metrics.worker_registered();
        let shutdown_token = CancellationToken::new();
        let slot_context_data = Arc::new(PermitDealerContextData {
            task_queue: config.task_queue.clone(),
            worker_identity: client.get_identity(),
            worker_deployment_version: config.computed_deployment_version(),
        });
        let mut max_cached_workflows = config.max_cached_workflows.load(Ordering::Relaxed);
        if max_cached_workflows == 1 {
            // Since we always need to be able to poll the normal task queue as well as the
            // sticky queue, we need a value of at least 2 here.
            max_cached_workflows = 2;
            config
                .max_cached_workflows
                .store(max_cached_workflows, Ordering::Relaxed);
        }
        let wft_slots = MeteredPermitDealer::new(
            tuner.workflow_task_slot_supplier(),
            metrics.with_new_attrs([workflow_worker_type()]),
            if max_cached_workflows > 0 {
                Some(config.max_cached_workflows.clone())
            } else {
                None
            },
            slot_context_data.clone(),
            meter.clone(),
        );
        let wft_permits = wft_slots.get_extant_count_rcv();
        let act_slots = MeteredPermitDealer::new(
            tuner.activity_task_slot_supplier(),
            metrics.with_new_attrs([activity_worker_type()]),
            None,
            slot_context_data.clone(),
            meter.clone(),
        );
        let act_permits = act_slots.get_extant_count_rcv();
        let (external_wft_tx, external_wft_rx) = unbounded_channel();
        let nexus_slots = MeteredPermitDealer::new(
            tuner.nexus_task_slot_supplier(),
            metrics.with_new_attrs([nexus_worker_type()]),
            None,
            slot_context_data.clone(),
            meter.clone(),
        );
        let (wft_stream, act_poller, nexus_poller) = match task_pollers {
            TaskPollers::Real => {
                let wft_stream = make_wft_poller(
                    &config,
                    &sticky_queue_name,
                    &client,
                    &metrics,
                    &shutdown_token,
                    &wft_slots,
                );
                let wft_stream = if !client.is_mock() {
                    // Some replay tests combine a mock client with real pollers,
                    // and they don't need to use the external stream
                    stream::select(wft_stream, UnboundedReceiverStream::new(external_wft_rx))
                        .left_stream()
                } else {
                    wft_stream.right_stream()
                };

                let act_poll_buffer = if config.no_remote_activities {
                    None
                } else {
                    let act_metrics = metrics.with_new_attrs([activity_poller()]);
                    let ap = LongPollBuffer::new_activity_task(
                        client.clone(),
                        config.task_queue.clone(),
                        config.activity_task_poller_behavior,
                        act_slots.clone(),
                        shutdown_token.child_token(),
                        Some(move |np| act_metrics.record_num_pollers(np)),
                        ActivityTaskOptions {
                            max_worker_acts_per_second: config.max_task_queue_activities_per_second,
                            max_tps: config.max_task_queue_activities_per_second,
                        },
                    );
                    Some(Box::from(ap) as BoxedActPoller)
                };

                let np_metrics = metrics.with_new_attrs([nexus_poller()]);
                // This starts the poller thread.
                let nexus_poll_buffer = Box::new(LongPollBuffer::new_nexus_task(
                    client.clone(),
                    config.task_queue.clone(),
                    config.nexus_task_poller_behavior,
                    nexus_slots.clone(),
                    shutdown_token.child_token(),
                    Some(move |np| np_metrics.record_num_pollers(np)),
                    shared_namespace_worker,
                )) as BoxedNexusPoller;

                #[cfg(test)]
                let wft_stream = wft_stream.left_stream();
                (wft_stream, act_poll_buffer, nexus_poll_buffer)
            }
            #[cfg(test)]
            TaskPollers::Mocked {
                wft_stream,
                act_poller,
                nexus_poller,
            } => {
                let ap = act_poller
                    .map(|ap| MockPermittedPollBuffer::new(Arc::new(act_slots.clone()), ap));
                let np = MockPermittedPollBuffer::new(Arc::new(nexus_slots.clone()), nexus_poller);
                let wft_semaphore = wft_slots.clone();
                let wfs = wft_stream.then(move |s| {
                    let wft_semaphore = wft_semaphore.clone();
                    async move {
                        let permit = wft_semaphore.acquire_owned().await;
                        s.map(|s| (s, permit))
                    }
                });
                let wfs = wfs.right_stream();
                (
                    wfs,
                    ap.map(|ap| Box::new(ap) as BoxedActPoller),
                    Box::new(np) as BoxedNexusPoller,
                )
            }
        };

        let (hb_tx, hb_rx) = unbounded_channel();
        let la_permit_dealer = MeteredPermitDealer::new(
            tuner.local_activity_slot_supplier(),
            metrics.with_new_attrs([local_activity_worker_type()]),
            None,
            slot_context_data.clone(),
            meter.clone(),
        );
        let la_permits = la_permit_dealer.get_extant_count_rcv();
        let local_act_mgr = Arc::new(LocalActivityManager::new(
            config.namespace.clone(),
            la_permit_dealer,
            hb_tx,
            metrics.clone(),
        ));
        let at_task_mgr = act_poller.map(|ap| {
            WorkerActivityTasks::new(
                act_slots,
                ap,
                client.clone(),
                metrics.clone(),
                config.max_heartbeat_throttle_interval,
                config.default_heartbeat_throttle_interval,
                config.graceful_shutdown_period,
                config.local_timeout_buffer_for_activities,
            )
        });
        let poll_on_non_local_activities = at_task_mgr.is_some();
        if !poll_on_non_local_activities {
            info!("Activity polling is disabled for this worker");
        };
        let la_sink = LAReqSink::new(local_act_mgr.clone());

        let nexus_mgr = NexusManager::new(
            nexus_poller,
            metrics.clone(),
            config.graceful_shutdown_period,
            shutdown_token.child_token(),
        );

        let provider = SlotProvider::new(
            config.namespace.clone(),
            config.task_queue.clone(),
            wft_slots.clone(),
            external_wft_tx,
        );
        let worker_key = Mutex::new(client.workers().register(Box::new(provider)));
        let sdk_name_and_ver = client.sdk_name_and_version();

        let worker_heartbeat_data = if !shared_namespace_worker {
            Some(Arc::new(Mutex::new(WorkerHeartbeatData::new(
                config.clone(),
                client.get_identity(),
                sdk_name_and_ver.clone(),
            ))))
        } else {
            None
        };

        Self {
            worker_key,
            client: client.clone(),
            workflows: Workflows::new(
                WorkflowBasics {
                    worker_config: Arc::new(config.clone()),
                    shutdown_token: shutdown_token.child_token(),
                    metrics,
                    server_capabilities: client.capabilities().unwrap_or_default(),
                    sdk_name: sdk_name_and_ver.0,
                    sdk_version: sdk_name_and_ver.1,
                    default_versioning_behavior: config
                        .versioning_strategy
                        .default_versioning_behavior(),
                },
                sticky_queue_name.map(|sq| StickyExecutionAttributes {
                    worker_task_queue: Some(TaskQueue {
                        name: sq,
                        kind: TaskQueueKind::Sticky as i32,
                        normal_name: config.task_queue.clone(),
                    }),
                    schedule_to_start_timeout: Some(
                        config
                            .sticky_queue_schedule_to_start_timeout
                            .try_into()
                            .expect("timeout fits into proto"),
                    ),
                }),
                client,
                wft_slots,
                wft_stream,
                la_sink,
                local_act_mgr.clone(),
                hb_rx,
                at_task_mgr.as_ref().and_then(|mgr| {
                    match config.max_task_queue_activities_per_second {
                        Some(persec) if persec > 0.0 => None,
                        _ => Some(mgr.get_handle_for_workflows()),
                    }
                }),
                telem_instance,
            ),
            at_task_mgr,
            local_act_mgr,
            config,
            shutdown_token,
            post_activate_hook: None,
            // Non-local activities are already complete if configured not to poll for them.
            non_local_activities_complete: Arc::new(AtomicBool::new(!poll_on_non_local_activities)),
            local_activities_complete: Default::default(),
            all_permits_tracker: tokio::sync::Mutex::new(AllPermitsTracker {
                wft_permits,
                act_permits,
                la_permits,
            }),
            nexus_mgr,
            worker_heartbeat_data,
            worker_heartbeat_shutdown_callback: OnceCell::new(),
        }
    }

    /// Will shutdown the worker. Does not resolve until all outstanding workflow tasks have been
    /// completed
    async fn shutdown(&self) {
        self.initiate_shutdown();
        if let Some(name) = self.workflows.get_sticky_queue_name() {
            // This is a best effort call and we can still shutdown the worker if it fails
            match self.client.shutdown_worker(name).await {
                Err(err)
                    if !matches!(
                        err.code(),
                        tonic::Code::Unimplemented | tonic::Code::Unavailable
                    ) =>
                {
                    warn!("Failed to shutdown sticky queue  {:?}", err);
                }
                _ => {}
            }
        }
        // We need to wait for all local activities to finish so no more workflow task heartbeats
        // will be generated
        self.local_act_mgr
            .wait_all_outstanding_tasks_finished()
            .await;
        // Wait for workflows to finish
        self.workflows
            .shutdown()
            .await
            .expect("Workflow processing terminates cleanly");
        // Wait for activities to finish
        if let Some(acts) = self.at_task_mgr.as_ref() {
            acts.shutdown().await;
        }
        // Wait for nexus tasks to finish
        self.nexus_mgr.shutdown().await;
        // Wait for all permits to be released, but don't totally hang real-world shutdown.
        tokio::select! {
            _ = async { self.all_permits_tracker.lock().await.all_done().await } => {},
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                dbg_panic!("Waiting for all slot permits to release took too long!");
            }
        }
        // If this worker is tracked by SharedNamespaceWorker, remove entry from SharedNamespaceWorker
        if let Some(shutdown_callback) = self.worker_heartbeat_shutdown_callback.get() {
            shutdown_callback();
        }
    }

    /// Finish shutting down by consuming the background pollers and freeing all resources
    async fn finalize_shutdown(self) {
        self.shutdown().await;
        if let Some(b) = self.at_task_mgr {
            b.shutdown().await;
        }
    }

    pub(crate) fn shutdown_token(&self) -> CancellationToken {
        self.shutdown_token.clone()
    }

    /// Returns number of currently cached workflows
    pub async fn cached_workflows(&self) -> usize {
        self.workflows
            .get_state_info()
            .await
            .map(|r| r.cached_workflows)
            .unwrap_or_default()
    }

    /// Returns number of currently outstanding workflow tasks
    #[cfg(test)]
    pub(crate) async fn outstanding_workflow_tasks(&self) -> usize {
        self.workflows
            .get_state_info()
            .await
            .map(|r| r.outstanding_wft)
            .unwrap_or_default()
    }

    #[allow(unused)]
    pub(crate) fn available_wft_permits(&self) -> Option<usize> {
        self.workflows.available_wft_permits()
    }
    #[cfg(test)]
    pub(crate) fn unused_wft_permits(&self) -> Option<usize> {
        self.workflows.unused_wft_permits()
    }

    /// Get new activity tasks (may be local or nonlocal). Local activities are returned first
    /// before polling the server if there are any.
    ///
    /// Returns `Ok(None)` in the event of a poll timeout or if the polling loop should otherwise
    /// be restarted
    async fn activity_poll(&self) -> Result<Option<ActivityTask>, PollError> {
        let local_activities_complete = self.local_activities_complete.load(Ordering::Relaxed);
        let non_local_activities_complete =
            self.non_local_activities_complete.load(Ordering::Relaxed);
        if local_activities_complete && non_local_activities_complete {
            return Err(PollError::ShutDown);
        }
        let act_mgr_poll = async {
            if non_local_activities_complete {
                future::pending::<()>().await;
                unreachable!()
            }
            if let Some(ref act_mgr) = self.at_task_mgr {
                let res = act_mgr.poll().await;
                if let Err(err) = res.as_ref()
                    && matches!(err, PollError::ShutDown)
                {
                    self.non_local_activities_complete
                        .store(true, Ordering::Relaxed);
                    return Ok(None);
                };
                res.map(Some)
            } else {
                // We expect the local activity branch below to produce shutdown when appropriate if
                // there are no activity pollers.
                future::pending::<()>().await;
                unreachable!()
            }
        };
        let local_activities_poll = async {
            if local_activities_complete {
                future::pending::<()>().await;
                unreachable!()
            }
            match self.local_act_mgr.next_pending().await {
                Some(NextPendingLAAction::Dispatch(r)) => Ok(Some(r)),
                Some(NextPendingLAAction::Autocomplete(action)) => {
                    Ok(self.handle_la_complete_action(action))
                }
                None => {
                    if self.shutdown_token.is_cancelled() {
                        self.local_activities_complete
                            .store(true, Ordering::Relaxed);
                    }
                    Ok(None)
                }
            }
        };

        let r = tokio::select! {
            biased;

            r = local_activities_poll => r,
            r = act_mgr_poll => r,
        };
        // Since we consider network errors (at this level) fatal, we want to start shutdown if one
        // is encountered
        if matches!(r, Err(PollError::TonicError(_))) {
            self.initiate_shutdown();
        }
        r
    }

    /// Attempt to record an activity heartbeat
    pub(crate) fn record_heartbeat(&self, details: ActivityHeartbeat) {
        if let Some(at_mgr) = self.at_task_mgr.as_ref() {
            let tt = TaskToken(details.task_token.clone());
            if let Err(e) = at_mgr.record_heartbeat(details) {
                warn!(task_token = %tt, details = ?e, "Activity heartbeat failed.");
            }
        }
    }

    #[instrument(skip(self, task_token, status),
        fields(task_token=%&task_token, status=%&status,
               task_queue=%self.config.task_queue, workflow_id, run_id))]
    pub(crate) async fn complete_activity(
        &self,
        task_token: TaskToken,
        status: activity_execution_result::Status,
    ) -> Result<(), CompleteActivityError> {
        validate_activity_completion(&status)?;
        if task_token.is_local_activity_task() {
            let as_la_res: LocalActivityExecutionResult = status.try_into()?;
            self.complete_local_act(task_token, as_la_res);
            return Ok(());
        }

        if let Some(atm) = &self.at_task_mgr {
            atm.complete(task_token, status, &*self.client).await;
        } else {
            error!(
                "Tried to complete activity {} on a worker that does not have an activity manager",
                task_token
            );
        }
        Ok(())
    }

    #[instrument(skip(self), fields(run_id, workflow_id, task_queue=%self.config.task_queue))]
    pub(crate) async fn next_workflow_activation(&self) -> Result<WorkflowActivation, PollError> {
        let r = self.workflows.next_workflow_activation().await;
        // In the event workflows are shutdown or erroring, begin shutdown of everything else. Once
        // they are shut down, tell the local activity manager that, so that it can know to cancel
        // any remaining outstanding LAs and shutdown.
        if let Err(ref e) = r {
            // This is covering the situation where WFT pollers dying is the reason for shutdown
            self.initiate_shutdown();
            if matches!(e, PollError::ShutDown) {
                self.local_act_mgr.workflows_have_shutdown();
            }
        }
        r
    }

    #[instrument(skip(self, completion),
        fields(completion=%&completion, run_id=%completion.run_id, workflow_id,
               task_queue=%self.config.task_queue))]
    pub(crate) async fn complete_workflow_activation(
        &self,
        completion: WorkflowActivationCompletion,
    ) -> Result<(), CompleteWfError> {
        self.workflows
            .activation_completed(
                completion,
                false,
                self.post_activate_hook
                    .as_ref()
                    .map(|h| |data: PostActivateHookData| h(self, data)),
            )
            .await?;
        Ok(())
    }

    #[instrument(
        skip(self, tt, status),
        fields(task_token=%&tt, status=%&status, task_queue=%self.config.task_queue)
    )]
    async fn complete_nexus_task(
        &self,
        tt: TaskToken,
        status: nexus_task_completion::Status,
    ) -> Result<(), CompleteNexusError> {
        self.nexus_mgr
            .complete_task(tt, status, &*self.client)
            .await
    }

    /// Request a workflow eviction
    pub(crate) fn request_wf_eviction(
        &self,
        run_id: &str,
        message: impl Into<String>,
        reason: EvictionReason,
    ) {
        self.workflows.request_eviction(run_id, message, reason);
    }

    /// Sets a function to be called at the end of each activation completion
    pub(crate) fn set_post_activate_hook(
        &mut self,
        callback: impl Fn(&Self, PostActivateHookData) + Send + Sync + 'static,
    ) {
        self.post_activate_hook = Some(Box::new(callback))
    }

    fn complete_local_act(&self, task_token: TaskToken, la_res: LocalActivityExecutionResult) {
        if self
            .handle_la_complete_action(self.local_act_mgr.complete(&task_token, la_res))
            .is_some()
        {
            dbg_panic!("Should never be a task from direct completion");
        }
    }

    fn handle_la_complete_action(&self, action: LACompleteAction) -> Option<ActivityTask> {
        match action {
            LACompleteAction::Report {
                run_id,
                resolution,
                task,
            } => {
                self.notify_local_result(&run_id, LocalResolution::LocalActivity(resolution));
                task
            }
            LACompleteAction::WillBeRetried(task) => task,
            LACompleteAction::Untracked => None,
        }
    }

    fn notify_local_result(&self, run_id: &str, res: LocalResolution) {
        self.workflows.notify_of_local_result(run_id, res);
    }

    async fn verify_namespace_exists(&self) -> Result<(), WorkerValidationError> {
        if let Err(e) = self.client.describe_namespace().await {
            // Ignore if unimplemented since we wouldn't want to fail against an old server, for
            // example.
            if e.code() != tonic::Code::Unimplemented {
                return Err(WorkerValidationError::NamespaceDescribeError {
                    source: e,
                    namespace: self.config.namespace.clone(),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn get_heartbeat_data(&self) -> Option<Arc<Mutex<WorkerHeartbeatData>>> {
        self.worker_heartbeat_data.clone()
    }

    pub(crate) fn register_heartbeat_shutdown_callback(
        &mut self,
        callback: Arc<dyn Fn() + Send + Sync>,
    ) {
        if let Err(e) = self.worker_heartbeat_shutdown_callback.set(callback) {
            dbg_panic!("Unable to set worker heartbeat shutdown callback: {}", e);
        }
    }
}

pub(crate) struct PostActivateHookData<'a> {
    pub(crate) run_id: &'a str,
    pub(crate) replaying: bool,
}

pub(crate) enum TaskPollers {
    Real,
    #[cfg(test)]
    Mocked {
        wft_stream: BoxStream<'static, Result<ValidPollWFTQResponse, tonic::Status>>,
        act_poller: Option<BoxedPoller<PollActivityTaskQueueResponse>>,
        nexus_poller: BoxedPoller<PollNexusTaskQueueResponse>,
    },
}

fn wft_poller_behavior(config: &WorkerConfigInner, is_sticky: bool) -> PollerBehavior {
    fn calc_max_nonsticky(max_polls: usize, ratio: f32) -> usize {
        ((max_polls as f32 * ratio) as usize).max(1)
    }

    if let PollerBehavior::SimpleMaximum(m) = config.workflow_task_poller_behavior {
        if !is_sticky {
            PollerBehavior::SimpleMaximum(calc_max_nonsticky(
                m,
                config.nonsticky_to_sticky_poll_ratio,
            ))
        } else {
            PollerBehavior::SimpleMaximum(
                m.saturating_sub(calc_max_nonsticky(m, config.nonsticky_to_sticky_poll_ratio))
                    .max(1),
            )
        }
    } else {
        config.workflow_task_poller_behavior
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        advance_fut,
        test_help::test_worker_cfg,
        worker::client::mocks::{mock_manual_worker_client, mock_worker_client},
    };
    use futures_util::FutureExt;
    use temporal_sdk_core_api::worker::PollerBehavior;
    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse;

    #[tokio::test]
    async fn activity_timeouts_maintain_permit() {
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_poll_activity_task()
            .returning(|_, _| Ok(PollActivityTaskQueueResponse::default()));

        let cfg = test_worker_cfg()
            .max_outstanding_activities(5_usize)
            .build()
            .unwrap();
        let worker = Worker::new_test(cfg, mock_client);
        let fut = worker.poll_activity_task();
        advance_fut!(fut);
        assert_eq!(
            worker.at_task_mgr.as_ref().unwrap().unused_permits(),
            Some(5)
        );
    }

    #[tokio::test]
    async fn activity_errs_dont_eat_permits() {
        // Return one error followed by simulating waiting on the poll, otherwise the poller will
        // loop very fast and be in some indeterminate state.
        let mut mock_client = mock_manual_worker_client();
        mock_client
            .expect_poll_activity_task()
            .returning(|_, _| async { Err(tonic::Status::internal("ahhh")) }.boxed())
            .times(1);
        mock_client
            .expect_poll_activity_task()
            .returning(|_, _| future::pending().boxed());

        let cfg = test_worker_cfg()
            .max_outstanding_activities(5_usize)
            .build()
            .unwrap();
        let worker = Worker::new_test(cfg, mock_client);
        assert!(worker.activity_poll().await.is_err());
        assert_eq!(worker.at_task_mgr.unwrap().unused_permits(), Some(5));
    }

    #[test]
    fn max_polls_calculated_properly() {
        let cfg = test_worker_cfg()
            .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(5_usize))
            .build()
            .unwrap()
            .into();
        assert_eq!(
            wft_poller_behavior(&cfg, false),
            PollerBehavior::SimpleMaximum(1)
        );
        assert_eq!(
            wft_poller_behavior(&cfg, true),
            PollerBehavior::SimpleMaximum(4)
        );
    }

    #[test]
    fn max_polls_zero_is_err() {
        assert!(
            test_worker_cfg()
                .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(0_usize))
                .build()
                .is_err()
        );
    }
}
