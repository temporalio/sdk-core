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
pub use workflow::LEGACY_QUERY_ID;

use crate::telemetry::WorkerHeartbeatMetrics;
use crate::worker::heartbeat::{HeartbeatFn, SharedNamespaceWorker};
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
use anyhow::bail;
use futures_util::{StreamExt, stream};
use gethostname::gethostname;
use parking_lot::{Mutex, RwLock};
use slot_provider::SlotProvider;
use std::time::SystemTime;
use std::{
    convert::TryInto,
    future,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use sysinfo::System;
use temporal_client::{ClientWorker, HeartbeatCallback, Slot as SlotTrait};
use temporal_client::{
    ConfiguredClient, SharedNamespaceWorkerTrait, TemporalServiceClientWithMetrics,
};
use temporal_sdk_core_api::telemetry::metrics::TemporalMeter;
use temporal_sdk_core_api::worker::{
    ActivitySlotKind, LocalActivitySlotKind, NexusSlotKind, SlotKind, WorkflowSlotKind,
};
use temporal_sdk_core_api::{
    errors::{CompleteNexusError, WorkerValidationError},
    worker::PollerBehavior,
};
use temporal_sdk_core_protos::temporal::api::deployment;
use temporal_sdk_core_protos::temporal::api::enums::v1::WorkerStatus;
use temporal_sdk_core_protos::temporal::api::worker::v1::{
    WorkerHeartbeat, WorkerHostInfo, WorkerPollerInfo, WorkerSlotsInfo,
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
use tokio::sync::{mpsc::unbounded_channel, watch};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::Subscriber;
use uuid::Uuid;
#[cfg(any(feature = "test-utilities", test))]
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
    config: WorkerConfig,
    client: Arc<dyn WorkerClient>,
    /// Worker instance key, unique identifier for this worker
    worker_instance_key: Uuid,
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
    post_activate_hook: Option<Box<dyn Fn(&Self, PostActivateHookData<'_>) + Send + Sync>>,
    /// Set when non-local activities are complete and should stop being polled
    non_local_activities_complete: Arc<AtomicBool>,
    /// Set when local activities are complete and should stop being polled
    local_activities_complete: Arc<AtomicBool>,
    /// Used to track all permits have been released
    all_permits_tracker: tokio::sync::Mutex<AllPermitsTracker>,
    /// Used to track worker client
    client_worker_registrator: Arc<ClientWorkerRegistrator>,
    /// Status
    status: Arc<Mutex<WorkerStatus>>,
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

#[derive(Clone)]
pub(crate) struct WorkerTelemetry {
    metric_meter: Option<TemporalMeter>,
    temporal_metric_meter: Option<TemporalMeter>,
    trace_subscriber: Option<Arc<dyn Subscriber + Send + Sync>>,
    in_memory_meter: Option<Arc<WorkerHeartbeatMetrics>>,
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

    fn get_config(&self) -> &WorkerConfig {
        &self.config
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
        *self.status.lock() = WorkerStatus::ShuttingDown;
        // First, unregister worker from the client
        if !self.client_worker_registrator.shared_namespace_worker {
            let _res = self
                .client
                .workers()
                .unregister_worker(self.worker_instance_key);
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

    fn worker_instance_key(&self) -> Uuid {
        self.worker_instance_key
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
        worker_heartbeat_interval: Option<Duration>,
        shared_namespace_worker: bool,
    ) -> Result<Worker, anyhow::Error> {
        info!(task_queue=%config.task_queue, namespace=%config.namespace, "Initializing worker");

        Self::new_with_pollers(
            config,
            sticky_queue_name,
            client,
            TaskPollers::Real,
            telem_instance,
            worker_heartbeat_interval,
            shared_namespace_worker,
        )
    }

    /// Replace client and return a new client.
    ///
    /// For eager workflow purposes, this new client will now apply to future eager start requests
    /// and the older client will not. Note, if this registration fails, the worker heartbeat will
    /// also not be registered.
    ///
    /// For worker heartbeat, this will remove an existing shared worker if it is the last worker of
    /// the old client and create a new nexus worker if it's the first client of the namespace on
    /// the new client.
    pub fn replace_client(
        &self,
        new_client: ConfiguredClient<TemporalServiceClientWithMetrics>,
    ) -> Result<(), anyhow::Error> {
        // Unregister worker from current client, register in new client at the end
        let client_worker = self
            .client
            .workers()
            .unregister_worker(self.worker_instance_key)?;
        let new_worker_client = super::init_worker_client(
            self.config.namespace.clone(),
            self.config.client_identity_override.clone(),
            new_client,
        );

        self.client.replace_client(new_worker_client);
        *self.client_worker_registrator.client.write() = self.client.clone();
        self.client.workers().register_worker(client_worker)
    }

    #[cfg(test)]
    pub(crate) fn new_test(config: WorkerConfig, client: impl WorkerClient + 'static) -> Self {
        Self::new(config, None, Arc::new(client), None, None, false).unwrap()
    }

    pub(crate) fn new_with_pollers(
        config: WorkerConfig,
        sticky_queue_name: Option<String>,
        client: Arc<dyn WorkerClient>,
        task_pollers: TaskPollers,
        telem_instance: Option<&TelemetryInstance>,
        worker_heartbeat_interval: Option<Duration>,
        shared_namespace_worker: bool,
    ) -> Result<Worker, anyhow::Error> {
        let worker_telemetry = telem_instance.map(|telem| WorkerTelemetry {
            metric_meter: telem.get_metric_meter(),
            temporal_metric_meter: telem.get_temporal_metric_meter(),
            trace_subscriber: telem.trace_subscriber(),
            in_memory_meter: telem.in_memory_metrics(),
        });

        Worker::new_with_pollers_inner(
            config,
            sticky_queue_name,
            client,
            task_pollers,
            worker_telemetry,
            worker_heartbeat_interval,
            shared_namespace_worker,
        )
    }

    pub(crate) fn new_with_pollers_inner(
        config: WorkerConfig,
        sticky_queue_name: Option<String>,
        client: Arc<dyn WorkerClient>,
        task_pollers: TaskPollers,
        worker_telemetry: Option<WorkerTelemetry>,
        worker_heartbeat_interval: Option<Duration>,
        shared_namespace_worker: bool, // TODO: is this unnecessary?
    ) -> Result<Worker, anyhow::Error> {
        // let shared_namespace_worker = in_memory_meter.is_none();
        let (metrics, meter) = if let Some(wt) = worker_telemetry.as_ref() {
            (
                MetricsContext::top_level_with_meter(
                    config.namespace.clone(),
                    config.task_queue.clone(),
                    wt.temporal_metric_meter.clone(),
                    wt.in_memory_meter.clone(),
                ),
                wt.metric_meter.clone(),
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
            worker_identity: client.identity(),
            worker_deployment_version: config.computed_deployment_version(),
        });
        let wft_slots = MeteredPermitDealer::new(
            tuner.workflow_task_slot_supplier(),
            metrics.with_new_attrs([workflow_worker_type()]),
            if config.max_cached_workflows > 0 {
                // Since we always need to be able to poll the normal task queue as well as the
                // sticky queue, we need a value of at least 2 here.
                Some(std::cmp::max(2, config.max_cached_workflows))
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

        let wf_last_suc_poll_time = Arc::new(Mutex::new(None));
        let wf_sticky_last_suc_poll_time = Arc::new(Mutex::new(None));
        let act_last_suc_poll_time = Arc::new(Mutex::new(None));
        let nexus_last_suc_poll_time = Arc::new(Mutex::new(None));

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
                    wf_last_suc_poll_time.clone(),
                    wf_sticky_last_suc_poll_time.clone(),
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
                    // activity poller
                    let ap = LongPollBuffer::new_activity_task(
                        client.clone(),
                        config.task_queue.clone(),
                        config.activity_task_poller_behavior,
                        act_slots.clone(),
                        shutdown_token.child_token(),
                        Some(move |np| act_metrics.record_num_pollers(np)),
                        ActivityTaskOptions {
                            max_worker_acts_per_second: config.max_worker_activities_per_second,
                            max_tps: config.max_task_queue_activities_per_second,
                        },
                        act_last_suc_poll_time.clone(),
                    );
                    Some(Box::from(ap) as BoxedActPoller)
                };

                let np_metrics = metrics.with_new_attrs([nexus_poller()]);

                let nexus_poll_buffer = Box::new(LongPollBuffer::new_nexus_task(
                    client.clone(),
                    config.task_queue.clone(),
                    config.nexus_task_poller_behavior,
                    nexus_slots.clone(),
                    shutdown_token.child_token(),
                    Some(move |np| np_metrics.record_num_pollers(np)),
                    nexus_last_suc_poll_time.clone(),
                    shared_namespace_worker,
                )) as BoxedNexusPoller;

                #[cfg(any(feature = "test-utilities", test))]
                let wft_stream = wft_stream.left_stream();
                (wft_stream, act_poll_buffer, nexus_poll_buffer)
            }
            #[cfg(any(feature = "test-utilities", test))]
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
            la_permit_dealer.clone(),
            hb_tx,
            metrics.clone(),
        ));
        let at_task_mgr = act_poller.map(|ap| {
            WorkerActivityTasks::new(
                act_slots.clone(),
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
        if !poll_on_non_local_activities && !shared_namespace_worker {
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
        let worker_instance_key = Uuid::new_v4();
        let worker_status = Arc::new(Mutex::new(WorkerStatus::Running));

        let sdk_name_and_ver = client.sdk_name_and_version();
        let worker_heartbeat = worker_heartbeat_interval.map(|hb_interval| {
            WorkerHeartbeatManager::new(
                config.clone(),
                worker_instance_key,
                hb_interval,
                worker_telemetry.clone(),
                wft_slots.clone(),
                act_slots,
                nexus_slots,
                la_permit_dealer,
                wf_last_suc_poll_time,
                wf_sticky_last_suc_poll_time,
                act_last_suc_poll_time,
                nexus_last_suc_poll_time,
                worker_status.clone(),
            )
        });

        let client_worker_registrator = Arc::new(ClientWorkerRegistrator {
            worker_instance_key,
            slot_provider: provider,
            heartbeat_manager: worker_heartbeat,
            client: RwLock::new(client.clone()),
            shared_namespace_worker,
        });

        if !shared_namespace_worker {
            client
                .workers()
                .register_worker(client_worker_registrator.clone())?;
        }

        Ok(Self {
            worker_instance_key,
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
                worker_telemetry
                    .as_ref()
                    .and_then(|telem| telem.trace_subscriber.clone()),
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
            client_worker_registrator,
            status: worker_status,
        })
    }

    /// Will shutdown the worker. Does not resolve until all outstanding workflow tasks have been
    /// completed
    async fn shutdown(&self) {
        self.initiate_shutdown();
        if let Some(name) = self.workflows.get_sticky_queue_name() {
            let heartbeat = self
                .client_worker_registrator
                .heartbeat_manager
                .as_ref()
                .map(|hm| hm.heartbeat_callback.clone()());

            // This is a best effort call and we can still shutdown the worker if it fails
            match self.client.shutdown_worker(name, heartbeat).await {
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
        callback: impl Fn(&Self, PostActivateHookData<'_>) + Send + Sync + 'static,
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
}

struct ClientWorkerRegistrator {
    worker_instance_key: Uuid,
    slot_provider: SlotProvider,
    heartbeat_manager: Option<WorkerHeartbeatManager>,
    client: RwLock<Arc<dyn WorkerClient>>,
    shared_namespace_worker: bool,
}

impl ClientWorker for ClientWorkerRegistrator {
    fn namespace(&self) -> &str {
        self.slot_provider.namespace()
    }
    fn task_queue(&self) -> &str {
        self.slot_provider.task_queue()
    }

    fn try_reserve_wft_slot(&self) -> Option<Box<dyn SlotTrait + Send>> {
        self.slot_provider.try_reserve_wft_slot()
    }

    fn worker_instance_key(&self) -> Uuid {
        self.worker_instance_key
    }

    fn heartbeat_enabled(&self) -> bool {
        self.heartbeat_manager.is_some()
    }

    fn heartbeat_callback(&self) -> Option<HeartbeatCallback> {
        if let Some(hb_mgr) = self.heartbeat_manager.as_ref() {
            Some(hb_mgr.heartbeat_callback.clone())
        } else {
            None
        }
    }

    fn new_shared_namespace_worker(
        &self,
    ) -> Result<Box<dyn SharedNamespaceWorkerTrait + Send + Sync>, anyhow::Error> {
        if let Some(ref hb_mgr) = self.heartbeat_manager {
            Ok(Box::new(SharedNamespaceWorker::new(
                self.client.read().clone(),
                self.namespace().to_string(),
                hb_mgr.heartbeat_interval,
                hb_mgr.telemetry.clone(),
            )?))
        } else {
            bail!("Shared namespace worker creation never be called without a heartbeat manager");
        }
    }
}

struct WorkerHeartbeatManager {
    /// Heartbeat interval, defaults to 60s
    heartbeat_interval: Duration,
    /// Telemetry instance, needed to initialize [SharedNamespaceWorker] when replacing client
    telemetry: Option<WorkerTelemetry>,
    /// Heartbeat callback
    heartbeat_callback: Arc<dyn Fn() -> WorkerHeartbeat + Send + Sync>,
}

impl WorkerHeartbeatManager {
    #[allow(clippy::too_many_arguments)]
    fn new(
        config: WorkerConfig,
        worker_instance_key: Uuid,
        heartbeat_interval: Duration,
        telemetry_instance: Option<WorkerTelemetry>,
        wft_slots: MeteredPermitDealer<WorkflowSlotKind>,
        act_slots: MeteredPermitDealer<ActivitySlotKind>,
        nexus_slots: MeteredPermitDealer<NexusSlotKind>,
        la_slots: MeteredPermitDealer<LocalActivitySlotKind>,
        wf_last_suc_poll_time: Arc<Mutex<Option<SystemTime>>>,
        wf_sticky_last_suc_poll_time: Arc<Mutex<Option<SystemTime>>>,
        act_last_suc_poll_time: Arc<Mutex<Option<SystemTime>>>,
        nexus_last_suc_poll_time: Arc<Mutex<Option<SystemTime>>>,
        status: Arc<Mutex<WorkerStatus>>,
    ) -> Self {
        let task_queue = config.task_queue.clone();
        let deployment_version = config.computed_deployment_version();
        let deployment_version =
            deployment_version.map(|dv| deployment::v1::WorkerDeploymentVersion {
                deployment_name: dv.deployment_name,
                build_id: dv.build_id,
            });

        let telemetry_instance_clone = telemetry_instance.clone();

        let worker_heartbeat_callback: HeartbeatFn = Arc::new(move || {
            let mut sys = System::new_all();
            sys.refresh_all();
            std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
            sys.refresh_cpu_usage();
            let current_host_cpu_usage: f32 =
                sys.cpus().iter().map(|cpu| cpu.cpu_usage()).sum::<f32>() / sys.cpus().len() as f32;
            let total_mem = sys.total_memory() as f64;
            let used_mem = sys.used_memory() as f64;
            let current_host_mem_usage = (used_mem / total_mem) as f32;

            let mut worker_heartbeat = WorkerHeartbeat {
                worker_instance_key: worker_instance_key.to_string(),
                host_info: Some(WorkerHostInfo {
                    host_name: gethostname().to_string_lossy().to_string(),
                    process_id: std::process::id().to_string(),
                    current_host_cpu_usage,
                    current_host_mem_usage,

                    // Set by SharedNamespaceWorker because it relies on the client
                    process_key: String::new(),
                }),
                task_queue: task_queue.clone(),
                deployment_version: deployment_version.clone(),

                status: (*status.lock()) as i32,
                start_time: Some(SystemTime::now().into()),
                plugins: config.plugins.clone(),

                // Metrics dependent, set below
                workflow_task_slots_info: None,
                activity_task_slots_info: None,
                nexus_task_slots_info: None,
                local_activity_slots_info: None,
                workflow_poller_info: None,
                workflow_sticky_poller_info: None,
                activity_poller_info: None,
                nexus_poller_info: None,
                total_sticky_cache_hit: 0,
                total_sticky_cache_miss: 0,
                current_sticky_cache_size: 0,

                // sdk_name, sdk_version, and worker_identity must be set by
                // SharedNamespaceWorker because they rely on the client, and
                // need to be pulled from the current client used by SharedNamespaceWorker
                worker_identity: String::new(),
                heartbeat_time: None,
                elapsed_since_last_heartbeat: None,
                sdk_name: String::new(),
                sdk_version: String::new(),
            };

            if let Some(telem_instance) = telemetry_instance_clone.as_ref()
                && let Some(in_mem) = telem_instance.in_memory_meter.as_ref()
            {
                worker_heartbeat.total_sticky_cache_hit =
                    in_mem.total_sticky_cache_hit.load(Ordering::Relaxed) as i32;
                worker_heartbeat.total_sticky_cache_miss =
                    in_mem.total_sticky_cache_miss.load(Ordering::Relaxed) as i32;
                worker_heartbeat.current_sticky_cache_size =
                    in_mem.sticky_cache_size.load(Ordering::Relaxed) as i32;
                // TODO: Is this ever not Some()?
                worker_heartbeat.workflow_poller_info = Some(WorkerPollerInfo {
                    current_pollers: in_mem
                        .num_pollers
                        .wft_current_pollers
                        .load(Ordering::Relaxed) as i32,
                    last_successful_poll_time: wf_last_suc_poll_time.lock().map(|time| time.into()),
                    is_autoscaling: config.workflow_task_poller_behavior.is_autoscaling(),
                });

                worker_heartbeat.workflow_sticky_poller_info = Some(WorkerPollerInfo {
                    current_pollers: in_mem
                        .num_pollers
                        .sticky_wft_current_pollers
                        .load(Ordering::Relaxed) as i32,
                    last_successful_poll_time: wf_sticky_last_suc_poll_time
                        .lock()
                        .map(|time| time.into()),
                    is_autoscaling: config.workflow_task_poller_behavior.is_autoscaling(),
                });
                worker_heartbeat.activity_poller_info = Some(WorkerPollerInfo {
                    current_pollers: in_mem
                        .num_pollers
                        .activity_current_pollers
                        .load(Ordering::Relaxed) as i32,
                    last_successful_poll_time: act_last_suc_poll_time
                        .lock()
                        .map(|time| time.into()),
                    is_autoscaling: config.activity_task_poller_behavior.is_autoscaling(),
                });
                worker_heartbeat.nexus_poller_info = Some(WorkerPollerInfo {
                    current_pollers: in_mem
                        .num_pollers
                        .nexus_current_pollers
                        .load(Ordering::Relaxed) as i32,
                    last_successful_poll_time: nexus_last_suc_poll_time
                        .lock()
                        .map(|time| time.into()),
                    is_autoscaling: config.nexus_task_poller_behavior.is_autoscaling(),
                });

                worker_heartbeat.workflow_task_slots_info = make_slots_info(
                    &wft_slots,
                    in_mem
                        .workflow_task_execution_latency
                        .load(Ordering::Relaxed),
                    in_mem
                        .workflow_task_execution_failed
                        .load(Ordering::Relaxed),
                );
                worker_heartbeat.activity_task_slots_info = make_slots_info(
                    &act_slots,
                    in_mem.activity_execution_latency.load(Ordering::Relaxed),
                    in_mem.activity_execution_failed.load(Ordering::Relaxed),
                );
                worker_heartbeat.nexus_task_slots_info = make_slots_info(
                    &nexus_slots,
                    in_mem.nexus_task_execution_latency.load(Ordering::Relaxed),
                    in_mem.nexus_task_execution_failed.load(Ordering::Relaxed),
                );
                worker_heartbeat.local_activity_slots_info = make_slots_info(
                    &la_slots,
                    in_mem
                        .local_activity_execution_latency
                        .load(Ordering::Relaxed),
                    in_mem
                        .local_activity_execution_failed
                        .load(Ordering::Relaxed),
                );
            }
            worker_heartbeat
        });

        WorkerHeartbeatManager {
            heartbeat_interval,
            telemetry: telemetry_instance,
            heartbeat_callback: worker_heartbeat_callback,
        }
    }
}

pub(crate) struct PostActivateHookData<'a> {
    pub(crate) run_id: &'a str,
    pub(crate) replaying: bool,
}

pub(crate) enum TaskPollers {
    Real,
    #[cfg(any(feature = "test-utilities", test))]
    Mocked {
        wft_stream: BoxStream<'static, Result<ValidPollWFTQResponse, tonic::Status>>,
        act_poller: Option<BoxedPoller<PollActivityTaskQueueResponse>>,
        nexus_poller: BoxedPoller<PollNexusTaskQueueResponse>,
    },
}

fn wft_poller_behavior(config: &WorkerConfig, is_sticky: bool) -> PollerBehavior {
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

fn make_slots_info<SK>(
    dealer: &MeteredPermitDealer<SK>,
    total_processed: u64,
    total_failed: u64,
) -> Option<WorkerSlotsInfo>
where
    SK: SlotKind + 'static,
{
    let avail_usize = dealer.available_permits()?;
    let max_usize = dealer.max_permits()?;

    let avail = i32::try_from(avail_usize).unwrap_or(i32::MAX);
    let max = i32::try_from(max_usize).unwrap_or(i32::MAX);

    let used = (max - avail).max(0);

    Some(WorkerSlotsInfo {
        current_available_slots: avail,
        current_used_slots: used,
        slot_supplier_kind: SK::kind().to_string(),
        total_processed_tasks: i32::try_from(total_processed).unwrap_or(i32::MAX),
        total_failed_tasks: i32::try_from(total_failed).unwrap_or(i32::MAX),

        // Filled in by heartbeat later
        last_interval_processed_tasks: 0,
        last_interval_failure_tasks: 0,
    })
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
            .unwrap();
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
