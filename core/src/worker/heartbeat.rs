use crate::WorkerClient;
use crate::worker::{TaskPollers, WorkerTelemetry};
use parking_lot::Mutex;
use prost_types::Duration as PbDuration;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use temporal_client::SharedNamespaceWorkerTrait;
use temporal_sdk_core_api::worker::{
    PollerBehavior, WorkerConfigBuilder, WorkerVersioningStrategy,
};
use temporal_sdk_core_protos::temporal::api::worker::v1::WorkerHeartbeat;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Callback used to collect heartbeat data from each worker at the time of heartbeat
pub(crate) type HeartbeatFn = Arc<dyn Fn() -> WorkerHeartbeat + Send + Sync>;

/// SharedNamespaceWorker is responsible for polling nexus-delivered worker commands and sending
/// worker heartbeats to the server. This invokes callbacks on all workers in the same process that
/// share the same namespace.
pub(crate) struct SharedNamespaceWorker {
    heartbeat_map: Arc<Mutex<HashMap<Uuid, HeartbeatFn>>>,
    namespace: String,
    cancel: CancellationToken,
}

impl SharedNamespaceWorker {
    pub(crate) fn new(
        client: Arc<dyn WorkerClient>,
        namespace: String,
        heartbeat_interval: Duration,
        telemetry: Option<WorkerTelemetry>,
    ) -> Result<Self, anyhow::Error> {
        let config = WorkerConfigBuilder::default()
            .namespace(namespace.clone())
            .task_queue(format!(
                "temporal-sys/worker-commands/{namespace}/{}",
                client.worker_grouping_key(),
            ))
            .no_remote_activities(true)
            .max_outstanding_nexus_tasks(5_usize)
            .versioning_strategy(WorkerVersioningStrategy::None {
                build_id: "1.0".to_owned(),
            })
            .nexus_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .build()
            .expect("all required fields should be implemented");
        let worker = crate::worker::Worker::new_with_pollers_inner(
            config,
            None,
            client.clone(),
            TaskPollers::Real,
            telemetry,
            None,
            true,
        )?;

        let reset_notify = Arc::new(Notify::new());
        let cancel = CancellationToken::new();
        let cancel_clone = cancel.clone();

        let client_clone = client;
        let namespace_clone = namespace.clone();

        let heartbeat_map = Arc::new(Mutex::new(HashMap::<Uuid, HeartbeatFn>::new()));
        let heartbeat_map_clone = heartbeat_map.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(heartbeat_interval);
            let mut last_heartbeat_time = HashMap::new();
            let mut last_processed_tasks = HashMap::new();
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let mut hb_to_send = Vec::new();
                        for (instance_key, heartbeat_callback) in heartbeat_map_clone.lock().iter() {
                            let mut heartbeat = heartbeat_callback();
                            let heartbeat_time = last_heartbeat_time.get(instance_key).cloned();
                            let now = SystemTime::now();
                            let elapsed_since_last_heartbeat = heartbeat_time.map(
                                |hb_time| {
                                    let dur = now.duration_since(hb_time).unwrap_or(Duration::ZERO);
                                    PbDuration {
                                        seconds: dur.as_secs() as i64,
                                        nanos: dur.subsec_nanos() as i32,
                                    }
                                }
                            );

                            heartbeat.elapsed_since_last_heartbeat = elapsed_since_last_heartbeat;
                            heartbeat.heartbeat_time = Some(now.into());

                            process_slot_info(*instance_key, &mut heartbeat, &mut last_processed_tasks);

                            // All of these heartbeat details rely on a client. To avoid circular
                            // dependencies, this must be populated from within SharedNamespaceWorker
                            // to get info from the current client
                            heartbeat.worker_identity = client_clone.identity();
                            let sdk_name_and_ver = client_clone.sdk_name_and_version();
                            heartbeat.sdk_name = sdk_name_and_ver.0;
                            heartbeat.sdk_version = sdk_name_and_ver.1;

                            hb_to_send.push(heartbeat);

                            last_heartbeat_time.insert(*instance_key, now);
                        }
                        if let Err(e) = client_clone.record_worker_heartbeat(namespace_clone.clone(), hb_to_send).await {
                            if matches!(e.code(), tonic::Code::Unimplemented) {
                                return;
                            }
                            warn!(error=?e, "Network error while sending worker heartbeat");
                        }
                    }
                    _ = reset_notify.notified() => {
                        ticker.reset();
                    }
                    _ = cancel_clone.cancelled() => {
                        worker.shutdown().await;
                        return;
                    }
                }
            }
        });

        Ok(Self {
            heartbeat_map,
            namespace,
            cancel,
        })
    }
}

impl SharedNamespaceWorkerTrait for SharedNamespaceWorker {
    fn namespace(&self) -> String {
        self.namespace.clone()
    }

    fn register_callback(&self, worker_instance_key: Uuid, heartbeat_callback: HeartbeatFn) {
        self.heartbeat_map
            .lock()
            .insert(worker_instance_key, heartbeat_callback);
    }
    fn unregister_callback(&self, worker_instance_key: Uuid) -> (Option<HeartbeatFn>, bool) {
        let mut heartbeat_map = self.heartbeat_map.lock();
        let heartbeat_callback = heartbeat_map.remove(&worker_instance_key);
        if heartbeat_map.is_empty() {
            self.cancel.cancel();
        }
        (heartbeat_callback, heartbeat_map.is_empty())
    }

    fn num_workers(&self) -> usize {
        self.heartbeat_map.lock().len()
    }
}

#[derive(Default)]
struct SlotsInfo {
    last_interval_processed_tasks: i32,
    last_interval_failure_tasks: i32,
}

#[derive(Default)]
struct HeartbeatSlotsInfo {
    workflow_task_slots_info: SlotsInfo,
    activity_task_slots_info: SlotsInfo,
    nexus_task_slots_info: SlotsInfo,
    local_activity_slots_info: SlotsInfo,
}

fn process_slot_info(
    worker_instance_key: Uuid,
    heartbeat: &mut WorkerHeartbeat,
    slots_map: &mut HashMap<Uuid, HeartbeatSlotsInfo>,
) {
    let slots_info = slots_map.entry(worker_instance_key).or_default();
    if let Some(wft_slot_info) = heartbeat.workflow_task_slots_info.as_mut() {
        wft_slot_info.last_interval_processed_tasks = wft_slot_info.total_processed_tasks
            - slots_info
                .workflow_task_slots_info
                .last_interval_processed_tasks;
        wft_slot_info.last_interval_failure_tasks = wft_slot_info.total_failed_tasks
            - slots_info
                .workflow_task_slots_info
                .last_interval_failure_tasks;

        slots_info
            .workflow_task_slots_info
            .last_interval_processed_tasks = wft_slot_info.total_processed_tasks;
        slots_info
            .workflow_task_slots_info
            .last_interval_failure_tasks = wft_slot_info.total_failed_tasks;
    }

    if let Some(act_slot_info) = heartbeat.activity_task_slots_info.as_mut() {
        act_slot_info.last_interval_processed_tasks = act_slot_info.total_processed_tasks
            - slots_info
                .activity_task_slots_info
                .last_interval_processed_tasks;
        act_slot_info.last_interval_failure_tasks = act_slot_info.total_failed_tasks
            - slots_info
                .activity_task_slots_info
                .last_interval_failure_tasks;

        slots_info
            .activity_task_slots_info
            .last_interval_processed_tasks = act_slot_info.total_processed_tasks;
        slots_info
            .activity_task_slots_info
            .last_interval_failure_tasks = act_slot_info.total_failed_tasks;
    }

    if let Some(nexus_slot_info) = heartbeat.nexus_task_slots_info.as_mut() {
        nexus_slot_info.last_interval_processed_tasks = nexus_slot_info.total_processed_tasks
            - slots_info
                .nexus_task_slots_info
                .last_interval_processed_tasks;
        nexus_slot_info.last_interval_failure_tasks = nexus_slot_info.total_failed_tasks
            - slots_info.nexus_task_slots_info.last_interval_failure_tasks;

        slots_info
            .nexus_task_slots_info
            .last_interval_processed_tasks = nexus_slot_info.total_processed_tasks;
        slots_info.nexus_task_slots_info.last_interval_failure_tasks =
            nexus_slot_info.total_failed_tasks;
    }

    if let Some(la_slot_info) = heartbeat.local_activity_slots_info.as_mut() {
        la_slot_info.last_interval_processed_tasks = la_slot_info.total_processed_tasks
            - slots_info
                .local_activity_slots_info
                .last_interval_processed_tasks;
        la_slot_info.last_interval_failure_tasks = la_slot_info.total_failed_tasks
            - slots_info
                .local_activity_slots_info
                .last_interval_failure_tasks;

        slots_info
            .local_activity_slots_info
            .last_interval_processed_tasks = la_slot_info.total_processed_tasks;
        slots_info
            .local_activity_slots_info
            .last_interval_failure_tasks = la_slot_info.total_failed_tasks;
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        test_help::{WorkerExt, test_worker_cfg},
        worker,
        worker::client::mocks::mock_worker_client,
    };
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use temporal_sdk_core_api::worker::PollerBehavior;
    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::RecordWorkerHeartbeatResponse;

    #[tokio::test]
    async fn worker_heartbeat_basic() {
        let mut mock = mock_worker_client();
        let heartbeat_count = Arc::new(AtomicUsize::new(0));
        let heartbeat_count_clone = heartbeat_count.clone();
        mock.expect_poll_workflow_task()
            .returning(move |_namespace, _task_queue| Ok(Default::default()));
        mock.expect_poll_nexus_task()
            .returning(move |_poll_options, _send_heartbeat| Ok(Default::default()));
        mock.expect_record_worker_heartbeat().times(3).returning(
            move |_namespace, worker_heartbeat| {
                assert_eq!(1, worker_heartbeat.len());
                let heartbeat = worker_heartbeat[0].clone();
                let host_info = heartbeat.host_info.clone().unwrap();
                assert_eq!("test-identity", heartbeat.worker_identity);
                assert!(!heartbeat.worker_instance_key.is_empty());
                assert_eq!(
                    host_info.host_name,
                    gethostname::gethostname().to_string_lossy().to_string()
                );
                assert_eq!(host_info.process_id, std::process::id().to_string());
                assert_eq!(heartbeat.sdk_name, "test-core");
                assert_eq!(heartbeat.sdk_version, "0.0.0");
                assert!(heartbeat.heartbeat_time.is_some());
                assert!(heartbeat.start_time.is_some());

                heartbeat_count_clone.fetch_add(1, Ordering::Relaxed);

                Ok(RecordWorkerHeartbeatResponse {})
            },
        );

        let config = test_worker_cfg()
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .max_outstanding_activities(1_usize)
            .build()
            .unwrap();

        let client = Arc::new(mock);
        let worker = worker::Worker::new(
            config,
            None,
            client.clone(),
            None,
            Some(Duration::from_millis(100)),
            false,
        )
        .unwrap();

        tokio::time::sleep(Duration::from_millis(250)).await;
        worker.drain_activity_poller_and_shutdown().await;

        assert_eq!(3, heartbeat_count.load(Ordering::Relaxed));
    }
}
