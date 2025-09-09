use crate::WorkerClient;
use crate::worker::{TaskPollers, WorkerTelemetry};
use parking_lot::Mutex;
use prost_types::Duration as PbDuration;
use std::collections::HashMap;
use std::{
    fmt,
    sync::Arc,
    time::{Duration, SystemTime},
};
use temporal_client::SharedNamespaceWorkerTrait;
use temporal_sdk_core_api::worker::{WorkerConfigBuilder, WorkerVersioningStrategy};
use temporal_sdk_core_protos::temporal::api::worker::v1::WorkerHeartbeat;
use tokio::sync::Notify;

/// Callback used to collect heartbeat data from each worker at the time of heartbeat
pub(crate) type HeartbeatFn = Box<dyn Fn() -> WorkerHeartbeat + Send + Sync>;

/// SharedNamespaceWorker is responsible for polling nexus-delivered worker commands and sending
/// worker heartbeats to the server. This communicates with all workers in the same process that
/// share the same namespace.
pub(crate) struct SharedNamespaceWorker {
    heartbeat_map: Arc<Mutex<HashMap<String, HeartbeatFn>>>,
    namespace: String,
}

impl SharedNamespaceWorker {
    pub(crate) fn new(
        client: Arc<dyn WorkerClient>,
        namespace: String,
        heartbeat_interval: Duration,
        telemetry: Option<WorkerTelemetry>,
    ) -> Self {
        let config = WorkerConfigBuilder::default()
            .namespace(namespace.clone())
            .task_queue(format!(
                "temporal-sys/worker-commands/{namespace}/{}",
                client.get_process_key()
            ))
            .no_remote_activities(true)
            .max_outstanding_nexus_tasks(5_usize)
            .versioning_strategy(WorkerVersioningStrategy::None {
                build_id: "1.0".to_owned(),
            })
            .build()
            .expect("all required fields should be implemented");
        let worker = crate::worker::Worker::new_with_pollers_inner(
            config,
            None,
            client.clone(),
            TaskPollers::Real,
            telemetry,
            None,
        );

        let last_heartbeat_time_map = Mutex::new(HashMap::new());

        // heartbeat task
        let reset_notify = Arc::new(Notify::new());
        let client_clone = client;
        let namespace_clone = namespace.clone();

        let heartbeat_map = Arc::new(Mutex::new(HashMap::<String, HeartbeatFn>::new()));
        let heartbeat_map_clone = heartbeat_map.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(heartbeat_interval);
            ticker.tick().await;
            loop {
                // TODO: Race condition here, can technically shut down before anything is ever initialized
                if heartbeat_map_clone.lock().is_empty() {
                    println!(
                        "// TODO: Race condition here, can technically shut down before anything is ever initialized"
                    );
                    worker.shutdown().await;
                    return;
                }
                
                tokio::select! {
                    _ = ticker.tick() => {
                        let mut hb_to_send = Vec::new();
                        for (instance_key, heartbeat_callback) in heartbeat_map_clone.lock().iter() {
                            let mut heartbeat = heartbeat_callback();
                            let mut last_heartbeat_time_map = last_heartbeat_time_map.lock();
                            let heartbeat_time = last_heartbeat_time_map.get(instance_key).cloned();

                            let now = SystemTime::now();
                            let elapsed_since_last_heartbeat = if let Some(heartbeat_time) = heartbeat_time {
                                let dur = now.duration_since(heartbeat_time).unwrap_or(Duration::ZERO);
                                Some(PbDuration {
                                    seconds: dur.as_secs() as i64,
                                    nanos: dur.subsec_nanos() as i32,
                                })
                            } else {
                                None
                            };

                            heartbeat.elapsed_since_last_heartbeat = elapsed_since_last_heartbeat;
                            heartbeat.heartbeat_time = Some(now.into());

                            hb_to_send.push(heartbeat);

                            last_heartbeat_time_map.insert(instance_key.clone(), now);
                        }
                        if let Err(e) = client_clone.record_worker_heartbeat(namespace_clone.clone(), hb_to_send).await {
                            if matches!(
                            e.code(),
                            tonic::Code::Unimplemented
                            ) {
                                return;
                            }
                            warn!(error=?e, "Network error while sending worker heartbeat");
                        }
                    }
                    _ = reset_notify.notified() => {
                        ticker.reset();
                    }
                }
            }
        });

        Self {
            heartbeat_map,
            namespace,
        }
    }
}

impl SharedNamespaceWorkerTrait for SharedNamespaceWorker {
    fn namespace(&self) -> String {
        self.namespace.clone()
    }

    fn unregister_callback(
        &self,
        worker_instance_key: String,
    ) -> (Option<Box<dyn Fn() -> WorkerHeartbeat + Send + Sync>>, bool) {
        let mut heartbeat_map = self.heartbeat_map.lock();
        (
            heartbeat_map.remove(&worker_instance_key),
            heartbeat_map.is_empty(),
        )
    }
    fn register_callback(
        &self,
        worker_instance_key: String,
        heartbeat_callback: Box<dyn Fn() -> WorkerHeartbeat + Send + Sync>,
    ) {
        self.heartbeat_map
            .lock()
            .insert(worker_instance_key, heartbeat_callback);
    }
}

impl fmt::Debug for SharedNamespaceWorker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.heartbeat_map.try_lock() {
            Some(guard) => {
                let keys: Vec<_> = guard.keys().cloned().collect();
                f.debug_struct("SharedNamespaceWorker")
                    .field("namespace", &self.namespace)
                    .field("heartbeat_keys", &keys)
                    .finish()
            }
            None => f
                .debug_struct("SharedNamespaceWorker")
                .field("namespace", &self.namespace)
                .field("heartbeat_map", &"<locked>")
                .finish(),
        }
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
        mock.expect_record_worker_heartbeat().times(2).returning(
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
                assert_eq!(heartbeat.task_queue, "q");
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
            .unwrap()
            .into();

        let client = Arc::new(mock);
        let worker = worker::Worker::new(
            config,
            None,
            client.clone(),
            None,
            Some(Duration::from_millis(100)),
        );

        tokio::time::sleep(Duration::from_millis(250)).await;
        worker.drain_activity_poller_and_shutdown().await;

        assert_eq!(2, heartbeat_count.load(Ordering::Relaxed));
    }
}
