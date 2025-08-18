use crate::{WorkerClient, abstractions::dbg_panic};
use gethostname::gethostname;
use parking_lot::Mutex;
use prost_types::Duration as PbDuration;
use std::{
    sync::{Arc, OnceLock},
    time::{Duration, SystemTime},
};
use temporal_sdk_core_api::worker::WorkerConfig;
use temporal_sdk_core_protos::temporal::api::worker::v1::{WorkerHeartbeat, WorkerHostInfo};
use tokio::{sync::Notify, task::JoinHandle, time::MissedTickBehavior};
use uuid::Uuid;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ClientIdentity {
    pub(crate) endpoint: String,
    pub(crate) namespace: String,
    pub(crate) task_queue: String,
}

/// SharedNamespaceWorker is responsible for polling nexus-delivered worker commands and sending
/// worker heartbeats to the server. This communicates with all workers in the same process that
/// share the same namespace.
pub(crate) struct SharedNamespaceWorker {
    heartbeat_map: Arc<Mutex<HashMap<String, HeartbeatFn>>>,
}

impl SharedNamespaceWorker {
    pub(crate) fn new(
        client: Arc<dyn WorkerClient>,
        client_identity: ClientIdentity,
        heartbeat_interval: Duration,
        telemetry: Option<&TelemetryInstance>,
        shutdown_callback: Arc<dyn Fn() + Send + Sync>,
        heartbeat_map: Arc<Mutex<HashMap<String, HeartbeatFn>>>,
    ) -> Self {
        let config = WorkerConfigBuilder::default()
            .namespace(client_identity.namespace.clone())
            .task_queue(client_identity.task_queue)
            .no_remote_activities(true)
            .max_outstanding_nexus_tasks(5_usize)
            .versioning_strategy(WorkerVersioningStrategy::None {
                build_id: "1.0".to_owned(),
            })
            .build()
            .expect("all required fields should be implemented");
        let worker =
            crate::worker::Worker::new(config.into(), None, client.clone(), telemetry, true);

        let last_heartbeat_time_map = Mutex::new(HashMap::new());

        // heartbeat task
        let reset_notify = Arc::new(Notify::new());
        let client_clone = client.clone();

        let heartbeat_map_clone = heartbeat_map.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(heartbeat_interval);
            ticker.tick().await;
            loop {
                // TODO: Race condition here, can technically shut down before anything is ever initialized
                if heartbeat_map_clone.lock().is_empty() {
                    worker.shutdown().await;
                    // remove this worker from the Runtime map
                    shutdown_callback();
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
                        if let Err(e) = client_clone.record_worker_heartbeat(client_identity.namespace.clone(), client_identity.endpoint.clone(), hb_to_send
                            ).await {
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

        Self { heartbeat_map }
    }

    /// Adds `WorkerHeartbeatData` to the `SharedNamespaceWorker` and adds a `HeartbeatCallback` to
    /// the client that Worker
    pub(crate) fn register_callback(
        &mut self,
        worker_instance_key: String,
        heartbeat_callback: HeartbeatFn,
    ) -> Arc<dyn Fn() + Send + Sync> {
        self.heartbeat_map
            .lock()
            .insert(worker_instance_key.clone(), heartbeat_callback);

        let heartbeat_map_clone = self.heartbeat_map.clone();
        Arc::new(move || {
            if heartbeat_map_clone
                .lock()
                .remove(&worker_instance_key)
                .is_none()
            {
                dbg_panic!(
                    "Attempted to remove key {} that was already removed",
                    worker_instance_key
                );
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_help::{WorkerExt, test_worker_cfg},
        worker,
        worker::client::mocks::mock_worker_client,
    };
    use std::{sync::Arc, time::Duration};
    use temporal_sdk_core_api::worker::PollerBehavior;
    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::RecordWorkerHeartbeatResponse;
    use uuid::Uuid;

    #[tokio::test]
    async fn worker_heartbeat() {
        let mut mock = mock_worker_client();
        let heartbeat_count = Arc::new(Mutex::new(0));
        let heartbeat_count_clone = heartbeat_count.clone();
        mock.expect_record_worker_heartbeat().times(2).returning(
            move |_namespace, _identity, worker_heartbeat| {
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

                let mut count = heartbeat_count_clone.lock();
                *count += 1;

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
        let worker = worker::Worker::new(config, None, client.clone(), None, false);

        let namespace = "test-namespace".to_string();
        let task_queue_key = Uuid::new_v4();
        let mut shared_namespace_worker = SharedNamespaceWorker::new(
            client,
            ClientIdentity {
                endpoint: "test-endpoint".to_string(),
                namespace: namespace.clone(),
                task_queue: format!(
                    "temporal-sys/worker-commands/{}/{}",
                    namespace, task_queue_key,
                ),
            },
            Duration::from_millis(100),
            None,
            Arc::new(move || {}),
            Arc::new(Mutex::new(HashMap::new())),
        );
        let worker_instance_key = worker.worker_instance_key();
        shared_namespace_worker.register_callback(
            worker_instance_key,
            worker
                .get_heartbeat_callback()
                .expect("heartbeat callback should be set"),
        );

        tokio::time::sleep(Duration::from_millis(250)).await;
        worker.drain_activity_poller_and_shutdown().await;

        assert_eq!(2, *heartbeat_count.lock());
    }
}
