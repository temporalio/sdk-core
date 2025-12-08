use crate::{
    WorkerClient,
    worker::{TaskPollers, WorkerTelemetry},
};
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc, time::Duration};
use temporalio_client::worker::SharedNamespaceWorkerTrait;
use temporalio_common::{
    protos::temporal::api::worker::v1::WorkerHeartbeat,
    worker::{PollerBehavior, WorkerConfig, WorkerTaskTypes, WorkerVersioningStrategy},
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Callback used to collect heartbeat data from each worker at the time of heartbeat
pub(crate) type HeartbeatFn = Arc<dyn Fn() -> WorkerHeartbeat + Send + Sync>;

/// SharedNamespaceWorker is responsible for polling nexus-delivered worker commands and sending
/// worker heartbeats to the server. This invokes callbacks on all workers in the same process that
/// share the same namespace.
pub(crate) struct SharedNamespaceWorker {
    heartbeat_map: Arc<RwLock<HashMap<Uuid, HeartbeatFn>>>,
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
        let config = WorkerConfig::builder()
            .namespace(namespace.clone())
            .task_queue(format!(
                "temporal-sys/worker-commands/{namespace}/{}",
                client.worker_grouping_key(),
            ))
            .task_types(WorkerTaskTypes::nexus_only())
            .max_outstanding_nexus_tasks(5_usize)
            .versioning_strategy(WorkerVersioningStrategy::None {
                build_id: "1.0".to_owned(),
            })
            .nexus_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .build()
            .expect("internal shared namespace worker options are valid");
        let worker = crate::worker::Worker::new_with_pollers(
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

        let heartbeat_map = Arc::new(RwLock::new(HashMap::<Uuid, HeartbeatFn>::new()));
        let heartbeat_map_clone = heartbeat_map.clone();

        tokio::spawn(async move {
            match client_clone.describe_namespace().await {
                Ok(namespace_resp) => {
                    if namespace_resp
                        .namespace_info
                        .and_then(|info| info.capabilities)
                        .map(|caps| caps.worker_heartbeats)
                        != Some(true)
                    {
                        warn!(
                            "Worker heartbeating configured for runtime, but server version does not support it."
                        );
                        worker.shutdown().await;
                        return;
                    }
                }
                Err(e) => {
                    warn!(error=?e, "Network error while describing namespace for heartbeat capabilities");
                    worker.shutdown().await;
                    return;
                }
            }
            let mut ticker = tokio::time::interval(heartbeat_interval);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let mut hb_to_send = Vec::new();
                        let hb_callbacks = {
                            heartbeat_map_clone.read().values().cloned().collect::<Vec<_>>()
                        };
                        for heartbeat_callback in hb_callbacks {
                            let mut heartbeat = heartbeat_callback();
                            // All of these heartbeat details rely on a client. To avoid circular
                            // dependencies, this must be populated from within SharedNamespaceWorker
                            // to get info from the current client
                            client_clone.set_heartbeat_client_fields(&mut heartbeat);
                            hb_to_send.push(heartbeat);
                        }
                        if let Err(e) = client_clone.record_worker_heartbeat(namespace_clone.clone(), hb_to_send).await {
                            if matches!(e.code(), tonic::Code::Unimplemented) {
                                worker.shutdown().await;
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
            .write()
            .insert(worker_instance_key, heartbeat_callback);
    }

    fn unregister_callback(&self, worker_instance_key: Uuid) -> (Option<HeartbeatFn>, bool) {
        let mut heartbeat_map = self.heartbeat_map.write();
        let heartbeat_callback = heartbeat_map.remove(&worker_instance_key);
        if heartbeat_map.is_empty() {
            self.cancel.cancel();
        }
        (heartbeat_callback, heartbeat_map.is_empty())
    }

    fn num_workers(&self) -> usize {
        self.heartbeat_map.read().len()
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
    use temporalio_common::{
        protos::temporal::api::{
            namespace::v1::{NamespaceInfo, namespace_info::Capabilities},
            workflowservice::v1::{DescribeNamespaceResponse, RecordWorkerHeartbeatResponse},
        },
        worker::PollerBehavior,
    };

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
        mock.expect_describe_namespace().returning(move || {
            Ok(DescribeNamespaceResponse {
                namespace_info: Some(NamespaceInfo {
                    capabilities: Some(Capabilities {
                        worker_heartbeats: true,
                        ..Capabilities::default()
                    }),
                    ..NamespaceInfo::default()
                }),
                ..DescribeNamespaceResponse::default()
            })
        });

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
        )
        .unwrap();

        tokio::time::sleep(Duration::from_millis(250)).await;
        worker.drain_activity_poller_and_shutdown().await;

        assert_eq!(3, heartbeat_count.load(Ordering::Relaxed));
    }
}
