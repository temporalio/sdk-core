use crate::WorkerClient;
use crate::abstractions::{MeteredPermitDealer, dbg_panic};
use crate::pollers::{BoxedNexusPoller, LongPollBuffer};
use crate::telemetry::metrics::{MetricsContext, nexus_poller};
use crate::worker::nexus::NexusManager;
use gethostname::gethostname;
use parking_lot::Mutex;
use prost_types::Duration as PbDuration;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};
use temporal_client::Client;
use temporal_sdk_core_api::worker::{NexusSlotKind, PollerBehavior, WorkerConfig};
use temporal_sdk_core_protos::temporal::api::worker::v1::{WorkerHeartbeat, WorkerHostInfo};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::{
    PollNexusTaskQueueResponse, RecordWorkerHeartbeatRequest,
};
use tokio::sync::Notify;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub(crate) type HeartbeatFn = Arc<dyn Fn() -> WorkerHeartbeat + Send + Sync>;
pub(crate) type HeartbeatMap = HashMap<ClientIdentity, HeartbeatNexusManager>;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ClientIdentity {
    pub(crate) endpoint: String,
    pub(crate) namespace: String,
    pub(crate) task_queue: String,
}

pub(crate) struct HeartbeatNexusManager {
    pub(crate) heartbeats: Vec<HeartbeatFn>,
    // pub(crate) manager: NexusManager,
}

impl HeartbeatNexusManager {
    pub(crate) fn new(
        client: Arc<dyn WorkerClient>,
        key: ClientIdentity,
        poller_behavior: PollerBehavior,
        permit_dealer: MeteredPermitDealer<NexusSlotKind>,
        shutdown: CancellationToken, // TODO: duplicate with shutdown_initiated_token?
        metrics: MetricsContext,
        graceful_shutdown: Option<Duration>,
        shutdown_initiated_token: CancellationToken,
    ) -> Self {
        let np_metrics = metrics.with_new_attrs([nexus_poller()]);
        let poller = Box::new(LongPollBuffer::new_nexus_task(
            client,
            key.task_queue.clone(),
            poller_behavior,
            permit_dealer,
            shutdown,
            Some(move |np| np_metrics.record_num_pollers(np)),
        )) as BoxedNexusPoller;

        let manager = NexusManager::new(
            poller,
            metrics.clone(),
            graceful_shutdown,
            shutdown_initiated_token,
        );

        tokio::spawn(async move {
            loop {
                let t = manager.next_nexus_task().await.unwrap(); // TODO: unwrap
                // TODO: How do I go from NexusTask to FetchWorkerConfigRequest or UpdateWorkerConfigRequest
                
                // TODO: pass to task_tx to let main task handle incoming requests
            }
        });


        Self {
            heartbeats: Vec::new(),
            // manager,
        }
    }

    pub(crate) fn register_heartbeat(&mut self, heartbeat_fn: HeartbeatFn) {
        self.heartbeats.push(heartbeat_fn)
    }
}

pub(crate) struct WorkerHeartbeatManager {
    config: WorkerConfig,
    client: Arc<dyn WorkerClient>,
    heartbeat_handle: JoinHandle<()>,
    heartbeat_map: Arc<Mutex<HeartbeatMap>>,
    permit_dealer: MeteredPermitDealer<NexusSlotKind>,
    shutdown_initiated_token: CancellationToken,
    metrics: MetricsContext,
    task_tx: UnboundedSender<()>,
    task_rx: UnboundedReceiver<()>,
}

impl WorkerHeartbeatManager {
    pub(crate) fn new(
        config: WorkerConfig,
        identity: String,
        client: Arc<dyn WorkerClient>,
        heartbeat_map: Arc<Mutex<HeartbeatMap>>,
        permit_dealer: MeteredPermitDealer<NexusSlotKind>,
        metrics: MetricsContext,
        shutdown_initiated_token: CancellationToken,
    ) -> Self {
        let sdk_name_and_ver = client.sdk_name_and_version();
        let reset_notify = Arc::new(Notify::new());
        let heartbeat_map_clone = heartbeat_map.clone();
        let config_clone = config.clone();
        // TODO: This client is just the client of the first worker registered to the process, seems
        //  okay to give it the whole responsibility to worker heartbeat on timer
        let client_clone = client.clone();
        let heartbeat_handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(config_clone.heartbeat_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        for (key, heartbeat_mgr) in heartbeat_map_clone.lock().iter() {
                            let heartbeats = &heartbeat_mgr.heartbeats;
                            let mut hb_to_send = Vec::new();
                            for hb in heartbeats.iter() {
                                hb_to_send.push(hb());
                            }
                            if let Err(e) = client_clone.record_worker_heartbeat(key.namespace.clone(), key.endpoint.clone(), hb_to_send
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
                    }
                    _ = reset_notify.notified() => {
                        ticker.reset();
                    }
                    task = self.task_rx.
                }
            }
        });

        let (task_tx, task_rx) = unbounded_channel();

        Self {
            heartbeat_handle,
            heartbeat_map,
            config,
            client,
            permit_dealer,
            shutdown_initiated_token,
            metrics,
            task_tx,
            task_rx,
        }
    }

    pub(crate) fn shutdown(&self) {
        self.heartbeat_handle.abort()
    }

    pub(crate) fn register_heartbeat(&self, key: ClientIdentity, heartbeat: HeartbeatFn) {
        if self.heartbeat_map.lock().contains_key(&key) {}
        self.heartbeat_map
            .lock()
            .entry(key.clone())
            .and_modify(|mgr| mgr.register_heartbeat(heartbeat))
            .or_insert_with(|| {
                // TODO: params need to be fixed, this is just a placeholder
                HeartbeatNexusManager::new(
                    self.client.clone(),
                    key,
                    self.config.nexus_task_poller_behavior,
                    self.permit_dealer.clone(),
                    self.shutdown_initiated_token.clone(),
                    self.metrics.clone(),
                    self.config.graceful_shutdown_period,
                    self.shutdown_initiated_token.clone(),
                )
            });
    }
}

#[derive(Debug, Clone)]
pub(crate) struct WorkerHeartbeatData {
    worker_instance_key: String,
    worker_identity: String,
    host_info: WorkerHostInfo,
    // Time of the last heartbeat. This is used to both for heartbeat_time and last_heartbeat_time
    heartbeat_time: Option<SystemTime>,
    task_queue: String,
    /// SDK name
    sdk_name: String,
    /// SDK version
    sdk_version: String,
    /// Worker start time
    start_time: SystemTime,
    heartbeat_interval: Duration,
}

impl WorkerHeartbeatData {
    pub(crate) fn new(
        worker_config: WorkerConfig,
        worker_identity: String,
        sdk_name_and_ver: (String, String),
    ) -> Self {
        Self {
            worker_identity,
            host_info: WorkerHostInfo {
                host_name: gethostname().to_string_lossy().to_string(),
                process_id: std::process::id().to_string(),
                ..Default::default()
            },
            sdk_name: sdk_name_and_ver.0,
            sdk_version: sdk_name_and_ver.1,
            task_queue: worker_config.task_queue.clone(),
            start_time: SystemTime::now(),
            heartbeat_time: None,
            worker_instance_key: Uuid::new_v4().to_string(),
            heartbeat_interval: worker_config.heartbeat_interval,
        }
    }

    pub(crate) fn capture_heartbeat(&mut self) -> WorkerHeartbeat {
        let now = SystemTime::now();
        let elapsed_since_last_heartbeat = if let Some(heartbeat_time) = self.heartbeat_time {
            let dur = now.duration_since(heartbeat_time).unwrap_or(Duration::ZERO);
            Some(PbDuration {
                seconds: dur.as_secs() as i64,
                nanos: dur.subsec_nanos() as i32,
            })
        } else {
            None
        };

        self.heartbeat_time = Some(now);

        WorkerHeartbeat {
            worker_instance_key: self.worker_instance_key.clone(),
            worker_identity: self.worker_identity.clone(),
            host_info: Some(self.host_info.clone()),
            task_queue: self.task_queue.clone(),
            sdk_name: self.sdk_name.clone(),
            sdk_version: self.sdk_version.clone(),
            status: 0,
            start_time: Some(self.start_time.into()),
            heartbeat_time: Some(SystemTime::now().into()),
            elapsed_since_last_heartbeat,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_help::WorkerExt;
    use crate::test_help::test_worker_cfg;
    use crate::worker;
    use crate::worker::client::mocks::mock_worker_client;
    use std::sync::Arc;
    use std::time::Duration;
    use temporal_sdk_core_api::worker::PollerBehavior;
    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::RecordWorkerHeartbeatResponse;

    #[tokio::test]
    async fn worker_heartbeat() {
        let mut mock = mock_worker_client();
        mock.expect_record_worker_heartbeat()
            .times(2)
            .returning(move |heartbeat| {
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

                Ok(RecordWorkerHeartbeatResponse {})
            });

        let config = test_worker_cfg()
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .max_outstanding_activities(1_usize)
            .heartbeat_interval(Duration::from_millis(200))
            .build()
            .unwrap();

        let heartbeat_fn = Arc::new(OnceLock::new());
        let client = Arc::new(mock);
        let worker = worker::Worker::new(config, None, client, None, Some(heartbeat_fn.clone()));
        heartbeat_fn.get().unwrap()();

        // heartbeat timer fires once
        advance_time(Duration::from_millis(300)).await;
        // it hasn't been >90% of the interval since the last heartbeat, so no data should be returned here
        assert_eq!(None, heartbeat_fn.get().unwrap()());
        // heartbeat timer fires once
        advance_time(Duration::from_millis(300)).await;

        worker.drain_activity_poller_and_shutdown().await;
    }

    async fn advance_time(dur: Duration) {
        tokio::time::pause();
        tokio::time::advance(dur).await;
        tokio::time::resume();
    }
}
