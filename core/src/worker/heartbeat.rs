use crate::WorkerClient;
use crate::WorkerTrait;
use crate::abstractions::{MeteredPermitDealer, dbg_panic};
use crate::pollers::{BoxedNexusPoller, LongPollBuffer};
use crate::telemetry::TelemetryInstance;
use crate::telemetry::metrics::{MetricsContext, nexus_poller};
use crate::worker::nexus::NexusManager;
use gethostname::gethostname;
use parking_lot::Mutex;
use prost_types::{Duration as PbDuration, FieldMask};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::task::Poll;
use std::time::{Duration, SystemTime};
use temporal_client::Client;
use temporal_sdk_core_api::Worker;
use temporal_sdk_core_api::worker::{
    NexusSlotKind, PollerBehavior, WorkerConfig, WorkerConfigBuilder, WorkerVersioningStrategy,
};
use temporal_sdk_core_protos::coresdk::nexus::{
    NexusTask, NexusTaskCompletion, nexus_task, nexus_task_completion,
};
use temporal_sdk_core_protos::coresdk::{AsJsonPayloadExt, FromJsonPayloadExt};
use temporal_sdk_core_protos::temporal::api::common::v1::WorkerSelector;
use temporal_sdk_core_protos::temporal::api::nexus::v1::request::Variant::StartOperation;
use temporal_sdk_core_protos::temporal::api::nexus::v1::{
    StartOperationResponse, start_operation_response,
};
use temporal_sdk_core_protos::temporal::api::sdk::v1::worker_config;
use temporal_sdk_core_protos::temporal::api::worker::v1::WorkerInfo;
use temporal_sdk_core_protos::temporal::api::worker::v1::{
    WorkerHeartbeat, WorkerHostInfo, fetch_worker_config_response,
};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::{
    PollNexusTaskQueueResponse, RecordWorkerHeartbeatRequest,
};
use temporal_sdk_core_protos::temporal::api::{nexus, sdk, worker};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::sync::{Notify, RwLock};
use tokio::task::JoinHandle;
use tokio::time::{Interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub(crate) type HeartbeatCallback = Arc<dyn Fn() -> WorkerHeartbeat + Send + Sync>;
/// Used by runtime to map Client identity to SharedNamespaceWorker
/// TODO: Arc is used here to let us mark the worker as "shutdown", and if somehow we re-register,
///  we can recreate a new SharedNamespaceWorker and replace the weak Arc.
///
///  no that won't work, SharedNamespaceWorker doesn't have access to shared_namespace_map, and
/// likewise, Worker doesn't have access to SharedNamespaceWorker.worker_data_map
/// // TODO: use a CancelCallback??
pub(crate) type SharedNamespaceMap = HashMap<ClientIdentity, Arc<Mutex<SharedNamespaceWorker>>>;
pub(crate) type WorkerDataMap = HashMap<String, Arc<Mutex<WorkerHeartbeatData>>>;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ClientIdentity {
    pub(crate) endpoint: String,
    pub(crate) namespace: String,
    pub(crate) task_queue: String,
}

/// SharedNamespaceWorker is responsible for polling worker commands and sending worker heartbeat
/// to the server. This communicates with all workers in the same process that share the same
/// namespace.
pub(crate) struct SharedNamespaceWorker {
    worker_data_map: Arc<Mutex<WorkerDataMap>>,
    join_handle: JoinHandle<()>,
    heartbeat_interval: Duration,
}

impl SharedNamespaceWorker {
    pub(crate) fn new(
        client: Arc<dyn WorkerClient>,
        client_identity: ClientIdentity,
        heartbeat_interval: Duration,
        telemetry: Option<&TelemetryInstance>,
        shutdown_callback: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        let config = WorkerConfigBuilder::default()
            .namespace(client_identity.namespace.clone())
            .task_queue(client_identity.task_queue)
            .no_remote_activities(true)
            .max_outstanding_nexus_tasks(5_usize) // TODO: arbitrary low number, feel free to change when needed
            .versioning_strategy(WorkerVersioningStrategy::None {
                build_id: "1.0".to_owned(),
            })
            .build()
            .expect("all required fields should be implemented");
        let worker_data_map = Arc::new(Mutex::new(WorkerDataMap::new()));

        let worker = crate::worker::Worker::new(
            config,
            None, // TODO: want sticky queue?
            client.clone(),
            telemetry,
            true,
        );

        // heartbeat task
        let sdk_name_and_ver = client.sdk_name_and_version();
        let reset_notify = Arc::new(Notify::new());
        let client_clone = client.clone();

        let worker_data_map_clone = worker_data_map.clone();

        let join_handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(heartbeat_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                if worker_data_map_clone.lock().is_empty() {
                    worker.shutdown().await;
                    // remove this worker from the Runtime map
                    shutdown_callback();
                }
                tokio::select! {
                    _ = ticker.tick() => {
                        let mut hb_to_send = Vec::new();
                        for data in worker_data_map_clone.lock().values() {
                            hb_to_send.push(data.lock().capture_heartbeat());
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
                    res = worker.poll_nexus_task() => {
                        match res {
                            Ok(task) => {
                                match task.variant.unwrap() {
                                    nexus_task::Variant::Task(res) => {
                                        let task_token = res.task_token;
                                        let _poller_scaling_decision = res.poller_scaling_decision;
                                        let req = res.request.unwrap();
                                        let variant = req.variant.unwrap();
                                        match variant {
                                            StartOperation(start_op) => {
                                                let _req_id = start_op.request_id;
                                                if start_op.service.as_str() != "sys-worker-service" {
                                                    dbg_panic!("Unexpected service name");
                                                }

                                                match start_op.operation.as_str() {
                                                    "FetchWorkerConfig" => {
                                                        match worker::v1::FetchWorkerConfigRequest::from_json_payload(&start_op.payload.unwrap()) {
                                                            Ok(req) => {
                                                                // today we only support a single worker_instance_key
                                                                if req.worker_instance_key.len() != 1 {
                                                                    warn!("Expected 1 worker_instance_key, got {}", req.worker_instance_key.len());
                                                                    continue
                                                                }
                                                                let worker_instance_key = req.worker_instance_key[0].clone();
                                                                let worker_data_map = worker_data_map_clone.lock();
                                                                if let Some(worker_heartbeat_data) = worker_data_map.get(&worker_instance_key) {
                                                                    let worker_data_from_map = worker_heartbeat_data.lock();

                                                                    let config = worker_data_from_map.fetch_config();
                                                                    let config_resp = worker::v1::FetchWorkerConfigResponse {
                                                                        worker_configs: vec![config],
                                                                    };
                                                                    WorkerTrait::complete_nexus_task(&worker, NexusTaskCompletion {
                                                                        task_token,
                                                                        status: Some(nexus_task_completion::Status::Completed(nexus::v1::Response{
                                                                            variant: Some(nexus::v1::response::Variant::StartOperation(StartOperationResponse {
                                                                                variant: Some(start_operation_response::Variant::SyncSuccess(start_operation_response::Sync {
                                                                                    payload: Some(config_resp.as_json_payload().unwrap()),
                                                                                    links: Vec::new(), // TODO: add links?
                                                                                }))
                                                                            })),
                                                                        }))
                                                                    }).await.unwrap();
                                                                } else {
                                                                    warn!("Worker {} not found", worker_instance_key);
                                                                }


                                                            }
                                                            Err(e) => {
                                                                dbg_panic!("Failed to parse FetchWorkerConfigRequest: {:?}", e)
                                                            },
                                                        }
                                                    }
                                                    "UpdateWorkerConfig" => {
                                                        match worker::v1::UpdateWorkerConfigRequest::from_json_payload(&start_op.payload.unwrap()) {
                                                            Ok(req) => {
                                                                // today we only support a single worker_instance_key
                                                                if req.worker_instance_key.len() != 1 {
                                                                    warn!("Expected 1 worker_instance_key, got {}", req.worker_instance_key.len());
                                                                    continue
                                                                }
                                                                let worker_instance_key = req.worker_instance_key[0].clone();
                                                                let worker_data_map = worker_data_map_clone.lock();
                                                                    if let Some(worker_heartbeat_data) = worker_data_map.get(&worker_instance_key) {
                                                                        let worker_data_from_map = worker_heartbeat_data.lock();
                                                                    for path in &req.update_mask.unwrap().paths {
                                                                        match path.as_str() {
                                                                            "workflow_cache_size" => {
                                                                                worker_data_from_map.workflow_cache_size.store(req.worker_config.unwrap().workflow_cache_size as usize, Ordering::Relaxed);
                                                                            }
                                                                        _ => todo!("unknown path"),
                                                                        }
                                                                    WorkerTrait::complete_nexus_task(&worker, NexusTaskCompletion {
                                                                            task_token: task_token.clone(),
                                                                            status: Some(nexus_task_completion::Status::Completed(nexus::v1::Response{
                                                                                variant: Some(nexus::v1::response::Variant::StartOperation(StartOperationResponse {
                                                                                    variant: Some(start_operation_response::Variant::SyncSuccess(start_operation_response::Sync {
                                                                                        payload: None,
                                                                                        links: Vec::new(),
                                                                                    }))
                                                                                })),
                                                                            }))
                                                                        }).await.unwrap();
                                                                    }
                                                                }
                                                            }
                                                            Err(e) => {
                                                                dbg_panic!("Failed to parse FetchWorkerConfigRequest: {:?}", e)
                                                            },
                                                        }
                                                    }
                                                    _ => todo!("unknown operation"),
                                                }
                                            }
                                            nexus::v1::request::Variant::CancelOperation(cancel_op) => todo!("cancel op")
                                        }

                                    },
                                    nexus_task::Variant::CancelTask(cancel) => todo!("handle cancel task"),
                                }


                            }
                            Err(e) => todo!("log error"),
                        }
                    }
                }
            }
        });

        Self {
            worker_data_map,
            join_handle,
            heartbeat_interval,
        }
    }

    pub(crate) fn add_data_to_map(
        &mut self,
        data: Arc<Mutex<WorkerHeartbeatData>>,
    ) -> Arc<dyn Fn() + Send + Sync> {
        let worker_instance_key = data.lock().worker_instance_key.clone();
        let mut worker_data_map = self.worker_data_map.lock();
        worker_data_map.insert(worker_instance_key.clone(), data);

        let worker_data_map_clone = self.worker_data_map.clone();
        Arc::new(move || {
            if worker_data_map_clone
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
// TODO: rename
// TODO: impl trait so entire struct doesn't need to be passed to Worker and SharedNamespaceWorker
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

    // Worker commands
    workflow_cache_size: Arc<AtomicUsize>,
    workflow_poller_behavior: PollerBehavior,
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
            workflow_cache_size: worker_config.max_cached_workflows.clone(),
            workflow_poller_behavior: worker_config.workflow_task_poller_behavior,
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

    fn fetch_config(&self) -> fetch_worker_config_response::WorkerConfigEntry {
        let poller_behavior = match self.workflow_poller_behavior {
            PollerBehavior::Autoscaling {
                minimum,
                maximum,
                initial,
            } => worker_config::PollerBehavior::AutoscalingPollerBehavior(
                worker_config::AutoscalingPollerBehavior {
                    min_pollers: minimum as i32,
                    max_pollers: maximum as i32,
                    initial_pollers: initial as i32,
                },
            ),
            PollerBehavior::SimpleMaximum(max) => {
                worker_config::PollerBehavior::SimplePollerBehavior(
                    worker_config::SimplePollerBehavior {
                        max_pollers: max as i32,
                    },
                )
            }
        };
        fetch_worker_config_response::WorkerConfigEntry {
            worker_instance_key: self.worker_instance_key.clone(),
            worker_config: Some(sdk::v1::WorkerConfig {
                poller_behavior: Some(poller_behavior),
                workflow_cache_size: self.workflow_cache_size.load(Ordering::Relaxed) as i32,
            }),
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
        let mut heartbeat_count = 0;
        mock.expect_record_worker_heartbeat()
            .times(2)
            .returning(move |namespace, identity, worker_heartbeat| {
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

                heartbeat_count += 1;

                Ok(RecordWorkerHeartbeatResponse {})
            });

        let config = test_worker_cfg()
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .max_outstanding_activities(1_usize)
            .heartbeat_interval(Duration::from_millis(200))
            .build()
            .unwrap();

        let heartbeat_callback = Arc::new(OnceLock::new());
        let client = Arc::new(mock);
        // TODO: Create worker, then create SharedNamespaceWorker
        let worker = worker::Worker::new(config, None, client, None, false);
        // heartbeat_callback.get().unwrap()();

        // heartbeat timer fires once
        advance_time(Duration::from_millis(300)).await;
        // it hasn't been >90% of the interval since the last heartbeat, so no data should be returned here
        // assert_eq!(None, heartbeat_callback.get().unwrap()());
        // heartbeat timer fires once
        advance_time(Duration::from_millis(300)).await;

        worker.drain_activity_poller_and_shutdown().await;

        assert_eq!(2, heartbeat_count);
    }

    async fn advance_time(dur: Duration) {
        tokio::time::pause();
        tokio::time::advance(dur).await;
        tokio::time::resume();
    }
}
