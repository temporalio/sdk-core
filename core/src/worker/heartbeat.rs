use crate::{WorkerClient};
use crate::abstractions::{MeteredPermitDealer, dbg_panic};
use crate::pollers::{BoxedNexusPoller, LongPollBuffer};
use crate::telemetry::metrics::{MetricsContext, nexus_poller};
use crate::worker::nexus::NexusManager;
use gethostname::gethostname;
use parking_lot::{Mutex};
use prost_types::Duration as PbDuration;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime};
use temporal_client::Client;
use temporal_sdk_core_api::worker::{NexusSlotKind, PollerBehavior, WorkerConfig, WorkerConfigBuilder, WorkerVersioningStrategy};
use temporal_sdk_core_protos::temporal::api::worker::v1::{WorkerHeartbeat, WorkerHostInfo};
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::{
    PollNexusTaskQueueResponse, RecordWorkerHeartbeatRequest,
};
use tokio::sync::{RwLock, Notify};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::JoinHandle;
use tokio::time::{Interval, MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use temporal_sdk_core_api::Worker;
use crate::telemetry::TelemetryInstance;

pub(crate) type HeartbeatCallback = Arc<dyn Fn() -> WorkerHeartbeat + Send + Sync>;
pub(crate) type SharedNamespaceMap = HashMap<ClientIdentity, SharedNamespaceWorker>;

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
    pub(crate) heartbeat_callbacks: Arc<Mutex<Vec<HeartbeatCallback>>>,
    join_handle: JoinHandle<()>,
    heartbeat_interval: Duration,
}

impl SharedNamespaceWorker {
    pub(crate) fn new(
        client: Arc<dyn WorkerClient>,
        client_identity: ClientIdentity,
        heartbeat_interval: Duration,
        telemetry: Option<&TelemetryInstance>,
    ) -> Self {
        let config = WorkerConfigBuilder::default()
            .namespace(client_identity.namespace.clone())
            .task_queue(client_identity.task_queue
            )
            .no_remote_activities(true)
            .max_outstanding_nexus_tasks(5_usize) // TODO: arbitrary low number, feel free to change when needed
            .versioning_strategy(WorkerVersioningStrategy::None {
                        build_id: "1.0".to_owned(),
                    })
            .build()
            .unwrap(); // TODO: Unwrap
        // let config = WorkerConfig {
        //     namespace: client_identity.namespace.clone(),
        //     task_queue: client_identity.task_queue,
        //     no_remote_activities: false,
        //     versioning_strategy: WorkerVersioningStrategy::None {
        //         build_id: "1.0".to_owned(),
        //     },
        //     max_outstanding_nexus_tasks: Some(5_usize),
        //     // The rest are default values
        //     client_identity_override: None,
        //     max_cached_workflows: 0,
        //     tuner: None,
        //     workflow_task_poller_behavior: PollerBehavior::SimpleMaximum(2_usize),
        //     nonsticky_to_sticky_poll_ratio: 0.0,
        //     activity_task_poller_behavior: PollerBehavior::SimpleMaximum(1_usize),
        //     nexus_task_poller_behavior: PollerBehavior::SimpleMaximum(1_usize),
        // 
        //     sticky_queue_schedule_to_start_timeout: Default::default(),
        //     max_heartbeat_throttle_interval: Default::default(),
        //     default_heartbeat_throttle_interval: Default::default(),
        //     max_task_queue_activities_per_second: None,
        //     max_worker_activities_per_second: None,
        //     ignore_evicts_on_shutdown: false,
        //     fetching_concurrency: 0,
        //     graceful_shutdown_period: None,
        //     local_timeout_buffer_for_activities: Default::default(),
        //     workflow_failure_errors: Default::default(),
        //     workflow_types_to_failure_errors: Default::default(),
        //     max_outstanding_workflow_tasks: None,
        //     max_outstanding_activities: None,
        //     max_outstanding_local_activities: None,
        // }
        let worker = crate::worker::Worker::new(
            config,
            None, // TODO: want sticky queue?
            client.clone(),
            telemetry,
        );

        let hb_callbacks = Arc::new(Mutex::new(Vec::<HeartbeatCallback>::new()));
        // heartbeat task
        let sdk_name_and_ver = client.sdk_name_and_version();
        let reset_notify = Arc::new(Notify::new());
        let hb_callbacks_clone = hb_callbacks.clone();
        let client_clone = client.clone();

        // runtime have option to turn off worker heartbeat,
        // set at runtime,
        let join_handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(heartbeat_interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        let mut hb_to_send = Vec::new();
                        for heartbeat_callback in hb_callbacks_clone.lock().iter() {
                            hb_to_send.push(heartbeat_callback.as_ref()());
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
                    // TODO: handle nexus tasks
                    res = worker.poll_nexus_task() => {
                        match res {
                            Ok(task) => todo!(),
                            Err(e) => todo!("log error"),
                        }
                    }
                }
            }
        });

        Self {
            heartbeat_callbacks: hb_callbacks,
            join_handle,
            heartbeat_interval,
        }
    }

    pub(crate) fn shutdown(&self) {
        // TODO: Implement shutdown logic for all namespace managers
        // TODO: Handle when a single worker on the process shuts down, need a way to remove that callback from the SharedRuntimeWorker
        //  Spencer suggested using Weak references to Arcs, arcs are dropped when all of the strong references are dropped,
    }

    pub(crate) fn add_callback(&mut self, heartbeat_callback: HeartbeatCallback) {
        self.heartbeat_callbacks.lock().push(heartbeat_callback);
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

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::test_help::WorkerExt;
//     use crate::test_help::test_worker_cfg;
//     use crate::worker;
//     use crate::worker::client::mocks::mock_worker_client;
//     use std::sync::Arc;
//     use std::time::Duration;
//     use temporal_sdk_core_api::worker::PollerBehavior;
//     use temporal_sdk_core_protos::temporal::api::workflowservice::v1::RecordWorkerHeartbeatResponse;
//
//     #[tokio::test]
//     async fn worker_heartbeat() {
//         let mut mock = mock_worker_client();
//         mock.expect_record_worker_heartbeat()
//             .times(2)
//             .returning(move |heartbeat| {
//                 let host_info = heartbeat.host_info.clone().unwrap();
//                 assert_eq!("test-identity", heartbeat.worker_identity);
//                 assert!(!heartbeat.worker_instance_key.is_empty());
//                 assert_eq!(
//                     host_info.host_name,
//                     gethostname::gethostname().to_string_lossy().to_string()
//                 );
//                 assert_eq!(host_info.process_id, std::process::id().to_string());
//                 assert_eq!(heartbeat.sdk_name, "test-core");
//                 assert_eq!(heartbeat.sdk_version, "0.0.0");
//                 assert_eq!(heartbeat.task_queue, "q");
//                 assert!(heartbeat.heartbeat_time.is_some());
//                 assert!(heartbeat.start_time.is_some());
//
//                 Ok(RecordWorkerHeartbeatResponse {})
//             });
//
//         let config = test_worker_cfg()
//             .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
//             .max_outstanding_activities(1_usize)
//             .heartbeat_interval(Duration::from_millis(200))
//             .build()
//             .unwrap();
//
//         let heartbeat_callback = Arc::new(OnceLock::new());
//         let client = Arc::new(mock);
//         let worker = worker::Worker::new(config, None, client, None, Some(heartbeat_callback.clone()));
//         heartbeat_callback.get().unwrap()();
//
//         // heartbeat timer fires once
//         advance_time(Duration::from_millis(300)).await;
//         // it hasn't been >90% of the interval since the last heartbeat, so no data should be returned here
//         assert_eq!(None, heartbeat_callback.get().unwrap()());
//         // heartbeat timer fires once
//         advance_time(Duration::from_millis(300)).await;
//
//         worker.drain_activity_poller_and_shutdown().await;
//     }
//
//     async fn advance_time(dur: Duration) {
//         tokio::time::pause();
//         tokio::time::advance(dur).await;
//         tokio::time::resume();
//     }
// }
