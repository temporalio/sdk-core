use std::sync::Arc;
use std::time::{Duration, SystemTime};
use futures_util::future;
use futures_util::future::AbortHandle;
use gethostname::gethostname;
use parking_lot::Mutex;
use prost_types::Duration as PbDuration;
use uuid::Uuid;
use temporal_sdk_core_api::worker::WorkerConfig;
use temporal_sdk_core_protos::temporal::api::worker::v1::{WorkerHeartbeat, WorkerHostInfo};
use crate::worker::client::WorkerClientBag;
use crate::WorkerClient;

type Result<T, E = tonic::Status> = std::result::Result<T, E>;

/// Heartbeat information
///
/// Note: Experimental
pub struct WorkerHeartbeatInfo {
    pub(crate) data: Arc<Mutex<WorkerHeartbeatData>>,
    timer_abort: AbortHandle,
    client: Arc<dyn WorkerClient>,
    interval: Option<Duration>,
}

impl WorkerHeartbeatInfo {
    /// Create a new WorkerHeartbeatInfo. A timer is immediately started to track the worker
    /// heartbeat interval.
    pub(crate) fn new(worker_config: WorkerConfig, client: Arc<dyn WorkerClient>) -> Self {
        // spawn heartbeat things here, then on capture_heartbeat, send signal to thread
        let (abort_handle, _) = AbortHandle::new_pair();

        let mut heartbeat = Self {
            data: Arc::new(Mutex::new(WorkerHeartbeatData::new(worker_config.clone()))),
            timer_abort: abort_handle,
            client,
            interval: worker_config.heartbeat_interval,
        };
        heartbeat.create_new_timer();
        heartbeat
    }

    // TODO: This is called by Client when it sends other requests.
    /// Transform heartbeat data into `WorkerHeartbeat` we can send in gRPC request. Some
    /// metrics are also cached for future calls of this function.
    pub(crate) fn capture_heartbeat(&mut self) -> WorkerHeartbeat {
        self.create_new_timer();

        self.data.lock().capture_heartbeat()
    }

    fn create_new_timer(&mut self) {
        println!("[create_new_timer]");
        self.timer_abort.abort();

        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let client = self.client.clone();
        let interval = if let Some(dur) = self.interval {
            dur
        } else {
            Duration::from_secs(60)
        };
        let data = self.data.clone();
        let client = self.client.clone();
        tokio::spawn(future::Abortable::new(
            async move {
                println!("sleeping for {:?}", interval);
                tokio::time::sleep(interval).await;
                println!("sleep done");

                if let Err(e) = client.clone().record_worker_heartbeat(data.lock().capture_heartbeat()).await {
                    warn!(error=?e, "Network error while sending worker heartbeat");
                }

            },
            abort_reg,
        ));

        self.timer_abort = abort_handle;
    }

    // pub(crate) fn add_client(&mut self, client: Arc<dyn WorkerClient>) {
    //     println!("[add_client]");
    //     self.client = Some(client);
    // }
}

#[derive(Debug, Clone)]
pub(crate) struct WorkerHeartbeatData {
    worker_instance_key: String,
    pub(crate) worker_identity: String,
    host_info: WorkerHostInfo,
    // Time of the last heartbeat. This is used to both for heartbeat_time and last_heartbeat_time
    pub(crate) heartbeat_time: Option<SystemTime>,
    pub(crate) task_queue: String,
    /// SDK name
    pub(crate) sdk_name: String,
    /// SDK version
    pub(crate) sdk_version: String,
    /// Worker start time
    pub(crate) start_time: SystemTime,
}

impl WorkerHeartbeatData {
    fn new(worker_config: WorkerConfig) -> Self {
        Self {
            worker_identity: worker_config.client_identity_override.clone().unwrap_or_default(),
            host_info: WorkerHostInfo {
                host_name: gethostname().to_string_lossy().to_string(),
                process_id: std::process::id().to_string(),
                ..Default::default()
            },
            sdk_name: String::new(),
            sdk_version: String::new(),
            task_queue: worker_config.task_queue.clone(),
            start_time: SystemTime::now(),
            heartbeat_time: None,
            worker_instance_key: Uuid::new_v4().to_string(),
        }
    }

    fn capture_heartbeat(&mut self) -> WorkerHeartbeat {
        let now = SystemTime::now();
        let elapsed_since_last_heartbeat = if let Some(heartbeat_time) = self.heartbeat_time {
            let dur = now.duration_since(heartbeat_time).unwrap_or(Duration::ZERO); // TODO: do we want to fall back to ZERO?
            Some(PbDuration {
                seconds: dur.as_secs() as i64,
                nanos: dur.subsec_nanos() as i32,
            })
        } else {
            None
        };

        self.heartbeat_time = Some(now.into());

        let heartbeat = WorkerHeartbeat {
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
        };
        println!("[hb]: {:#?}", heartbeat);
        heartbeat
    }
}