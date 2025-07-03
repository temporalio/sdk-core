use gethostname::gethostname;
use prost_types::Duration as PbDuration;
use std::time::{Duration, SystemTime};
use temporal_sdk_core_api::worker::WorkerConfig;
use temporal_sdk_core_protos::temporal::api::worker::v1::{WorkerHeartbeat, WorkerHostInfo};
use tokio::sync::watch;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct WorkerHeartbeatData {
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
    pub(crate) heartbeat_interval: Duration,
    pub(crate) reset_tx: Option<watch::Sender<()>>,
}

impl WorkerHeartbeatData {
    pub fn new(worker_config: WorkerConfig, worker_identity: String) -> Self {
        Self {
            worker_identity,
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
            heartbeat_interval: worker_config.heartbeat_interval,
            reset_tx: None,
        }
    }

    pub fn capture_heartbeat_if_needed(&mut self) -> Option<WorkerHeartbeat> {
        let now = SystemTime::now();
        let elapsed_since_last_heartbeat = if let Some(heartbeat_time) = self.heartbeat_time {
            let dur = now.duration_since(heartbeat_time).unwrap_or(Duration::ZERO);

            // Only send poll data if it's nearly been a full interval since this data has been sent
            // In this case, "nearly" is 90% of the interval
            if dur.as_secs_f64() < 0.9 * self.heartbeat_interval.as_secs_f64() {
                println!("Heartbeat interval not yet elapsed, not sending poll data");
                return None;
            }
            Some(PbDuration {
                seconds: dur.as_secs() as i64,
                nanos: dur.subsec_nanos() as i32,
            })
        } else {
            None
        };

        self.heartbeat_time = Some(now);
        if let Some(reset_tx) = &self.reset_tx {
            let _ = reset_tx.send(());
        } else {
            warn!(
                "No reset_tx attached to heartbeat_info, worker heartbeat was not properly setup"
            );
        }

        Some(WorkerHeartbeat {
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
        })
    }

    pub(crate) fn set_reset_tx(&mut self, reset_tx: watch::Sender<()>) {
        self.reset_tx = Some(reset_tx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_help::WorkerExt;
    use crate::test_help::test_worker_cfg;
    use crate::worker;
    use crate::worker::client::mocks::mock_worker_client;
    use parking_lot::Mutex;
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
                assert_eq!("test_identity", heartbeat.worker_identity);
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

        let heartbeat_data = Arc::new(Mutex::new(WorkerHeartbeatData::new(
            config.clone(),
            "test_identity".to_string(),
        )));
        let client = Arc::new(mock);
        let worker = worker::Worker::new(config, None, client, None, Some(heartbeat_data.clone()));
        let _ = heartbeat_data.lock().capture_heartbeat_if_needed();

        // heartbeat timer fires once
        tokio::time::sleep(Duration::from_millis(300)).await;
        // it hasn't been >90% of the interval since the last heartbeat, so no data should be returned here
        assert_eq!(None, heartbeat_data.lock().capture_heartbeat_if_needed());
        // heartbeat timer fires once
        tokio::time::sleep(Duration::from_millis(150)).await;

        worker.drain_activity_poller_and_shutdown().await;
    }
}
