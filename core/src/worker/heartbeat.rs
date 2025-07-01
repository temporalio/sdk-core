use crate::WorkerClient;
use futures_util::future;
use futures_util::future::AbortHandle;
use gethostname::gethostname;
use parking_lot::Mutex;
use prost_types::Duration as PbDuration;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use temporal_sdk_core_api::worker::WorkerConfig;
use temporal_sdk_core_protos::temporal::api::worker::v1::{WorkerHeartbeat, WorkerHostInfo};
use uuid::Uuid;

/// Heartbeat information
///
/// Note: Experimental
pub struct WorkerHeartbeatInfo {
    pub(crate) data: Arc<Mutex<WorkerHeartbeatData>>,
    timer_abort: AbortHandle,
    client: Option<Arc<dyn WorkerClient>>,
    interval: Option<Duration>,
    #[cfg(test)]
    heartbeats_sent: Arc<Mutex<usize>>,
}

impl WorkerHeartbeatInfo {
    /// Create a new WorkerHeartbeatInfo. A timer is immediately started to track the worker
    /// heartbeat interval.
    pub(crate) fn new(worker_config: WorkerConfig) -> Self {
        // unused abort handle, will be replaced with a new one when we start a new timer
        let (abort_handle, _) = AbortHandle::new_pair();

        Self {
            data: Arc::new(Mutex::new(WorkerHeartbeatData::new(worker_config.clone()))),
            timer_abort: abort_handle,
            client: None,
            interval: worker_config.heartbeat_interval,
            #[cfg(test)]
            heartbeats_sent: Arc::new(Mutex::new(0)),
        }
    }

    /// Transform heartbeat data into `WorkerHeartbeat` we can send in gRPC request. Some
    /// metrics are also cached for future calls of this function.
    pub(crate) fn capture_heartbeat(&mut self) -> WorkerHeartbeat {
        self.create_new_timer();

        self.data.lock().capture_heartbeat()
    }

    fn create_new_timer(&mut self) {
        self.timer_abort.abort();

        let (abort_handle, abort_reg) = AbortHandle::new_pair();
        let interval = if let Some(dur) = self.interval {
            dur
        } else {
            Duration::from_secs(60)
        };
        let data = self.data.clone();
        #[cfg(test)]
        let heartbeats_sent = self.heartbeats_sent.clone();
        self.timer_abort = abort_handle.clone();
        if let Some(client) = self.client.clone() {
            tokio::spawn(future::Abortable::new(
                async move {
                    loop {
                        tokio::time::sleep(interval).await;
                        #[cfg(test)]
                        {
                            let mut num = heartbeats_sent.lock();
                            *num += 1;
                        }

                        let heartbeat = data.lock().capture_heartbeat();
                        if let Err(e) = client.clone().record_worker_heartbeat(heartbeat).await {
                            warn!(error=?e, "Network error while sending worker heartbeat");
                        }
                    }
                },
                abort_reg,
            ));
        } else {
            warn!("No client attached to heartbeat_info")
        };
    }

    pub(crate) fn add_client(&mut self, client: Arc<dyn WorkerClient>) {
        self.client = Some(client);
        self.create_new_timer();
    }
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
            // TODO: Is this right for worker_identity?
            worker_identity: worker_config
                .client_identity_override
                .clone()
                .unwrap_or_default(),
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
    use crate::test_help::WorkerExt;
    use crate::test_help::test_worker_cfg;
    use crate::worker;
    use crate::worker::WorkerHeartbeatInfo;
    use crate::worker::client::mocks::mock_worker_client;
    use parking_lot::Mutex;
    use std::ops::Deref;
    use std::sync::Arc;
    use std::time::Duration;
    use temporal_sdk_core_api::Worker;
    use temporal_sdk_core_api::worker::PollerBehavior;
    use temporal_sdk_core_protos::coresdk::ActivityTaskCompletion;
    use temporal_sdk_core_protos::coresdk::activity_result::ActivityExecutionResult;
    use temporal_sdk_core_protos::temporal::api::workflowservice::v1::{
        PollActivityTaskQueueResponse, RecordWorkerHeartbeatResponse,
        RespondActivityTaskCompletedResponse,
    };

    #[rstest::rstest]
    #[tokio::test]
    async fn worker_heartbeat(#[values(true, false)] extra_heartbeat: bool) {
        let mut mock = mock_worker_client();
        let record_heartbeat_calls = if extra_heartbeat { 2 } else { 3 };
        mock.expect_record_worker_heartbeat()
            .times(record_heartbeat_calls)
            .returning(|heartbeat| {
                let host_info = heartbeat.host_info.clone().unwrap();
                assert!(heartbeat.worker_identity.is_empty());
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
        mock.expect_poll_activity_task()
            .times(1)
            .returning(move |_, _| {
                Ok(PollActivityTaskQueueResponse {
                    task_token: vec![1],
                    activity_id: "act1".to_string(),
                    ..Default::default()
                })
            });
        mock.expect_complete_activity_task()
            .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));

        let config = test_worker_cfg()
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .max_outstanding_activities(1_usize)
            .heartbeat_interval(Duration::from_millis(100))
            .build()
            .unwrap();

        let heartbeat_info = Arc::new(Mutex::new(WorkerHeartbeatInfo::new(config.clone())));
        let client = Arc::new(mock);
        heartbeat_info.lock().add_client(client.clone());
        let worker = worker::Worker::new(config, None, client, None, Some(heartbeat_info.clone()));
        let _ = heartbeat_info.lock().capture_heartbeat();

        // heartbeat timer fires once
        tokio::time::sleep(Duration::from_millis(150)).await;
        if extra_heartbeat {
            // reset heartbeat timer
            heartbeat_info.lock().capture_heartbeat();
        }
        // heartbeat timer fires once
        tokio::time::sleep(Duration::from_millis(180)).await;

        if extra_heartbeat {
            assert_eq!(2, *heartbeat_info.lock().heartbeats_sent.lock().deref());
        } else {
            assert_eq!(3, *heartbeat_info.lock().heartbeats_sent.lock().deref());
        }

        let task = worker.poll_activity_task().await.unwrap();
        worker
            .complete_activity_task(ActivityTaskCompletion {
                task_token: task.task_token,
                result: Some(ActivityExecutionResult::ok(vec![1].into())),
            })
            .await
            .unwrap();
        worker.drain_activity_poller_and_shutdown().await;
    }
}
