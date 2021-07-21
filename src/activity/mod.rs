use crate::{
    protos::coresdk::{
        activity_result::{self as ar, activity_result},
        activity_task::ActivityTask,
        ActivityTaskCompletion,
    },
    task_token::TaskToken,
    worker::{WorkerDispatcher, WorkerStatus},
    ActivityHeartbeat, CompleteActivityError, PollActivityError, ServerGatewayApis,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;

/// Tracks activity tasks across the entire process. This is important because typically all we
/// know about an activity task when it's being completed or heartbeated is a task token, so we
/// need some way to know what worker/queue it's associated with
pub(crate) struct ActivityTaskManager {
    /// Activities that have been issued to lang but not yet completed
    outstanding_activity_tasks: Arc<RwLock<HashMap<TaskToken, ManagedActivity>>>,
}
#[derive(Debug)]
struct ManagedActivity {
    /// What task queue did this task originate from
    task_queue: String,
    /// True if we've already learned from server it doesn't know about this task any longer
    is_known_not_found: bool,
}

impl ActivityTaskManager {
    pub fn new() -> Self {
        Self {
            outstanding_activity_tasks: Default::default(),
        }
    }

    pub async fn poll(
        &self,
        task_q: &str,
        workers: &WorkerDispatcher,
    ) -> Result<Option<ActivityTask>, PollActivityError> {
        let worker = workers.get(task_q).await;
        let worker = worker
            .as_deref()
            .ok_or_else(|| PollActivityError::NoWorkerForQueue(task_q.to_owned()))?;
        let worker = if let WorkerStatus::Live(w) = worker {
            w
        } else {
            return Err(PollActivityError::ShutDown);
        };

        let res = worker.activity_poll().await;
        if let Ok(Some(ref at)) = res {
            self.outstanding_activity_tasks.write().await.insert(
                TaskToken(at.task_token.clone()),
                ManagedActivity {
                    task_queue: task_q.to_string(),
                    is_known_not_found: false,
                },
            );
        }
        res
    }

    pub async fn complete(
        &self,
        completion: ActivityTaskCompletion,
        workers: &WorkerDispatcher,
        gateway: &(dyn ServerGatewayApis + Send + Sync),
    ) -> Result<(), CompleteActivityError> {
        let task_token = TaskToken(completion.task_token);
        let status = if let Some(s) = completion.result.and_then(|r| r.status) {
            s
        } else {
            return Err(CompleteActivityError::MalformedActivityCompletion {
                reason: "Activity completion had empty result/status field".to_owned(),
                completion: None,
            });
        };

        let (res, should_remove) = if let Some(is_known_not_found) = self
            .outstanding_activity_tasks
            .read()
            .await
            .get(&task_token)
            .map(|t| t.is_known_not_found)
        {
            // No need to report activities which we already know the server doesn't care about
            if !is_known_not_found {
                let maybe_net_err = match status {
                    activity_result::Status::Completed(ar::Success { result }) => gateway
                        .complete_activity_task(task_token.clone(), result.map(Into::into))
                        .await
                        .err(),
                    activity_result::Status::Failed(ar::Failure { failure }) => gateway
                        .fail_activity_task(task_token.clone(), failure.map(Into::into))
                        .await
                        .err(),
                    activity_result::Status::Canceled(ar::Cancelation { details }) => gateway
                        .cancel_activity_task(task_token.clone(), details.map(Into::into))
                        .await
                        .err(),
                };
                match maybe_net_err {
                    Some(e) if e.code() == tonic::Code::NotFound => {
                        warn!(task_token = ?task_token, details = ?e, "Activity not found on \
                        completion. This may happen if the activity has already been cancelled but \
                        completed anyway.");
                        (Ok(()), true)
                    }
                    Some(err) => (Err(err), false),
                    None => (Ok(()), true),
                }
            } else {
                (Ok(()), true)
            }
        } else {
            warn!(
                "Attempted to complete activity task {} but we were not tracking it",
                &task_token
            );
            return Ok(());
        };

        if should_remove {
            // Remove the activity from tracking and tell the worker a slot is free
            if let Some(t) = self
                .outstanding_activity_tasks
                .write()
                .await
                .remove(&task_token)
            {
                if let Some(WorkerStatus::Live(worker)) =
                    workers.get(&t.task_queue).await.as_deref()
                {
                    worker.activity_done(&task_token);
                }
            }
        }
        Ok(res?)
    }

    pub fn record_heartbeat(&self, details: ActivityHeartbeat, workers: Arc<WorkerDispatcher>) {
        let ats = self.outstanding_activity_tasks.clone();
        // TODO: Ugh. Can we avoid making this async in a better way?
        let _ = tokio::task::spawn(async move {
            if let Some(tq) = ats
                .read()
                .await
                .get(&TaskToken(details.task_token.clone()))
                .and_then(|t| {
                    if !t.is_known_not_found {
                        Some(t.task_queue.clone())
                    } else {
                        None
                    }
                })
            {
                if let Some(WorkerStatus::Live(w)) = workers.get(&tq).await.as_deref() {
                    w.record_heartbeat(details)
                }
            } else {
                warn!(
                    "Tried to record heartbeat for an unknown activity {:?}",
                    details
                );
            }
        });
    }
}

#[cfg(test)]
mod tests {

    // #[tokio::test]
    // async fn only_returns_cancels_for_desired_queue() {
    //     let mut mock_gateway = MockServerGatewayApis::new();
    //     // Mark the activity as needing cancel
    //     mock_gateway
    //         .expect_record_activity_heartbeat()
    //         .times(1)
    //         .returning(|_, _| {
    //             Ok(RecordActivityTaskHeartbeatResponse {
    //                 cancel_requested: true,
    //             })
    //         });
    //
    //     let mock_gateway = Arc::new(mock_gateway);
    //     let w1cfg = WorkerConfigBuilder::default()
    //         .task_queue("q1")
    //         .build()
    //         .unwrap();
    //     let w2cfg = WorkerConfigBuilder::default()
    //         .task_queue("q2")
    //         .build()
    //         .unwrap();
    //
    //     let mock_poller = mock_poller_from_resps(vec![].into());
    //     let mock_act_poller = mock_poller_from_resps(
    //         vec![PollActivityTaskQueueResponse {
    //             task_token: vec![1],
    //             activity_id: "act1".to_string(),
    //             heartbeat_timeout: Some(Duration::from_millis(1).into()),
    //             ..Default::default()
    //         }]
    //         .into(),
    //     );
    //     let worker1 = Worker::new_with_pollers(w1cfg, None, mock_poller, Some(mock_act_poller));
    //     let mock_poller = mock_poller_from_resps(vec![].into());
    //     // Worker 2's poller is slow to ensure cancel has time to propagate. It then returns a poll
    //     // "timeout" w/ default value.
    //     let mut mock_act_poller = mock_manual_poller();
    //     mock_act_poller.expect_poll().times(2).returning(|| {
    //         async {
    //             sleep(Duration::from_micros(100)).await;
    //             Default::default()
    //         }
    //         .boxed()
    //     });
    //     let worker2 =
    //         Worker::new_with_pollers(w2cfg, None, mock_poller, Some(Box::from(mock_act_poller)));
    //
    //     let task_mgr = WorkerActivityTasks::new(mock_gateway);
    //
    //     // First poll should get the activity
    //     let act = task_mgr.poll(&worker1).await.unwrap().unwrap();
    //     // Now record a heartbeat which will get the cancel response and mark the act as cancelled
    //     task_mgr
    //         .record_heartbeat(ActivityHeartbeat {
    //             task_token: vec![1],
    //             details: vec![],
    //         })
    //         .unwrap();
    //     // Poll with worker two, we should *not* receive the cancel for the first activity
    //     assert!(dbg!(task_mgr.poll(&worker2).await.unwrap()).is_none());
    //
    //     task_mgr.shutdown().await;
    // }
}
