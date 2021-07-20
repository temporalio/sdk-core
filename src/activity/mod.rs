mod activity_heartbeat_manager;
mod details;

pub(crate) use details::InflightActivityDetails;

use crate::{
    pollers::BoxedActPoller,
    protos::{
        coresdk::{
            activity_result::{self as ar, activity_result},
            activity_task::{ActivityCancelReason, ActivityTask},
            ActivityTaskCompletion,
        },
        temporal::api::workflowservice::v1::PollActivityTaskQueueResponse,
    },
    task_token::TaskToken,
    worker::{WorkerDispatcher, WorkerStatus},
    ActivityHeartbeat, ActivityHeartbeatError, CompleteActivityError, PollActivityError,
    ServerGatewayApis,
};
use activity_heartbeat_manager::ActivityHeartbeatManager;
use dashmap::DashMap;
use std::collections::HashMap;
use std::{convert::TryInto, ops::Div, sync::Arc, time::Duration};
use tokio::sync::{RwLock, Semaphore};

#[derive(Debug, derive_more::Constructor)]
pub(crate) struct PendingActivityCancel {
    task_token: TaskToken,
    reason: ActivityCancelReason,
}

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

pub(crate) struct WorkerActivityTasks {
    /// Centralizes management of heartbeat issuing / throttling
    heartbeat_manager: ActivityHeartbeatManager,
    /// Activities that have been issued to lang but not yet completed
    outstanding_activity_tasks: DashMap<TaskToken, InflightActivityDetails>,
    /// Buffers activity task polling in the event we need to return a cancellation while a poll is
    /// ongoing.
    poller: BoxedActPoller,
    /// Ensures we stay at or below this worker's maximum concurrent activity limit
    activities_semaphore: Semaphore,
}

impl WorkerActivityTasks {
    pub(crate) fn new<SG: ServerGatewayApis + Send + Sync + 'static>(
        max_activity_tasks: usize,
        poller: BoxedActPoller,
        sg: Arc<SG>,
    ) -> Self {
        Self {
            heartbeat_manager: ActivityHeartbeatManager::new(sg),
            outstanding_activity_tasks: Default::default(),
            poller,
            activities_semaphore: Semaphore::new(max_activity_tasks),
        }
    }

    pub(crate) fn notify_shutdown(&self) {
        self.poller.notify_shutdown();
    }

    pub(crate) async fn shutdown(self) {
        self.poller.shutdown_box().await;
        self.heartbeat_manager.shutdown().await;
    }

    /// Poll for an activity task. Returns `Ok(None)` if no activity is ready and the overall
    /// polling loop should be retried.
    pub(crate) async fn poll(&self) -> Result<Option<ActivityTask>, PollActivityError> {
        let poll_with_semaphore = async {
            // Acquire and subsequently forget a permit for an outstanding activity. When they are
            // completed, we must add a new permit to the semaphore, since holding the permit the
            // entire time lang does work would be a challenge.
            let sem = self
                .activities_semaphore
                .acquire()
                .await
                .expect("outstanding activity semaphore not closed");
            (self.poller.poll().await, sem)
        };

        tokio::select! {
            biased;

            cancel_task = self.next_pending_cancel_task() => {
                cancel_task
            }
            (work, sem) = poll_with_semaphore => {
                match work {
                    Some(Ok(work)) => {
                        if work == PollActivityTaskQueueResponse::default() {
                            // Timeout
                            return Ok(None)
                        }
                        self.outstanding_activity_tasks.insert(
                            work.task_token.clone().into(),
                            InflightActivityDetails::new(
                                work.activity_id.clone(),
                                work.heartbeat_timeout.clone(),
                                false,
                                false,
                            ),
                        );
                        // Only permanently take a permit in the event the poll finished properly
                        sem.forget();
                        Ok(Some(ActivityTask::start_from_poll_resp(work)))
                    }
                    None => {
                        Err(PollActivityError::ShutDown)
                    }
                    Some(Err(e)) => Err(e.into())
                }
            }
        }
    }

    /// Mark the activity associated with the task token as complete, returning the details about it
    /// if it was present.
    pub(crate) fn mark_complete(&self, task_token: &TaskToken) -> Option<InflightActivityDetails> {
        self.heartbeat_manager.evict(task_token.clone());
        self.activities_semaphore.add_permits(1);
        self.outstanding_activity_tasks
            .remove(&task_token)
            .map(|x| x.1)
    }

    /// Attempt to record an activity heartbeat
    pub(crate) fn record_heartbeat(
        &self,
        details: ActivityHeartbeat,
    ) -> Result<(), ActivityHeartbeatError> {
        // TODO: Propagate these back as cancels. Silent fails is too nonobvious
        let t: Duration = self
            .outstanding_activity_tasks
            .get(&TaskToken(details.task_token.clone()))
            .ok_or(ActivityHeartbeatError::UnknownActivity)?
            .heartbeat_timeout
            .clone()
            .ok_or(ActivityHeartbeatError::HeartbeatTimeoutNotSet)?
            .try_into()
            .or(Err(ActivityHeartbeatError::InvalidHeartbeatTimeout))?;
        // There is a bug in the server that translates non-set heartbeat timeouts into 0 duration.
        // That's why we treat 0 the same way as None, otherwise we wouldn't know which aggregation
        // delay to use, and using 0 is not a good idea as SDK would hammer the server too hard.
        if t.as_millis() == 0 {
            return Err(ActivityHeartbeatError::HeartbeatTimeoutNotSet);
        }
        self.heartbeat_manager.record(details, t.div(2))
    }

    async fn next_pending_cancel_task(&self) -> Result<Option<ActivityTask>, PollActivityError> {
        let next_pc = self.heartbeat_manager.next_pending_cancel().await;
        // Issue cancellations for anything we noticed was cancelled during heartbeating
        if let Some(PendingActivityCancel { task_token, reason }) = next_pc {
            // It's possible that activity has been completed and we no longer have an
            // outstanding activity task. This is fine because it means that we no
            // longer need to cancel this activity, so we'll just ignore such orphaned
            // cancellations.
            if let Some(mut details) = self.outstanding_activity_tasks.get_mut(&task_token) {
                if details.issued_cancel_to_lang {
                    // Don't double-issue cancellations
                    return Ok(None);
                }

                details.issued_cancel_to_lang = true;
                if reason == ActivityCancelReason::NotFound {
                    details.known_not_found = true;
                }
                Ok(Some(ActivityTask::cancel_from_ids(
                    task_token,
                    details.activity_id.clone(),
                    reason,
                )))
            } else {
                warn!(task_token = ?task_token,
                              "Unknown activity task when issuing cancel");
                // If we can't find the activity here, it's already been completed,
                // in which case issuing a cancel again is pointless.
                Ok(None)
            }
        } else {
            // The only situation where the next cancel would return none is if the manager
            // was dropped, which can only happen on shutdown.
            Err(PollActivityError::ShutDown)
        }
    }

    #[cfg(test)]
    pub(crate) fn remaining_activity_capacity(&self) -> usize {
        self.activities_semaphore.available_permits()
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
