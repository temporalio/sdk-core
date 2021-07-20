mod activity_heartbeat_manager;
mod details;

pub(crate) use activity_heartbeat_manager::ActivityHeartbeatManager;
pub(crate) use details::InflightActivityDetails;

use crate::{
    protos::coresdk::activity_task::{ActivityCancelReason, ActivityTask},
    task_token::TaskToken,
    worker::Worker,
    ActivityHeartbeat, ActivityHeartbeatError, PollActivityError, ServerGatewayApis,
};
use dashmap::DashMap;
use std::{convert::TryInto, ops::Div, sync::Arc, time::Duration};

#[derive(Debug, derive_more::Constructor)]
pub(crate) struct PendingActivityCancel {
    task_token: TaskToken,
    reason: ActivityCancelReason,
}

pub(crate) struct ActivityTaskManager {
    /// Centralizes management of heartbeat issuing / throttling
    activity_heartbeat_manager: ActivityHeartbeatManager,
    /// Activities that have been issued to lang but not yet completed
    outstanding_activity_tasks: DashMap<TaskToken, InflightActivityDetails>,
}

impl ActivityTaskManager {
    pub(crate) fn new<SG: ServerGatewayApis + Send + Sync + 'static>(sg: Arc<SG>) -> Self {
        Self {
            activity_heartbeat_manager: ActivityHeartbeatManager::new(sg),
            outstanding_activity_tasks: Default::default(),
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.activity_heartbeat_manager.shutdown().await;
    }

    /// Poll for an activity task using the provided worker. Returns `Ok(None)` if no activity is
    /// ready and the overall polling loop should be retried.
    pub(crate) async fn poll(
        &self,
        worker: &Worker,
    ) -> Result<Option<ActivityTask>, PollActivityError> {
        tokio::select! {
            biased;

            cancel_task = self.next_pending_cancel_task_for_queue(worker.task_queue()) => {
                cancel_task
            }
            work = worker.activity_poll() => {
                match work {
                    Ok(None) => Ok(None), // Timeout
                    Ok(Some(work)) => {
                        self.outstanding_activity_tasks.insert(
                            work.task_token.clone().into(),
                            InflightActivityDetails::new(
                                work.activity_id.clone(),
                                work.heartbeat_timeout.clone(),
                                false,
                                false,
                                worker.task_queue().to_owned()
                            ),
                        );
                        Ok(Some(ActivityTask::start_from_poll_resp(work)))
                    }
                    Err(e) => Err(e)
                }
            }
        }
    }

    /// Mark the activity associated with the task token as complete, returning the details about it
    /// if it was present.
    pub(crate) fn mark_complete(&self, task_token: &TaskToken) -> Option<InflightActivityDetails> {
        self.activity_heartbeat_manager.evict(task_token.clone());
        self.outstanding_activity_tasks
            .remove(&task_token)
            .map(|x| x.1)
    }

    /// Attempt to record an activity heartbeat
    pub(crate) fn record_activity_heartbeat(
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
        self.activity_heartbeat_manager.record(details, t.div(2))
    }

    /// Returns true if we already know the activity with the given task token has been forgotten
    /// by server
    pub(crate) fn is_known_not_found(&self, task_token: &TaskToken) -> bool {
        self.outstanding_activity_tasks
            .get(&task_token)
            .map(|d| d.known_not_found)
            .unwrap_or_default()
    }

    async fn next_pending_cancel_task_for_queue(
        &self,
        task_queue: &str,
    ) -> Result<Option<ActivityTask>, PollActivityError> {
        let next_pc = self.activity_heartbeat_manager.next_pending_cancel().await;
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

                // If this cancel is not for this worker, tell the worker it *is* for about it
                if &details.task_queue != task_queue {}

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pollers::MockServerGatewayApis,
        protos::temporal::api::workflowservice::v1::{
            PollActivityTaskQueueResponse, RecordActivityTaskHeartbeatResponse,
        },
        test_help::{mock_manual_poller, mock_poller_from_resps},
        worker::WorkerConfigBuilder,
    };
    use futures::FutureExt;
    use tokio::time::sleep;

    #[tokio::test]
    async fn only_returns_cancels_for_desired_queue() {
        let mut mock_gateway = MockServerGatewayApis::new();
        // Mark the activity as needing cancel
        mock_gateway
            .expect_record_activity_heartbeat()
            .times(1)
            .returning(|_, _| {
                Ok(RecordActivityTaskHeartbeatResponse {
                    cancel_requested: true,
                })
            });

        let mock_gateway = Arc::new(mock_gateway);
        let w1cfg = WorkerConfigBuilder::default()
            .task_queue("q1")
            .build()
            .unwrap();
        let w2cfg = WorkerConfigBuilder::default()
            .task_queue("q2")
            .build()
            .unwrap();

        let mock_poller = mock_poller_from_resps(vec![].into());
        let mock_act_poller = mock_poller_from_resps(
            vec![PollActivityTaskQueueResponse {
                task_token: vec![1],
                activity_id: "act1".to_string(),
                heartbeat_timeout: Some(Duration::from_millis(1).into()),
                ..Default::default()
            }]
            .into(),
        );
        let worker1 = Worker::new_with_pollers(w1cfg, None, mock_poller, Some(mock_act_poller));
        let mock_poller = mock_poller_from_resps(vec![].into());
        // Worker 2's poller is slow to ensure cancel has time to propagate. It then returns a poll
        // "timeout" w/ default value.
        let mut mock_act_poller = mock_manual_poller();
        mock_act_poller.expect_poll().times(2).returning(|| {
            async {
                sleep(Duration::from_micros(100)).await;
                Default::default()
            }
            .boxed()
        });
        let worker2 =
            Worker::new_with_pollers(w2cfg, None, mock_poller, Some(Box::from(mock_act_poller)));

        let task_mgr = ActivityTaskManager::new(mock_gateway);

        // First poll should get the activity
        let act = task_mgr.poll(&worker1).await.unwrap().unwrap();
        // Now record a heartbeat which will get the cancel response and mark the act as cancelled
        task_mgr
            .record_activity_heartbeat(ActivityHeartbeat {
                task_token: vec![1],
                details: vec![],
            })
            .unwrap();
        // Poll with worker two, we should *not* receive the cancel for the first activity
        assert!(dbg!(task_mgr.poll(&worker2).await.unwrap()).is_none());

        task_mgr.shutdown().await;
    }
}
