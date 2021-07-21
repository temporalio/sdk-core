mod activity_heartbeat_manager;

use crate::{
    pollers::BoxedActPoller,
    protos::{
        coresdk::{
            activity_task::{ActivityCancelReason, ActivityTask},
            ActivityHeartbeat,
        },
        temporal::api::workflowservice::v1::PollActivityTaskQueueResponse,
    },
    task_token::TaskToken,
    ActivityHeartbeatError, PollActivityError, ServerGatewayApis,
};
use activity_heartbeat_manager::ActivityHeartbeatManager;
use dashmap::DashMap;
use std::{convert::TryInto, ops::Div, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[derive(Debug, derive_more::Constructor)]
struct PendingActivityCancel {
    task_token: TaskToken,
    reason: ActivityCancelReason,
}

/// Contains minimal set of details that core needs to store, while activity is running.
#[derive(derive_more::Constructor, Debug)]
struct InflightActivityDetails {
    pub activity_id: String,
    /// Used to calculate aggregation delay between activity heartbeats.
    pub heartbeat_timeout: Option<prost_types::Duration>,
    /// Set to true if we have already issued a cancellation activation to lang for this activity
    pub issued_cancel_to_lang: bool,
    /// Set to true if we have already learned from the server this activity doesn't exist. EX:
    /// we have learned from heartbeating and issued a cancel task, in which case we may simply
    /// discard the reply.
    pub known_not_found: bool,
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
    pub(crate) fn mark_complete(&self, task_token: &TaskToken) {
        self.heartbeat_manager.evict(task_token.clone());
        self.activities_semaphore.add_permits(1);
        self.outstanding_activity_tasks
            .remove(&task_token)
            .map(|x| x.1);
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
