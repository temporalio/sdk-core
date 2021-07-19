mod activity_heartbeat_manager;
mod details;

pub(crate) use activity_heartbeat_manager::ActivityHeartbeatManager;
pub(crate) use details::InflightActivityDetails;

use crate::{
    protos::coresdk::activity_task::ActivityTask, task_token::TaskToken, worker::Worker,
    ActivityHeartbeat, ActivityHeartbeatError, PollActivityError, ServerGatewayApis,
};
use dashmap::DashMap;
use std::{convert::TryInto, ops::Div, sync::Arc, time::Duration};

pub(crate) struct ActivityTaskManager {
    /// Handle to the heartbeat manager
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

            maybe_tt = self.activity_heartbeat_manager.next_pending_cancel() => {
                // Issue cancellations for anything we noticed was cancelled during heartbeating
                if let Some((task_token, cancel_reason)) = maybe_tt {
                    // It's possible that activity has been completed and we no longer have an
                    // outstanding activity task. This is fine because it means that we no
                    // longer need to cancel this activity, so we'll just ignore such orphaned
                    // cancellations.
                    if let Some(mut details) =
                        self.outstanding_activity_tasks.get_mut(&task_token) {
                            if details.issued_cancel_to_lang {
                                // Don't double-issue cancellations
                                return Ok(None)
                            }
                            details.issued_cancel_to_lang = true;
                            return Ok(Some(ActivityTask::cancel_from_ids(
                                task_token,
                                details.activity_id.clone(),
                                cancel_reason
                            )));
                    } else {
                        warn!(task_token = ?task_token,
                              "Unknown activity task when issuing cancel");
                        // If we can't find the activity here, it's already been completed,
                        // in which case issuing a cancel again is pointless.
                        return Ok(None)
                    }
                }
                // The only situation where the next cancel would return none is if the manager
                // was dropped, which can only happen on shutdown.
                Err(PollActivityError::ShutDown)
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
}
