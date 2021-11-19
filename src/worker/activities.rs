mod activity_heartbeat_manager;

use crate::{
    pollers::BoxedActPoller,
    task_token::TaskToken,
    telemetry::metrics::{activity_type, workflow_type, MetricsContext},
    ActivityHeartbeatError, CompleteActivityError, PollActivityError, ServerGatewayApis,
};
use activity_heartbeat_manager::ActivityHeartbeatManager;
use dashmap::DashMap;
use std::{
    convert::TryInto,
    ops::Div,
    sync::Arc,
    time::{Duration, Instant},
};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{self as ar, activity_result},
        activity_task::{ActivityCancelReason, ActivityTask},
        ActivityHeartbeat,
    },
    temporal::api::{
        failure::v1::{failure::FailureInfo, CanceledFailureInfo, Failure},
        workflowservice::v1::PollActivityTaskQueueResponse,
    },
};
use tokio::sync::Semaphore;

#[derive(Debug, derive_more::Constructor)]
struct PendingActivityCancel {
    task_token: TaskToken,
    reason: ActivityCancelReason,
}

/// Contains minimal set of details that core needs to store, while activity is running.
#[derive(Debug)]
struct InflightActivityDetails {
    pub activity_id: String,
    pub activity_type: String,
    pub workflow_type: String,
    /// Used to calculate aggregation delay between activity heartbeats.
    pub heartbeat_timeout: Option<prost_types::Duration>,
    /// Set to true if we have already issued a cancellation activation to lang for this activity
    pub issued_cancel_to_lang: bool,
    /// Set to true if we have already learned from the server this activity doesn't exist. EX:
    /// we have learned from heartbeating and issued a cancel task, in which case we may simply
    /// discard the reply.
    pub known_not_found: bool,
    start_time: Instant,
}
impl InflightActivityDetails {
    fn new(
        activity_id: String,
        activity_type: String,
        workflow_type: String,
        heartbeat_timeout: Option<prost_types::Duration>,
    ) -> Self {
        Self {
            activity_id,
            activity_type,
            workflow_type,
            heartbeat_timeout,
            issued_cancel_to_lang: false,
            known_not_found: false,
            start_time: Instant::now(),
        }
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

    metrics: MetricsContext,
}

impl WorkerActivityTasks {
    pub(crate) fn new(
        max_activity_tasks: usize,
        poller: BoxedActPoller,
        sg: Arc<impl ServerGatewayApis + Send + Sync + 'static + ?Sized>,
        metrics: MetricsContext,
    ) -> Self {
        Self {
            heartbeat_manager: ActivityHeartbeatManager::new(sg),
            outstanding_activity_tasks: Default::default(),
            poller,
            activities_semaphore: Semaphore::new(max_activity_tasks),
            metrics,
        }
    }

    pub(crate) fn notify_shutdown(&self) {
        self.poller.notify_shutdown();
        self.heartbeat_manager.notify_shutdown();
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
                            self.metrics.act_poll_timeout();
                            return Ok(None)
                        }

                        if let Some(dur) = work.sched_to_start() {
                            self.metrics
                                .act_sched_to_start_latency(dur);
                        }

                        self.outstanding_activity_tasks.insert(
                            work.task_token.clone().into(),
                            InflightActivityDetails::new(
                                work.activity_id.clone(),
                                work.activity_type.clone().unwrap_or_default().name,
                                work.workflow_type.clone().unwrap_or_default().name,
                                work.heartbeat_timeout.clone()
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

    pub(crate) async fn complete(
        &self,
        task_token: TaskToken,
        status: activity_result::Status,
        gateway: &(dyn ServerGatewayApis + Send + Sync),
    ) -> Result<(), CompleteActivityError> {
        if let Some(act_info) = self.outstanding_activity_tasks.get(&task_token) {
            let act_metrics = self.metrics.with_new_attrs([
                activity_type(act_info.activity_type.clone()),
                workflow_type(act_info.workflow_type.clone()),
            ]);
            act_metrics.act_execution_latency(act_info.start_time.elapsed());

            // No need to report activities which we already know the server doesn't care about
            let should_remove = if act_info.known_not_found {
                true
            } else {
                drop(act_info); // TODO: Get rid of dashmap. If we hold ref across await, bad stuff.
                let maybe_net_err = match status {
                    activity_result::Status::WillCompleteAsync(_) => None,
                    activity_result::Status::Completed(ar::Success { result }) => gateway
                        .complete_activity_task(task_token.clone(), result.map(Into::into))
                        .await
                        .err(),
                    activity_result::Status::Failed(ar::Failure { failure }) => {
                        act_metrics.act_execution_failed();
                        gateway
                            .fail_activity_task(task_token.clone(), failure.map(Into::into))
                            .await
                            .err()
                    }
                    activity_result::Status::Cancelled(ar::Cancellation { failure }) => {
                        let details = if let Some(Failure {
                            failure_info:
                                Some(FailureInfo::CanceledFailureInfo(CanceledFailureInfo { details })),
                            ..
                        }) = failure
                        {
                            details
                        } else {
                            warn!(task_token = ? task_token,
                                "Expected activity cancelled status with CanceledFailureInfo");
                            None
                        };
                        gateway
                            .cancel_activity_task(task_token.clone(), details.map(Into::into))
                            .await
                            .err()
                    }
                };
                match maybe_net_err {
                    Some(e) if e.code() == tonic::Code::NotFound => {
                        warn!(task_token = ?task_token, details = ?e, "Activity not found on \
                        completion. This may happen if the activity has already been cancelled but \
                        completed anyway.");
                        true
                    }
                    Some(err) => return Err(err.into()),
                    None => true,
                }
            };

            if should_remove
                && self
                    .outstanding_activity_tasks
                    .remove(&task_token)
                    .is_some()
            {
                self.activities_semaphore.add_permits(1);
                self.heartbeat_manager.evict(task_token);
            }
            Ok(())
        } else {
            warn!(
                "Attempted to complete activity task {} but we were not tracking it",
                &task_token
            );
            Ok(())
        }
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
                    task_token.0,
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
