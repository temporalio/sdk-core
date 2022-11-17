mod activity_heartbeat_manager;
mod local_activities;

pub(crate) use local_activities::{
    DispatchOrTimeoutLA, ExecutingLAId, LACompleteAction, LocalActRequest,
    LocalActivityExecutionResult, LocalActivityManager, LocalActivityResolution,
    LocalInFlightActInfo, NewLocalAct,
};

use crate::telemetry::metrics::eager;
use crate::{
    abstractions::{MeteredSemaphore, OwnedMeteredSemPermit},
    pollers::BoxedActPoller,
    telemetry::metrics::{activity_type, activity_worker_type, workflow_type, MetricsContext},
    worker::{
        activities::activity_heartbeat_manager::ActivityHeartbeatError, client::WorkerClient,
    },
    PollActivityError, TaskToken,
};
use activity_heartbeat_manager::ActivityHeartbeatManager;
use dashmap::DashMap;
use governor::{
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
    Quota, RateLimiter,
};
use std::{
    convert::TryInto,
    sync::Arc,
    time::{Duration, Instant},
};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{self as ar, activity_execution_result as aer},
        activity_task::{ActivityCancelReason, ActivityTask},
        ActivityHeartbeat,
    },
    temporal::api::{
        failure::v1::{failure::FailureInfo, CanceledFailureInfo, Failure},
        workflowservice::v1::PollActivityTaskQueueResponse,
    },
};
use tokio::sync::Notify;
use tracing::Span;

#[derive(Debug, derive_more::Constructor)]
struct PendingActivityCancel {
    task_token: TaskToken,
    reason: ActivityCancelReason,
}

/// Contains details that core wants to store while an activity is running.
#[derive(Debug)]
struct InFlightActInfo {
    pub activity_type: String,
    pub workflow_type: String,
    /// Only kept for logging reasons
    pub workflow_id: String,
    /// Only kept for logging reasons
    pub workflow_run_id: String,
    start_time: Instant,
}

/// Augments [InFlightActInfo] with details specific to remote activities
struct RemoteInFlightActInfo {
    pub base: InFlightActInfo,
    /// Used to calculate aggregation delay between activity heartbeats.
    pub heartbeat_timeout: Option<prost_types::Duration>,
    /// Set to true if we have already issued a cancellation activation to lang for this activity
    pub issued_cancel_to_lang: bool,
    /// Set to true if we have already learned from the server this activity doesn't exist. EX:
    /// we have learned from heartbeating and issued a cancel task, in which case we may simply
    /// discard the reply.
    pub known_not_found: bool,
    /// The permit from the max concurrent semaphore
    _permit: OwnedMeteredSemPermit,
}
impl RemoteInFlightActInfo {
    fn new(poll_resp: &PollActivityTaskQueueResponse, permit: OwnedMeteredSemPermit) -> Self {
        let wec = poll_resp.workflow_execution.clone().unwrap_or_default();
        Self {
            base: InFlightActInfo {
                activity_type: poll_resp.activity_type.clone().unwrap_or_default().name,
                workflow_type: poll_resp.workflow_type.clone().unwrap_or_default().name,
                workflow_id: wec.workflow_id,
                workflow_run_id: wec.run_id,
                start_time: Instant::now(),
            },
            heartbeat_timeout: poll_resp.heartbeat_timeout.clone(),
            issued_cancel_to_lang: false,
            known_not_found: false,
            _permit: permit,
        }
    }
}

struct NonPollActBuffer {
    tx: async_channel::Sender<PermittedTqResp>,
    rx: async_channel::Receiver<PermittedTqResp>,
}
impl NonPollActBuffer {
    pub fn new() -> Self {
        let (tx, rx) = async_channel::unbounded();
        Self { tx, rx }
    }

    pub async fn next(&self) -> PermittedTqResp {
        self.rx.recv().await.expect("Send half cannot be dropped")
    }
}

pub(crate) struct WorkerActivityTasks {
    /// Centralizes management of heartbeat issuing / throttling
    heartbeat_manager: ActivityHeartbeatManager,
    /// Activities that have been issued to lang but not yet completed
    outstanding_activity_tasks: DashMap<TaskToken, RemoteInFlightActInfo>,
    /// Buffers activity task polling in the event we need to return a cancellation while a poll is
    /// ongoing.
    poller: BoxedActPoller,
    /// Holds activity tasks we have received by non-polling means. EX: In direct response to
    /// workflow task completion.
    non_poll_tasks: NonPollActBuffer,
    /// Ensures we stay at or below this worker's maximum concurrent activity limit
    activities_semaphore: Arc<MeteredSemaphore>,
    /// Enables per-worker rate-limiting of activity tasks
    ratelimiter: Option<RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware>>,
    /// Wakes every time an activity is removed from the outstanding map
    complete_notify: Notify,

    metrics: MetricsContext,

    max_heartbeat_throttle_interval: Duration,
    default_heartbeat_throttle_interval: Duration,
}

impl WorkerActivityTasks {
    pub(crate) fn new(
        max_activity_tasks: usize,
        max_worker_act_per_sec: Option<f64>,
        poller: BoxedActPoller,
        client: Arc<dyn WorkerClient>,
        metrics: MetricsContext,
        max_heartbeat_throttle_interval: Duration,
        default_heartbeat_throttle_interval: Duration,
    ) -> Self {
        Self {
            heartbeat_manager: ActivityHeartbeatManager::new(client),
            outstanding_activity_tasks: Default::default(),
            poller,
            non_poll_tasks: NonPollActBuffer::new(),
            activities_semaphore: Arc::new(MeteredSemaphore::new(
                max_activity_tasks,
                metrics.with_new_attrs([activity_worker_type()]),
                MetricsContext::available_task_slots,
            )),
            ratelimiter: max_worker_act_per_sec.and_then(|ps| {
                Quota::with_period(Duration::from_secs_f64(ps.recip())).map(RateLimiter::direct)
            }),
            complete_notify: Notify::new(),
            metrics,
            max_heartbeat_throttle_interval,
            default_heartbeat_throttle_interval,
        }
    }

    pub(crate) fn notify_shutdown(&self) {
        self.poller.notify_shutdown();
    }

    /// Wait for all outstanding activity tasks to finish
    pub(crate) async fn wait_all_finished(&self) {
        while !self.outstanding_activity_tasks.is_empty() {
            self.complete_notify.notified().await
        }
    }

    pub(crate) async fn shutdown(self) {
        self.poller.shutdown_box().await;
        self.heartbeat_manager.shutdown().await;
    }

    /// Wait until not at the outstanding activity limit, and then poll for an activity task.
    ///
    /// Returns `Ok(None)` if no activity is ready and the overall polling loop should be retried.
    pub(crate) async fn poll(&self) -> Result<Option<ActivityTask>, PollActivityError> {
        let poll_with_semaphore = async {
            // Acquire and subsequently forget a permit for an outstanding activity. When they are
            // completed, we must add a new permit to the semaphore, since holding the permit the
            // entire time lang does work would be a challenge.
            let perm = self
                .activities_semaphore
                .acquire_owned()
                .await
                .expect("outstanding activity semaphore not closed");
            if let Some(ref rl) = self.ratelimiter {
                rl.until_ready().await;
            }
            (self.poller.poll().await, perm)
        };

        tokio::select! {
            biased;

            cancel_task = self.next_pending_cancel_task() => {
                cancel_task
            }
            task = self.non_poll_tasks.next() => {
                Ok(Some(self.about_to_issue_task(task, true)))
            }
            (work, permit) = poll_with_semaphore => {
                match work {
                    Some(Ok(work)) => {
                        if work == PollActivityTaskQueueResponse::default() {
                            // Timeout
                            self.metrics.act_poll_timeout();
                            return Ok(None)
                        }
                        let work = self.about_to_issue_task(PermittedTqResp {
                           resp: work, permit
                        }, false);
                        Ok(Some(work))
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
        status: aer::Status,
        client: &dyn WorkerClient,
    ) {
        if let Some((_, act_info)) = self.outstanding_activity_tasks.remove(&task_token) {
            let act_metrics = self.metrics.with_new_attrs([
                activity_type(act_info.base.activity_type),
                workflow_type(act_info.base.workflow_type),
            ]);
            Span::current().record("workflow_id", act_info.base.workflow_id);
            Span::current().record("run_id", act_info.base.workflow_run_id);
            act_metrics.act_execution_latency(act_info.base.start_time.elapsed());
            let known_not_found = act_info.known_not_found;

            self.heartbeat_manager.evict(task_token.clone()).await;
            self.complete_notify.notify_waiters();

            // No need to report activities which we already know the server doesn't care about
            if !known_not_found {
                let maybe_net_err = match status {
                    aer::Status::WillCompleteAsync(_) => None,
                    aer::Status::Completed(ar::Success { result }) => client
                        .complete_activity_task(task_token.clone(), result.map(Into::into))
                        .await
                        .err(),
                    aer::Status::Failed(ar::Failure { failure }) => {
                        act_metrics.act_execution_failed();
                        client
                            .fail_activity_task(task_token.clone(), failure.map(Into::into))
                            .await
                            .err()
                    }
                    aer::Status::Cancelled(ar::Cancellation { failure }) => {
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
                        client
                            .cancel_activity_task(task_token.clone(), details.map(Into::into))
                            .await
                            .err()
                    }
                };

                if let Some(e) = maybe_net_err {
                    if e.code() == tonic::Code::NotFound {
                        warn!(task_token = ?task_token, details = ?e, "Activity not found on \
                        completion. This may happen if the activity has already been cancelled but \
                        completed anyway.");
                    } else {
                        warn!(error=?e, "Network error while completing activity");
                    };
                };
            };
        } else {
            warn!(
                "Attempted to complete activity task {} but we were not tracking it",
                &task_token
            );
        }
    }

    /// Attempt to record an activity heartbeat
    pub(crate) fn record_heartbeat(
        &self,
        details: ActivityHeartbeat,
    ) -> Result<(), ActivityHeartbeatError> {
        // TODO: Propagate these back as cancels. Silent fails is too nonobvious
        let heartbeat_timeout: Duration = self
            .outstanding_activity_tasks
            .get(&TaskToken(details.task_token.clone()))
            .ok_or(ActivityHeartbeatError::UnknownActivity)?
            .heartbeat_timeout
            .clone()
            // We treat None as 0 (even though heartbeat_timeout is never set to None by the server)
            .unwrap_or_default()
            .try_into()
            // This technically should never happen since prost duration should be directly mappable
            // to std::time::Duration.
            .or(Err(ActivityHeartbeatError::InvalidHeartbeatTimeout))?;

        // There is a bug in the server that translates non-set heartbeat timeouts into 0 duration.
        // That's why we treat 0 the same way as None, otherwise we wouldn't know which aggregation
        // delay to use, and using 0 is not a good idea as SDK would hammer the server too hard.
        let throttle_interval = if heartbeat_timeout.as_millis() == 0 {
            self.default_heartbeat_throttle_interval
        } else {
            heartbeat_timeout.mul_f64(0.8)
        };
        let throttle_interval =
            std::cmp::min(throttle_interval, self.max_heartbeat_throttle_interval);
        self.heartbeat_manager.record(details, throttle_interval)
    }

    /// Returns a handle that the workflows management side can use to interact with this manager
    pub(crate) fn get_handle_for_workflows(&self) -> ActivitiesFromWFTsHandle {
        ActivitiesFromWFTsHandle {
            sem: self.activities_semaphore.clone(),
            tx: self.non_poll_tasks.tx.clone(),
        }
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
                Ok(Some(ActivityTask::cancel_from_ids(task_token.0, reason)))
            } else {
                debug!(task_token = ?task_token, "Unknown activity task when issuing cancel");
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

    /// Called when there is a new act task about to be bubbled up out of the manager
    fn about_to_issue_task(&self, task: PermittedTqResp, is_eager: bool) -> ActivityTask {
        if let Some(ref act_type) = task.resp.activity_type {
            if let Some(ref wf_type) = task.resp.workflow_type {
                self.metrics
                    .with_new_attrs([
                        activity_type(act_type.name.clone()),
                        workflow_type(wf_type.name.clone()),
                        eager(is_eager),
                    ])
                    .act_task_received();
            }
        }
        // There could be an else statement here but since the response should always contain both
        // activity_type and workflow_type, we won't bother.

        if let Some(dur) = task.resp.sched_to_start() {
            self.metrics.act_sched_to_start_latency(dur);
        };

        self.outstanding_activity_tasks.insert(
            task.resp.task_token.clone().into(),
            RemoteInFlightActInfo::new(&task.resp, task.permit),
        );

        ActivityTask::start_from_poll_resp(task.resp)
    }

    #[cfg(test)]
    pub(crate) fn remaining_activity_capacity(&self) -> usize {
        self.activities_semaphore.available_permits()
    }
}

/// Provides facilities for the workflow side of things to interact with the activity manager.
/// Allows for the handling of activities returned by WFT completions.
pub(crate) struct ActivitiesFromWFTsHandle {
    sem: Arc<MeteredSemaphore>,
    tx: async_channel::Sender<PermittedTqResp>,
}

impl ActivitiesFromWFTsHandle {
    /// Returns a handle that can be used to reserve an activity slot. EX: When requesting eager
    /// dispatch of an activity to this worker upon workflow task completion
    pub(crate) fn reserve_slot(&self) -> Option<OwnedMeteredSemPermit> {
        self.sem.try_acquire_owned().ok()
    }

    /// Queue new activity tasks for dispatch received from non-polling sources (ex: eager returns
    /// from WFT completion)
    pub(crate) fn add_tasks(&self, tasks: impl IntoIterator<Item = PermittedTqResp>) {
        for t in tasks.into_iter() {
            // Technically we should be reporting `activity_task_received` here, but for simplicity
            // and time insensitivity, that metric is tracked in `about_to_issue_task`.
            self.tx.try_send(t).expect("Receive half cannot be dropped");
        }
    }
}

pub(crate) struct PermittedTqResp {
    pub permit: OwnedMeteredSemPermit,
    pub resp: PollActivityTaskQueueResponse,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        test_help::mock_poller_from_resps, worker::client::mocks::mock_manual_workflow_client,
    };

    #[tokio::test]
    async fn per_worker_ratelimit() {
        let poller = mock_poller_from_resps([
            PollActivityTaskQueueResponse {
                task_token: vec![1],
                activity_id: "act1".to_string(),
                ..Default::default()
            }
            .into(),
            PollActivityTaskQueueResponse {
                task_token: vec![2],
                activity_id: "act2".to_string(),
                ..Default::default()
            }
            .into(),
        ]);
        let atm = WorkerActivityTasks::new(
            10,
            Some(2.0),
            poller,
            Arc::new(mock_manual_workflow_client()),
            MetricsContext::no_op(),
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        let start = Instant::now();
        atm.poll().await.unwrap().unwrap();
        atm.poll().await.unwrap().unwrap();
        // At least half a second will have elapsed since we only allow 2 tasks per second.
        // With no ratelimit, even on a slow CI server with lots of load, this would typically take
        // low single digit ms or less.
        assert!(start.elapsed() > Duration::from_secs_f64(0.5));
    }
}
