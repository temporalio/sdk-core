mod activity_heartbeat_manager;
mod activity_task_poller_stream;
mod local_activities;

pub(crate) use local_activities::{
    DispatchOrTimeoutLA, ExecutingLAId, LACompleteAction, LocalActRequest,
    LocalActivityExecutionResult, LocalActivityManager, LocalActivityResolution,
    LocalInFlightActInfo, NewLocalAct,
};

use crate::abstractions::{ClosableMeteredSemaphore, TrackedOwnedMeteredSemPermit};
use crate::worker::activities::activity_task_poller_stream::new_activity_task_poller;
use crate::{
    abstractions::{MeteredSemaphore, OwnedMeteredSemPermit, UsedMeteredSemPermit},
    pollers::BoxedActPoller,
    telemetry::metrics::{
        activity_type, activity_worker_type, eager, workflow_type, MetricsContext,
    },
    worker::{
        activities::activity_heartbeat_manager::ActivityHeartbeatError, client::WorkerClient,
    },
    PollActivityError, TaskToken,
};
use activity_heartbeat_manager::ActivityHeartbeatManager;
use dashmap::DashMap;
use futures::{stream, stream::BoxStream, stream::PollNext, Stream, StreamExt};
use governor::{Quota, RateLimiter};
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
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::Span;

#[derive(Debug, derive_more::Constructor)]
pub(crate) struct PendingActivityCancel {
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
    _permit: UsedMeteredSemPermit,
}
impl RemoteInFlightActInfo {
    fn new(poll_resp: &PollActivityTaskQueueResponse, permit: UsedMeteredSemPermit) -> Self {
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

pub(crate) struct WorkerActivityTasks {
    /// Token used to as a shutdown signal to the server task poller
    poller_shutdown_token: CancellationToken,
    /// Centralizes management of heartbeat issuing / throttling
    heartbeat_manager: Arc<ActivityHeartbeatManager>,
    /// Combined stream for any ActivityTask producing source (polls, eager activities, cancellations)
    activity_task_stream: Mutex<BoxStream<'static, Result<ActivityTask, PollActivityError>>>,
    /// Activities that have been issued to lang but not yet completed
    outstanding_activity_tasks: Arc<DashMap<TaskToken, RemoteInFlightActInfo>>,
    /// Holds activity tasks we have received by non-polling means. EX: In direct response to
    /// workflow task completion.
    eager_activities_tx: UnboundedSender<TrackedPermittedTqResp>,
    /// Ensures we stay at or below this worker's maximum concurrent activity limit
    eager_activities_semaphore: Arc<ClosableMeteredSemaphore>,

    metrics: MetricsContext,

    max_heartbeat_throttle_interval: Duration,
    default_heartbeat_throttle_interval: Duration,

    /// Wakes every time an activity is removed from the outstanding map
    complete_notify: Arc<Notify>,
    /// Token to notify when poll returned a shutdown error
    poll_returned_shutdown_token: CancellationToken,
    drain_watch_task: JoinHandle<()>,
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
        let semaphore = Arc::new(MeteredSemaphore::new(
            max_activity_tasks,
            metrics.with_new_attrs([activity_worker_type()]),
            MetricsContext::available_task_slots,
        ));
        let shutdown_token = CancellationToken::new();
        let ratelimiter = max_worker_act_per_sec.and_then(|ps| {
            Quota::with_period(Duration::from_secs_f64(ps.recip())).map(RateLimiter::direct)
        });
        let outstanding_activity_tasks = Arc::new(DashMap::new());
        let server_poller_stream = new_activity_task_poller(
            poller,
            semaphore.clone(),
            ratelimiter,
            metrics.clone(),
            shutdown_token.clone(),
        );
        let (eager_activities_tx, eager_activities_rx) = unbounded_channel();
        let eager_activities_semaphore = Arc::new(ClosableMeteredSemaphore::new(semaphore));

        let start_tasks_stream_complete = CancellationToken::new();
        let starts_stream = Self::merge_start_task_sources(
            eager_activities_rx,
            server_poller_stream,
            metrics.clone(),
            outstanding_activity_tasks.clone(),
            eager_activities_semaphore.clone(),
            start_tasks_stream_complete.clone(),
        );
        let (heartbeat_manager, cancels_rx) = ActivityHeartbeatManager::new(client);
        let heartbeat_manager = Arc::new(heartbeat_manager);
        let cancels_stream =
            Self::make_cancel_task_stream(cancels_rx.into(), outstanding_activity_tasks.clone());
        // Create a task stream composed of (in poll preference order):
        //  cancels_stream ------------------------------+--- activity_task_stream
        //  eager_activities_rx ---+--- starts_stream ---|
        //  server_poll_stream  ---|
        let activity_task_stream =
            stream::select_with_strategy(cancels_stream, starts_stream, |_: &mut ()| {
                PollNext::Left
            });

        let complete_notify = Arc::new(Notify::new());
        let drain_watch_task = tokio::spawn(Self::wait_all_finished(
            start_tasks_stream_complete,
            outstanding_activity_tasks.clone(),
            heartbeat_manager.clone(),
            complete_notify.clone(),
        ));
        Self {
            poller_shutdown_token: shutdown_token,
            eager_activities_tx,
            heartbeat_manager,
            activity_task_stream: Mutex::new(activity_task_stream.boxed()),
            outstanding_activity_tasks,
            eager_activities_semaphore,
            complete_notify,
            metrics,
            max_heartbeat_throttle_interval,
            default_heartbeat_throttle_interval,
            poll_returned_shutdown_token: CancellationToken::new(),
            drain_watch_task,
        }
    }

    /// Merges the server poll and eager [ActivityTask] sources
    fn merge_start_task_sources(
        non_poll_tasks_rx: UnboundedReceiver<TrackedPermittedTqResp>,
        poller_stream: impl Stream<Item = Result<PermittedTqResp, tonic::Status>>,
        metrics: MetricsContext,
        outstanding_tasks: Arc<DashMap<TaskToken, RemoteInFlightActInfo>>,
        eager_activities_semaphore: Arc<ClosableMeteredSemaphore>,
        on_complete_token: CancellationToken,
    ) -> impl Stream<Item = Result<ActivityTask, PollActivityError>> {
        let non_poll_stream = stream::unfold(
            (non_poll_tasks_rx, eager_activities_semaphore),
            |(mut non_poll_tasks_rx, eager_activities_semaphore)| async move {
                loop {
                    tokio::select! {
                        biased;

                        task_opt = non_poll_tasks_rx.recv() => {
                            // Add is_eager true and wrap in Result
                            return task_opt.map(|task| (Ok((PermittedTqResp{ permit: task.permit.into(), resp: task.resp }, true)), (non_poll_tasks_rx, eager_activities_semaphore)));
                        }
                        _ = eager_activities_semaphore.close_complete() => {
                            // Once shutting down, we stop accepting eager activities
                            non_poll_tasks_rx.close();
                            continue;
                        }
                    }
                }
            },
        );
        let cloning_stream = stream::unfold(
            (metrics, outstanding_tasks),
            |(metrics, outstanding_tasks)| async move {
                Some((
                    (metrics.clone(), outstanding_tasks.clone()),
                    (metrics, outstanding_tasks),
                ))
            },
        );
        // Add is_eager false
        let poller_stream = poller_stream.map(|res| res.map(|task| (task, false)));

        // Prefer eager activities over new tasks
        stream::select_with_strategy(non_poll_stream, poller_stream, |_: &mut ()| PollNext::Left)
            .zip(cloning_stream)
            .map(|(res, (metrics, outstanding_tasks))| {
                res.map(|(task, is_eager)| {
                    Self::about_to_issue_task(outstanding_tasks, task, is_eager, metrics)
                })
                .map_err(|err| err.into())
            })
            .map(Some)
            .chain(futures::stream::once(async move {
                on_complete_token.cancel();
                None
            }))
            .filter_map(|item| async { item })
    }

    /// Builds an [ActivityTask] stream for cancellation tasks from cancels delivered by the `heartbeat_manager`
    fn make_cancel_task_stream(
        cancels_stream: UnboundedReceiverStream<PendingActivityCancel>,
        outstanding_tasks: Arc<DashMap<TaskToken, RemoteInFlightActInfo>>,
    ) -> impl Stream<Item = Result<ActivityTask, PollActivityError>> {
        let cloning_stream = stream::unfold(outstanding_tasks, |outstanding_tasks| async move {
            Some((outstanding_tasks.clone(), outstanding_tasks))
        });
        cancels_stream.zip(cloning_stream).filter_map(|(next_pc, outstanding_tasks)| async move {
            // It's possible that activity has been completed and we no longer have an
            // outstanding activity task. This is fine because it means that we no
            // longer need to cancel this activity, so we'll just ignore such orphaned
            // cancellations.
            if let Some(mut details) = outstanding_tasks.get_mut(&next_pc.task_token) {
                if details.issued_cancel_to_lang {
                    // Don't double-issue cancellations
                    return None
                }

                details.issued_cancel_to_lang = true;
                if next_pc.reason == ActivityCancelReason::NotFound {
                    details.known_not_found = true;
                }
                Some(Ok(ActivityTask::cancel_from_ids(next_pc.task_token.0, next_pc.reason)))
            } else {
                debug!(task_token = ?next_pc.task_token, "Unknown activity task when issuing cancel");
                // If we can't find the activity here, it's already been completed,
                // in which case issuing a cancel again is pointless.
                None
            }
        })
    }

    pub(crate) fn notify_shutdown(&self) {
        self.poller_shutdown_token.cancel();
        self.eager_activities_semaphore.close();
    }

    /// Wait for all outstanding activity tasks to finish
    async fn wait_all_finished(
        task_start_stream_complete: CancellationToken,
        outstanding_tasks: Arc<DashMap<TaskToken, RemoteInFlightActInfo>>,
        heartbeat_manager: Arc<ActivityHeartbeatManager>,
        complete_notify: Arc<Notify>,
    ) {
        task_start_stream_complete.cancelled().await;
        while !outstanding_tasks.is_empty() {
            complete_notify.notified().await
        }
        heartbeat_manager.notify_shutdown();
    }

    pub(crate) async fn shutdown2(&self) {
        self.notify_shutdown();
        self.poll_returned_shutdown_token.cancelled().await;
    }
    pub(crate) async fn shutdown(self) {
        self.notify_shutdown();
        self.poll_returned_shutdown_token.cancelled().await;
        if let Err(e) = self.drain_watch_task.await {
            if !e.is_cancelled() {
                error!(
                    "Unexpected error joining activity tasks during shutdown: {:?}",
                    e
                )
            }
        }
    }

    /// Exclusive poll for activity tasks
    ///
    /// Polls the various task sources (server polls, eager activities, cancellations) while respecting the provided rate limits and allowed concurrency.
    /// Returns Err(PollActivityError::ShutDown) after shutdown is completed and all tasks sources are depleted.
    pub(crate) async fn poll(&self) -> Result<ActivityTask, PollActivityError> {
        let mut poller_stream = self.activity_task_stream.lock().await;
        poller_stream.next().await.unwrap_or_else(|| {
            self.poll_returned_shutdown_token.cancel();
            Err(PollActivityError::ShutDown)
        })
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
            sem: self.eager_activities_semaphore.clone(),
            tx: self.eager_activities_tx.clone(),
        }
    }

    /// Called when there is a new [ActivityTask] about to be bubbled up out of the poller
    fn about_to_issue_task(
        outstanding_tasks: Arc<DashMap<TaskToken, RemoteInFlightActInfo>>,
        task: PermittedTqResp,
        is_eager: bool,
        metrics: MetricsContext,
    ) -> ActivityTask {
        if let Some(ref act_type) = task.resp.activity_type {
            if let Some(ref wf_type) = task.resp.workflow_type {
                metrics
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
            metrics.act_sched_to_start_latency(dur);
        };

        outstanding_tasks.insert(
            task.resp.task_token.clone().into(),
            RemoteInFlightActInfo::new(&task.resp, task.permit.into_used()),
        );

        ActivityTask::start_from_poll_resp(task.resp)
    }

    #[cfg(test)]
    pub(crate) fn remaining_activity_capacity(&self) -> usize {
        self.eager_activities_semaphore.available_permits()
    }
}

/// Provides facilities for the workflow side of things to interact with the activity manager.
/// Allows for the handling of activities returned by WFT completions.
pub(crate) struct ActivitiesFromWFTsHandle {
    sem: Arc<ClosableMeteredSemaphore>,
    tx: UnboundedSender<TrackedPermittedTqResp>,
}

impl ActivitiesFromWFTsHandle {
    /// Returns a handle that can be used to reserve an activity slot. EX: When requesting eager
    /// dispatch of an activity to this worker upon workflow task completion
    pub(crate) fn reserve_slot(&self) -> Option<TrackedOwnedMeteredSemPermit> {
        // TODO: check if rate limit is not exceeded and count this reservation towards the rate limit
        self.sem.try_acquire_owned().ok()
    }

    /// Queue new activity tasks for dispatch received from non-polling sources (ex: eager returns
    /// from WFT completion)
    pub(crate) fn add_tasks(&self, tasks: impl IntoIterator<Item = TrackedPermittedTqResp>) {
        for t in tasks.into_iter() {
            // Technically we should be reporting `activity_task_received` here, but for simplicity
            // and time insensitivity, that metric is tracked in `about_to_issue_task`.
            self.tx.send(t).expect("Receive half cannot be dropped");
        }
    }
}

#[derive(Debug)]
pub(crate) struct PermittedTqResp {
    pub permit: OwnedMeteredSemPermit,
    pub resp: PollActivityTaskQueueResponse,
}

#[derive(Debug)]
pub(crate) struct TrackedPermittedTqResp {
    pub permit: TrackedOwnedMeteredSemPermit,
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
        atm.poll().await.unwrap();
        atm.poll().await.unwrap();
        // At least half a second will have elapsed since we only allow 2 tasks per second.
        // With no ratelimit, even on a slow CI server with lots of load, this would typically take
        // low single digit ms or less.
        assert!(start.elapsed() > Duration::from_secs_f64(0.5));
    }
}
