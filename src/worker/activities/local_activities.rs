use crate::protosext::TryIntoOrNone;
use crate::{
    machines::LocalActivityExecutionResult, retry_logic::RetryPolicyExt, task_token::TaskToken,
};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    time::{Duration, Instant, SystemTime},
};
use temporal_sdk_core_protos::coresdk::{
    activity_task::{activity_task, ActivityTask, Start},
    common::WorkflowExecution,
    workflow_commands::ScheduleLocalActivity,
};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    Notify, Semaphore,
};

#[derive(Debug)]
pub(crate) struct LocalInFlightActInfo {
    pub la_info: NewLocalAct,
    pub dispatch_time: Instant,
    pub attempt: u32,
}

#[derive(Clone)]
pub(crate) struct NewLocalAct {
    pub schedule_cmd: ScheduleLocalActivity,
    pub workflow_type: String,
    pub workflow_exec_info: WorkflowExecution,
    pub schedule_time: SystemTime,
}

impl Debug for NewLocalAct {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "LocalActivity({}, {})",
            self.schedule_cmd.seq, self.schedule_cmd.activity_type
        )
    }
}

#[derive(Debug)]
enum NewOrRetry {
    New(NewLocalAct),
    Retry {
        in_flight: NewLocalAct,
        attempt: u32,
    },
}

pub(crate) struct LocalActivityManager {
    /// Just so we can provide activity tasks the same namespace as the worker
    namespace: String,
    /// Constrains number of currently executing local activities
    semaphore: Semaphore,
    /// Sink for new activity execution requests
    act_req_tx: UnboundedSender<NewOrRetry>,
    /// Activities that need to be executed by lang
    act_req_rx: tokio::sync::Mutex<UnboundedReceiver<NewOrRetry>>,
    /// Wakes every time a complete is processed
    complete_notify: Notify,

    dat: Mutex<LAMData>,
}

struct LAMData {
    /// Activities that have been issued to lang but not yet completed
    outstanding_activity_tasks: HashMap<TaskToken, LocalInFlightActInfo>,
    next_tt_num: u32,
}

impl LocalActivityManager {
    pub(crate) fn new(max_concurrent: usize, namespace: String) -> Self {
        let (act_req_tx, act_req_rx) = unbounded_channel();
        Self {
            namespace,
            semaphore: Semaphore::new(max_concurrent),
            act_req_tx,
            act_req_rx: tokio::sync::Mutex::new(act_req_rx),
            complete_notify: Notify::new(),
            dat: Mutex::new(LAMData {
                outstanding_activity_tasks: Default::default(),
                next_tt_num: 0,
            }),
        }
    }

    pub(crate) fn num_outstanding(&self) -> usize {
        self.dat.lock().outstanding_activity_tasks.len()
    }

    pub(crate) fn enqueue(&self, acts: impl IntoIterator<Item = NewLocalAct> + Debug) {
        debug!("Queuing local activities: {:?}", &acts);
        for act in acts {
            self.act_req_tx
                .send(NewOrRetry::New(act))
                .expect("Receive half of LA request channel cannot be dropped");
        }
    }

    pub(crate) async fn next_pending(&self) -> Option<ActivityTask> {
        // Wait for a permit to take a task
        let permit = self.semaphore.acquire().await.expect("is never closed");
        // It is important that there are no await points after receiving from the channel, as
        // it would mean dropping this future would cause us to drop the activity request.
        if let Some(new_or_retry) = self.act_req_rx.lock().await.recv().await {
            let (new_la, attempt) = match new_or_retry {
                NewOrRetry::New(n) => {
                    let explicit_attempt_num_or_1 = n.schedule_cmd.attempt.max(1);
                    (n, explicit_attempt_num_or_1)
                }
                NewOrRetry::Retry { in_flight, attempt } => (in_flight, attempt),
            };
            let orig = new_la.clone();
            let sa = new_la.schedule_cmd;

            let mut dat = self.dat.lock();
            dat.next_tt_num += 1;
            let tt = TaskToken::new_local_activity_token(dat.next_tt_num.to_le_bytes());
            dat.outstanding_activity_tasks.insert(
                tt.clone(),
                LocalInFlightActInfo {
                    la_info: orig,
                    dispatch_time: Instant::now(),
                    attempt,
                },
            );

            // Forget the permit. Permits are removed until a completion.
            permit.forget();

            Some(ActivityTask {
                task_token: tt.0,
                activity_id: sa.activity_id,
                variant: Some(activity_task::Variant::Start(Start {
                    workflow_namespace: self.namespace.clone(),
                    workflow_type: new_la.workflow_type,
                    workflow_execution: Some(new_la.workflow_exec_info),
                    activity_type: sa.activity_type,
                    header_fields: sa.header_fields,
                    input: sa.arguments,
                    heartbeat_details: vec![],
                    scheduled_time: Some(new_la.schedule_time.into()),
                    current_attempt_scheduled_time: Some(new_la.schedule_time.into()),
                    started_time: Some(SystemTime::now().into()),
                    attempt,
                    schedule_to_close_timeout: sa.schedule_to_close_timeout,
                    start_to_close_timeout: sa.start_to_close_timeout,
                    heartbeat_timeout: None,
                    retry_policy: sa.retry_policy,
                    is_local: true,
                })),
            })
        } else {
            None
        }
    }

    /// Mark a local activity as having completed (pass or fail)
    pub(crate) fn complete(
        &self,
        task_token: &TaskToken,
        status: &LocalActivityExecutionResult,
    ) -> LACompleteAction {
        if let Some(info) = self
            .dat
            .lock()
            .outstanding_activity_tasks
            .remove(task_token)
        {
            self.semaphore.add_permits(1);

            match status {
                LocalActivityExecutionResult::Completed(_) => {
                    self.complete_notify.notify_one();
                    LACompleteAction::Report(info)
                }
                LocalActivityExecutionResult::Failed(f) => {
                    match &info.la_info.schedule_cmd.retry_policy {
                        None => {
                            // If there's no retry policy... don't retry
                            LACompleteAction::Report(info)
                        }
                        Some(rp) => {
                            if let Some(backoff_dur) = rp.should_retry(
                                info.attempt as usize,
                                &f.failure
                                    .as_ref()
                                    .map(|f| format!("{:?}", f))
                                    .unwrap_or_else(|| "".to_string()),
                            ) {
                                warn!(
                                    "Local activity {} failed on attempt {}, will retry after \
                                     backing off for {:?}",
                                    info.la_info.schedule_cmd.seq, info.attempt, backoff_dur
                                );
                                if backoff_dur
                                    > info
                                        .la_info
                                        .schedule_cmd
                                        .local_retry_threshold
                                        .clone()
                                        .try_into_or_none()
                                        .unwrap_or_else(|| Duration::from_secs(60))
                                {
                                    warn!("Backoff is past local retry threshold");
                                    // We want this to be reported, as the workflow will mark this
                                    // failure down, then start a timer for backoff.
                                    return LACompleteAction::LangDoesTimerBackoff(
                                        backoff_dur.into(),
                                        info,
                                    );
                                }
                                // Send the retry request after waiting the backoff duration
                                let send_chan = self.act_req_tx.clone();
                                tokio::spawn(async move {
                                    tokio::time::sleep(backoff_dur).await;

                                    send_chan
                                        .send(NewOrRetry::Retry {
                                            in_flight: info.la_info,
                                            attempt: info.attempt + 1,
                                        })
                                        .expect(
                                            "Receive half of LA request channel cannot be dropped",
                                        );
                                });

                                LACompleteAction::WillBeRetried
                            } else {
                                LACompleteAction::Report(info)
                            }
                        }
                    }
                }
            }
        } else {
            LACompleteAction::Untracked
        }
    }

    pub(crate) async fn wait_all_finished(&self) {
        while !self.dat.lock().outstanding_activity_tasks.is_empty() {
            self.complete_notify.notified().await;
        }
    }
}

#[derive(Debug)]
#[allow(clippy::large_enum_variant)] // Most will be reported
pub(crate) enum LACompleteAction {
    /// Caller should report the status to the workflow
    Report(LocalInFlightActInfo),
    /// Lang needs to be told to do the schedule-a-timer-then-rerun hack
    LangDoesTimerBackoff(prost_types::Duration, LocalInFlightActInfo),
    /// The activity will be re-enqueued for another attempt (and so status should not be reported
    /// to the workflow)
    WillBeRetried,
    /// The activity was unknown
    Untracked,
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporal_sdk_core_protos::coresdk::common::RetryPolicy;
    use tokio::task::yield_now;

    #[tokio::test]
    async fn max_concurrent_respected() {
        let lam = LocalActivityManager::new(1, "whatever".to_string());
        lam.enqueue((1..=50).map(|i| NewLocalAct {
            schedule_cmd: ScheduleLocalActivity {
                seq: i,
                activity_id: i.to_string(),
                ..Default::default()
            },
            workflow_type: "".to_string(),
            workflow_exec_info: Default::default(),
            schedule_time: SystemTime::now(),
        }));
        for i in 1..=50 {
            let next = lam.next_pending().await.unwrap();
            assert_eq!(next.activity_id, i.to_string());
            let next_tt = TaskToken(next.task_token);
            let complete_branch = async {
                lam.complete(
                    &next_tt,
                    &LocalActivityExecutionResult::Completed(Default::default()),
                )
            };
            tokio::select! {
                // Next call will not resolve until we complete the first
                _ = lam.next_pending() => {
                    panic!("Branch must not be selected")
                }
                _ = complete_branch => {}
            }
        }
    }

    #[tokio::test]
    async fn no_work_doesnt_deadlock_with_complete() {
        let lam = LocalActivityManager::new(5, "whatever".to_string());
        lam.enqueue([NewLocalAct {
            schedule_cmd: ScheduleLocalActivity {
                seq: 1,
                activity_id: 1.to_string(),
                ..Default::default()
            },
            workflow_type: "".to_string(),
            workflow_exec_info: Default::default(),
            schedule_time: SystemTime::now(),
        }]);

        let next = lam.next_pending().await.unwrap();
        let tt = TaskToken(next.task_token);
        tokio::select! {
            biased;

            _ = lam.next_pending() => {
                panic!("Complete branch must win")
            }
            _ = async {
                // Spin until the receive lock has been grabbed by the call to pending, to ensure
                // it's advanced enough
                while lam.act_req_rx.try_lock().is_ok() { yield_now().await; }
                lam.complete(&tt, &LocalActivityExecutionResult::Completed(Default::default()));
            } => (),
        };
    }

    #[tokio::test]
    async fn respects_timer_backoff_threshold() {
        let lam = LocalActivityManager::new(1, "whatever".to_string());
        lam.enqueue([NewLocalAct {
            schedule_cmd: ScheduleLocalActivity {
                seq: 1,
                activity_id: 1.to_string(),
                attempt: 5,
                retry_policy: Some(RetryPolicy {
                    initial_interval: Some(Duration::from_secs(1).into()),
                    backoff_coefficient: 10.0,
                    maximum_interval: Some(Duration::from_secs(10).into()),
                    maximum_attempts: 10,
                    non_retryable_error_types: vec![],
                }),
                local_retry_threshold: Some(Duration::from_secs(5).into()),
                ..Default::default()
            },
            workflow_type: "".to_string(),
            workflow_exec_info: Default::default(),
            schedule_time: SystemTime::now(),
        }]);

        let next = lam.next_pending().await.unwrap();
        let tt = TaskToken(next.task_token);
        let res = lam.complete(
            &tt,
            &LocalActivityExecutionResult::Failed(Default::default()),
        );
        assert_matches!(res, LACompleteAction::LangDoesTimerBackoff(dur, info)
            if dur.seconds == 10 && info.attempt == 5
        )
    }
}
