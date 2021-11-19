use crate::task_token::TaskToken;
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
};
use temporal_sdk_core_protos::coresdk::{
    activity_task::{activity_task, ActivityTask, Start},
    common::WorkflowExecution,
    workflow_commands::ScheduleActivity,
};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    Notify, Semaphore,
};

pub(crate) struct LocalInFlightActInfo {
    pub seq: u32,
    pub workflow_execution: WorkflowExecution,
}

pub(crate) struct NewLocalAct {
    pub schedule_cmd: ScheduleActivity,
    pub workflow_type: String,
    pub workflow_exec_info: WorkflowExecution,
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

pub(crate) struct LocalActivityManager {
    /// Constrains number of currently executing local activities
    semaphore: Semaphore,
    /// Sink for new activity execution requests
    act_req_tx: UnboundedSender<NewLocalAct>,
    /// Activities that need to be executed by lang
    act_req_rx: tokio::sync::Mutex<UnboundedReceiver<NewLocalAct>>,
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
    pub(crate) fn new(max_concurrent: usize) -> Self {
        let (act_req_tx, act_req_rx) = unbounded_channel();
        Self {
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

    pub(crate) fn enqueue(&self, acts: impl IntoIterator<Item = NewLocalAct> + Debug) {
        debug!("Queuing local activities: {:?}", &acts);
        for act in acts {
            self.act_req_tx
                .send(act)
                .expect("Receive half of LA request channel cannot be dropped");
        }
    }

    pub(crate) async fn next_pending(&self) -> Option<ActivityTask> {
        // Wait for a permit to take a task
        let permit = self.semaphore.acquire().await.expect("is never closed");
        // It is important that there are no await points after receiving from the channel, as
        // it would mean dropping this future would cause us to drop the activity request.
        if let Some(new_la) = self.act_req_rx.lock().await.recv().await {
            let sa = new_la.schedule_cmd;

            let mut dat = self.dat.lock();
            dat.next_tt_num += 1;
            let tt = TaskToken::new_local_activity_token(dat.next_tt_num.to_le_bytes());
            dat.outstanding_activity_tasks.insert(
                tt.clone(),
                LocalInFlightActInfo {
                    seq: sa.seq,
                    workflow_execution: new_la.workflow_exec_info.clone(),
                },
            );

            // Forget the permit. Permits are removed until a completion.
            permit.forget();

            Some(ActivityTask {
                task_token: tt.0,
                activity_id: sa.activity_id,
                variant: Some(activity_task::Variant::Start(Start {
                    workflow_namespace: sa.namespace,
                    workflow_type: new_la.workflow_type,
                    workflow_execution: Some(new_la.workflow_exec_info),
                    activity_type: sa.activity_type,
                    header_fields: sa.header_fields,
                    input: sa.arguments,
                    heartbeat_details: vec![],
                    // TODO: Get these times somehow
                    scheduled_time: None,
                    current_attempt_scheduled_time: None,
                    started_time: None,
                    attempt: 0,
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

    /// Mark a local activity as having completed. Returns the information about the local activity
    /// so the appropriate workflow instance can be notified of completion.
    pub(crate) fn complete(&self, task_token: &TaskToken) -> Option<LocalInFlightActInfo> {
        warn!("Local act completing");
        let info = self
            .dat
            .lock()
            .outstanding_activity_tasks
            .remove(task_token)?;
        self.semaphore.add_permits(1);
        self.complete_notify.notify_one();
        Some(info)
    }

    pub(crate) async fn wait_all_finished(&self) {
        while !self.dat.lock().outstanding_activity_tasks.is_empty() {
            self.complete_notify.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::task::yield_now;

    #[tokio::test]
    async fn max_concurrent_respected() {
        let lam = LocalActivityManager::new(1);
        lam.enqueue((1..=50).map(|i| NewLocalAct {
            schedule_cmd: ScheduleActivity {
                seq: i,
                activity_id: i.to_string(),
                ..Default::default()
            },
            workflow_type: "".to_string(),
            workflow_exec_info: Default::default(),
        }));
        for i in 1..=50 {
            let next = lam.next_pending().await.unwrap();
            assert_eq!(next.activity_id, i.to_string());
            let next_tt = TaskToken(next.task_token);
            tokio::select! {
                // Next call will not resolve until we complete the first
                _ = lam.next_pending() => {
                    panic!("Branch must not be selected")
                }
                _ = async { lam.complete(&next_tt) } => {}
            }
        }
    }

    #[tokio::test]
    async fn no_work_doesnt_deadlock_with_complete() {
        let lam = LocalActivityManager::new(5);
        lam.enqueue([NewLocalAct {
            schedule_cmd: ScheduleActivity {
                seq: 1,
                activity_id: 1.to_string(),
                ..Default::default()
            },
            workflow_type: "".to_string(),
            workflow_exec_info: Default::default(),
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
                lam.complete(&tt).unwrap();
            } => (),
        };
    }
}
