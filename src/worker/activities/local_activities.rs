use crate::task_token::TaskToken;
use std::collections::{HashMap, VecDeque};
use std::fmt::Debug;
use temporal_sdk_core_protos::coresdk::{
    activity_task::{activity_task, ActivityTask, Start},
    common::WorkflowExecution,
    workflow_commands::ScheduleActivity,
};

pub(crate) struct InFlightLocalActInfo {
    pub seq: u32,
    pub workflow_execution: WorkflowExecution,
}

#[derive(Debug)]
pub(crate) struct NewLocalAct {
    pub schedule_cmd: ScheduleActivity,
    pub workflow_type: String,
    pub workflow_exec_info: WorkflowExecution,
}

pub(crate) struct LocalActivityManager {
    /// Activities that need to be executed by lang
    poll_queue: VecDeque<NewLocalAct>,
    /// Activities that have been issued to lang but not yet completed
    outstanding_activity_tasks: HashMap<TaskToken, InFlightLocalActInfo>,

    next_tt_num: u32,
}

impl LocalActivityManager {
    pub(crate) fn new() -> Self {
        Self {
            poll_queue: Default::default(),
            outstanding_activity_tasks: Default::default(),
            next_tt_num: 0,
        }
    }

    pub(crate) fn enqueue(&mut self, acts: impl IntoIterator<Item = NewLocalAct> + Debug) {
        debug!("Queuing local activities: {:?}", &acts);
        self.poll_queue.extend(acts);
    }

    pub(crate) fn next_pending(&mut self) -> Option<ActivityTask> {
        // TODO: Semaphore
        self.poll_queue.pop_front().map(|new_la| {
            let sa = new_la.schedule_cmd;

            self.next_tt_num += 1;
            let tt = TaskToken::new_local_activity_token(self.next_tt_num.to_le_bytes());
            self.outstanding_activity_tasks.insert(
                tt.clone(),
                InFlightLocalActInfo {
                    seq: sa.seq,
                    workflow_execution: new_la.workflow_exec_info.clone(),
                },
            );

            ActivityTask {
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
            }
        })
    }

    /// Mark a local activity as having completed. Returns the information about the local activity
    /// so the appropriate workflow instance can be notified of completion.
    pub(crate) fn complete(&mut self, task_token: &TaskToken) -> Option<InFlightLocalActInfo> {
        let info = self.outstanding_activity_tasks.remove(task_token)?;
        Some(info)
    }
}
