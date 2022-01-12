use crate::history_replay::DEFAULT_WORKFLOW_TYPE;
use rand::{thread_rng, Rng};
use temporal_sdk_core_protos::temporal::api::{
    common::v1::WorkflowType,
    enums::v1::{EventType, TaskQueueKind},
    history::v1::{history_event, History, HistoryEvent},
    taskqueue::v1::TaskQueue,
    workflowservice::v1::PollWorkflowTaskQueueResponse,
};

#[derive(Clone, Debug, PartialEq)]
pub struct HistoryInfo {
    pub previous_started_event_id: i64,
    pub workflow_task_started_event_id: i64,
    // This needs to stay private so the struct can't be instantiated outside of the constructor,
    // which enforces some invariants regarding history structure that need to be upheld.
    events: Vec<HistoryEvent>,
    wf_task_count: usize,
}

type Result<T, E = HistoryInfoError> = std::result::Result<T, E>;

#[derive(thiserror::Error, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum HistoryInfoError {
    #[error("Latest wf started id and previous one are equal! ${previous_started_event_id:?}")]
    UnexpectedEventId {
        previous_started_event_id: i64,
        workflow_task_started_event_id: i64,
    },
    #[error("Invalid history! Event {0:?} should be WF task completed, failed, or timed out")]
    FailedOrTimeout(HistoryEvent),
    #[error("Last item in history wasn't WorkflowTaskStarted")]
    HistoryEndsUnexpectedly,
}

impl HistoryInfo {
    /// Constructs a new instance, retaining only enough events to reach the provided workflow
    /// task number. If not provided, all events are retained.
    pub(crate) fn new_from_history(h: &History, to_wf_task_num: Option<usize>) -> Result<Self> {
        let events = &h.events;
        if events.is_empty() {
            return Err(HistoryInfoError::HistoryEndsUnexpectedly);
        }

        let is_all_hist = to_wf_task_num.is_none();
        let to_wf_task_num = to_wf_task_num.unwrap_or(usize::MAX);
        let mut workflow_task_started_event_id = 0;
        let mut wf_task_count = 0;
        let mut history = events.iter().peekable();
        let mut events = vec![];

        while let Some(event) = history.next() {
            events.push(event.clone());
            let next_event = history.peek();

            if event.event_type == EventType::WorkflowTaskStarted as i32 {
                let next_is_completed = next_event.map_or(false, |ne| {
                    ne.event_type == EventType::WorkflowTaskCompleted as i32
                });
                let next_is_failed_or_timeout = next_event.map_or(false, |ne| {
                    ne.event_type == EventType::WorkflowTaskFailed as i32
                        || ne.event_type == EventType::WorkflowTaskTimedOut as i32
                });

                if next_event.is_none() || next_is_completed {
                    let previous_started_event_id = workflow_task_started_event_id;
                    workflow_task_started_event_id = event.event_id;
                    if workflow_task_started_event_id == previous_started_event_id {
                        return Err(HistoryInfoError::UnexpectedEventId {
                            previous_started_event_id,
                            workflow_task_started_event_id,
                        });
                    }
                    wf_task_count += 1;
                    if wf_task_count == to_wf_task_num || next_event.is_none() {
                        return Ok(Self {
                            previous_started_event_id,
                            workflow_task_started_event_id,
                            events,
                            wf_task_count,
                        });
                    }
                } else if next_event.is_some() && !next_is_failed_or_timeout {
                    return Err(HistoryInfoError::FailedOrTimeout(event.clone()));
                }
            }

            if next_event.is_none() {
                if event.is_final_wf_execution_event() || is_all_hist {
                    // Since this is the end of execution, we are pretending that the SDK is
                    // replaying *complete* history, which would mean the previously started ID is
                    // in fact the last task.
                    return Ok(Self {
                        previous_started_event_id: workflow_task_started_event_id,
                        workflow_task_started_event_id,
                        events,
                        wf_task_count,
                    });
                }
                // No more events
                if workflow_task_started_event_id != event.event_id {
                    return Err(HistoryInfoError::HistoryEndsUnexpectedly);
                }
            }
        }
        unreachable!()
    }

    /// Remove events from the beginning of this history such that it looks like what would've been
    /// delivered on a sticky queue where the previously started task was the one before the last
    /// task in this history.
    pub(crate) fn make_incremental(&mut self) {
        let last_complete_ix = self
            .events
            .iter()
            .rposition(|he| he.event_type() == EventType::WorkflowTaskCompleted)
            .expect("Must be a WFT completed event in history");
        self.events.drain(0..=last_complete_ix);
    }

    pub fn events(&self) -> &[HistoryEvent] {
        &self.events
    }

    /// Attempt to extract run id from internal events. If the first event is not workflow execution
    /// started, it will panic.
    pub fn orig_run_id(&self) -> &str {
        if let Some(history_event::Attributes::WorkflowExecutionStartedEventAttributes(wes)) =
            &self.events[0].attributes
        {
            &wes.original_execution_run_id
        } else {
            panic!("First event is wrong type")
        }
    }

    /// Return total workflow task count in this history
    pub const fn wf_task_count(&self) -> usize {
        self.wf_task_count
    }

    /// Create a workflow task polling response containing all the events in this history and a
    /// randomly generated task token. Caller should attach a meaningful `workflow_execution` if
    /// needed.
    pub fn as_poll_wft_response(&self, task_q: impl Into<String>) -> PollWorkflowTaskQueueResponse {
        let task_token: [u8; 16] = thread_rng().gen();
        PollWorkflowTaskQueueResponse {
            history: Some(History {
                events: self.events.clone(),
            }),
            task_token: task_token.to_vec(),
            workflow_type: Some(WorkflowType {
                name: DEFAULT_WORKFLOW_TYPE.to_owned(),
            }),
            workflow_execution_task_queue: Some(TaskQueue {
                name: task_q.into(),
                kind: TaskQueueKind::Normal as i32,
            }),
            previous_started_event_id: self.previous_started_event_id,
            started_event_id: self.workflow_task_started_event_id,
            ..Default::default()
        }
    }
}

impl From<HistoryInfo> for History {
    fn from(i: HistoryInfo) -> Self {
        Self { events: i.events }
    }
}

#[cfg(test)]
mod tests {
    use crate::canned_histories;

    #[test]
    fn history_info_constructs_properly() {
        let t = canned_histories::single_timer("timer1");

        let history_info = t.get_history_info(1).unwrap();
        assert_eq!(3, history_info.events.len());
        let history_info = t.get_history_info(2).unwrap();
        assert_eq!(8, history_info.events.len());
    }

    #[test]
    fn incremental_works() {
        let t = canned_histories::single_timer("timer1");
        let hi = t.get_one_wft(2).unwrap();
        assert_eq!(hi.events.len(), 4);
        assert_eq!(hi.events[0].event_id, 5);
    }
}
