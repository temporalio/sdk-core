use crate::protos::temporal::api::enums::v1::EventType;
use crate::protos::temporal::api::history::v1::{History, HistoryEvent};
use crate::protosext::history_info::HistoryInfoError::UnexpectedEventId;

#[derive(Clone, Debug, derive_more::Constructor, PartialEq)]
pub struct HistoryInfo {
    pub previous_started_event_id: i64,
    pub workflow_task_started_event_id: i64,
    pub events: Vec<HistoryEvent>,
}

#[derive(thiserror::Error, Debug)]
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
    pub fn new_from_events(
        events: Vec<HistoryEvent>,
        to_wf_task_num: usize,
    ) -> Result<Self, HistoryInfoError> {
        let mut workflow_task_started_event_id = 0;
        let mut previous_started_event_id = 0;
        let mut count = 0;
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
                    previous_started_event_id = workflow_task_started_event_id;
                    workflow_task_started_event_id = event.event_id;
                    if workflow_task_started_event_id == previous_started_event_id {
                        return Err(HistoryInfoError::UnexpectedEventId {
                            previous_started_event_id,
                            workflow_task_started_event_id,
                        });
                    }
                    count += 1;
                    if count == to_wf_task_num || next_event.is_none() {
                        return Ok(Self {
                            previous_started_event_id,
                            workflow_task_started_event_id,
                            events,
                        });
                    }
                } else if next_event.is_some() && !next_is_failed_or_timeout {
                    Err(HistoryInfoError::FailedOrTimeout(event.clone()));
                }
            }

            if next_event.is_none() {
                if event.is_final_wf_execution_event() {
                    return Ok(Self {
                        previous_started_event_id,
                        workflow_task_started_event_id,
                        events,
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

    pub fn new(h: History, to_wf_task_num: usize) -> Self {
        Self::new_from_events(h.events, to_wf_task_num)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::machines::test_help::TestHistoryBuilder;
    use crate::protos::temporal::api::history::v1::{history_event, TimerFiredEventAttributes};

    #[test]
    fn blah() {
        /*
            1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
            2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
            3: EVENT_TYPE_WORKFLOW_TASK_STARTED
            4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED
            5: EVENT_TYPE_TIMER_STARTED
            6: EVENT_TYPE_TIMER_FIRED
            7: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
            8: EVENT_TYPE_WORKFLOW_TASK_STARTED
        */
        let mut t = TestHistoryBuilder::default();

        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_workflow_task();
        let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
        t.add(
            EventType::TimerFired,
            history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
                started_event_id: timer_started_event_id,
                timer_id: "timer1".to_string(),
                ..Default::default()
            }),
        );
        t.add_workflow_task_scheduled_and_started();
        t.get
    }
}
