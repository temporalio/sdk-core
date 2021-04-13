use crate::{
    protos::temporal::api::enums::v1::EventType,
    protos::temporal::api::history::v1::{History, HistoryEvent},
};

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct HistoryInfo {
    pub previous_started_event_id: i64,
    pub workflow_task_started_event_id: i64,
    // This needs to stay private so the struct can't be instantiated outside of the constructor,
    // which enforces some invariants regarding history structure that need to be upheld.
    events: Vec<HistoryEvent>,
    pub wf_task_count: usize,
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
    pub(crate) fn new_from_events(
        events: &[HistoryEvent],
        to_wf_task_num: Option<usize>,
    ) -> Result<Self> {
        if events.is_empty() {
            return Err(HistoryInfoError::HistoryEndsUnexpectedly);
        }

        let to_wf_task_num = to_wf_task_num.unwrap_or(usize::MAX);
        let mut workflow_task_started_event_id = 0;
        let mut previous_started_event_id = 0;
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
                    previous_started_event_id = workflow_task_started_event_id;
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
                if event.is_final_wf_execution_event() {
                    return Ok(Self {
                        previous_started_event_id,
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

    pub(crate) fn new_from_history(h: &History, to_wf_task_num: Option<usize>) -> Result<Self> {
        Self::new_from_events(&h.events, to_wf_task_num)
    }

    pub(crate) fn events(&self) -> &[HistoryEvent] {
        &self.events
    }
}

#[cfg(test)]
mod tests {
    use crate::test_help::canned_histories;

    #[test]
    fn history_info_constructs_properly() {
        let t = canned_histories::single_timer("timer1");

        let history_info = t.get_history_info(1).unwrap();
        assert_eq!(3, history_info.events.len());
        let history_info = t.get_history_info(2).unwrap();
        assert_eq!(8, history_info.events.len());
    }
}
