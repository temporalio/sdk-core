use crate::{
    protos::temporal::api::enums::v1::EventType,
    protos::temporal::api::history::v1::{History, HistoryEvent},
    protosext::ValidPollWFTQResponse,
};
use core::convert::From;
use std::collections::VecDeque;

// TODO: Rename PollRespHistory ?
/// A slimmed down version of a poll workflow task response which includes just the info needed
/// by [WorkflowManager]. History events are expected to be consumed from it and applied to the
/// state machines.
#[derive(Debug, Clone)]
pub struct HistoryUpdate {
    events: VecDeque<HistoryEvent>,
    pub previous_started_event_id: i64,
    pub started_event_id: i64,
}

impl HistoryUpdate {
    pub fn new(
        history: History,
        previous_wft_started_id: i64,
        current_wft_started_id: i64,
    ) -> Self {
        Self {
            events: history.events.into(),
            previous_started_event_id: previous_wft_started_id,
            started_event_id: current_wft_started_id,
        }
    }

    #[cfg(test)]
    pub fn new_from_events(
        events: Vec<HistoryEvent>,
        previous_wft_started_id: i64,
        current_wft_started_id: i64,
    ) -> Self {
        Self {
            events: events.into(),
            previous_started_event_id: previous_wft_started_id,
            started_event_id: current_wft_started_id,
        }
    }
    /// Given a workflow task started id, return all events starting at that number (inclusive) to
    /// the next WFT started event (inclusive). If there is no subsequent WFT started event,
    /// remaining history is returned.
    ///
    /// Events are *consumed* by this process, to keep things efficient in workflow machines
    pub async fn take_next_wft_sequence(&mut self, from_wft_started_id: i64) -> Vec<HistoryEvent> {
        let mut events_to_next_wft_started = vec![];

        // This flag tracks if, while determining events to be returned, we have seen the next
        // logically significant WFT started event which follows the one that was passed in as a
        // parameter. If a WFT fails or times out, it is not significant. So we will stop returning
        // events (exclusive) as soon as we see an event following a WFT started that is *not*
        // failed or timed out.
        let mut saw_next_wft = false;
        let mut should_pop = |e: &HistoryEvent| {
            if e.event_id <= from_wft_started_id {
                return true;
            } else if e.event_type == EventType::WorkflowTaskStarted as i32 {
                saw_next_wft = true;
                return true;
            }

            if saw_next_wft {
                // Must ignore failures and timeouts
                if e.event_type == EventType::WorkflowTaskFailed as i32
                    || e.event_type == EventType::WorkflowTaskTimedOut as i32
                {
                    saw_next_wft = false;
                    return true;
                }
                return false;
            }

            true
        };

        while let Some(e) = self.events.front() {
            if !should_pop(e) {
                break;
            }
            // Unwrap OK because we have just peeked it
            let popped = self.events.pop_front().unwrap();
            // It's possible to have gotten a new history update without eviction (ex: unhandled
            // command on completion), where we may need to skip events we already handled.
            if popped.event_id > from_wft_started_id {
                events_to_next_wft_started.push(popped);
            }
        }

        events_to_next_wft_started
    }
}

impl From<ValidPollWFTQResponse> for HistoryUpdate {
    fn from(v: ValidPollWFTQResponse) -> Self {
        Self {
            events: v.history.events.into(),
            previous_started_event_id: v.previous_started_event_id,
            started_event_id: v.started_event_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::test_help::canned_histories;

    #[tokio::test]
    async fn consumes_standard_wft_sequence() {
        let timer_hist = canned_histories::single_timer("t");
        let mut update = timer_hist.as_history_update();
        let seq_1 = update.take_next_wft_sequence(0).await;
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2 = update.take_next_wft_sequence(3).await;
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
    }

    #[tokio::test]
    async fn skips_wft_failed() {
        let failed_hist = canned_histories::workflow_fails_with_reset_after_timer("t", "runid");
        let mut update = failed_hist.as_history_update();
        let seq_1 = update.take_next_wft_sequence(0).await;
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2 = update.take_next_wft_sequence(3).await;
        assert_eq!(seq_2.len(), 8);
        assert_eq!(seq_2.last().unwrap().event_id, 11);
    }

    #[tokio::test]
    async fn skips_wft_timeout() {
        let failed_hist = canned_histories::wft_timeout_repro();
        let mut update = failed_hist.as_history_update();
        let seq_1 = update.take_next_wft_sequence(0).await;
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2 = update.take_next_wft_sequence(3).await;
        assert_eq!(seq_2.len(), 11);
        assert_eq!(seq_2.last().unwrap().event_id, 14);
    }

    #[tokio::test]
    async fn skips_events_before_desired_wft() {
        let timer_hist = canned_histories::single_timer("t");
        let mut update = timer_hist.as_history_update();
        // We haven't processed the first 3 events, but we should still only get the second sequence
        let seq_2 = update.take_next_wft_sequence(3).await;
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
    }
}
