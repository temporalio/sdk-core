use crate::{pollers, pollers::BoxedWFPoller};
use crossbeam::queue::SegQueue;
use temporal_sdk_core_protos::temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse;
use tokio::sync::Notify;

/// Workflow tasks typically come from polling, but may also come as a response to task completion.
/// This struct allows fetching WFTs to be centralized while prioritizing tasks from completes.
pub(crate) struct WFTSource {
    from_completions: SegQueue<PollWorkflowTaskQueueResponse>,
    poll_buffer: BoxedWFPoller,
    task_taken_notifier: Notify,
}

impl WFTSource {
    pub fn new(poller: BoxedWFPoller) -> Self {
        Self {
            from_completions: SegQueue::new(),
            poll_buffer: poller,
            task_taken_notifier: Notify::new(),
        }
    }

    /// Returns the next available WFT if one is already stored from a completion, otherwise
    /// forwards to the poller.
    pub async fn next_wft(&self) -> Option<pollers::Result<PollWorkflowTaskQueueResponse>> {
        if let Some(wft) = self.from_completions.pop() {
            self.task_taken_notifier.notify_one();
            return Some(Ok(wft));
        }
        self.poll_buffer.poll().await
    }

    pub fn add_wft_from_completion(&self, wft: PollWorkflowTaskQueueResponse) {
        self.from_completions.push(wft);
    }

    pub fn stop_pollers(&self) {
        self.poll_buffer.notify_shutdown();
    }

    pub async fn wait_for_tasks_from_complete_to_drain(&self) {
        while !self.from_completions.is_empty() {
            self.task_taken_notifier.notified().await;
        }
    }

    pub async fn shutdown(self) {
        self.poll_buffer.shutdown_box().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_help::mock_poller;

    #[tokio::test]
    async fn drains_from_completes_on_shutdown() {
        let mp = mock_poller();
        let wftsrc = WFTSource::new(Box::new(mp));
        let fake_wft = PollWorkflowTaskQueueResponse {
            started_event_id: 1,
            ..Default::default()
        };
        wftsrc.add_wft_from_completion(fake_wft);
        let fake_wft = PollWorkflowTaskQueueResponse {
            started_event_id: 2,
            ..Default::default()
        };
        wftsrc.add_wft_from_completion(fake_wft);
    }
}
