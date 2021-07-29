use crate::{
    protos::temporal::api::enums::v1::EventType,
    protos::temporal::api::history::v1::{History, HistoryEvent},
    protos::temporal::api::workflowservice::v1::GetWorkflowExecutionHistoryResponse,
    ServerGatewayApis,
};
use futures::{future::BoxFuture, stream::BoxStream, FutureExt, Stream, StreamExt};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// A slimmed down version of a poll workflow task response which includes just the info needed
/// by [WorkflowManager]. History events are expected to be consumed from it and applied to the
/// state machines.
pub struct HistoryUpdate {
    events: BoxStream<'static, Result<HistoryEvent, tonic::Status>>,
    /// Sometimes we consume an event from the stream but do not return it in the last wft sequence.
    /// It must be kept here and returned in the next call.
    buffered: Option<HistoryEvent>,
    pub previous_started_event_id: i64,
}

pub struct HistoryPaginator {
    gateway: Arc<dyn ServerGatewayApis + Send + Sync>,
    event_queue: VecDeque<HistoryEvent>,
    wf_id: String,
    run_id: String,
    next_page_token: Vec<u8>,
    open_history_request:
        Option<BoxFuture<'static, Result<GetWorkflowExecutionHistoryResponse, tonic::Status>>>,
}

impl HistoryPaginator {
    pub fn new(
        initial_history: History,
        wf_id: String,
        run_id: String,
        next_page_token: Vec<u8>,
        gateway: Arc<dyn ServerGatewayApis + Send + Sync>,
    ) -> Self {
        Self {
            gateway,
            event_queue: initial_history.events.into(),
            wf_id,
            run_id,
            next_page_token,
            open_history_request: None,
        }
    }
}

impl Stream for HistoryPaginator {
    type Item = Result<HistoryEvent, tonic::Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(e) = self.event_queue.pop_front() {
            return Poll::Ready(Some(Ok(e)));
        }
        if self.next_page_token.is_empty() {
            return Poll::Ready(None);
        }

        let history_req = if let Some(req) = self.open_history_request.as_mut() {
            req
        } else {
            debug!(run_id=%self.run_id, "Fetching new history page");
            // We're out of stored events and we have a page token - fetch additional history from
            // the server. Note that server can return page tokens that point to an empty page.
            let gw = self.gateway.clone();
            let wid = self.wf_id.clone();
            let rid = self.run_id.clone();
            let npt = self.next_page_token.clone();
            let resp_fut =
                async move { gw.get_workflow_execution_history(wid, Some(rid), npt).await };
            self.open_history_request.insert(resp_fut.boxed())
        };

        return match Future::poll(history_req.as_mut(), cx) {
            Poll::Ready(resp) => {
                self.open_history_request = None;
                match resp {
                    Err(neterr) => Poll::Ready(Some(Err(neterr))),
                    Ok(resp) => {
                        self.next_page_token = resp.next_page_token;
                        self.event_queue
                            .extend(resp.history.map(|h| h.events).unwrap_or_default());
                        Poll::Ready(self.event_queue.pop_front().map(Ok))
                    }
                }
            }
            Poll::Pending => Poll::Pending,
        };
    }
}

impl HistoryUpdate {
    pub fn new(history_iterator: HistoryPaginator, previous_wft_started_id: i64) -> Self {
        Self {
            events: history_iterator.fuse().boxed(),
            buffered: None,
            previous_started_event_id: previous_wft_started_id,
        }
    }

    #[cfg(test)]
    pub fn new_from_events(events: Vec<HistoryEvent>, previous_wft_started_id: i64) -> Self {
        use futures::stream;

        Self {
            events: stream::iter(events.into_iter().map(Ok)).boxed(),
            buffered: None,
            previous_started_event_id: previous_wft_started_id,
        }
    }

    /// Given a workflow task started id, return all events starting at that number (inclusive) to
    /// the next WFT started event (inclusive). If there is no subsequent WFT started event,
    /// remaining history is returned.
    ///
    /// Events are *consumed* by this process, to keep things efficient in workflow machines, and
    /// the function may call out to server to fetch more pages if they are known to exist and
    /// needed to complete the WFT sequence.
    ///
    /// Can return a tonic error in the event that fetching additional history was needed and failed
    pub async fn take_next_wft_sequence(
        &mut self,
        from_wft_started_id: i64,
    ) -> Result<Vec<HistoryEvent>, tonic::Status> {
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

        if let Some(e) = self.buffered.take() {
            if e.event_id > from_wft_started_id {
                events_to_next_wft_started.push(e);
            }
        }

        while let Some(e) = self.events.next().await {
            let e = e?;
            // It's possible to have gotten a new history update without eviction (ex: unhandled
            // command on completion), where we may need to skip events we already handled.
            if e.event_id > from_wft_started_id {
                // This check that we don't re-buffer the same ID is only necessary if we get duplicate
                // events from the stream, but that should never happen in real life, only test.
                if !should_pop(&e) {
                    self.buffered.insert(e);
                    break;
                }
                events_to_next_wft_started.push(e);
            }
        }

        Ok(events_to_next_wft_started)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pollers::MockServerGatewayApis;
    use crate::test_help::canned_histories;

    #[tokio::test]
    async fn consumes_standard_wft_sequence() {
        let timer_hist = canned_histories::single_timer("t");
        let mut update = timer_hist.as_history_update();
        let seq_1 = update.take_next_wft_sequence(0).await.unwrap();
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2 = update.take_next_wft_sequence(3).await.unwrap();
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
    }

    #[tokio::test]
    async fn skips_wft_failed() {
        let failed_hist = canned_histories::workflow_fails_with_reset_after_timer("t", "runid");
        let mut update = failed_hist.as_history_update();
        let seq_1 = update.take_next_wft_sequence(0).await.unwrap();
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2 = update.take_next_wft_sequence(3).await.unwrap();
        assert_eq!(seq_2.len(), 8);
        assert_eq!(seq_2.last().unwrap().event_id, 11);
    }

    #[tokio::test]
    async fn skips_wft_timeout() {
        let failed_hist = canned_histories::wft_timeout_repro();
        let mut update = failed_hist.as_history_update();
        let seq_1 = update.take_next_wft_sequence(0).await.unwrap();
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2 = update.take_next_wft_sequence(3).await.unwrap();
        assert_eq!(seq_2.len(), 11);
        assert_eq!(seq_2.last().unwrap().event_id, 14);
    }

    #[tokio::test]
    async fn skips_events_before_desired_wft() {
        let timer_hist = canned_histories::single_timer("t");
        let mut update = timer_hist.as_history_update();
        // We haven't processed the first 3 events, but we should still only get the second sequence
        let seq_2 = update.take_next_wft_sequence(3).await.unwrap();
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
    }

    #[tokio::test]
    async fn paginator_fetches_new_pages() {
        let wft_count = 500;
        let long_hist = canned_histories::long_sequential_timers(wft_count);
        let initial_hist = long_hist.get_history_info(10).unwrap();
        let prev_started = initial_hist.previous_started_event_id;
        let mut mock_gateway = MockServerGatewayApis::new();

        let mut npt = 2u8;
        mock_gateway
            .expect_get_workflow_execution_history()
            .returning(move |_, _, passed_npt| {
                assert_eq!(passed_npt, vec![npt]);
                let history = long_hist.get_history_info(10 * npt as usize).unwrap();
                npt += 1;
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(history.into()),
                    raw_history: vec![],
                    next_page_token: vec![npt],
                    archived: false,
                })
            });

        let mut update = HistoryUpdate::new(
            HistoryPaginator::new(
                initial_hist.into(),
                "wfid".to_string(),
                "runid".to_string(),
                vec![2], // Start at page "2"
                Arc::new(mock_gateway),
            ),
            prev_started,
        );

        let seq = update.take_next_wft_sequence(0).await.unwrap();
        assert_eq!(seq.len(), 3);

        let mut last_event_id = 3;
        let mut last_started_id = 3;
        for _ in 1..wft_count {
            let seq = update
                .take_next_wft_sequence(last_started_id)
                .await
                .unwrap();
            for e in seq.iter() {
                last_event_id += 1;
                assert_eq!(e.event_id, last_event_id);
            }
            assert_eq!(seq.len(), 5);
            last_started_id += 5;
        }
    }
}
