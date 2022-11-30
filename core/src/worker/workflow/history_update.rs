use crate::{
    replay::{HistoryInfo, TestHistoryBuilder},
    worker::client::WorkerClient,
};
use futures::{future::BoxFuture, stream, stream::BoxStream, FutureExt, Stream, StreamExt};
use std::{
    collections::VecDeque,
    fmt::Debug,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use temporal_sdk_core_protos::temporal::api::{
    enums::v1::EventType,
    history::v1::{History, HistoryEvent},
    workflowservice::v1::GetWorkflowExecutionHistoryResponse,
};
use tracing::Instrument;

/// A slimmed down version of a poll workflow task response which includes just the info needed
/// by [WorkflowManager]. History events are expected to be consumed from it and applied to the
/// state machines.
pub struct HistoryUpdate {
    events: BoxStream<'static, Result<HistoryEvent, tonic::Status>>,
    /// It is useful to be able to look ahead up to one workflow task beyond the currently
    /// requested one. The initial (possibly only) motivation for this being to be able to
    /// pre-emptively notify lang about patch markers so that calls to `changed` do not need to
    /// be async.
    buffered: VecDeque<HistoryEvent>,
    pub previous_started_event_id: i64,
}
impl Debug for HistoryUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "HistoryUpdate(previous_started_event_id: {})",
            self.previous_started_event_id
        )
    }
}

pub struct HistoryPaginator {
    // Potentially this could actually be a ref w/ lifetime here
    client: Arc<dyn WorkerClient>,
    event_queue: VecDeque<HistoryEvent>,
    wf_id: String,
    run_id: String,
    next_page_token: NextPageToken,
    open_history_request:
        Option<BoxFuture<'static, Result<GetWorkflowExecutionHistoryResponse, tonic::Status>>>,
    /// These are events that should be returned once pagination has finished. This only happens
    /// during cache misses, where we got a partial task but need to fetch history from the start.
    /// We use this to apply any
    final_events: Vec<HistoryEvent>,
}

#[derive(Clone, Debug)]
pub enum NextPageToken {
    /// There is no page token, we need to fetch history from the beginning
    FetchFromStart,
    /// There is a page token
    Next(Vec<u8>),
    /// There is no page token, we are done fetching history
    Done,
}

// If we're converting from a page token from the server, if it's empty, then we're done.
impl From<Vec<u8>> for NextPageToken {
    fn from(page_token: Vec<u8>) -> Self {
        if page_token.is_empty() {
            NextPageToken::Done
        } else {
            NextPageToken::Next(page_token)
        }
    }
}

impl HistoryPaginator {
    pub(crate) fn new(
        initial_history: History,
        wf_id: String,
        run_id: String,
        next_page_token: impl Into<NextPageToken>,
        client: Arc<dyn WorkerClient>,
    ) -> Self {
        let next_page_token = next_page_token.into();
        let (event_queue, final_events) =
            if matches!(next_page_token, NextPageToken::FetchFromStart) {
                (VecDeque::new(), initial_history.events)
            } else {
                (initial_history.events.into(), vec![])
            };
        Self {
            client,
            event_queue,
            wf_id,
            run_id,
            next_page_token,
            open_history_request: None,
            final_events,
        }
    }

    fn extend_queue_with_new_page(&mut self, resp: GetWorkflowExecutionHistoryResponse) {
        self.next_page_token = resp.next_page_token.into();
        self.event_queue
            .extend(resp.history.map(|h| h.events).unwrap_or_default());
        if matches!(&self.next_page_token, NextPageToken::Done) {
            // If finished, we need to extend the queue with the final events, skipping any
            // which are already present.
            if let Some(last_event_id) = self.event_queue.back().map(|e| e.event_id) {
                let final_events = std::mem::take(&mut self.final_events);
                self.event_queue.extend(
                    final_events
                        .into_iter()
                        .skip_while(|e2| e2.event_id <= last_event_id),
                );
            }
        };
    }
}

impl Stream for HistoryPaginator {
    type Item = Result<HistoryEvent, tonic::Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(e) = self.event_queue.pop_front() {
            return Poll::Ready(Some(Ok(e)));
        }
        let history_req = if let Some(req) = self.open_history_request.as_mut() {
            req
        } else {
            let npt = match std::mem::replace(&mut self.next_page_token, NextPageToken::Done) {
                // If there's no open request and the last page token we got was empty, we're done.
                NextPageToken::Done => return Poll::Ready(None),
                NextPageToken::FetchFromStart => vec![],
                NextPageToken::Next(v) => v,
            };
            debug!(run_id=%self.run_id, "Fetching new history page");
            let gw = self.client.clone();
            let wid = self.wf_id.clone();
            let rid = self.run_id.clone();
            let resp_fut = async move {
                gw.get_workflow_execution_history(wid, Some(rid), npt)
                    .instrument(span!(tracing::Level::TRACE, "fetch_history_in_paginator"))
                    .await
            };
            self.open_history_request.insert(resp_fut.boxed())
        };

        return match Future::poll(history_req.as_mut(), cx) {
            Poll::Ready(resp) => {
                self.open_history_request = None;
                match resp {
                    Err(neterr) => Poll::Ready(Some(Err(neterr))),
                    Ok(resp) => {
                        self.extend_queue_with_new_page(resp);
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
            buffered: VecDeque::new(),
            previous_started_event_id: previous_wft_started_id,
        }
    }

    /// Create an instance of an update directly from events - should only be used for replaying.
    pub fn new_from_events<I: IntoIterator<Item = HistoryEvent>>(
        events: I,
        previous_wft_started_id: i64,
    ) -> Self
    where
        <I as IntoIterator>::IntoIter: Send + 'static,
    {
        Self {
            events: stream::iter(events.into_iter().map(Ok)).boxed(),
            buffered: VecDeque::new(),
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
    /// Always buffers the WFT sequence *after* the returned one as well, if it is available.
    ///
    /// Can return a tonic error in the event that fetching additional history was needed and failed
    pub async fn take_next_wft_sequence(
        &mut self,
        from_wft_started_id: i64,
    ) -> Result<Vec<HistoryEvent>, tonic::Status> {
        let (next_wft_events, maybe_bonus_events) = self
            .take_next_wft_sequence_impl(from_wft_started_id)
            .await?;
        if !maybe_bonus_events.is_empty() {
            self.buffered.extend(maybe_bonus_events);
        }

        if let Some(last_event_id) = next_wft_events.last().map(|he| he.event_id) {
            // Always attempt to fetch the *next* WFT sequence as well, to buffer it for lookahead
            let (buffer_these_events, maybe_bonus_events) =
                self.take_next_wft_sequence_impl(last_event_id).await?;
            self.buffered.extend(buffer_these_events);
            if !maybe_bonus_events.is_empty() {
                self.buffered.extend(maybe_bonus_events);
            }
        }

        Ok(next_wft_events)
    }

    /// Lets the caller peek ahead at the next WFT sequence that will be returned by
    /// [take_next_wft_sequence]. Will always return an empty iterator if that has not been called
    /// first. May also return an empty iterator or incomplete sequence if we are at the end of
    /// history.
    pub fn peek_next_wft_sequence(&self) -> impl Iterator<Item = &HistoryEvent> {
        self.buffered.iter()
    }

    /// Retrieve the next WFT sequence, first from buffered events and then from the real stream.
    /// Returns (events up to the next logical wft sequence, extra events that were taken but
    /// should be re-appended to the end of the buffer).
    async fn take_next_wft_sequence_impl(
        &mut self,
        from_event_id: i64,
    ) -> Result<(Vec<HistoryEvent>, Vec<HistoryEvent>), tonic::Status> {
        let mut events_to_next_wft_started: Vec<HistoryEvent> = vec![];

        // This flag tracks if, while determining events to be returned, we have seen the next
        // logically significant WFT started event which follows the one that was passed in as a
        // parameter. If a WFT fails, times out, or is devoid of commands (ie: a heartbeat) it is
        // not significant. So we will stop returning events (exclusive) as soon as we see an event
        // following a WFT started that is *not* failed, timed out, or completed with a command.
        let mut next_wft_state = NextWftState::NotSeen;
        let mut should_pop = |e: &HistoryEvent| {
            if e.event_id <= from_event_id {
                return true;
            } else if e.event_type() == EventType::WorkflowTaskStarted {
                next_wft_state = NextWftState::Seen;
                return true;
            }

            match next_wft_state {
                NextWftState::Seen => {
                    // Must ignore failures and timeouts
                    if e.event_type() == EventType::WorkflowTaskFailed
                        || e.event_type() == EventType::WorkflowTaskTimedOut
                    {
                        next_wft_state = NextWftState::NotSeen;
                        return true;
                    } else if e.event_type() == EventType::WorkflowTaskCompleted {
                        next_wft_state = NextWftState::SeenCompleted;
                        return true;
                    }
                    false
                }
                NextWftState::SeenCompleted => {
                    // If we've seen the WFT be completed, and this event is another scheduled, then
                    // this was an empty heartbeat we should ignore.
                    if e.event_type() == EventType::WorkflowTaskScheduled {
                        next_wft_state = NextWftState::NotSeen;
                        return true;
                    }
                    // Otherwise, we're done here
                    false
                }
                NextWftState::NotSeen => true,
            }
        };

        // Fetch events from the buffer first, then from the network
        let mut event_q = stream::iter(self.buffered.drain(..).map(Ok)).chain(&mut self.events);

        let mut extra_e = vec![];
        let mut last_seen_id = None;
        while let Some(e) = event_q.next().await {
            let e = e?;

            // This little block prevents us from infinitely fetching work from the server in the
            // event that, for whatever reason, it keeps returning stuff we've already seen.
            if let Some(last_id) = last_seen_id {
                if e.event_id <= last_id {
                    error!("Server returned history event IDs that went backwards!");
                    break;
                }
            }
            last_seen_id = Some(e.event_id);

            // It's possible to have gotten a new history update without eviction (ex: unhandled
            // command on completion), where we may need to skip events we already handled.
            if e.event_id > from_event_id {
                if !should_pop(&e) {
                    if next_wft_state == NextWftState::SeenCompleted {
                        // We have seen the wft completed event, but decided to exit. We don't
                        // want to return that event as part of this sequence, so include it for
                        // re-buffering along with the event we're currently on.
                        extra_e.push(
                            events_to_next_wft_started
                                .pop()
                                .expect("There is an element here by definition"),
                        );
                    }
                    extra_e.push(e);
                    break;
                }
                events_to_next_wft_started.push(e);
            }
        }

        Ok((events_to_next_wft_started, extra_e))
    }
}

#[derive(Eq, PartialEq, Debug)]
enum NextWftState {
    NotSeen,
    Seen,
    SeenCompleted,
}

impl From<HistoryInfo> for HistoryUpdate {
    fn from(v: HistoryInfo) -> Self {
        Self::new_from_events(v.events().to_vec(), v.previous_started_event_id())
    }
}

pub trait TestHBExt {
    fn as_history_update(&self) -> HistoryUpdate;
}

impl TestHBExt for TestHistoryBuilder {
    fn as_history_update(&self) -> HistoryUpdate {
        self.get_full_history_info().unwrap().into()
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{test_help::canned_histories, worker::client::mocks::mock_workflow_client};

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
    async fn history_ends_abruptly() {
        let mut timer_hist = canned_histories::single_timer("t");
        timer_hist.add_workflow_execution_terminated();
        let mut update = timer_hist.as_history_update();
        let seq_2 = update.take_next_wft_sequence(3).await.unwrap();
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
    }

    #[tokio::test]
    async fn heartbeats_skipped() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_get_event_id(EventType::TimerStarted, None);
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_get_event_id(EventType::TimerStarted, None);
        t.add_full_wf_task();
        t.add_we_signaled("whee", vec![]);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let mut update = t.as_history_update();
        let seq = update.take_next_wft_sequence(0).await.unwrap();
        assert_eq!(seq.len(), 6);
        let seq = update.take_next_wft_sequence(6).await.unwrap();
        assert_eq!(seq.len(), 13);
        let seq = update.take_next_wft_sequence(19).await.unwrap();
        assert_eq!(seq.len(), 4);
        let seq = update.take_next_wft_sequence(23).await.unwrap();
        assert_eq!(seq.len(), 4);
        let seq = update.take_next_wft_sequence(27).await.unwrap();
        assert_eq!(seq.len(), 2);
    }

    #[tokio::test]
    async fn paginator_fetches_new_pages() {
        // Note that this test triggers the "event ids that went backwards" error, acceptably.
        // Can be fixed by having mock not return earlier events.
        let wft_count = 500;
        let long_hist = canned_histories::long_sequential_timers(wft_count);
        let initial_hist = long_hist.get_history_info(10).unwrap();
        let prev_started = initial_hist.previous_started_event_id();
        let mut mock_client = mock_workflow_client();

        let mut npt = 2;
        mock_client
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
                Arc::new(mock_client),
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
            for e in &seq {
                last_event_id += 1;
                assert_eq!(e.event_id, last_event_id);
            }
            assert_eq!(seq.len(), 5);
            last_started_id += 5;
        }
    }

    #[tokio::test]
    async fn handles_cache_misses() {
        let timer_hist = canned_histories::single_timer("t");
        let partial_task = timer_hist.get_one_wft(2).unwrap();
        let mut history_from_get: GetWorkflowExecutionHistoryResponse =
            timer_hist.get_history_info(2).unwrap().into();
        // Chop off the last event, which is WFT started, which server doesn't return in get
        // history
        history_from_get.history.as_mut().map(|h| h.events.pop());
        let mut mock_client = mock_workflow_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(history_from_get.clone()));

        let mut update = HistoryUpdate::new(
            HistoryPaginator::new(
                partial_task.into(),
                "wfid".to_string(),
                "runid".to_string(),
                // A cache miss means we'll try to fetch from start
                NextPageToken::FetchFromStart,
                Arc::new(mock_client),
            ),
            1,
        );
        // We expect if we try to take the first task sequence that the first event is the first
        // event in the sequence.
        let seq = update.take_next_wft_sequence(0).await.unwrap();
        assert_eq!(seq[0].event_id, 1);
        let seq = update.take_next_wft_sequence(3).await.unwrap();
        // Verify anything extra (which should only ever be WFT started) was re-appended to the
        // end of the event iteration after fetching the old history.
        assert_eq!(seq.last().unwrap().event_id, 8);
    }
}
