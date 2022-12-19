use crate::{
    replay::{HistoryInfo, TestHistoryBuilder},
    worker::client::WorkerClient,
};
use futures::{future::BoxFuture, FutureExt, Stream};
use itertools::Itertools;
use std::{
    collections::VecDeque,
    fmt::Debug,
    future::Future,
    mem,
    mem::transmute,
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
    events: VecDeque<HistoryEvent>,
    pub previous_started_event_id: i64,
    /// The last event id in the WFT returned the last time [HistoryUpdate::take_next_wft_sequence]
    /// was called. Used to enable [HistoryUpdate::peek_next_wft_sequence]
    last_wft_final_event_id: i64,
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
    /// These are events that should be returned once pagination has finished. This only happens
    /// during cache misses, where we got a partial task but need to fetch history from the start.
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
            final_events,
        }
    }

    /// Return at least the next two WFT sequences (as determined by the passed-in ID) as a
    /// [HistoryUpdate]. Two sequences supports the required peek-ahead during replay without
    /// unnecessary back-and-forth.
    ///
    /// If there are already enough events buffered in memory, they will all be returned. Including
    /// possibly (likely, during replay) more than just the next two WFTs.
    ///
    /// If there are insufficient events to constitute two WFTs, then we will fetch pages until
    /// we have two, or until we are at the end of history.
    async fn extract_next_update(
        &mut self,
        last_processed_event_id: i64,
    ) -> Result<HistoryUpdate, tonic::Status> {
        // If the existing event queue contains enough events for this WFT and the next, just
        // take that wholesale. (Assuming we don't need to fetch from start).

        self.get_next_page_if_needed().await?;
        let current_events = mem::take(&mut self.event_queue);
        Ok(HistoryUpdate::new_from_events(
            current_events,
            last_processed_event_id,
        ))
    }

    async fn get_next_page_if_needed(&mut self) -> Result<(), tonic::Status> {
        if self.event_queue.is_empty() {
            let npt = match mem::replace(&mut self.next_page_token, NextPageToken::Done) {
                // If there's no open request and the last page token we got was empty, we're done.
                NextPageToken::Done => return Ok(()),
                NextPageToken::FetchFromStart => vec![],
                NextPageToken::Next(v) => v,
            };
            debug!(run_id=%self.run_id, "Fetching new history page");
            let gw = self.client.clone();
            let wid = self.wf_id.clone();
            let rid = self.run_id.clone();
            let fetch_res = gw
                .get_workflow_execution_history(wid, Some(rid), npt)
                .instrument(span!(tracing::Level::TRACE, "fetch_history_in_paginator"))
                .await?;

            self.extend_queue_with_new_page(fetch_res);
        }
        Ok(())
    }

    fn extend_queue_with_new_page(&mut self, resp: GetWorkflowExecutionHistoryResponse) {
        self.next_page_token = resp.next_page_token.into();
        self.event_queue
            .extend(resp.history.map(|h| h.events).unwrap_or_default());
        if matches!(&self.next_page_token, NextPageToken::Done) {
            // If finished, we need to extend the queue with the final events, skipping any
            // which are already present.
            if let Some(last_event_id) = self.event_queue.back().map(|e| e.event_id) {
                let final_events = mem::take(&mut self.final_events);
                self.event_queue.extend(
                    final_events
                        .into_iter()
                        .skip_while(|e2| e2.event_id <= last_event_id),
                );
            }
        };
    }
}

#[pin_project::pin_project]
struct StreamingHistoryPaginator {
    inner: HistoryPaginator,
    #[pin]
    open_history_request: Option<BoxFuture<'static, Result<(), tonic::Status>>>,
}

impl StreamingHistoryPaginator {
    pub fn new(inner: HistoryPaginator) -> Self {
        Self {
            inner,
            open_history_request: None,
        }
    }
}

impl Stream for StreamingHistoryPaginator {
    type Item = Result<HistoryEvent, tonic::Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if let Some(e) = this.inner.event_queue.pop_front() {
            return Poll::Ready(Some(Ok(e)));
        }
        if this.open_history_request.is_none() {
            // SAFETY: This is safe because the inner paginator cannot be dropped before the future,
            //   and the future won't be moved from out of this struct.
            this.open_history_request.set(Some(unsafe {
                transmute(HistoryPaginator::get_next_page_if_needed(this.inner).boxed())
            }));
        }
        let history_req = this.open_history_request.as_mut().as_pin_mut().unwrap();

        return match Future::poll(history_req, cx) {
            Poll::Ready(resp) => {
                this.open_history_request.set(None);
                match resp {
                    Err(neterr) => Poll::Ready(Some(Err(neterr))),
                    Ok(_) => Poll::Ready(this.inner.event_queue.pop_front().map(Ok)),
                }
            }
            Poll::Pending => Poll::Pending,
        };
    }
}

impl HistoryUpdate {
    /// Create an instance of an update directly from events - should only be used for replaying.
    pub fn new_from_events<I: IntoIterator<Item = HistoryEvent>>(
        events: I,
        previous_wft_started_id: i64,
    ) -> Self
    where
        <I as IntoIterator>::IntoIter: Send + 'static,
    {
        Self {
            events: events.into_iter().collect(),
            previous_started_event_id: previous_wft_started_id,
            last_wft_final_event_id: 0,
        }
    }

    /// Given a workflow task started id, return all events starting at that number (inclusive) to
    /// the next WFT started event (inclusive). If there is no subsequent WFT started event,
    /// remaining history is returned.
    ///
    /// Events are *consumed* by this process, to keep things efficient in workflow machines.
    ///
    /// If we are out of WFT sequences that can be yielded by this update, it will return an empty
    /// vec, indicating more pages will need to be fetched.
    pub fn take_next_wft_sequence(&mut self, from_wft_started_id: i64) -> Vec<HistoryEvent> {
        // First, drop any events from the queue which are earlier than the passed-in id.
        if let Some((ix_first_relevant, _)) = self
            .events
            .iter()
            .find_position(|e| e.event_id > from_wft_started_id)
        {
            self.events.drain(0..ix_first_relevant);
        }
        let next_wft_ix = self
            .find_end_index_of_next_wft_sequence(from_wft_started_id)
            .index();
        if next_wft_ix == 0 {
            return vec![];
        }
        let next_wft_events: Vec<_> = self.events.drain(0..=next_wft_ix).collect();
        self.last_wft_final_event_id = next_wft_events
            .last()
            .map(|he| he.event_id)
            .unwrap_or(i64::MAX);

        next_wft_events
    }

    /// Lets the caller peek ahead at the next WFT sequence that will be returned by
    /// [take_next_wft_sequence]. Will always return the first available WFT sequence if that has
    /// not been called first. May also return an empty iterator or incomplete sequence if we are at
    /// the end of history.
    pub fn peek_next_wft_sequence(&self) -> impl Iterator<Item = &HistoryEvent> {
        self.events.iter().take(
            self.find_end_index_of_next_wft_sequence(self.last_wft_final_event_id)
                .index()
                + 1,
        )
    }

    /// Discovers the index of the last event in next WFT sequence within the event queue.
    fn find_end_index_of_next_wft_sequence(&self, from_event_id: i64) -> NextWFTSeqEndIndex {
        find_end_index_of_next_wft_seq_in_iterator(self.events.iter(), from_event_id)
    }
}

enum NextWFTSeqEndIndex {
    /// The next WFT sequence is completely contained within the passed-in iterator
    Complete(usize),
    /// The next WFT sequence is not found within the passed-in iterator, and the contained
    /// value is the last index of the iterator.
    Incomplete(usize),
}
impl NextWFTSeqEndIndex {
    fn index(self) -> usize {
        match self {
            NextWFTSeqEndIndex::Complete(ix) | NextWFTSeqEndIndex::Incomplete(ix) => ix,
        }
    }
}

/// Discovers the index of the last event in next WFT sequence within the passed-in iterator
fn find_end_index_of_next_wft_seq_in_iterator<'a>(
    events: impl Iterator<Item = &'a HistoryEvent>,
    from_event_id: i64,
) -> NextWFTSeqEndIndex {
    // This flag tracks if, while determining events to be returned, we have seen the next
    // logically significant WFT started event which follows the one that was passed in as a
    // parameter. If a WFT fails, times out, or is devoid of commands (ie: a heartbeat) it is
    // not significant. So we will stop returning events (exclusive) as soon as we see an event
    // following a WFT started that is *not* failed, timed out, or completed with a command.
    let mut next_wft_state = NextWftState::NotSeen;
    let mut should_pop = |e: &HistoryEvent| {
        if e.event_type() == EventType::WorkflowTaskStarted {
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

    let mut last_seen_id = None;
    let mut last_index = 0;
    for (ix, e) in events.enumerate() {
        // This little block prevents us from infinitely fetching work from the server in the
        // event that, for whatever reason, it keeps returning stuff we've already seen.
        // TODO: This may not make sense any more?
        if let Some(last_id) = last_seen_id {
            if e.event_id <= last_id {
                error!("Server returned history event IDs that went backwards!");
                return NextWFTSeqEndIndex::Complete(ix);
            }
        }
        last_seen_id = Some(e.event_id);
        last_index = ix;

        // It's possible to have gotten a new history update without eviction (ex: unhandled
        // command on completion), where we may need to skip events we already handled.
        if e.event_id > from_event_id {
            if !should_pop(&e) {
                if next_wft_state == NextWftState::SeenCompleted {
                    // We have seen the wft completed event, but decided to exit. We don't
                    // want to return that event as part of this sequence, so we step back one
                    // event
                    return NextWFTSeqEndIndex::Complete(ix - 2);
                }
                return NextWFTSeqEndIndex::Complete(ix - 1);
            }
        }
    }

    NextWFTSeqEndIndex::Incomplete(last_index)
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
    use futures_util::{StreamExt, TryStreamExt};

    fn next_check_peek(update: &mut HistoryUpdate, from_id: i64) -> Vec<HistoryEvent> {
        let seq_peeked = update.peek_next_wft_sequence().cloned().collect::<Vec<_>>();
        let seq = update.take_next_wft_sequence(from_id);
        assert_eq!(seq, seq_peeked);
        seq
    }

    #[test]
    fn consumes_standard_wft_sequence() {
        let timer_hist = canned_histories::single_timer("t");
        let mut update = timer_hist.as_history_update();
        let seq_1 = next_check_peek(&mut update, 0);
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2_peeked = update.peek_next_wft_sequence().cloned().collect::<Vec<_>>();
        let seq_2 = next_check_peek(&mut update, 3);
        assert_eq!(seq_2, seq_2_peeked);
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
    }

    #[test]
    fn skips_wft_failed() {
        let failed_hist = canned_histories::workflow_fails_with_reset_after_timer("t", "runid");
        let mut update = failed_hist.as_history_update();
        let seq_1 = next_check_peek(&mut update, 0);
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2 = next_check_peek(&mut update, 3);
        assert_eq!(seq_2.len(), 8);
        assert_eq!(seq_2.last().unwrap().event_id, 11);
    }

    #[test]
    fn skips_wft_timeout() {
        let failed_hist = canned_histories::wft_timeout_repro();
        let mut update = failed_hist.as_history_update();
        let seq_1 = next_check_peek(&mut update, 0);
        assert_eq!(seq_1.len(), 3);
        assert_eq!(seq_1.last().unwrap().event_id, 3);
        let seq_2 = next_check_peek(&mut update, 3);
        assert_eq!(seq_2.len(), 11);
        assert_eq!(seq_2.last().unwrap().event_id, 14);
    }

    #[test]
    fn skips_events_before_desired_wft() {
        let timer_hist = canned_histories::single_timer("t");
        let mut update = timer_hist.as_history_update();
        // We haven't processed the first 3 events, but we should still only get the second sequence
        let seq_2 = update.take_next_wft_sequence(3);
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
    }

    #[test]
    fn history_ends_abruptly() {
        let mut timer_hist = canned_histories::single_timer("t");
        timer_hist.add_workflow_execution_terminated();
        let mut update = timer_hist.as_history_update();
        let seq_2 = update.take_next_wft_sequence(3);
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
    }

    #[test]
    fn heartbeats_skipped() {
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
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.len(), 6);
        let seq = next_check_peek(&mut update, 6);
        assert_eq!(seq.len(), 13);
        let seq = next_check_peek(&mut update, 19);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 23);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 27);
        assert_eq!(seq.len(), 2);
    }

    fn paginator_setup(wft_count: usize) -> HistoryPaginator {
        let long_hist = canned_histories::long_sequential_timers(wft_count);
        let chunk_size = 10;
        let full_hist = long_hist.get_full_history_info().unwrap().into_events();
        let initial_hist = full_hist.chunks(chunk_size).nth(0).unwrap().to_vec();
        let mut mock_client = mock_workflow_client();

        let mut npt = 1;
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, passed_npt| {
                assert_eq!(passed_npt, vec![npt]);
                let mut hist_chunks = full_hist.chunks(10);
                let next_chunks = hist_chunks.nth(npt.into()).unwrap_or_default();
                npt += 1;
                let next_page_token = if next_chunks.is_empty() {
                    vec![]
                } else {
                    vec![npt]
                };
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History {
                        events: next_chunks.into(),
                    }),
                    raw_history: vec![],
                    next_page_token,
                    archived: false,
                })
            });

        HistoryPaginator::new(
            History {
                events: initial_hist,
            },
            "wfid".to_string(),
            "runid".to_string(),
            vec![1],
            Arc::new(mock_client),
        )
    }

    #[tokio::test]
    async fn paginator_extracts_updates() {
        let wft_count = 100;
        let mut paginator = paginator_setup(wft_count);
        let mut update = paginator.extract_next_update(0).await.unwrap();

        let seq = update.take_next_wft_sequence(0);
        assert_eq!(seq.len(), 3);

        let mut last_event_id = 3;
        let mut last_started_id = 3;
        for _ in 1..wft_count {
            let seq = {
                let seq = update.take_next_wft_sequence(last_started_id);
                if !seq.is_empty() {
                    seq
                } else {
                    update = paginator.extract_next_update(last_event_id).await.unwrap();
                    update.take_next_wft_sequence(last_started_id)
                }
            };
            for e in &seq {
                last_event_id += 1;
                assert_eq!(e.event_id, last_event_id);
            }
            assert_eq!(seq.len(), 5);
            last_started_id += 5;
        }
    }

    #[tokio::test]
    async fn paginator_streams() {
        let wft_count = 10;
        let paginator = StreamingHistoryPaginator::new(paginator_setup(wft_count));
        let everything: Vec<_> = paginator.try_collect().await.unwrap();
        assert_eq!(everything.len(), (wft_count + 1) * 5);
        everything.iter().fold(1, |event_id, e| {
            assert_eq!(event_id, e.event_id);
            e.event_id + 1
        });
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

        let mut paginator = HistoryPaginator::new(
            partial_task.into(),
            "wfid".to_string(),
            "runid".to_string(),
            // A cache miss means we'll try to fetch from start
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
        );
        let mut update = paginator.extract_next_update(1).await.unwrap();
        // We expect if we try to take the first task sequence that the first event is the first
        // event in the sequence.
        let seq = update.take_next_wft_sequence(0);
        assert_eq!(seq[0].event_id, 1);
        let seq = update.take_next_wft_sequence(3);
        // Verify anything extra (which should only ever be WFT started) was re-appended to the
        // end of the event iteration after fetching the old history.
        assert_eq!(seq.last().unwrap().event_id, 8);
    }
}
