use crate::{
    protosext::ValidPollWFTQResponse,
    worker::{
        client::WorkerClient,
        workflow::{HistoryFetchReq, PermittedWFT, PreparedWFT},
    },
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

/// Represents one or more complete WFT sequences. History events are expected to be consumed from
/// it and applied to the state machines via [HistoryUpdate::take_next_wft_sequence]
pub struct HistoryUpdate {
    events: Vec<HistoryEvent>,
    pub previous_started_event_id: i64,
    /// True if this update contains the final WFT in history, and no more attempts to extract
    /// additional updates should be made.
    has_last_wft: bool,
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
impl HistoryUpdate {
    pub fn first_event_id(&self) -> Option<i64> {
        self.events.get(0).map(|e| e.event_id)
    }
    pub fn history_length(&self) -> usize {
        self.events.len()
    }
}

#[derive(Debug)]
pub enum NextWFT {
    ReplayOver,
    WFT(Vec<HistoryEvent>),
    NeedFetch,
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
    /// Use a new poll response to create a new [WFTPaginator], returning it and the
    /// [PreparedWFT] extracted from it that can be fed into workflow state.
    pub(super) async fn from_poll(
        wft: ValidPollWFTQResponse,
        client: Arc<dyn WorkerClient>,
        last_processed_event_id: i64,
    ) -> Result<(Self, PreparedWFT), tonic::Status> {
        let empty_hist = wft.history.events.is_empty();
        let npt = if empty_hist {
            NextPageToken::FetchFromStart
        } else {
            wft.next_page_token.into()
        };
        let mut paginator = HistoryPaginator::new(
            wft.history,
            wft.workflow_execution.workflow_id.clone(),
            wft.workflow_execution.run_id.clone(),
            npt,
            client,
        );
        let update = if empty_hist {
            HistoryUpdate::from_events([], 0, true).0
        } else {
            paginator
                .extract_next_update(last_processed_event_id)
                .await?
        };
        let prepared = PreparedWFT {
            task_token: wft.task_token,
            attempt: wft.attempt,
            execution: wft.workflow_execution,
            workflow_type: wft.workflow_type,
            legacy_query: wft.legacy_query,
            query_requests: wft.query_requests,
            update,
        };
        Ok((paginator, prepared))
    }

    pub(super) async fn from_fetchreq(
        mut req: HistoryFetchReq,
        client: Arc<dyn WorkerClient>,
    ) -> Result<(Self, PermittedWFT), tonic::Status> {
        let mut paginator = HistoryPaginator::from_start(
            req.original_wft.work.execution.workflow_id.clone(),
            req.original_wft.work.execution.run_id.clone(),
            client,
        );
        let first_update = paginator
            .extract_next_update(req.original_wft.work.update.previous_started_event_id)
            .await?;
        req.original_wft.work.update = first_update;
        Ok((paginator, req.original_wft))
    }
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
    pub(crate) fn from_start(wf_id: String, run_id: String, client: Arc<dyn WorkerClient>) -> Self {
        Self {
            client,
            event_queue: Default::default(),
            wf_id,
            run_id,
            next_page_token: NextPageToken::FetchFromStart,
            final_events: vec![],
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
    pub(crate) async fn extract_next_update(
        &mut self,
        last_processed_event_id: i64,
    ) -> Result<HistoryUpdate, tonic::Status> {
        loop {
            self.get_next_page().await?;
            let current_events = mem::take(&mut self.event_queue);
            if current_events.is_empty() {
                panic!("Don't call extract update when nothing is left");
            }
            let first_event_id = current_events.front().unwrap().event_id;
            // If there are some events at the end of the fetched events which represent only a portion
            // of a complete WFT, retain them to be used in the next extraction.
            let no_more = matches!(self.next_page_token, NextPageToken::Done);
            let (update, extra) =
                HistoryUpdate::from_events(current_events, last_processed_event_id, no_more);
            let extra_eid_same = extra
                .first()
                .map(|e| e.event_id == first_event_id)
                .unwrap_or_default();
            self.event_queue = extra.into();
            if !no_more && extra_eid_same {
                // There was not a meaningful WFT in the whole page. We must fetch more
                continue;
            }
            return Ok(update);
        }
    }

    /// Fetches the next page and adds it to the internal queue. Returns true if a fetch was
    /// performed, false if there is no next page.
    async fn get_next_page(&mut self) -> Result<bool, tonic::Status> {
        let npt = match mem::replace(&mut self.next_page_token, NextPageToken::Done) {
            // If there's no open request and the last page token we got was empty, we're done.
            NextPageToken::Done => return Ok(false),
            NextPageToken::FetchFromStart => vec![],
            NextPageToken::Next(v) => v,
        };
        debug!(run_id=%self.run_id, "Fetching new history page");
        let fetch_res = self
            .client
            .get_workflow_execution_history(self.wf_id.clone(), Some(self.run_id.clone()), npt)
            .instrument(span!(tracing::Level::TRACE, "fetch_history_in_paginator"))
            .await?;

        self.extend_queue_with_new_page(fetch_res);
        Ok(true)
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
    // Kept since can be used for history downloading
    #[cfg(test)]
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
                transmute(HistoryPaginator::get_next_page(this.inner).boxed())
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
    /// Create an instance of an update directly from events. If the passed in event iterator has a
    /// partial WFT sequence at the end, all events after the last complete WFT sequence (ending
    /// with WFT started) are returned back to the caller, since the history update only works in
    /// terms of complete WFT sequences.
    pub fn from_events<I: IntoIterator<Item = HistoryEvent>>(
        events: I,
        previous_wft_started_id: i64,
        has_last_wft: bool,
    ) -> (Self, Vec<HistoryEvent>)
    where
        <I as IntoIterator>::IntoIter: Send + 'static,
    {
        let mut all_events: Vec<_> = events.into_iter().collect();
        let mut last_end =
            find_end_index_of_next_wft_seq(all_events.as_slice(), previous_wft_started_id);
        if matches!(last_end, NextWFTSeqEndIndex::Incomplete(_)) {
            return if has_last_wft {
                (
                    Self {
                        events: all_events,
                        previous_started_event_id: previous_wft_started_id,
                        has_last_wft,
                    },
                    vec![],
                )
            } else {
                (
                    Self {
                        events: vec![],
                        previous_started_event_id: 0,
                        has_last_wft,
                    },
                    all_events,
                )
            };
        }
        while let NextWFTSeqEndIndex::Complete(next_end_ix) = last_end {
            let next_end_eid = all_events[next_end_ix].event_id;
            // TODO: can pass portion of slice to skip already scanned events
            let next_end = find_end_index_of_next_wft_seq(&all_events, next_end_eid);
            if matches!(next_end, NextWFTSeqEndIndex::Incomplete(_)) {
                break;
            }
            last_end = next_end;
        }
        let remaining_events = if all_events.is_empty() {
            vec![]
        } else {
            all_events.split_off(last_end.index() + 1)
        };

        (
            Self {
                events: all_events,
                previous_started_event_id: previous_wft_started_id,
                has_last_wft,
            },
            remaining_events,
        )
    }

    /// Create an instance of an update directly from events. The passed in events *must* consist
    /// of one or more complete WFT sequences. IE: The event iterator must not end in the middle
    /// of a WFT sequence.
    #[cfg(test)]
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
            has_last_wft: true,
        }
    }

    /// Given a workflow task started id, return all events starting at that number (exclusive) to
    /// the next WFT started event (inclusive).
    ///
    /// Events are *consumed* by this process, to keep things efficient in workflow machines.
    ///
    /// If we are out of WFT sequences that can be yielded by this update, it will return an empty
    /// vec, indicating more pages will need to be fetched.
    pub fn take_next_wft_sequence(&mut self, from_wft_started_id: i64) -> NextWFT {
        // First, drop any events from the queue which are earlier than the passed-in id.
        if let Some(ix_first_relevant) = self.starting_index_after_skipping(from_wft_started_id) {
            self.events.drain(0..ix_first_relevant);
        }
        let next_wft_ix = find_end_index_of_next_wft_seq(&self.events, from_wft_started_id);
        match next_wft_ix {
            NextWFTSeqEndIndex::Incomplete(siz) => {
                if self.has_last_wft {
                    if siz == 0 {
                        NextWFT::ReplayOver
                    } else {
                        NextWFT::WFT(self.events.drain(0..=siz).collect())
                    }
                } else {
                    if siz != 0 {
                        panic!(
                            "HistoryUpdate was created with an incomplete WFT. This is an SDK bug."
                        );
                    }
                    debug!("No more valid WFT sequences in HistoryUpdate, must fetch more events.");
                    NextWFT::NeedFetch
                }
            }
            NextWFTSeqEndIndex::Complete(next_wft_ix) => {
                NextWFT::WFT(self.events.drain(0..=next_wft_ix).collect())
            }
        }
    }
    /// Lets the caller peek ahead at the next WFT sequence that will be returned by
    /// [take_next_wft_sequence]. Will always return the first available WFT sequence if that has
    /// not been called first. May also return an empty iterator or incomplete sequence if we are at
    /// the end of history.
    pub fn peek_next_wft_sequence(&self, from_wft_started_id: i64) -> &[HistoryEvent] {
        let ix_first_relevant = self
            .starting_index_after_skipping(from_wft_started_id)
            .unwrap_or_default();
        let relevant_events = &self.events[ix_first_relevant..];
        if relevant_events.len() == 0 {
            return relevant_events;
        }
        let ix_end = find_end_index_of_next_wft_seq(relevant_events, from_wft_started_id).index();
        &relevant_events[0..=ix_end]
    }

    fn starting_index_after_skipping(&self, from_wft_started_id: i64) -> Option<usize> {
        self.events
            .iter()
            .find_position(|e| e.event_id > from_wft_started_id)
            .map(|(ix, _)| ix)
    }
}

#[derive(Debug, Copy, Clone)]
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

/// Discovers the index of the last event in next WFT sequence within the passed-in slice
fn find_end_index_of_next_wft_seq(
    events: &[HistoryEvent],
    from_event_id: i64,
) -> NextWFTSeqEndIndex {
    if events.is_empty() {
        return NextWFTSeqEndIndex::Incomplete(0);
    }
    let mut last_seen_id = None;
    let mut last_index = 0;
    let mut saw_any_non_wft_event = false;
    // let mut saw_any_non_wft_event_besides_execution_start = false;
    for (ix, e) in events.iter().enumerate() {
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
        if e.event_id <= from_event_id {
            continue;
        }

        if !matches!(
            e.event_type(),
            EventType::WorkflowTaskFailed
                | EventType::WorkflowTaskTimedOut
                | EventType::WorkflowTaskScheduled
                | EventType::WorkflowTaskStarted
                | EventType::WorkflowTaskCompleted
        ) {
            saw_any_non_wft_event = true;
        }
        if e.is_final_wf_execution_event() {
            return NextWFTSeqEndIndex::Complete(last_index);
        }

        if e.event_type() == EventType::WorkflowTaskStarted {
            if let Some(next_event) = events.get(ix + 1) {
                let et = next_event.event_type();
                // If the next event is WFT timeout or fail, or abrupt WF execution end, that
                // doesn't conclude a WFT sequence.
                if matches!(
                    et,
                    EventType::WorkflowTaskFailed
                        | EventType::WorkflowTaskTimedOut
                        | EventType::WorkflowExecutionTimedOut
                        | EventType::WorkflowExecutionTerminated
                        | EventType::WorkflowExecutionCanceled
                ) {
                    continue;
                }
                // If we've never seen an interesting event and the next two events are a completion
                // followed immediately again by scheduled, then this is a WFT heartbeat and also
                // doesn't conclude the sequence.
                else if et == EventType::WorkflowTaskCompleted {
                    if let Some(next_next_event) = events.get(ix + 2) {
                        if next_next_event.event_type() == EventType::WorkflowTaskScheduled {
                            continue;
                        } else {
                            saw_any_non_wft_event = true;
                        }
                    }
                }
            }
            if saw_any_non_wft_event {
                return NextWFTSeqEndIndex::Complete(ix);
            }
        }
    }

    NextWFTSeqEndIndex::Incomplete(last_index)
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::{
        replay::{HistoryInfo, TestHistoryBuilder},
        test_help::canned_histories,
        worker::client::mocks::mock_workflow_client,
    };
    use futures_util::TryStreamExt;

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

    impl NextWFT {
        fn unwrap_events(self) -> Vec<HistoryEvent> {
            match self {
                NextWFT::WFT(e) => e,
                o => panic!("Must be complete WFT: {:?}", o),
            }
        }
    }

    fn next_check_peek(update: &mut HistoryUpdate, from_id: i64) -> Vec<HistoryEvent> {
        let seq_peeked = update.peek_next_wft_sequence(from_id).to_vec();
        let seq = update.take_next_wft_sequence(from_id).unwrap_events();
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
        let seq_2_peeked = update.peek_next_wft_sequence(0).to_vec();
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
        let seq_2 = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
    }

    #[test]
    fn history_ends_abruptly() {
        let mut timer_hist = canned_histories::single_timer("t");
        timer_hist.add_workflow_execution_terminated();
        let mut update = timer_hist.as_history_update();
        let seq_2 = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq_2.len(), 6);
        assert_eq!(seq_2.last().unwrap().event_id, 9);
    }

    #[test]
    fn heartbeats_skipped() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task(); // wft started 6
        t.add_get_event_id(EventType::TimerStarted, None);
        t.add_full_wf_task(); // wft started 10
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_full_wf_task(); // wft started 19
        t.add_get_event_id(EventType::TimerStarted, None);
        t.add_full_wf_task(); // wft started 23
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

    #[test]
    fn heartbeat_marker_end() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_local_activity_result_marker(1, "1", "done".into());
        t.add_workflow_execution_completed();

        let mut update = t.as_history_update();
        let seq = next_check_peek(&mut update, 3);
        // completed, sched, started
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 6);
        assert_eq!(seq.len(), 3);
    }

    fn paginator_setup(history: TestHistoryBuilder, chunk_size: usize) -> HistoryPaginator {
        let full_hist = history.get_full_history_info().unwrap().into_events();
        let initial_hist = full_hist.chunks(chunk_size).nth(0).unwrap().to_vec();
        let mut mock_client = mock_workflow_client();

        let mut npt = 1;
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, passed_npt| {
                assert_eq!(passed_npt, vec![npt]);
                let mut hist_chunks = full_hist.chunks(chunk_size).peekable();
                let next_chunks = hist_chunks.nth(npt.into()).unwrap_or_default();
                npt += 1;
                let next_page_token = if hist_chunks.peek().is_none() {
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

    #[rstest::rstest]
    #[tokio::test]
    async fn paginator_extracts_updates(#[values(10, 11, 12, 13, 14)] chunk_size: usize) {
        let wft_count = 100;
        let mut paginator = paginator_setup(
            canned_histories::long_sequential_timers(wft_count),
            chunk_size,
        );
        let mut update = paginator.extract_next_update(0).await.unwrap();

        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.len(), 3);

        let mut last_event_id = 3;
        let mut last_started_id = 3;
        for i in 1..wft_count {
            let seq = {
                match update.take_next_wft_sequence(last_started_id) {
                    NextWFT::WFT(seq) => seq,
                    NextWFT::NeedFetch => {
                        update = paginator.extract_next_update(last_event_id).await.unwrap();
                        update
                            .take_next_wft_sequence(last_started_id)
                            .unwrap_events()
                    }
                    NextWFT::ReplayOver => {
                        assert_eq!(i, wft_count - 1);
                        break;
                    }
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
        let paginator = StreamingHistoryPaginator::new(paginator_setup(
            canned_histories::long_sequential_timers(wft_count),
            10,
        ));
        let everything: Vec<_> = paginator.try_collect().await.unwrap();
        assert_eq!(everything.len(), (wft_count + 1) * 5);
        everything.iter().fold(1, |event_id, e| {
            assert_eq!(event_id, e.event_id);
            e.event_id + 1
        });
    }

    fn three_wfts_then_heartbeats() -> TestHistoryBuilder {
        let mut t = TestHistoryBuilder::default();
        // Start with two complete normal WFTs
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task(); // wft start - 3
        t.add_get_event_id(EventType::TimerStarted, None);
        t.add_full_wf_task(); // wft start - 7
        t.add_get_event_id(EventType::TimerStarted, None);
        t.add_full_wf_task(); // wft start - 11
        for _ in 1..50 {
            // Add a bunch of heartbeats with no commands, which count as one task
            t.add_full_wf_task();
        }
        t.add_workflow_execution_completed();
        t
    }

    #[tokio::test]
    async fn needs_fetch_if_ending_in_middle_of_wft_seq() {
        let t = three_wfts_then_heartbeats();
        let mut ends_in_middle_of_seq = t.as_history_update().events;
        ends_in_middle_of_seq.truncate(19);
        // The update should contain the first two complete WFTs, ending on the 8th event which
        // is WFT started. The remaining events should be returned. False flags means the creator
        // knows there are more events, so we should return need fetch
        let (mut update, remaining) = HistoryUpdate::from_events(ends_in_middle_of_seq, 0, false);
        assert_eq!(remaining[0].event_id, 8);
        assert_eq!(remaining.last().unwrap().event_id, 19);
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 7);
        let next = update.take_next_wft_sequence(7);
        assert_matches!(next, NextWFT::NeedFetch);
    }

    // Like the above, but if the history happens to be cut off at a wft boundary, (even though
    // there may have been many heartbeats after we have no way of knowing about), it's going to
    // count events 7-20 as a WFT since there is started, completed, timer command, ..heartbeats..
    #[tokio::test]
    async fn needs_fetch_after_complete_seq_with_heartbeats() {
        let t = three_wfts_then_heartbeats();
        let mut ends_in_middle_of_seq = t.as_history_update().events;
        ends_in_middle_of_seq.truncate(20);
        let (mut update, remaining) = HistoryUpdate::from_events(ends_in_middle_of_seq, 0, false);
        assert!(remaining.is_empty());
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 7);
        let seq = update.take_next_wft_sequence(7).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 20);
        let next = update.take_next_wft_sequence(20);
        assert_matches!(next, NextWFT::NeedFetch);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn paginator_works_with_wft_over_multiple_pages(
        #[values(10, 11, 12, 13, 14)] chunk_size: usize,
    ) {
        let t = three_wfts_then_heartbeats();
        let mut paginator = paginator_setup(t, chunk_size);
        let mut update = paginator.extract_next_update(0).await.unwrap();
        let mut last_id = 0;
        loop {
            let seq = update.take_next_wft_sequence(last_id);
            match seq {
                NextWFT::WFT(seq) => {
                    last_id = seq.last().unwrap().event_id;
                }
                NextWFT::NeedFetch => {
                    update = paginator.extract_next_update(last_id).await.unwrap();
                }
                NextWFT::ReplayOver => break,
            }
        }
        assert_eq!(last_id, 160);
    }

    #[tokio::test]
    async fn task_just_before_heartbeat_chain_is_taken() {
        let t = three_wfts_then_heartbeats();
        let mut update = t.as_history_update();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 7);
        let seq = update.take_next_wft_sequence(7).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 158);
        let seq = update.take_next_wft_sequence(158).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 160);
        assert_eq!(
            seq.last().unwrap().event_type(),
            EventType::WorkflowExecutionCompleted
        );
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
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq[0].event_id, 1);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        // Verify anything extra (which should only ever be WFT started) was re-appended to the
        // end of the event iteration after fetching the old history.
        assert_eq!(seq.last().unwrap().event_id, 8);
    }

    #[test]
    fn la_marker_chunking() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_full_wf_task(); // started - 7
        t.add_local_activity_result_marker(1, "hi", Default::default());
        let act_s = t.add_activity_task_scheduled("1");
        let act_st = t.add_activity_task_started(act_s);
        t.add_activity_task_completed(act_s, act_st, Default::default());
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_timed_out();
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_timed_out();
        t.add_workflow_task_scheduled_and_started();

        let mut update = t.as_history_update();
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 7);
        assert_eq!(seq.len(), 13);
        dbg!(seq);
    }
}
