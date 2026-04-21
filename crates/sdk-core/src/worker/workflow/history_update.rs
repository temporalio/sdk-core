use crate::{
    protosext::ValidPollWFTQResponse,
    worker::{
        client::WorkerClient,
        workflow::{CacheMissFetchReq, PermittedWFT, PreparedWFT},
    },
};
use futures_util::{FutureExt, Stream, TryFutureExt, future::BoxFuture};
use itertools::Itertools;
use std::{
    collections::VecDeque,
    fmt::Debug,
    future::Future,
    mem,
    mem::transmute,
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
};
use temporalio_common::protos::temporal::api::{
    enums::v1::EventType,
    history::v1::{
        History, HistoryEvent, WorkflowTaskCompletedEventAttributes, history_event::Attributes,
    },
};
use tracing::Instrument;

static ENABLE_AD_1: bool = true;
static ENABLE_AD_2: bool = true;
static ENABLE_AD_3: bool = true;
static ENABLE_AD_4: bool = true;

static EMPTY_FETCH_ERR: LazyLock<tonic::Status> =
    LazyLock::new(|| tonic::Status::unknown("Fetched empty history page"));
static EMPTY_TASK_ERR: LazyLock<tonic::Status> = LazyLock::new(|| {
    tonic::Status::unknown("Received an empty workflow task with no queries or history")
});

/// Represents one or more complete WFT sequences. History events are expected to be consumed from
/// it and applied to the state machines via [HistoryUpdate::take_next_wft_sequence]
pub(crate) struct HistoryUpdate {
    events: Vec<HistoryEvent>,
    /// The event ID of the last started WFT, as according to the WFT which this update was
    /// extracted from. Hence, while processing multiple logical WFTs during replay which were part
    /// of one large history fetched from server, multiple updates may have the same value here.
    pub(crate) previous_wft_started_id: i64,
    /// The `started_event_id` field from the WFT which this update is tied to. Multiple updates
    /// may have the same value if they're associated with the same WFT.
    pub(crate) wft_started_id: i64,
    /// True if this update contains the final WFT in history, and no more attempts to extract
    /// additional updates should be made.
    has_last_wft: bool,
    wft_count: usize,
    /// True if the speculative WFT (i.e. the current, non-replayed task from the
    /// server) carries pending update messages. When set, the heartbeat-collapsing
    /// heuristic will avoid merging the last WFT in history into a preceding
    /// heartbeat chain, because the update needs its own activation.
    has_pending_speculative_updates: bool,
}

impl Debug for HistoryUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_real() {
            write!(
                f,
                "HistoryUpdate(previous_started_event_id: {}, started_id: {}, \
                 length: {}, first_event_id: {:?})",
                self.previous_wft_started_id,
                self.wft_started_id,
                self.events.len(),
                self.events.first().map(|e| e.event_id)
            )
        } else {
            write!(f, "DummyHistoryUpdate")
        }
    }
}

impl HistoryUpdate {
    pub(crate) fn get_events(&self) -> &[HistoryEvent] {
        &self.events
    }
}

#[derive(Debug)]
pub(crate) enum NextWFT {
    ReplayOver,
    WFT(Vec<HistoryEvent>, bool),
    NeedFetch,
}

#[derive(derive_more::Debug)]
#[debug("HistoryPaginator(run_id: {run_id})")]
pub(crate) struct HistoryPaginator {
    pub(crate) wf_id: String,
    pub(crate) run_id: String,
    pub(crate) previous_wft_started_id: i64,
    pub(crate) wft_started_event_id: i64,
    id_of_last_event_in_last_extracted_update: Option<i64>,

    client: Arc<dyn WorkerClient>,
    event_queue: VecDeque<HistoryEvent>,
    next_page_token: NextPageToken,
    /// These are events that should be returned once pagination has finished. This only happens
    /// during cache misses, where we got a partial task but need to fetch history from the start.
    final_events: Vec<HistoryEvent>,
    /// True if the speculative WFT associated with this paginator carries pending update
    /// messages. Passed through to `find_end_index_of_next_wft_seq` so the heartbeat
    /// heuristic avoids collapsing the last WFT when an update needs its own activation.
    has_pending_speculative_updates: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum NextPageToken {
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
    ) -> Result<(Self, PreparedWFT), tonic::Status> {
        eprintln!("DEBUG: shadow chunking active (from_poll)");
        let empty_hist = wft.history.events.is_empty();
        let npt = if empty_hist {
            NextPageToken::FetchFromStart
        } else {
            wft.next_page_token.into()
        };
        let has_pending_speculative_updates = !wft.messages.is_empty();
        let mut paginator = HistoryPaginator::new(
            wft.history,
            wft.previous_started_event_id,
            wft.started_event_id,
            wft.workflow_execution.workflow_id.clone(),
            wft.workflow_execution.run_id.clone(),
            npt,
            client,
            has_pending_speculative_updates,
        );
        if empty_hist && wft.legacy_query.is_none() && wft.query_requests.is_empty() {
            return Err(EMPTY_TASK_ERR.clone());
        }
        let update = if empty_hist {
            HistoryUpdate::from_events(
                [],
                wft.previous_started_event_id,
                wft.started_event_id,
                true,
                has_pending_speculative_updates,
            )
            .0
        } else {
            paginator.extract_next_update().await?
        };
        let prepared = PreparedWFT {
            task_token: wft.task_token,
            attempt: wft.attempt,
            execution: wft.workflow_execution,
            workflow_type: wft.workflow_type,
            legacy_query: wft.legacy_query,
            query_requests: wft.query_requests,
            update,
            messages: wft.messages,
        };
        Ok((paginator, prepared))
    }

    pub(super) async fn from_fetchreq(
        mut req: Box<CacheMissFetchReq>,
        client: Arc<dyn WorkerClient>,
    ) -> Result<PermittedWFT, tonic::Status> {
        let mut paginator = Self {
            wf_id: req.original_wft.work.execution.workflow_id.clone(),
            run_id: req.original_wft.work.execution.run_id.clone(),
            previous_wft_started_id: req.original_wft.work.update.previous_wft_started_id,
            wft_started_event_id: req.original_wft.work.update.wft_started_id,
            id_of_last_event_in_last_extracted_update: req
                .original_wft
                .paginator
                .id_of_last_event_in_last_extracted_update,
            client,
            event_queue: Default::default(),
            next_page_token: NextPageToken::FetchFromStart,
            final_events: req.original_wft.work.update.events,
            has_pending_speculative_updates: !req.original_wft.work.messages.is_empty(),
        };
        let first_update = paginator.extract_next_update().await?;
        req.original_wft.work.update = first_update;
        req.original_wft.paginator = paginator;
        Ok(req.original_wft)
    }

    fn new(
        initial_history: History,
        previous_wft_started_id: i64,
        wft_started_event_id: i64,
        wf_id: String,
        run_id: String,
        next_page_token: impl Into<NextPageToken>,
        client: Arc<dyn WorkerClient>,
        has_pending_speculative_updates: bool,
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
            previous_wft_started_id,
            wft_started_event_id,
            id_of_last_event_in_last_extracted_update: None,
            has_pending_speculative_updates,
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
    pub(crate) async fn extract_next_update(&mut self) -> Result<HistoryUpdate, tonic::Status> {
        loop {
            let no_next_page = !self.get_next_page().await?;
            let current_events = mem::take(&mut self.event_queue);
            let seen_enough_events = current_events
                .back()
                .map(|e| e.event_id)
                .unwrap_or_default()
                >= self.wft_started_event_id;

            // This handles a special case where the server might send us a page token along with
            // a real page which ends at the current end of history. The page token then points to
            // en empty page. We need to detect this, and consider it the end of history.
            //
            // This case unfortunately cannot be handled earlier, because we might fetch a page
            // from the server which contains two complete WFTs, and thus we are happy to return
            // an update at that time. But, if the page has a next page token, we *cannot* conclude
            // we are done with replay until we fetch that page. So, we have to wait until the next
            // extraction to determine (after fetching the next page and finding it to be empty)
            // that we are done. Fetching the page eagerly is another option, but would be wasteful
            // the overwhelming majority of the time.
            let already_sent_update_with_enough_events = self
                .id_of_last_event_in_last_extracted_update
                .unwrap_or_default()
                >= self.wft_started_event_id;
            if current_events.is_empty() && no_next_page && already_sent_update_with_enough_events {
                // We must return an empty update which also says is contains the final WFT so we
                // know we're done with replay.
                return Ok(HistoryUpdate::from_events(
                    [],
                    self.previous_wft_started_id,
                    self.wft_started_event_id,
                    true,
                    self.has_pending_speculative_updates,
                )
                .0);
            }

            if current_events.is_empty() || (no_next_page && !seen_enough_events) {
                // If next page fetching happened, and we still ended up with no or insufficient
                // events, something is wrong. We're expecting there to be more events to be able to
                // extract this update, but server isn't giving us any. We have no choice except to
                // give up and evict.
                error!(
                    current_events=?current_events,
                    no_next_page,
                    seen_enough_events,
                    "We expected to be able to fetch more events but server says there are none"
                );
                return Err(EMPTY_FETCH_ERR.clone());
            }
            let first_event_id = current_events.front().unwrap().event_id;
            // We only *really* have the last WFT if the events go all the way up to at least the
            // WFT started event id. Otherwise we somehow still have partial history.
            let no_more = matches!(self.next_page_token, NextPageToken::Done) && seen_enough_events;
            let (update, extra) = HistoryUpdate::from_events(
                current_events,
                self.previous_wft_started_id,
                self.wft_started_event_id,
                no_more,
                self.has_pending_speculative_updates,
            );

            // If there are potentially more events and we haven't extracted two WFTs yet, keep
            // trying.
            if !matches!(self.next_page_token, NextPageToken::Done) && update.wft_count < 2 {
                // Unwrap the update and stuff it all back in the queue
                self.event_queue.extend(update.events);
                self.event_queue.extend(extra);
                continue;
            }

            let extra_eid_same = extra
                .first()
                .map(|e| e.event_id == first_event_id)
                .unwrap_or_default();
            // If there are some events at the end of the fetched events which represent only a
            // portion of a complete WFT, retain them to be used in the next extraction.
            self.event_queue = extra.into();
            if !no_more && extra_eid_same {
                // There was not a meaningful WFT in the whole page. We must fetch more.
                continue;
            }
            self.id_of_last_event_in_last_extracted_update =
                update.events.last().map(|e| e.event_id);
            #[cfg(debug_assertions)]
            update.assert_contiguous();
            return Ok(update);
        }
    }

    /// Fetches the next page and adds it to the internal queue.
    /// Returns true if we still have a next page token after fetching.
    async fn get_next_page(&mut self) -> Result<bool, tonic::Status> {
        let history = loop {
            let npt = match mem::replace(&mut self.next_page_token, NextPageToken::Done) {
                // If the last page token we got was empty, we're done.
                NextPageToken::Done => break None,
                NextPageToken::FetchFromStart => vec![],
                NextPageToken::Next(v) => v,
            };
            debug!(run_id=%self.run_id, "Fetching new history page");
            let fetch_res = self
                .client
                .get_workflow_execution_history(self.wf_id.clone(), Some(self.run_id.clone()), npt)
                .instrument(span!(tracing::Level::TRACE, "fetch_history_in_paginator"))
                .await?;

            self.next_page_token = fetch_res.next_page_token.into();

            let history_is_empty = fetch_res
                .history
                .as_ref()
                .map(|h| h.events.is_empty())
                .unwrap_or(true);
            if history_is_empty && matches!(&self.next_page_token, NextPageToken::Next(_)) {
                // If the fetch returned an empty history, but there *was* a next page token,
                // immediately try to get that.
                continue;
            }
            // Async doesn't love recursion so we do this instead.
            break fetch_res.history;
        };

        let queue_back_id = self
            .event_queue
            .back()
            .map(|e| e.event_id)
            .unwrap_or_default();
        self.event_queue.extend(
            history
                .map(|h| h.events)
                .unwrap_or_default()
                .into_iter()
                .skip_while(|e| e.event_id <= queue_back_id),
        );
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
        Ok(!matches!(&self.next_page_token, NextPageToken::Done))
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
    fn new(inner: HistoryPaginator) -> Self {
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
                transmute::<
                    BoxFuture<'_, Result<(), tonic::Status>>,
                    BoxFuture<'static, Result<(), tonic::Status>>,
                >(this.inner.get_next_page().map_ok(|_| ()).boxed())
            }));
        }
        let history_req = this.open_history_request.as_mut().as_pin_mut().unwrap();

        match Future::poll(history_req, cx) {
            Poll::Ready(resp) => {
                this.open_history_request.set(None);
                match resp {
                    Err(neterr) => Poll::Ready(Some(Err(neterr))),
                    Ok(_) => Poll::Ready(this.inner.event_queue.pop_front().map(Ok)),
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl HistoryUpdate {
    /// Sometimes it's useful to take an update out of something without needing to use an option
    /// field. Use this to replace the field with an empty update.
    pub(crate) fn dummy() -> Self {
        Self {
            events: vec![],
            previous_wft_started_id: -1,
            wft_started_id: -1,
            has_last_wft: false,
            wft_count: 0,
            has_pending_speculative_updates: false,
        }
    }

    pub(crate) fn is_real(&self) -> bool {
        self.previous_wft_started_id >= 0
    }

    pub(crate) fn first_event_id(&self) -> Option<i64> {
        self.events.first().map(|e| e.event_id)
    }

    #[cfg(debug_assertions)]
    fn assert_contiguous(&self) -> bool {
        use crate::abstractions::dbg_panic;

        for win in self.events.as_slice().windows(2) {
            if let &[e1, e2] = &win
                && e2.event_id != e1.event_id + 1
            {
                dbg_panic!("HistoryUpdate isn't contiguous! {:?} -> {:?}", e1, e2);
            }
        }
        true
    }

    /// Create an instance of an update directly from events. If the passed in event iterator has a
    /// partial WFT sequence at the end, all events after the last complete WFT sequence (ending
    /// with WFT started) are returned back to the caller, since the history update only works in
    /// terms of complete WFT sequences.
    pub(crate) fn from_events<I: IntoIterator<Item = HistoryEvent>>(
        events: I,
        previous_wft_started_id: i64,
        wft_started_id: i64,
        has_last_wft: bool,
        has_pending_speculative_updates: bool,
    ) -> (Self, Vec<HistoryEvent>)
    where
        <I as IntoIterator>::IntoIter: Send + 'static,
    {
        let all_events: Vec<_> = events.into_iter().collect();

        let old_t = Self::from_events_partition_tuple_old(
            all_events.as_slice(),
            wft_started_id,
            has_last_wft,
            has_pending_speculative_updates,
        );
        let new_t = Self::from_events_partition_tuple_new(
            all_events.as_slice(),
            wft_started_id,
            has_last_wft,
            has_pending_speculative_updates,
        );

        // Differences on element 0 (early_incomplete) is not significant by itself
        // from an upstream caller's perspective, so ignore it for divergence comparison.
        if (old_t.1..=old_t.3) != (new_t.1..=new_t.3) {
            let n = all_events.len();

            let matches_accepted_2 = ENABLE_AD_2
                && !has_last_wft
                && new_t == (true, 0, 0, n)
                && !old_t.0
                && old_t.2 == n
                && old_t.3 == 0
                && all_events
                    .last()
                    .is_some_and(|e| e.event_type() == EventType::WorkflowTaskStarted);

            // Accepted divergence 3: legacy `Complete` spans through a terminal `WorkflowExecution*`
            // in cases where new stops at `WorkflowTaskStarted` and leaves completion + terminal
            // as `Incomplete` (see `Divergences.md`).
            let matches_accepted_3 = ENABLE_AD_3
                && !old_t.0
                && !new_t.0
                && ({
                    // Paginated slice: adjacent `WFTStarted` -> terminal (original AD3 tuple shape).
                    old_t.1 == new_t.1
                        && old_t.2 == new_t.2.saturating_add(1)
                        && old_t.3.saturating_add(1) == new_t.3
                        && !has_last_wft
                        && all_events.windows(2).any(|w| {
                            w[0].event_type() == EventType::WorkflowTaskStarted
                                && w[1].is_final_wf_execution_event()
                        })
                } || {
                    // Full buffer: `WFTStarted` -> `WFTCompleted` -> terminal execution; legacy
                    // emits an extra chained `Complete` through the terminal (`OLD = NEW + 1` on
                    // `wft_count`).
                    has_last_wft
                        && old_t.1 == new_t.1.saturating_add(1)
                        && old_t.2 == new_t.2
                        && old_t.3 == new_t.3
                        && shadow_history_has_wft_completed_before_terminal_execution(&all_events)
                });

            // Accepted divergence 4: `has_last_wft` + history ending in open `WorkflowTaskStarted` —
            // new chunking emits one more chained `Complete` than legacy (NEW wft_count = OLD + 1);
            // `kept` / `remaining` still match. See `Divergences.md`.
            let matches_accepted_4 = ENABLE_AD_4
                && has_last_wft
                && !old_t.0
                && !new_t.0
                && new_t.1 == old_t.1.saturating_add(1)
                && old_t.2 == new_t.2
                && old_t.3 == new_t.3
                && all_events
                    .last()
                    .is_some_and(|e| e.event_type() == EventType::WorkflowTaskStarted);

            if !matches_accepted_2 && !matches_accepted_3 && !matches_accepted_4 {
                let old_chunks = shadow_collect_from_events_chunks_old(
                    all_events.as_slice(),
                    wft_started_id,
                    has_last_wft,
                    has_pending_speculative_updates,
                );
                let new_chunks = shadow_collect_from_events_chunks_new(
                    all_events.as_slice(),
                    wft_started_id,
                    has_last_wft,
                    has_pending_speculative_updates,
                );
                eprintln!("=== WFT SHADOW DIVERGENCE (HistoryUpdate::from_events) ===");
                eprintln!(
                    "  Note: previous_wft_started_id is NEW-authoritative; OLD lines are legacy find_end under the same watermark."
                );
                eprintln!(
                    "  previous_wft_started_id={previous_wft_started_id}, wft_started_id={wft_started_id}, \
                 has_last_wft={has_last_wft}, has_pending_speculative_updates={has_pending_speculative_updates}"
                );
                eprint_shadow_events_with_watermark(&all_events, wft_started_id);
                eprint_shadow_from_events_chunk_lines("OLD", &old_chunks);
                eprint_shadow_from_events_chunk_lines("NEW", &new_chunks);
                eprintln!(
                    "  tuple OLD => (early_incomplete={}, wft_count={}, kept={}, remaining={})",
                    old_t.0, old_t.1, old_t.2, old_t.3
                );
                eprintln!(
                    "  tuple NEW => (early_incomplete={}, wft_count={}, kept={}, remaining={})",
                    new_t.0, new_t.1, new_t.2, new_t.3
                );
                eprintln!("=== END WFT SHADOW (from_events) ===");
            }
        }

        Self::from_events_apply_new(
            all_events,
            previous_wft_started_id,
            wft_started_id,
            has_last_wft,
            has_pending_speculative_updates,
        )
    }

    /// Shadow-only: `(early_incomplete, wft_count, kept_event_count, remaining_event_count)` using
    /// legacy chunking. See [`Self::from_events_partition_tuple_new`].
    ///
    /// This uses the **same chained** `find_end → suffix` loop as [`Self::from_events_apply_new`]
    /// (and the legacy equivalent), not a separate metric — when it disagrees with
    /// [`Self::peek_next_wft_sequence`], that is expected: peek applies **one** `find_end` call and
    /// uses [`NextWFTSeqEndIndex::end_index_in_slice`], while `wft_count` counts how many
    /// `Complete` boundaries appear before the chain hits `Incomplete`.
    fn from_events_partition_tuple_old(
        all: &[HistoryEvent],
        previous_wft_started_id: i64,
        has_last_wft: bool,
        has_pending_speculative_updates: bool,
    ) -> (bool, usize, usize, usize) {
        let mut last_end = find_end_index_of_next_wft_seq_old(
            all,
            previous_wft_started_id,
            has_last_wft,
            has_pending_speculative_updates,
        );
        if matches!(last_end, NextWFTSeqEndIndexOld::Incomplete(_)) {
            return if has_last_wft {
                (true, 1, all.len(), 0)
            } else {
                (true, 0, 0, all.len())
            };
        }
        let mut wft_count = 0;
        while let NextWFTSeqEndIndexOld::Complete(next_end_ix) = last_end {
            wft_count += 1;
            let next_end_eid = all[next_end_ix].event_id;
            let next_end = find_end_index_of_next_wft_seq_old(
                &all[next_end_ix..],
                next_end_eid,
                has_last_wft,
                has_pending_speculative_updates,
            )
            .add_base(next_end_ix);
            if matches!(next_end, NextWFTSeqEndIndexOld::Incomplete(_)) {
                break;
            }
            last_end = next_end;
        }
        let split_at = match last_end {
            NextWFTSeqEndIndexOld::Complete(ix) => ix,
            NextWFTSeqEndIndexOld::Incomplete(_) => unreachable!(),
        };
        let remaining_event_count = if all.is_empty() || has_last_wft {
            0
        } else {
            all.len() - (split_at + 1)
        };
        let kept = all.len() - remaining_event_count;
        (false, wft_count, kept, remaining_event_count)
    }

    /// Shadow-only: same tuple as [`Self::from_events_partition_tuple_old`], using new chunking.
    ///
    /// Intentional OLD/NEW tuple differences are documented in `Divergences.md` (accepted
    /// divergences 1–4). In particular, accepted divergence 4 covers extra `wft_count` on the
    /// new side when `has_last_wft` and history ends in an open `WorkflowTaskStarted`.
    fn from_events_partition_tuple_new(
        all: &[HistoryEvent],
        previous_wft_started_id: i64,
        has_last_wft: bool,
        has_pending_speculative_updates: bool,
    ) -> (bool, usize, usize, usize) {
        let mut last_end = find_end_index_of_next_wft_seq_new(
            all,
            previous_wft_started_id,
            has_last_wft,
            has_pending_speculative_updates,
        );
        if matches!(
            last_end,
            NextWFTSeqEndIndex::NeedMore | NextWFTSeqEndIndex::Tail
        ) {
            return if has_last_wft {
                (true, 1, all.len(), 0)
            } else {
                (true, 0, 0, all.len())
            };
        }
        let mut wft_count = 0;
        while let NextWFTSeqEndIndex::Complete(next_end_ix) = last_end {
            wft_count += 1;
            let next_end_eid = all[next_end_ix].event_id;
            let next_end = find_end_index_of_next_wft_seq_new(
                &all[next_end_ix..],
                next_end_eid,
                has_last_wft,
                has_pending_speculative_updates,
            )
            .add(next_end_ix);
            if matches!(
                next_end,
                NextWFTSeqEndIndex::NeedMore | NextWFTSeqEndIndex::Tail
            ) {
                break;
            }
            last_end = next_end;
        }
        let remaining_event_count = if all.is_empty() || has_last_wft {
            0
        } else {
            all.len() - (last_end.end_index_in_slice(all.len()) + 1)
        };
        let kept = all.len() - remaining_event_count;
        (false, wft_count, kept, remaining_event_count)
    }

    fn from_events_apply_new(
        mut all_events: Vec<HistoryEvent>,
        previous_wft_started_id: i64,
        wft_started_id: i64,
        has_last_wft: bool,
        has_pending_speculative_updates: bool,
    ) -> (Self, Vec<HistoryEvent>) {
        let mut last_end = find_end_index_of_next_wft_seq_new(
            all_events.as_slice(),
            previous_wft_started_id,
            has_last_wft,
            has_pending_speculative_updates,
        );

        if matches!(
            last_end,
            NextWFTSeqEndIndex::NeedMore | NextWFTSeqEndIndex::Tail
        ) {
            return if has_last_wft {
                (
                    Self {
                        events: all_events,
                        previous_wft_started_id,
                        wft_started_id,
                        has_last_wft,
                        wft_count: 1,
                        has_pending_speculative_updates,
                    },
                    vec![],
                )
            } else {
                (
                    Self {
                        events: vec![],
                        previous_wft_started_id,
                        wft_started_id,
                        has_last_wft,
                        wft_count: 0,
                        has_pending_speculative_updates,
                    },
                    all_events,
                )
            };
        }

        let mut wft_count = 0;
        while let NextWFTSeqEndIndex::Complete(next_end_ix) = last_end {
            wft_count += 1;
            let next_end_eid = all_events[next_end_ix].event_id;
            let next_end = find_end_index_of_next_wft_seq_new(
                &all_events[next_end_ix..],
                next_end_eid,
                has_last_wft,
                has_pending_speculative_updates,
            )
            .add(next_end_ix);
            if matches!(
                next_end,
                NextWFTSeqEndIndex::NeedMore | NextWFTSeqEndIndex::Tail
            ) {
                break;
            }
            last_end = next_end;
        }

        let remaining_events = if all_events.is_empty() || has_last_wft {
            vec![]
        } else {
            all_events.split_off(last_end.end_index_in_slice(all_events.len()) + 1)
        };

        (
            Self {
                events: all_events,
                previous_wft_started_id,
                wft_started_id,
                has_last_wft,
                wft_count,
                has_pending_speculative_updates,
            },
            remaining_events,
        )
    }

    /// Create an instance of an update directly from events. The passed in events *must* consist
    /// of one or more complete WFT sequences. IE: The event iterator must not end in the middle
    /// of a WFT sequence.
    #[cfg(test)]
    fn new_from_events<I: IntoIterator<Item = HistoryEvent>>(
        events: I,
        previous_wft_started_id: i64,
        wft_started_id: i64,
        has_last_wft: bool,
        has_pending_speculative_updates: bool,
    ) -> Self
    where
        <I as IntoIterator>::IntoIter: Send + 'static,
    {
        let events: Vec<_> = events.into_iter().collect();
        Self {
            events,
            previous_wft_started_id,
            wft_started_id,
            has_last_wft,
            wft_count: 0,
            has_pending_speculative_updates,
        }
    }

    /// Given a workflow task started id, return all events starting at that number (exclusive) to
    /// the next WFT started event (inclusive).
    ///
    /// Events are *consumed* by this process, to keep things efficient in workflow machines.
    ///
    /// If we are out of WFT sequences that can be yielded by this update, it will return an empty
    /// vec, indicating more pages will need to be fetched.
    pub(crate) fn take_next_wft_sequence(&mut self, from_wft_started_id: i64) -> NextWFT {
        // First, drop any events from the queue which are earlier than the passed-in id.
        if let Some(ix_first_relevant) =
            starting_index_after_skipping(&self.events, from_wft_started_id)
        {
            self.events.drain(0..ix_first_relevant);
        }

        let old_seq = self.take_next_wft_sequence_old(from_wft_started_id);
        let new_seq = self.take_next_wft_sequence_new(from_wft_started_id);

        let old_t = take_next_wft_shadow_tuple(&old_seq);
        let new_t = take_next_wft_shadow_tuple(&new_seq);
        if old_t != new_t {
            let matches_accepted_1 = match (&old_seq, &new_seq) {
                (NextWFT::WFT(old_ev, _), NextWFT::WFT(new_ev, _)) => {
                    old_ev.len() == new_ev.len() + 1
                        && old_ev.starts_with(new_ev.as_slice())
                        && matches!(
                            new_ev.last().map(|e| e.event_type()),
                            Some(EventType::WorkflowTaskStarted)
                        )
                        && matches!(
                            old_ev.get(new_ev.len()).map(|e| e.event_type()),
                            Some(EventType::WorkflowTaskCompleted)
                        )
                }
                _ => false,
            };

            let matches_accepted_2 = ENABLE_AD_2
                && !self.has_last_wft
                && matches!(&new_seq, NextWFT::NeedFetch)
                && match (&old_seq, self.events.last()) {
                    (NextWFT::WFT(old_ev, _), Some(buf_last)) => old_ev.last().is_some_and(|e| {
                        e.event_type() == EventType::WorkflowTaskStarted && e == buf_last
                    }),
                    _ => false,
                };

            let matches_accepted_3 =
                ENABLE_AD_3 && shadow_matches_accepted_divergence_3(&old_seq, &new_seq);

            // Downstream effect of AD_1: old algorithm consumed WFTCompleted as part
            // of the previous chunk, so this call sees empty buffer → ReplayOver.
            // New algorithm stopped at WFTStarted, leaving WFTCompleted as tail →
            // returns it as a 1-event WFT with is_final=true.
            let matches_accepted_1_tail = ENABLE_AD_1
                && matches!(&old_seq, NextWFT::ReplayOver)
                && matches!(
                    &new_seq,
                    NextWFT::WFT(ev, true)
                        if !ev.is_empty()
                            && ev[0].event_type() == EventType::WorkflowTaskCompleted
                );

            if !matches_accepted_1
                && !matches_accepted_1_tail
                && !matches_accepted_2
                && !matches_accepted_3
            {
                eprintln!("=== WFT SHADOW DIVERGENCE (take_next_wft_sequence) ===");
                eprintln!(
                    "  from_wft_started_id={from_wft_started_id}, has_last_wft={}, \
                 has_pending_speculative_updates={}",
                    self.has_last_wft, self.has_pending_speculative_updates
                );
                eprint_shadow_next_wft_summary("OLD", &old_seq);
                eprint_shadow_next_wft_summary("NEW", &new_seq);
                eprint_shadow_history_lines("buffer", &self.events);
                eprintln!("=== END WFT SHADOW (take_next_wft_sequence) ===");
            }
        }

        match &new_seq {
            NextWFT::WFT(taken, _) if !taken.is_empty() => {
                self.events.drain(0..taken.len());
            }
            _ => {}
        }

        new_seq
    }

    fn take_next_wft_sequence_old(&mut self, from_wft_started_id: i64) -> NextWFT {
        let old_chunk = find_end_index_of_next_wft_seq_old(
            &self.events,
            from_wft_started_id,
            self.has_last_wft,
            self.has_pending_speculative_updates,
        );

        match old_chunk {
            NextWFTSeqEndIndexOld::Incomplete(_) => {
                let siz = self.events.len().saturating_sub(1);
                if self.has_last_wft {
                    if siz == 0 {
                        NextWFT::ReplayOver
                    } else {
                        self.build_next_wft_no_drain(siz)
                    }
                } else {
                    // Chunking chose Incomplete (uncertain boundary); fetch more before draining.
                    NextWFT::NeedFetch
                }
            }
            NextWFTSeqEndIndexOld::Complete(next_wft_ix) => {
                self.build_next_wft_no_drain(next_wft_ix)
            }
        }
    }

    fn take_next_wft_sequence_new(&mut self, from_wft_started_id: i64) -> NextWFT {
        let new_chunk = find_end_index_of_next_wft_seq_new(
            &self.events,
            from_wft_started_id,
            self.has_last_wft,
            self.has_pending_speculative_updates,
        );

        match new_chunk {
            NextWFTSeqEndIndex::NeedMore => NextWFT::NeedFetch,
            NextWFTSeqEndIndex::Tail => {
                if !self.has_last_wft {
                    // We don't have the full history yet; what looks like tail events may
                    // just be the end of the current page. Fetch more.
                    NextWFT::NeedFetch
                } else if self.events.is_empty() {
                    NextWFT::ReplayOver
                } else {
                    // Remaining events are trailing matter (e.g. terminal events, WFTCompleted
                    // + commands after the last WFTStarted). Include them all so the caller
                    // can process them (e.g. to set have_seen_terminal_event).
                    self.build_next_wft_no_drain(self.events.len() - 1)
                }
            }
            NextWFTSeqEndIndex::Complete(next_wft_ix) => self.build_next_wft_no_drain(next_wft_ix),
        }
    }

    // Do not remove this function.
    // We'll restore it once we remove the new/old divergence checks.
    //
    // fn build_next_wft(&mut self, drain_this_much: usize) -> NextWFT {
    //     NextWFT::WFT(
    //         self.events.drain(0..=drain_this_much).collect(),
    //         self.events.is_empty() && self.has_last_wft,
    //     )
    // }

    fn build_next_wft_no_drain(&mut self, drain_this_much: usize) -> NextWFT {
        let is_final = (drain_this_much + 1 == self.events.len()) && self.has_last_wft;
        NextWFT::WFT(self.events[0..=drain_this_much].to_vec(), is_final)
    }

    /// Lets the caller peek ahead at the next WFT sequence that will be returned by
    /// [take_next_wft_sequence]. Will always return the first available WFT sequence if that has
    /// not been called first. May also return an empty iterator or incomplete sequence if we are at
    /// the end of history.
    pub(crate) fn peek_next_wft_sequence(&self, from_wft_started_id: i64) -> &[HistoryEvent] {
        let old = self.peek_next_wft_sequence_old(from_wft_started_id);
        let new = self.peek_next_wft_sequence_new(from_wft_started_id);

        if old != new {
            let matches_accepted_1 = ENABLE_AD_1
                && old.len() == new.len() + 1
                && old.starts_with(new)
                && matches!(
                    new.last().map(|e| e.event_type()),
                    Some(EventType::WorkflowTaskStarted)
                )
                && matches!(
                    old.get(new.len()).map(|e| e.event_type()),
                    Some(EventType::WorkflowTaskCompleted)
                );

            let matches_accepted_3 = ENABLE_AD_3 && shadow_peek_slices_match_ad3(old, new);

            if !matches_accepted_1 && !matches_accepted_3 {
                eprintln!("=== WFT CHUNKING DIVERGENCE in peek_next_wft_sequence ===");
                eprintln!(
                    "  from_wft_started_id={from_wft_started_id}, has_last_wft={}, \
                 has_pending_speculative_updates={}",
                    self.has_last_wft, self.has_pending_speculative_updates
                );
                eprint_shadow_history_lines("peek OLD slice", old);
                eprint_shadow_history_lines("peek NEW slice", new);
                eprint_shadow_history_lines("full buffer", &self.events);
            }
        }

        new
    }

    fn peek_next_wft_sequence_old(&self, from_wft_started_id: i64) -> &[HistoryEvent] {
        let ix_first_relevant =
            starting_index_after_skipping(&self.events, from_wft_started_id).unwrap_or_default();

        let relevant_events = &self.events[ix_first_relevant..];
        if relevant_events.is_empty() {
            return relevant_events;
        }

        let ix_end = find_end_index_of_next_wft_seq_old(
            relevant_events,
            from_wft_started_id,
            self.has_last_wft,
            self.has_pending_speculative_updates,
        )
        .end_index_in_slice(relevant_events.len());

        &relevant_events[0..=ix_end]
    }

    fn peek_next_wft_sequence_new(&self, from_wft_started_id: i64) -> &[HistoryEvent] {
        let ix_first_relevant =
            starting_index_after_skipping(&self.events, from_wft_started_id).unwrap_or_default();

        let relevant_events = &self.events[ix_first_relevant..];
        if relevant_events.is_empty() {
            return relevant_events;
        }

        let ix_end = find_end_index_of_next_wft_seq_new(
            relevant_events,
            from_wft_started_id,
            self.has_last_wft,
            self.has_pending_speculative_updates,
        )
        .end_index_in_slice(relevant_events.len());

        &relevant_events[0..=ix_end]
    }

    /// Returns true if this update has the next needed WFT sequence, false if events will need to
    /// be fetched in order to create a complete update with the entire next WFT sequence.
    pub(crate) fn can_take_next_wft_sequence(&self, from_wft_started_id: i64) -> bool {
        let new = self.can_take_next_wft_sequence_new(from_wft_started_id);
        let old = self.can_take_next_wft_sequence_old(from_wft_started_id);

        if new.0 != old.0 {
            let has_last_wft = self.has_last_wft;
            let has_pending_speculative_updates = self.has_pending_speculative_updates;

            eprintln!("=== START DIVERGENCE IN can_take_next_wft_sequence ===");
            eprintln!(
                "  from_event_id={from_wft_started_id}, has_last_wft={has_last_wft}, has_pending_speculative_updates={has_pending_speculative_updates}"
            );
            eprintln!("  OLD => {old:?}");
            eprintln!("  NEW => {new:?}");
            eprintln!("  events ({} total):", self.events.len());
            for (ix, e) in self.events.iter().enumerate() {
                eprintln!("    [{ix}] eid={} {:?}", e.event_id, e.event_type());
            }
            eprintln!("=== END DIVERGENCE IN can_take_next_wft_sequence ===");
        }

        new.0
    }

    fn can_take_next_wft_sequence_old(
        &self,
        from_wft_started_id: i64,
    ) -> (bool, NextWFTSeqEndIndexOld) {
        let next_wft_ix = find_end_index_of_next_wft_seq_old(
            &self.events,
            from_wft_started_id,
            self.has_last_wft,
            self.has_pending_speculative_updates,
        );
        if let NextWFTSeqEndIndexOld::Incomplete(_) = next_wft_ix
            && !self.has_last_wft
        {
            return (false, next_wft_ix);
        }
        (true, next_wft_ix)
    }

    fn can_take_next_wft_sequence_new(
        &self,
        from_wft_started_id: i64,
    ) -> (bool, NextWFTSeqEndIndex) {
        let next_wft_ix = find_end_index_of_next_wft_seq_new(
            &self.events,
            from_wft_started_id,
            self.has_last_wft,
            self.has_pending_speculative_updates,
        );
        if matches!(next_wft_ix, NextWFTSeqEndIndex::NeedMore)
            || (matches!(next_wft_ix, NextWFTSeqEndIndex::Tail) && !self.has_last_wft)
        {
            return (false, next_wft_ix);
        }
        (true, next_wft_ix)
    }

    /// Returns the next WFT completed event attributes, if any, starting at (inclusive) the
    /// `from_id`
    pub(crate) fn peek_next_wft_completed(
        &self,
        from_id: i64,
    ) -> Option<&WorkflowTaskCompletedEventAttributes> {
        self.events
            .iter()
            .skip_while(|e| e.event_id < from_id)
            .find_map(|e| match &e.attributes {
                Some(Attributes::WorkflowTaskCompletedEventAttributes(a)) => Some(a),
                _ => None,
            })
    }
}

fn starting_index_after_skipping(
    events: &[HistoryEvent],
    from_wft_started_id: i64,
) -> Option<usize> {
    events
        .iter()
        .find_position(|e| e.event_id > from_wft_started_id)
        .map(|(ix, _)| ix)
}

/// True if some `WorkflowTaskCompleted` is followed later by a terminal `WorkflowExecution*`
/// event (accepted divergence 3, `from_events` shadow).
fn shadow_history_has_wft_completed_before_terminal_execution(events: &[HistoryEvent]) -> bool {
    events.iter().enumerate().any(|(i, e)| {
        e.event_type() == EventType::WorkflowTaskCompleted
            && events
                .get(i + 1..)
                .is_some_and(|tail| tail.iter().any(|e2| e2.is_final_wf_execution_event()))
    })
}

/// Accepted divergence 3 for legacy vs new **slices** (e.g. [`HistoryUpdate::peek_next_wft_sequence`],
/// [`NextWFT::WFT`] sequences in `take_next_wft_sequence`): legacy includes a terminal
/// `WorkflowExecution*` past `WorkflowTaskStarted` where new stops at `WorkflowTaskStarted`, or
/// legacy includes `WFTCompleted` + terminal after that `WorkflowTaskStarted`.
fn shadow_peek_slices_match_ad3(old: &[HistoryEvent], new: &[HistoryEvent]) -> bool {
    if old.len() == new.len() + 1
        && old.starts_with(new)
        && matches!(
            new.last().map(|e| e.event_type()),
            Some(EventType::WorkflowTaskStarted)
        )
        && old.last().is_some_and(|e| e.is_final_wf_execution_event())
    {
        return true;
    }
    old.len() >= new.len().saturating_add(2)
        && old.starts_with(new)
        && matches!(
            new.last().map(|e| e.event_type()),
            Some(EventType::WorkflowTaskStarted)
        )
        && old.last().is_some_and(|e| e.is_final_wf_execution_event())
        && old.get(new.len()..).is_some_and(|suffix| {
            suffix
                .iter()
                .any(|e| e.event_type() == EventType::WorkflowTaskCompleted)
        })
}

/// Accepted divergence 3: legacy `Complete` spanned through a terminal `WorkflowExecution*`
/// where new chunking completes at `WorkflowTaskStarted` and leaves `WFTCompleted` + terminal as
/// a subsequent `Incomplete` boundary (see `Divergences.md`).
///
/// Covers: (a) terminal immediately after `WorkflowTaskStarted`; (b) `WorkflowTaskStarted` ->
/// `WorkflowTaskCompleted` -> terminal execution. Follow-on: when the buffer is only that
/// terminal event, legacy still yields a one-event `WFT`; new reports `Incomplete` for the lone
/// terminal and then `ReplayOver` (or `NeedFetch` when `!has_last_wft`).
fn shadow_matches_accepted_divergence_3(old_seq: &NextWFT, new_seq: &NextWFT) -> bool {
    match (old_seq, new_seq) {
        (NextWFT::WFT(old_ev, _), NextWFT::WFT(new_ev, _)) => {
            shadow_peek_slices_match_ad3(old_ev.as_slice(), new_ev.as_slice())
        }
        (NextWFT::WFT(old_ev, _), NextWFT::ReplayOver | NextWFT::NeedFetch)
            if old_ev.len() == 1 && old_ev[0].is_final_wf_execution_event() =>
        {
            true
        }
        _ => false,
    }
}

/// Shadow-only fingerprint for [`NextWFT`]: `(kind, event_count, first_eid, last_eid, is_final)`.
/// `kind` is 0 = ReplayOver, 1 = NeedFetch, 2 = WFT.
fn take_next_wft_shadow_tuple(w: &NextWFT) -> (u8, usize, i64, i64, bool) {
    match w {
        NextWFT::ReplayOver => (0, 0, 0, 0, false),
        NextWFT::NeedFetch => (1, 0, 0, 0, false),
        NextWFT::WFT(ev, is_final) => {
            if let (Some(first), Some(last)) = (ev.first(), ev.last()) {
                (2, ev.len(), first.event_id, last.event_id, *is_final)
            } else {
                (2, 0, 0, 0, *is_final)
            }
        }
    }
}

fn shadow_tuple_kind_name(kind: u8) -> &'static str {
    match kind {
        0 => "ReplayOver",
        1 => "NeedFetch",
        2 => "WFT",
        _ => "?",
    }
}

fn eprint_shadow_history_lines(label: &str, events: &[HistoryEvent]) {
    eprintln!("  {label} ({} events):", events.len());
    for (ix, e) in events.iter().enumerate() {
        let extra =
            if let Some(Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(ref attr)) =
                e.attributes
            {
                format!(" (seq_eid={})", attr.accepted_request_sequencing_event_id)
            } else {
                String::new()
            };
        eprintln!(
            "    [{ix}] eid={} {:?} {}",
            e.event_id,
            e.event_type(),
            extra
        );
    }
}

fn eprint_shadow_next_wft_summary(label: &str, w: &NextWFT) {
    let t = take_next_wft_shadow_tuple(w);
    eprintln!(
        "  {label}: tuple (kind={} {}), n={}, first_eid={}, last_eid={}, is_final={}",
        t.0,
        shadow_tuple_kind_name(t.0),
        t.1,
        t.2,
        t.3,
        t.4
    );
    if let NextWFT::WFT(ev, _) = w {
        if ev.is_empty() {
            eprintln!("  {label}: WFT event list empty");
        } else {
            eprint_shadow_history_lines(&format!("{label} (taken seq)"), ev);
        }
    }
}

/// Reference return type for [`find_end_index_of_next_wft_seq_old`] (shadow / validation only).
#[derive(Debug, Copy, Clone)]
enum NextWFTSeqEndIndexOld {
    Complete(usize),
    Incomplete(usize),
}

impl NextWFTSeqEndIndexOld {
    /// Last event index within a slice of length `slice_len` that this result refers to.
    fn end_index_in_slice(self, _slice_len: usize) -> usize {
        match self {
            NextWFTSeqEndIndexOld::Complete(ix) => ix,
            NextWFTSeqEndIndexOld::Incomplete(ix) => ix,
        }
    }

    /// Re-base a chunking result from a suffix slice onto absolute indices in the full buffer.
    fn add_base(self, base: usize) -> Self {
        match self {
            NextWFTSeqEndIndexOld::Complete(ix) => NextWFTSeqEndIndexOld::Complete(ix + base),
            NextWFTSeqEndIndexOld::Incomplete(ix) => NextWFTSeqEndIndexOld::Incomplete(ix + base),
        }
    }
}

#[derive(Debug, Copy, Clone)]
enum NextWFTSeqEndIndex {
    /// The next Virtual WFT sequence is completely contained within the passed-in slice.
    /// The index corresponds to the index of the last `WorkflowTaskStarted` event.
    Complete(usize),

    /// Not enough events in the slice to positively determine the next WFT boundary.
    /// The caller should fetch more events before attempting to chunk again.
    NeedMore,

    /// No more WFT boundaries exist in this slice. Any remaining events are trailing matter
    /// after the last WFT (e.g. terminal `WorkflowExecution*` events, `WorkflowTaskCompleted`
    /// with its commands). These events still need to be processed by the caller.
    Tail,
}

impl NextWFTSeqEndIndex {
    /// Last event index within a slice of length `slice_len` that this result refers to.
    fn end_index_in_slice(self, slice_len: usize) -> usize {
        match self {
            NextWFTSeqEndIndex::Complete(ix) => ix,
            NextWFTSeqEndIndex::NeedMore | NextWFTSeqEndIndex::Tail => slice_len.saturating_sub(1),
        }
    }

    fn add(self, val: usize) -> Self {
        match self {
            NextWFTSeqEndIndex::Complete(ix) => NextWFTSeqEndIndex::Complete(ix + val),
            NextWFTSeqEndIndex::NeedMore => NextWFTSeqEndIndex::NeedMore,
            NextWFTSeqEndIndex::Tail => NextWFTSeqEndIndex::Tail,
        }
    }
}

/// Discovers the index of the last event in next WFT sequence within the passed-in slice
/// For more on workflow task chunking, see arch_docs/workflow_task_chunking.md
fn find_end_index_of_next_wft_seq_old(
    events: &[HistoryEvent],
    from_event_id: i64,
    has_last_wft: bool,
    _has_pending_speculative_updates: bool,
) -> NextWFTSeqEndIndexOld {
    if events.is_empty() {
        return NextWFTSeqEndIndexOld::Incomplete(0);
    }
    let mut last_index = 0;
    let mut saw_command_or_started = false;
    let mut saw_command = false;
    let mut wft_started_event_id_to_index = vec![];
    for (ix, e) in events.iter().enumerate() {
        last_index = ix;

        // It's possible to have gotten a new history update without eviction (ex: unhandled
        // command on completion), where we may need to skip events we already handled.
        if e.event_id <= from_event_id {
            continue;
        }

        if e.is_command_event() {
            saw_command = true;
            saw_command_or_started = true;
        }
        if e.event_type() == EventType::WorkflowExecutionStarted {
            saw_command_or_started = true;
        }
        if e.is_final_wf_execution_event() {
            return NextWFTSeqEndIndexOld::Complete(last_index);
        }

        if e.event_type() == EventType::WorkflowTaskStarted {
            wft_started_event_id_to_index.push((e.event_id, ix));
            if let Some(next_event) = events.get(ix + 1) {
                let next_event_type = next_event.event_type();
                // If the next event is WFT timeout or fail, or abrupt WF execution end, that
                // doesn't conclude a WFT sequence.
                if matches!(
                    next_event_type,
                    EventType::WorkflowTaskFailed
                        | EventType::WorkflowTaskTimedOut
                        | EventType::WorkflowExecutionTimedOut
                        | EventType::WorkflowExecutionTerminated
                        | EventType::WorkflowExecutionCanceled
                ) {
                    // Since we're skipping this WFT, we don't want to include it in the vec used
                    // for update accepted sequencing lookups.
                    wft_started_event_id_to_index.pop();
                    continue;
                } else if next_event_type == EventType::WorkflowTaskCompleted {
                    if let Some(next_next_event) = events.get(ix + 2) {
                        if !saw_command
                            && next_next_event.event_type() == EventType::WorkflowTaskScheduled
                        {
                            // If we've never seen an interesting event and the next two events are
                            // a completion followed immediately again by scheduled, then this is a
                            // WFT heartbeat and also doesn't conclude the sequence.
                            continue;
                        } else {
                            // If we see an update accepted command after WFT completed, we want to
                            // conclude the WFT sequence where that update should have been
                            // processed. We don't need to check for any other command types,
                            // because the only thing that can run before an update validator is a
                            // signal handler - but if a signal handler ran then there would have
                            // been a previous signal event, and we would've already concluded the
                            // previous WFT sequence.
                            if let Some(
                                Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(
                                    ref attr,
                                ),
                            ) = next_next_event.attributes
                            {
                                // Find index of closest unskipped WFT started before sequencing id.
                                // The fact that the WFT wasn't skipped is important. If it was, we
                                // need to avoid stopping at that point even though that's where the
                                // update was sequenced. If we did, we'll fail to actually include
                                // the update accepted event and therefore fail to generate the
                                // request to run the update handler on replay.
                                if let Some(ret_ix) = wft_started_event_id_to_index
                                    .iter()
                                    .rev()
                                    .find_map(|(eid, ix)| {
                                        if *eid < attr.accepted_request_sequencing_event_id {
                                            return Some(*ix);
                                        }
                                        None
                                    })
                                {
                                    return NextWFTSeqEndIndexOld::Complete(ret_ix);
                                }
                            }
                            return NextWFTSeqEndIndexOld::Complete(ix);
                        }
                    } else if !has_last_wft && !saw_command_or_started {
                        // Don't have enough events to look ahead of the WorkflowTaskCompleted. Need
                        // to fetch more.
                        continue;
                    }
                }
            } else if !has_last_wft && !saw_command_or_started {
                // Don't have enough events to look ahead of the WorkflowTaskStarted. Need to fetch
                // more.
                continue;
            }
            if saw_command_or_started {
                return NextWFTSeqEndIndexOld::Complete(ix);
            }
        }
    }

    NextWFTSeqEndIndexOld::Incomplete(last_index)
}

/// Return the event _index_ (not ID!) of the last event of the logical workflow task starting
/// at event ID `from_event_id`. The virtual WFT is guaranteed to be "complete", meaning that all
/// events required to process that virtual WFT are contained in the provided slice.
///
/// Returns one of three variants:
///
/// - `Complete(ix)` — the WFT boundary is at the `WorkflowTaskStarted` event at index `ix`.
///   All events required to process the vWFT are present in the slice.
/// - `NeedMore` — not enough events to determine the boundary; the caller should fetch more
///   history pages before retrying. This can happen when the slice ends at a point where
///   look-ahead is required (e.g. `WFTStarted → WFTCompleted → EOS` with `!has_last_wft`).
/// - `Tail` — no more WFT boundaries exist in the remaining events. Any events still in the
///   slice are trailing matter after the last WFT (e.g. terminal `WorkflowExecution*` events,
///   `WorkflowTaskCompleted` + commands). The caller must still process these events (e.g. to
///   set `have_seen_terminal_event`).
///
/// When `has_last_wft` is true, the slice is the full history for this update: a trailing
/// `WorkflowTaskStarted` with no following event (open task) **is** a `Complete` boundary at
/// that started event—there is no further history to page in that could change the decision.
///
/// The index returned by `Complete(x)` always corresponds to the event index of a
/// `WorkflowTaskStarted` event.
///
/// A logical wft may span multiple real wfts in history, in the following cases:
///
/// - Empty Workflow Tasks sequences, like those resulting from WFT heartbeats;
/// - WFT attempts that failed or timed out.
///
/// In both cases, the ignored wft is swallowed by the _preceding_ workflow task,
/// resulting in a single virtual workflow task.
fn find_end_index_of_next_wft_seq_new(
    events: &[HistoryEvent],
    from_event_id: i64,
    has_last_wft: bool,
    has_pending_speculative_updates: bool,
) -> NextWFTSeqEndIndex {
    use EventType::*;
    use NextWFTSeqEndIndex::*;

    if events.is_empty() {
        return if has_last_wft { Tail } else { NeedMore };
    }

    // It's possible to have gotten a new history update without eviction (ex: unhandled
    // command on completion), where we may need to skip events we already handled.
    let mut ix = starting_index_after_skipping(events, from_event_id).unwrap_or(events.len());

    // Set to true if we've seen any event that prevents extending the present vWFT past the next `WFTStarted` event.
    // FIXME: Can we and should we change prevent_heartbeat to include all inbound events (i.e. not only commands)? Is it safe to change?
    let mut prevent_heartbeat = false;

    // Skip the initial `WFExecutionStarted` event, if present.
    //
    // 1. consume `WFExecutionStarted?`
    //
    if let Some(WorkflowExecutionStarted) = events.get(ix).map(|e| e.event_type()) {
        ix += 1;
    }

    // We're at the begining of a vWFT. Any command here results from the _previous_ WFT,
    // and therefore shouldn't affect chunking of the present vWFT, besides
    //
    // 1. consume `(WFTCompleted -> Command*)?`
    // 2. if any command was seen, set `prevent_heartbeat=true`
    //
    if let Some(WorkflowTaskCompleted) = events.get(ix).map(|e| e.event_type()) {
        ix += 1; // WFTCompleted

        while ix < events.len() {
            if !events[ix].is_command_event() {
                break;
            }

            prevent_heartbeat = true;
            ix += 1; // Command
        }
    }

    // From this point on, there should be:
    // `InboundEvent* -> WFTScheduled -> WFTStarted -> WFTCompleted -> Command*`
    //
    // 1. consume `WFTScheduled`
    //
    while ix < events.len() {
        // let ahead = &events[ix + 1..events.len().min(ix + 6)];
        // let ahead: Vec<_> = ahead.iter().map(|e| e.event_type()).collect();

        let e0 = &events[ix];
        let e1 = events.get(ix + 1);
        let e2 = events.get(ix + 2);
        let e3 = events.get(ix + 3);
        let e4 = events.get(ix + 4);
        let e5 = events.get(ix + 5);

        match e0.event_type() {
            // WFTStarted -> ...
            EventType::WorkflowTaskStarted => {
                match e1.map(|e| e.event_type()) {
                    // WFTStarted -> EOH
                    None if has_last_wft => {
                        // History ends on this WFTStarted.
                        // Conclusion is safe and replay is over after this vWFT.
                        return NextWFTSeqEndIndex::Complete(ix);
                    } 
                    
                    // WFTStarted -> (unknown)
                    None /* !has_last_wft */ => {
                        // Can't conclude yet: unknown could be a WFTCompleted, WFTFailed, or WFTTimedOut event.
                        return NextWFTSeqEndIndex::NeedMore;
                    }
 
                    // WFTStarted -> WFTCompleted -> ...
                    Some(EventType::WorkflowTaskCompleted) => {
                        match e2.map(|e| e.event_type()) {
                            // WFTStarted -> WFTCompleted -> EOH
                            None if has_last_wft => { 
                                // There's no more event to look ahead.
                                // It is safe to conclude the vWFT at the current WFTStarted event.
                                return NextWFTSeqEndIndex::Complete(ix);
                            }

                            // WFTStarted -> WFTCompleted -> (unknown)
                            None /* !has_last_wft */ => {
                                // Can't conclcude yet, as unknown could be a WFTScheduled or UpdateAccepted event.
                                // Note that we are not making an exception for prevent_heartbeat=true here,
                                // because we'd still need to if there's an UpdateAccepted event ahead.
                                return NextWFTSeqEndIndex::NeedMore;
                            }

                            // WFTStarted -> WFTCompleted -> WFTScheduled -> ...
                            Some(EventType::WorkflowTaskScheduled) => {
                                if prevent_heartbeat {
                                    // For some reason (e.g. we saw a command preceding this WFTStarted), we know
                                    // that we can't collapse the current WFT with the one ahead, and we've seen
                                    // one event that can't belong to the current WFT (the WFTScheduled), so it
                                    // is safe to conclude a Complete vWFT at the current WFTStarted event.
                                    return NextWFTSeqEndIndex::Complete(ix);
                                }

                                match e3.map(|e| e.event_type()) {
                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> EOH
                                    None if has_last_wft => {
                                            // History ends on this WFTScheduled. That's somewhat unexpected,
                                            // but still means there can't be nothing affecting decision on the
                                            // present vWFT, so it is safe to conclude a Complete vWFT
                                            // at the current WFTStarted event.
                                            return NextWFTSeqEndIndex::Complete(ix);
                                    }
                                        
                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> (unknown)
                                    None /* !has_last_wft */ => {
                                        // There might be more events ahead that would affect the conclusion,
                                        // e.g. a `WFTScheduled -> WFTStarted` sequence that would make this
                                        // a heartbeat. Delay the conclusion until we see more events.
                                        return NextWFTSeqEndIndex::NeedMore;
                                    }

                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> ...
                                    Some(EventType::WorkflowTaskStarted) => {
                                        match e4.map(|e| e.event_type()) {
                                            // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> EOH
                                            None if has_last_wft => {
                                                if has_pending_speculative_updates {
                                                    // There's a pending speculative update, which necessarily affects
                                                    // the last WFTStarted event, which is the one we're looking ahead
                                                    // to. We therefore can't collapse the current WFT (WFTStarted at ix)
                                                    // with the one ahead (WFTStarted at ix + 3).
                                                    return NextWFTSeqEndIndex::Complete(ix);
                                                } else {
                                                    // We got a full noop WFT sequence. Collapse the current WFT
                                                    // (WFTStarted at ix) with the one ahead (WFTStarted at ix + 3),
                                                    // and return that as this is the final event in history.
                                                    return NextWFTSeqEndIndex::Complete(ix + 3);
                                                }
                                            }

                                            // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> (unknown)
                                            None /* !has_last_wft */ => {
                                                // Can't conclude yet: unknown could be a WFTCompleted, WFTFailed, or WFTTimedOut.
                                                return NextWFTSeqEndIndex::NeedMore;
                                            }

                                            // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> ...
                                            Some(EventType::WorkflowTaskCompleted) => {
                                                match e5.map(|e| e.event_type()) {
                                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> EOH
                                                    None if has_last_wft => {
                                                        assert!(!has_pending_speculative_updates);

                                                        // We got a full noop WFT sequence. Collapse the current WFT
                                                        // (WFTStarted at ix) with the one ahead (WFTStarted at ix + 3),
                                                        // and return that as this is the final event in history.
                                                        return NextWFTSeqEndIndex::Complete(ix + 3);
                                                    }

                                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> (unknown)
                                                    None /* !has_last_wft */ => {
                                                        // Can't conclude yet, as unknown could be a WFTStarted, WFTFailed, or WFTTimedOut event.
                                                        return NextWFTSeqEndIndex::NeedMore;
                                                    }

                                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> UpdateAccepted -> ...
                                                    Some(EventType::WorkflowExecutionUpdateAccepted) => {
                                                        // Found an UpdateAccepted event, which must affect the WFTStarted at ix + 3.
                                                        // That means we can't collapse the current WFT (WFTStarted at ix) with the
                                                        // one ahead (WFTStarted at ix + 3). Conclude the current WFTStarted event.
                                                        return NextWFTSeqEndIndex::Complete(ix);
                                                    }
                                                    
                                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> <something else>
                                                    Some(_) => {
                                                        // We found a full noop WFT sequence (ix..ix+3), and we've looked
                                                        // ahead far enough to be sure that we won't need to walk back on
                                                        // previous WFTStarted events. Jump ahead to the next WFTStarted
                                                        // event, and continue the loop.
                                                        ix += 3; // WFTStarted + WFTCompleted + WFTScheduled
                                                        continue;
                                                    }
                                                }

                                            }

                                            // WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> <something else>
                                            Some(_) => {
                                                return NextWFTSeqEndIndex::Complete(ix);
                                            }
                                        }
                                    }

                                    // WFTStarted -> WFTCompleted -> WFTScheduled -> <something else>
                                    Some(_) => {
                                        return NextWFTSeqEndIndex::Complete(ix);
                                    }
                                }
                            }

                            // WFTStarted -> WFTCompleted -> <something else>
                            Some(_) => {
                                return NextWFTSeqEndIndex::Complete(ix);
                            }
                        }
                    }

                    // WFTStarted -> WFT(Failed|TimedOut) -> ...
                    Some(EventType::WorkflowTaskFailed) | Some(EventType::WorkflowTaskTimedOut) => {
                        // Failed WFT. Skip over it.
                        ix += 2; // Started + Failed/TimedOut
                        continue;
                    }

                    // Workflow execution terminates after WFTStarted without WFTCompleted.
                    // Complete points at the WFTStarted; the terminal event is left as
                    // trailing matter (will be returned as `Tail` on the next call).
                    // `WFTStarted -> WFExecution(Terminated|TimedOut|...)`
                    Some(_) if e1.is_some_and(|e| e.is_final_wf_execution_event()) => {
                        return NextWFTSeqEndIndex::Complete(ix);
                    }

                    // `WFTStarted -> <something else>`
                    Some(_) => {
                        panic!(
                            "Unexpected event type: {:?} after WorkflowTaskStarted event, {:?}",
                            e0.event_type(),
                            events
                        );
                    }
                }
            }

            // Sudden workflow execution termination. That's the end of history,
            // but we still don't have a "complete" vWFT. The terminal event is trailing
            // matter that the caller must still process (to set have_seen_terminal_event).
            // `WFExecution(Failed|TimedOut|Canceled|Terminated|TimedOut|CAN)`
            _ if e0.is_final_wf_execution_event() => {
                if e1.is_some() || !has_last_wft || has_pending_speculative_updates {
                    panic!(
                        "{:?} event at index {ix} is not the last event in history",
                        e0.event_type()
                    );
                }
                return Tail;
            }

            // `Command`
            _ if e0.is_command_event() => {
                panic!("Command event at index {ix} is not expected here");
                // Any command at this point ends the vWFT.

                // let (_, latest_wft_started_ix) = wft_started_event_id_to_index
                //     .pop()
                //     .expect(&format!("command events can only appear after a WFT started event (at index {:?}, event type {:?}): {:?}", ix, e0.event_type(), events));

                // return NextWFTSeqEndIndex::Complete(latest_wft_started_ix);
            }

            // Just skip over any other event type.
            _ => {
                ix += 1;
                continue;
            }
        }

        #[allow(unreachable_code)]
        {
            panic!("All match arms above must diverge (return/continue/panic)");
        }
    }

    // Fell off the main loop without finding a WFTStarted. Any events consumed by the
    // preamble (WFTCompleted + commands) or remaining inbound events are trailing matter.
    NextWFTSeqEndIndex::Tail
}

/// Shadow-only: one virtual WFT span in event-id space (inclusive), or an incomplete tail.
#[derive(Debug, Clone)]
enum ShadowWftChunkLine {
    Complete { lo: i64, hi: i64 },
    Incomplete { lo: i64, hi: i64 },
}

fn shadow_eid_range_for_complete_chunk(
    all: &[HistoryEvent],
    from_event_id: i64,
    end_ix: usize,
) -> (i64, i64) {
    let start_ix = starting_index_after_skipping(all, from_event_id).unwrap_or(all.len());
    let end_ix = end_ix.min(all.len().saturating_sub(1));
    let lo = all.get(start_ix).map(|e| e.event_id).unwrap_or(0);
    let hi = all.get(end_ix).map(|e| e.event_id).unwrap_or(lo);
    (lo, hi)
}

/// Events in the buffer after `split_ix` that are past the WFTStarted at `split_ix` but do not yet
/// form a complete virtual WFT (inclusive event-id range).
fn shadow_incomplete_tail_after_split(all: &[HistoryEvent], split_ix: usize) -> Option<(i64, i64)> {
    let suffix = all.get(split_ix..)?;
    let from_id = all.get(split_ix)?.event_id;
    let rel = starting_index_after_skipping(suffix, from_id).unwrap_or(suffix.len());
    if rel >= suffix.len() {
        return None;
    }
    let lo = suffix.get(rel)?.event_id;
    let hi = all.last()?.event_id;
    Some((lo, hi))
}

fn shadow_incomplete_tail_from_buffer_start(
    all: &[HistoryEvent],
    previous_wft_started_id: i64,
) -> Option<(i64, i64)> {
    let start_ix = starting_index_after_skipping(all, previous_wft_started_id).unwrap_or(all.len());
    if start_ix >= all.len() {
        return None;
    }
    let lo = all[start_ix].event_id;
    let hi = all.last()?.event_id;
    Some((lo, hi))
}

fn shadow_collect_from_events_chunks_old(
    all: &[HistoryEvent],
    previous_wft_started_id: i64,
    has_last_wft: bool,
    has_pending_speculative_updates: bool,
) -> Vec<ShadowWftChunkLine> {
    let mut out = Vec::new();
    let mut last_end = find_end_index_of_next_wft_seq_old(
        all,
        previous_wft_started_id,
        has_last_wft,
        has_pending_speculative_updates,
    );
    if matches!(last_end, NextWFTSeqEndIndexOld::Incomplete(_)) {
        if let Some((lo, hi)) =
            shadow_incomplete_tail_from_buffer_start(all, previous_wft_started_id)
        {
            out.push(ShadowWftChunkLine::Incomplete { lo, hi });
        }
        return out;
    }
    let mut from_id = previous_wft_started_id;
    while let NextWFTSeqEndIndexOld::Complete(end_ix) = last_end {
        let (lo, hi) = shadow_eid_range_for_complete_chunk(all, from_id, end_ix);
        out.push(ShadowWftChunkLine::Complete { lo, hi });
        let next_end = find_end_index_of_next_wft_seq_old(
            &all[end_ix..],
            all[end_ix].event_id,
            has_last_wft,
            has_pending_speculative_updates,
        )
        .add_base(end_ix);
        from_id = all[end_ix].event_id;
        if matches!(next_end, NextWFTSeqEndIndexOld::Incomplete(_)) {
            if let Some((lo, hi)) = shadow_incomplete_tail_after_split(all, end_ix) {
                out.push(ShadowWftChunkLine::Incomplete { lo, hi });
            }
            break;
        }
        last_end = next_end;
    }
    out
}

fn shadow_collect_from_events_chunks_new(
    all: &[HistoryEvent],
    previous_wft_started_id: i64,
    has_last_wft: bool,
    has_pending_speculative_updates: bool,
) -> Vec<ShadowWftChunkLine> {
    let mut out = Vec::new();
    let mut last_end = find_end_index_of_next_wft_seq_new(
        all,
        previous_wft_started_id,
        has_last_wft,
        has_pending_speculative_updates,
    );
    if matches!(
        last_end,
        NextWFTSeqEndIndex::NeedMore | NextWFTSeqEndIndex::Tail
    ) {
        if let Some((lo, hi)) =
            shadow_incomplete_tail_from_buffer_start(all, previous_wft_started_id)
        {
            out.push(ShadowWftChunkLine::Incomplete { lo, hi });
        }
        return out;
    }
    let mut from_id = previous_wft_started_id;
    while let NextWFTSeqEndIndex::Complete(end_ix) = last_end {
        let (lo, hi) = shadow_eid_range_for_complete_chunk(all, from_id, end_ix);
        out.push(ShadowWftChunkLine::Complete { lo, hi });
        let next_end = find_end_index_of_next_wft_seq_new(
            &all[end_ix..],
            all[end_ix].event_id,
            has_last_wft,
            has_pending_speculative_updates,
        )
        .add(end_ix);
        from_id = all[end_ix].event_id;
        if matches!(
            next_end,
            NextWFTSeqEndIndex::NeedMore | NextWFTSeqEndIndex::Tail
        ) {
            if let Some((lo, hi)) = shadow_incomplete_tail_after_split(all, end_ix) {
                out.push(ShadowWftChunkLine::Incomplete { lo, hi });
            }
            break;
        }
        last_end = next_end;
    }
    out
}

fn eprint_shadow_events_with_watermark(all: &[HistoryEvent], previous_wft_started_id: i64) {
    eprintln!("  EVENTS ({} events):", all.len());
    let last_prefix_ix = all
        .iter()
        .enumerate()
        .filter(|(_, e)| e.event_id <= previous_wft_started_id)
        .map(|(ix, _)| ix)
        .last();
    for (ix, e) in all.iter().enumerate() {
        eprintln!("    [{ix}] eid={} {:?}", e.event_id, e.event_type());
        if Some(ix) == last_prefix_ix {
            eprintln!("    ------------------------");
        }
    }
}

fn eprint_shadow_from_events_chunk_lines(label: &str, chunks: &[ShadowWftChunkLine]) {
    eprintln!("  {label} chunks:");
    if chunks.is_empty() {
        eprintln!("    (none)");
        return;
    }
    for c in chunks {
        match c {
            ShadowWftChunkLine::Complete { lo, hi } => {
                eprintln!("    Complete => eid=[{lo}..{hi}]");
            }
            ShadowWftChunkLine::Incomplete { lo, hi } => {
                eprintln!("    Incomplete => eid=[{lo}..{hi}]");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        replay::{HistoryInfo, TestHistoryBuilder},
        test_help::{ResponseType, hist_to_poll_resp},
        worker::client::mocks::mock_worker_client,
    };
    use futures_util::TryStreamExt;
    use temporalio_common::protos::{
        canned_histories,
        temporal::api::{
            common::v1::WorkflowExecution,
            enums::v1::WorkflowTaskFailedCause,
            history::v1::{History, history_event::Attributes},
            workflowservice::v1::{
                GetWorkflowExecutionHistoryResponse, update_activity_options_request,
            },
        },
    };

    impl From<HistoryInfo> for HistoryUpdate {
        fn from(v: HistoryInfo) -> Self {
            Self::new_from_events(
                v.events().to_vec(),
                v.previous_started_event_id(),
                v.workflow_task_started_event_id(),
                true,
                false,
            )
        }
    }

    trait TestHBExt {
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
                NextWFT::WFT(e, _) => e,
                o => panic!("Must be complete WFT: {o:?}"),
            }
        }

        fn is_complete(&self) -> bool {
            match self {
                NextWFT::WFT(_, true) => true,
                _ => false,
            }
        }
    }

    fn next_check_peek(update: &mut HistoryUpdate, from_id: i64) -> Vec<HistoryEvent> {
        let seq_peeked = update.peek_next_wft_sequence(from_id).to_vec();
        let seq = update.take_next_wft_sequence(from_id).unwrap_events();
        assert_eq!(seq, seq_peeked);
        seq
    }

    fn next_check_peek2(update: &mut HistoryUpdate, from_id: i64) -> (usize, bool) {
        let seq_peek = update.peek_next_wft_sequence(from_id).to_vec();
        let next = update.take_next_wft_sequence(from_id);
        let is_complete = next.is_complete();
        let seq_take = next.unwrap_events();
        assert_eq!(seq_take, seq_peek);
        (seq_take.len(), is_complete)
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
        // Accepted divergence 3: terminal event is no longer part of the WFTStarted vWFT.
        assert_eq!(seq_2.len(), 5);
        assert_eq!(seq_2.last().unwrap().event_id, 8);
        // The terminal event is returned as trailing matter so callers can process it
        // (e.g. to set have_seen_terminal_event).
        let seq_3 = update.take_next_wft_sequence(8).unwrap_events();
        assert_eq!(seq_3.len(), 1);
        assert!(seq_3[0].is_final_wf_execution_event());
        // Now replay is truly over.
        let seq_4 = update.take_next_wft_sequence(8);
        assert_matches!(seq_4, NextWFT::ReplayOver);
    }

    /// Verifies that non-command terminal events (`WorkflowExecutionTerminated`,
    /// `WorkflowExecutionTimedOut`) following a `WorkflowTaskStarted` are returned as
    /// trailing tail events rather than being silently dropped. This is critical because
    /// callers need to process them to set `have_seen_terminal_event`.
    #[test]
    fn terminal_events_not_dropped_after_wft_started() {
        // Test both non-command terminal event types that can follow WFTStarted.
        for add_terminal in [
            TestHistoryBuilder::add_workflow_execution_terminated as fn(&mut TestHistoryBuilder),
            TestHistoryBuilder::add_workflow_execution_timed_out,
        ] {
            let mut t = TestHistoryBuilder::default();
            t.add_by_type(EventType::WorkflowExecutionStarted);
            t.add_full_wf_task(); // Sched(2), Started(3), Completed(4)
            t.add_by_type(EventType::TimerStarted); // TimerStarted(5)
            t.add_workflow_task_scheduled_and_started(); // Sched(6), Started(7)
            add_terminal(&mut t); // terminal(8)

            let mut update = t.as_history_update();
            // First vWFT: events 1-3
            let seq_1 = update.take_next_wft_sequence(0).unwrap_events();
            assert_eq!(seq_1.last().unwrap().event_id, 3);

            // Second vWFT ends at WFTStarted(7); terminal is NOT included.
            let seq_2 = update.take_next_wft_sequence(3).unwrap_events();
            assert_eq!(seq_2.last().unwrap().event_id, 7);
            assert_eq!(
                seq_2.last().unwrap().event_type(),
                EventType::WorkflowTaskStarted
            );

            // Terminal event must be returned as tail, not dropped.
            let seq_3 = update.take_next_wft_sequence(7).unwrap_events();
            assert_eq!(seq_3.len(), 1, "terminal event must not be dropped");
            assert!(
                seq_3[0].is_final_wf_execution_event(),
                "tail event must be the terminal event, got {:?}",
                seq_3[0].event_type()
            );

            // Now replay is over.
            assert_matches!(update.take_next_wft_sequence(7), NextWFT::ReplayOver);
        }
    }

    #[test]
    fn heartbeats_skipped() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task(); // wft started 6
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task(); // wft started 10
        t.add_full_wf_task();
        t.add_full_wf_task();
        t.add_full_wf_task(); // wft started 19
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task(); // wft started 23
        t.add_we_signaled("whee", vec![]);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let mut update = t.as_history_update();
        // First vWFT: WFExStarted through WFTStarted(6). The two initial empty WFTs
        // (started at 3 and 6) are folded as heartbeats because no commands appear between
        // them. Both old and new chunking agree on this (heartbeat check uses `saw_command`,
        // not `saw_command_or_started`).
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.len(), 6);
        let seq = next_check_peek(&mut update, 6);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 10);
        assert_eq!(seq.len(), 9);
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
        let hinfo = history.get_full_history_info().unwrap();
        let wft_started = hinfo.workflow_task_started_event_id();
        let full_hist = hinfo.into_events();
        let initial_hist = full_hist.chunks(chunk_size).next().unwrap().to_vec();
        let mut mock_client = mock_worker_client();

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
            0,
            wft_started,
            "wfid".to_string(),
            "runid".to_string(),
            vec![1],
            Arc::new(mock_client),
            false,
        )
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn paginator_extracts_updates(#[values(10, 11, 12, 13, 14)] chunk_size: usize) {
        let wft_count = 100;
        let hist = canned_histories::long_sequential_timers(wft_count);
        let expected_final_eid = hist
            .get_full_history_info()
            .unwrap()
            .into_events()
            .last()
            .unwrap()
            .event_id;

        let mut paginator = paginator_setup(hist, chunk_size);
        let mut update = paginator.extract_next_update().await.unwrap();
        let mut last_id = 0;
        loop {
            let seq = loop {
                match update.take_next_wft_sequence(last_id) {
                    NextWFT::WFT(seq, _) => break seq,
                    NextWFT::NeedFetch => {
                        update = paginator.extract_next_update().await.unwrap();
                    }
                    NextWFT::ReplayOver => {
                        assert_eq!(last_id, expected_final_eid);
                        return;
                    }
                }
            };
            assert!(!seq.is_empty());
            for e in &seq {
                assert!(
                    e.event_id > last_id,
                    "event ids must increase monotonically (last_id={last_id}, got {})",
                    e.event_id
                );
                last_id = e.event_id;
            }
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
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task(); // wft start - 7
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task(); // wft start - 11
        for _ in 1..50 {
            // Add a bunch of heartbeats with no commands, which count as one task
            t.add_full_wf_task();
        }
        t.add_workflow_execution_completed();
        t
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn needs_fetch_if_ending_in_middle_of_wft_seq(
        // These values test points truncation could've occurred in the middle of the heartbeat
        #[values(18, 19, 20, 21)] truncate_at: usize,
    ) {
        let t = three_wfts_then_heartbeats();
        let mut ends_in_middle_of_seq = t.as_history_update().events;
        ends_in_middle_of_seq.truncate(truncate_at);
        // The update should contain the first three complete WFTs, ending on the 11th event which
        // is WFT started. The remaining events should be returned. False flags means the creator
        // knows there are more events, so we should return need fetch
        let (mut update, remaining) = HistoryUpdate::from_events(
            ends_in_middle_of_seq,
            0,
            t.get_full_history_info()
                .unwrap()
                .workflow_task_started_event_id(),
            false,
            false,
        );
        assert_eq!(remaining[0].event_id, 12);
        assert_eq!(remaining.last().unwrap().event_id, truncate_at as i64);
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 7);
        // Third virtual WFT ends at `WorkflowTaskStarted` (id 11) in this history shape, but the
        // buffer has no following event — `find_end` returns Incomplete until more history exists.
        let next = update.take_next_wft_sequence(7);
        assert_matches!(next, NextWFT::NeedFetch);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn paginator_works_with_wft_over_multiple_pages(
        #[values(10, 11, 12, 13, 14)] chunk_size: usize,
    ) {
        let t = three_wfts_then_heartbeats();
        let mut paginator = paginator_setup(t, chunk_size);
        let mut update = paginator.extract_next_update().await.unwrap();
        let mut last_id = 0;
        loop {
            let seq = update.take_next_wft_sequence(last_id);
            match seq {
                NextWFT::WFT(seq, _) => {
                    last_id = seq.last().unwrap().event_id;
                }
                NextWFT::NeedFetch => {
                    update = paginator.extract_next_update().await.unwrap();
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
        assert_eq!(seq.last().unwrap().event_id, 11);
        let seq = update.take_next_wft_sequence(11).unwrap_events();
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
        let prev_started_wft_id = partial_task.previous_started_event_id();
        let wft_started_id = partial_task.workflow_task_started_event_id();
        let mut history_from_get: GetWorkflowExecutionHistoryResponse =
            timer_hist.get_history_info(2).unwrap().into();
        // Chop off the last event, which is WFT started, which server doesn't return in get
        // history
        history_from_get.history.as_mut().map(|h| h.events.pop());
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(history_from_get.clone()));

        let mut paginator = HistoryPaginator::new(
            partial_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            // A cache miss means we'll try to fetch from start
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
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
    }

    #[tokio::test]
    async fn handles_blank_fetch_response() {
        let timer_hist = canned_histories::single_timer("t");
        let partial_task = timer_hist.get_one_wft(2).unwrap();
        let prev_started_wft_id = partial_task.previous_started_event_id();
        let wft_started_id = partial_task.workflow_task_started_event_id();
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(Default::default()));

        let mut paginator = HistoryPaginator::new(
            partial_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            // A cache miss means we'll try to fetch from start
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
            false,
        );
        let err = paginator.extract_next_update().await.unwrap_err();
        assert_matches!(err.code(), tonic::Code::Unknown);
    }

    #[tokio::test]
    async fn handles_empty_page_with_next_token() {
        let timer_hist = canned_histories::single_timer("t");
        let partial_task = timer_hist.get_one_wft(2).unwrap();
        let prev_started_wft_id = partial_task.previous_started_event_id();
        let wft_started_id = partial_task.workflow_task_started_event_id();
        let full_resp: GetWorkflowExecutionHistoryResponse =
            timer_hist.get_full_history_info().unwrap().into();
        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History { events: vec![] }),
                    raw_history: vec![],
                    next_page_token: vec![2],
                    archived: false,
                })
            })
            .times(1);
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(full_resp.clone()))
            .times(1);

        let mut paginator = HistoryPaginator::new(
            partial_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            // A cache miss means we'll try to fetch from start
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 8);
        assert_matches!(update.take_next_wft_sequence(8), NextWFT::ReplayOver);
    }

    // TODO: Test we dont re-feed pointless updates if fetching returns <= events we already
    //   processed

    #[tokio::test]
    async fn handles_fetching_page_with_complete_wft_and_page_token_to_empty_page() {
        let timer_hist = canned_histories::single_timer("t");
        let workflow_task = timer_hist.get_full_history_info().unwrap();
        let prev_started_wft_id = workflow_task.previous_started_event_id();
        let wft_started_id = workflow_task.workflow_task_started_event_id();

        let mut full_resp_with_npt: GetWorkflowExecutionHistoryResponse =
            timer_hist.get_full_history_info().unwrap().into();
        full_resp_with_npt.next_page_token = vec![1];

        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(full_resp_with_npt.clone()))
            .times(1);
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History { events: vec![] }),
                    raw_history: vec![],
                    next_page_token: vec![],
                    archived: false,
                })
            })
            .times(1);

        let mut paginator = HistoryPaginator::new(
            workflow_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 8);
        assert_matches!(update.take_next_wft_sequence(8), NextWFT::ReplayOver);
    }

    #[tokio::test]
    async fn extreme_pagination_doesnt_drop_wft_events_paginator() {
        // 1: EVENT_TYPE_WORKFLOW_EXECUTION_STARTED
        // 2: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
        // 3: EVENT_TYPE_WORKFLOW_TASK_STARTED // <- previous_started_event_id
        // 4: EVENT_TYPE_WORKFLOW_TASK_COMPLETED

        // 5: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 6: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
        // 7: EVENT_TYPE_WORKFLOW_TASK_STARTED
        // 8: EVENT_TYPE_WORKFLOW_TASK_FAILED

        // 9: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 10: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 11: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 12: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 13: EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED
        // 14: EVENT_TYPE_WORKFLOW_TASK_SCHEDULED
        // 15: EVENT_TYPE_WORKFLOW_TASK_STARTED // <- started_event_id

        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();

        t.add_we_signaled("hi", vec![]);
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_failed_with_failure(
            WorkflowTaskFailedCause::UnhandledCommand,
            Default::default(),
        );

        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_we_signaled("hi", vec![]);
        t.add_workflow_task_scheduled_and_started();

        let mut mock_client = mock_worker_client();

        let events: Vec<HistoryEvent> = t.get_full_history_info().unwrap().into_events();
        let first_event = events[0].clone();
        for (i, event) in events.into_iter().enumerate() {
            // Add an empty page
            mock_client
                .expect_get_workflow_execution_history()
                .returning(move |_, _, _| {
                    Ok(GetWorkflowExecutionHistoryResponse {
                        history: Some(History { events: vec![] }),
                        raw_history: vec![],
                        next_page_token: vec![(i * 10) as u8],
                        archived: false,
                    })
                })
                .times(1);

            // Add a page with only event i
            mock_client
                .expect_get_workflow_execution_history()
                .returning(move |_, _, _| {
                    Ok(GetWorkflowExecutionHistoryResponse {
                        history: Some(History {
                            events: vec![event.clone()],
                        }),
                        raw_history: vec![],
                        next_page_token: vec![(i * 10 + 1) as u8],
                        archived: false,
                    })
                })
                .times(1);
        }

        // Add an extra empty page at the end, with no NPT
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| {
                Ok(GetWorkflowExecutionHistoryResponse {
                    history: Some(History { events: vec![] }),
                    raw_history: vec![],
                    next_page_token: vec![],
                    archived: false,
                })
            })
            .times(1);

        let mut paginator = HistoryPaginator::new(
            History {
                events: vec![first_event],
            },
            3,
            15,
            "wfid".to_string(),
            "runid".to_string(),
            vec![1],
            Arc::new(mock_client),
            false,
        );

        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.first().unwrap().event_id, 1);
        assert_eq!(seq.last().unwrap().event_id, 3);

        let seq = update.take_next_wft_sequence(3).unwrap_events();
        assert_eq!(seq.first().unwrap().event_id, 4);
        assert_eq!(seq.last().unwrap().event_id, 15);
    }

    #[tokio::test]
    async fn finding_end_index_with_started_as_last_event() {
        let wf_id = "fakeid";
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();

        t.add_we_signaled("hi", vec![]);
        t.add_workflow_task_scheduled_and_started();
        // We need to see more after this - it's not sufficient to end on a started event when
        // we know there might be more

        let workflow_task = t.get_history_info(1).unwrap();
        let prev_started_wft_id = workflow_task.previous_started_event_id();
        let wft_started_id = workflow_task.workflow_task_started_event_id();
        let mut wft_resp = workflow_task.as_poll_wft_response();
        wft_resp.workflow_execution = Some(WorkflowExecution {
            workflow_id: wf_id.to_string(),
            run_id: t.get_orig_run_id().to_string(),
        });
        wft_resp.next_page_token = vec![1];

        let mut resp_1: GetWorkflowExecutionHistoryResponse =
            t.get_full_history_info().unwrap().into();
        resp_1.next_page_token = vec![2];

        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(resp_1.clone()))
            .times(1);
        // Since there aren't sufficient events, we should try to see another fetch, and that'll
        // say there aren't any
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(Default::default()))
            .times(1);

        let mut paginator = HistoryPaginator::new(
            workflow_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = update.take_next_wft_sequence(0).unwrap_events();
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = update.take_next_wft_sequence(3).unwrap_events();
        // We're done since the last fetch revealed nothing
        assert_eq!(seq.last().unwrap().event_id, 7);
    }

    #[tokio::test]
    async fn just_signal_is_complete_wft() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_full_wf_task();
        t.add_workflow_execution_completed();

        let workflow_task = t.get_full_history_info().unwrap();
        let prev_started_wft_id = workflow_task.previous_started_event_id();
        let wft_started_id = workflow_task.workflow_task_started_event_id();
        let mock_client = mock_worker_client();
        let mut paginator = HistoryPaginator::new(
            workflow_task.into(),
            prev_started_wft_id,
            wft_started_id,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::Done,
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 7);
        assert_eq!(seq.len(), 4);
        let seq = next_check_peek(&mut update, 11);
        assert_eq!(seq.len(), 2);
    }

    #[tokio::test]
    async fn heartbeats_then_signal() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        let mut need_fetch_resp =
            hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::AllHistory).resp;
        need_fetch_resp.next_page_token = vec![1];
        t.add_full_wf_task();
        t.add_we_signaled("whatever", vec![]);
        t.add_workflow_task_scheduled_and_started();

        let full_resp: GetWorkflowExecutionHistoryResponse =
            t.get_full_history_info().unwrap().into();

        let mut mock_client = mock_worker_client();
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(full_resp.clone()))
            .times(1);

        let mut paginator = HistoryPaginator::new(
            need_fetch_resp.history.unwrap(),
            // Pretend we have already processed first WFT
            3,
            6,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::Next(vec![1]),
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        // Starting past first wft
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 6);
        let seq = next_check_peek(&mut update, 9);
        assert_eq!(seq.len(), 4);
    }

    #[tokio::test]
    async fn cache_miss_with_only_one_wft_available_orders_properly() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_by_type(EventType::TimerStarted);
        t.add_full_wf_task();
        t.add_by_type(EventType::TimerStarted);
        t.add_workflow_task_scheduled_and_started();

        let incremental_task =
            hist_to_poll_resp(&t, "wfid".to_owned(), ResponseType::OneTask(3)).resp;

        let mut mock_client = mock_worker_client();
        let mut one_task_resp: GetWorkflowExecutionHistoryResponse =
            t.get_history_info(1).unwrap().into();
        one_task_resp.next_page_token = vec![1];
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(one_task_resp.clone()))
            .times(1);
        let mut up_to_sched_start: GetWorkflowExecutionHistoryResponse =
            t.get_full_history_info().unwrap().into();
        up_to_sched_start
            .history
            .as_mut()
            .unwrap()
            .events
            .truncate(9);
        mock_client
            .expect_get_workflow_execution_history()
            .returning(move |_, _, _| Ok(up_to_sched_start.clone()))
            .times(1);

        let mut paginator = HistoryPaginator::new(
            incremental_task.history.unwrap(),
            6,
            9,
            "wfid".to_string(),
            "runid".to_string(),
            NextPageToken::FetchFromStart,
            Arc::new(mock_client),
            false,
        );
        let mut update = paginator.extract_next_update().await.unwrap();
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.last().unwrap().event_id, 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.last().unwrap().event_id, 7);
        let seq = next_check_peek(&mut update, 7);
        assert_eq!(seq.last().unwrap().event_id, 11);
    }

    #[tokio::test]
    async fn wft_fail_on_first_task_with_update() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_workflow_task_scheduled_and_started();
        t.add_workflow_task_failed_with_failure(
            WorkflowTaskFailedCause::Unspecified,
            Default::default(),
        );
        t.add_full_wf_task();
        let accept_id = t.add_update_accepted("1", "upd");
        let timer_id = t.add_timer_started("1".to_string());
        t.add_update_completed(accept_id);
        t.add_timer_fired(timer_id, "1".to_string());
        t.add_full_wf_task();

        let mut update = t.as_history_update();
        let seq = next_check_peek(&mut update, 0);
        // In this case, we expect to see up to the task with update, since the task failure
        // should be skipped. This means that the peek of the _next_ task will include the update
        // and thus properly synthesize the update request with the first activation.
        assert_eq!(seq.len(), 6);
        let seq = next_check_peek(&mut update, 6);
        assert_eq!(seq.len(), 7);
    }

    #[test]
    fn update_accepted_after_empty_wft() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task();
        let accept_id = t.add_update_accepted("1", "upd");
        let timer_id = t.add_timer_started("1".to_string());
        t.add_update_completed(accept_id);
        t.add_timer_fired(timer_id, "1".to_string());
        t.add_full_wf_task();

        let mut update = t.as_history_update();
        let seq = next_check_peek(&mut update, 0);
        // unlike the case with a wft failure, here the first task should not extend through to
        // the update, because here the first empty WFT happened with _just_ the workflow init,
        // not also with the update.
        assert_eq!(seq.len(), 3);
        let seq = next_check_peek(&mut update, 3);
        assert_eq!(seq.len(), 3);
        //         // Heartbeat: first empty WFT collapses into the second; boundary is the second WFTStarted.
        // assert_eq!(seq.len(), 6);
        // assert_eq!(seq.last().unwrap().event_id, 6);
        // let seq = next_check_peek(&mut update, 6);
        // // Through timer command, next WFTStarted (open until following completion is visible as end index).
        // assert_eq!(seq.len(), 7);
        // assert_eq!(seq.last().unwrap().event_id, 13);
    }

    // /// Issue 1146 p
    // /// : first poll ends after the first empty WFT's
    // /// `WorkflowTaskCompleted` (event 4). The server may record `WorkflowExecutionUpdateAccepted`
    // /// next (`add_update_accepted` sets `accepted_request_sequencing_event_id` to the preceding
    // /// `WorkflowTaskScheduled`, here event 2). Chunking must not close the virtual WFT at
    // /// `WorkflowTaskStarted` without fetching — that would miss the update on replay.
    // #[test]
    // fn pagination_break_before_update_accepted_after_empty_first_wft_needs_fetch() {
    //     let mut t = TestHistoryBuilder::default();
    //     t.add_by_type(EventType::WorkflowExecutionStarted);
    //     t.add_full_wf_task();
    //     let partial: Vec<_> = t.get_full_history_info().unwrap().into_events();
    //     assert_eq!(partial.len(), 4);

    //     let mut t_full = TestHistoryBuilder::from_history(partial.clone());
    //     t_full.add_update_accepted("1", "upd");
    //     let full_events = t_full.get_full_history_info().unwrap().into_events();
    //     let accept = &full_events[4];
    //     assert_eq!(
    //         accept.event_type(),
    //         EventType::WorkflowExecutionUpdateAccepted
    //     );
    //     let Some(Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(attr)) =
    //         accept.attributes.as_ref()
    //     else {
    //         panic!("expected UpdateAccepted attributes");
    //     };
    //     assert_eq!(
    //         attr.accepted_request_sequencing_event_id, 2,
    //         "sequencing id targets the first WFT's scheduled event"
    //     );

    //     let partial_hi = HistoryInfo::new_from_history(
    //         &History {
    //             events: partial.clone(),
    //         },
    //         None,
    //     )
    //     .expect("partial history is valid");
    //     let (update, remaining) = HistoryUpdate::from_events(
    //         partial,
    //         0,
    //         partial_hi.workflow_task_started_event_id(),
    //         false,
    //         false,
    //     );
    //     assert!(
    //         update.events.is_empty(),
    //         "must not consume page: Incomplete until we can see past WFT completed"
    //     );
    //     assert_eq!(remaining.len(), 4);
    //     assert_eq!(remaining.last().unwrap().event_id, 4);
    // }

    // /// Scenario 2: page ends after the second `WorkflowTaskStarted` (id 7); next page may add
    // /// `WorkflowExecutionUpdateAccepted` (sequencing id 6).
    // #[test]
    // fn pagination_break_after_second_wft_started_before_update_accepted() {
    //     let mut t = TestHistoryBuilder::default();
    //     t.add_by_type(EventType::WorkflowExecutionStarted);
    //     t.add_full_wf_task();
    //     t.add_by_type(EventType::TimerStarted);
    //     t.add_workflow_task_scheduled_and_started();

    //     let partial = t.get_full_history_info().unwrap().into_events();
    //     assert_eq!(partial.len(), 7);
    //     assert_eq!(
    //         partial.last().unwrap().event_type(),
    //         EventType::WorkflowTaskStarted
    //     );
    //     assert_eq!(partial.last().unwrap().event_id, 7);

    //     let mut t_full = TestHistoryBuilder::from_history(partial.clone());
    //     t_full.add_workflow_task_completed();
    //     let _accept_id = t_full.add_update_accepted("1", "upd");
    //     let full_events = t_full.get_full_history_info().unwrap().into_events();
    //     let accept = full_events
    //         .iter()
    //         .find(|e| e.event_type() == EventType::WorkflowExecutionUpdateAccepted)
    //         .expect("UpdateAccepted in full history");
    //     let Some(Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(attr)) =
    //         accept.attributes.as_ref()
    //     else {
    //         panic!("expected UpdateAccepted attributes");
    //     };
    //     assert_eq!(
    //         attr.accepted_request_sequencing_event_id, 6,
    //         "sequencing id targets the second WFT scheduled event"
    //     );

    //     let partial_hi = HistoryInfo::new_from_history(
    //         &History {
    //             events: partial.clone(),
    //         },
    //         None,
    //     )
    //     .expect("partial history is valid");
    //     let (update, remaining) = HistoryUpdate::from_events(
    //         partial,
    //         0,
    //         partial_hi.workflow_task_started_event_id(),
    //         false,
    //         false,
    //     );
    //     assert_eq!(update.wft_count, 1);
    //     assert_eq!(update.events.len(), 3);
    //     assert_eq!(remaining.len(), 4);
    //     assert_eq!(remaining.last().unwrap().event_id, 7);
    // }

    /// Scenario 3: same as scenario 2 but the page includes the second `WorkflowTaskCompleted`
    /// (id 8); `e2` is still missing so chunking matches scenario 2.
    // #[test]
    // fn pagination_break_after_second_wft_completed_before_update_accepted() {
    //     let mut t = TestHistoryBuilder::default();
    //     t.add_by_type(EventType::WorkflowExecutionStarted);
    //     t.add_full_wf_task();
    //     t.add_by_type(EventType::TimerStarted);
    //     t.add_workflow_task_scheduled_and_started();
    //     t.add_workflow_task_completed();

    //     let partial = t.get_full_history_info().unwrap().into_events();
    //     assert_eq!(partial.len(), 8);
    //     assert_eq!(
    //         partial.last().unwrap().event_type(),
    //         EventType::WorkflowTaskCompleted
    //     );
    //     assert_eq!(partial.last().unwrap().event_id, 8);

    //     let mut t_full = TestHistoryBuilder::from_history(partial.clone());
    //     let _accept_id = t_full.add_update_accepted("1", "upd");
    //     let full_events = t_full.get_full_history_info().unwrap().into_events();
    //     let accept = full_events
    //         .iter()
    //         .find(|e| e.event_type() == EventType::WorkflowExecutionUpdateAccepted)
    //         .expect("UpdateAccepted in full history");
    //     let Some(Attributes::WorkflowExecutionUpdateAcceptedEventAttributes(attr)) =
    //         accept.attributes.as_ref()
    //     else {
    //         panic!("expected UpdateAccepted attributes");
    //     };
    //     assert_eq!(attr.accepted_request_sequencing_event_id, 6);

    //     let partial_hi = HistoryInfo::new_from_history(
    //         &History {
    //             events: partial.clone(),
    //         },
    //         None,
    //     )
    //     .expect("partial history is valid");
    //     let (update, remaining) = HistoryUpdate::from_events(
    //         partial,
    //         0,
    //         partial_hi.workflow_task_started_event_id(),
    //         false,
    //         false,
    //     );
    //     assert_eq!(update.wft_count, 1);
    //     assert_eq!(update.events.len(), 3);
    //     assert_eq!(remaining.len(), 5);
    //     assert_eq!(remaining.last().unwrap().event_id, 8);
    // }

    /// Same scenario as [`pagination_break_before_update_accepted_after_empty_first_wft_needs_fetch`],
    /// but routed through [`HistoryPaginator::extract_next_update`] with a 4-event first page.
    ///
    /// Documents that the paginator normally buffers/fetches until `wft_count >= 2` (see doc on
    /// `extract_next_update`), which masks incomplete lookahead in `find_end_index_of_next_wft_seq`
    /// for the live worker path — while the unit test above still guards direct `from_events` and
    /// keeps chunking semantics explicit.
    // #[tokio::test]
    // async fn paginator_fetches_past_page_break_before_update_accepted_after_empty_first_wft() {
    //     let mut t = TestHistoryBuilder::default();
    //     t.add_by_type(EventType::WorkflowExecutionStarted);
    //     t.add_full_wf_task();
    //     t.add_update_accepted("1", "upd");
    //     t.add_full_wf_task();

    //     let hinfo = t.get_full_history_info().unwrap();
    //     let wft_started = hinfo.workflow_task_started_event_id();
    //     let full_hist = hinfo.into_events();
    //     let chunk_size = 4;
    //     let initial_hist = full_hist.chunks(chunk_size).next().unwrap().to_vec();
    //     assert_eq!(initial_hist.len(), chunk_size);

    //     let mut mock_client = mock_worker_client();
    //     let full_for_mock = full_hist.clone();
    //     let mut npt = 1;
    //     mock_client
    //         .expect_get_workflow_execution_history()
    //         .returning(move |_, _, passed_npt| {
    //             assert_eq!(passed_npt, vec![npt]);
    //             let mut hist_chunks = full_for_mock.chunks(chunk_size).peekable();
    //             let next_chunks = hist_chunks.nth(npt.into()).unwrap_or_default();
    //             npt += 1;
    //             let next_page_token = if hist_chunks.peek().is_none() {
    //                 vec![]
    //             } else {
    //                 vec![npt]
    //             };
    //             Ok(GetWorkflowExecutionHistoryResponse {
    //                 history: Some(History {
    //                     events: next_chunks.into(),
    //                 }),
    //                 raw_history: vec![],
    //                 next_page_token,
    //                 archived: false,
    //             })
    //         });

    //     let mut paginator = HistoryPaginator::new(
    //         History {
    //             events: initial_hist,
    //         },
    //         0,
    //         wft_started,
    //         "wfid".to_string(),
    //         "runid".to_string(),
    //         vec![1],
    //         Arc::new(mock_client),
    //         false,
    //     );

    //     let update = paginator
    //         .extract_next_update()
    //         .await
    //         .expect("extract update");
    //     assert!(
    //         update
    //             .events
    //             .iter()
    //             .any(|e| { e.event_type() == EventType::WorkflowExecutionUpdateAccepted }),
    //         "paginator should merge the second page so UpdateAccepted is visible in the update"
    //     );
    // }

    /// Builds a history with an empty WFT followed by a WFT with an update:
    ///   Event 1:  WorkflowExecutionStarted
    ///   Event 2:  WFTScheduled  ─┐
    ///   Event 3:  WFTStarted    ─┤ WFT1 (empty, no commands)
    ///   Event 4:  WFTCompleted  ─┘
    ///   Event 5:  WFTScheduled  ─┐
    ///   Event 6:  WFTStarted    ─┤ WFT2 (empty, no commands)
    ///   Event 7:  WFTCompleted  ─┘
    ///   Event 8:  WFTScheduled  ─┐
    ///   Event 9:  WFTStarted    ─┤ WFT3 (update + commands follow)
    ///   Event 10: WFTCompleted  ─┘
    ///   Event 11: UpdateAccepted  (sequencing_event_id = 8)
    ///   Event 12: UpdateCompleted
    ///   Event 13: TimerStarted
    ///   Event 14: TimerFired
    ///   Event 15: WFTScheduled  ─┐
    ///   Event 16: WFTStarted    ─┤ WFT4
    ///   Event 17: WFTCompleted  ─┘
    ///   Event 18: WorkflowExecutionCompleted
    fn build_empty_wft_then_update_history() -> TestHistoryBuilder {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task(); // WFT1: events 2-4 (empty)
        t.add_full_wf_task(); // WFT2: events 5-7 (empty)
        t.add_full_wf_task(); // WFT3: events 8-10
        let accept_id = t.add_update_accepted("upd-1", "startWork"); // 11, seq=8
        t.add_update_completed(accept_id); // 12
        let timer_id = t.add_timer_started("1".to_string()); // 13
        t.add_timer_fired(timer_id, "1".to_string()); // 14
        t.add_full_wf_task(); // WFT4: events 15-17 (command)
        t.add_workflow_execution_completed(); // 18
        t
    }

    /// Empty WFT followed by WFT with update: the heuristic collapses WFT1+WFT2
    /// when UpdateAccepted is not yet visible. This is the known behavior that must
    /// be preserved for backward compatibility. When full history IS visible,
    /// the UpdateAccepted event breaks the sequence.
    ///
    /// IN THIS TEST, WE ALWAYS HAVE has_last_wft = true.
    #[test]
    fn empty_wft_then_update_heuristic_has_last_wft() {
        let t = build_empty_wft_then_update_history();
        let all_events = t.get_full_history_info().unwrap().into_events();

        // 3. Up to WFT1 Started — single WFT visible.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..3].to_vec(), 0, 3, true, false);

            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::ReplayOver);
        }

        // 4. Up to WFT1 Completed.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..4].to_vec(), 0, 3, true, false);

            // WFEStarted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // WFTCompleted
            assert_eq!(next_check_peek2(&mut update, 3), (1, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(4), NextWFT::ReplayOver);
        }

        // 6. Up to WFT2 Started.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..6].to_vec(), 0, 6, true, false);

            // It is ok to collapse WFT1+WFT2, as there is no new event on WFT2.

            // WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (6, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(6), NextWFT::ReplayOver);
        }

        // 7. Up to WFT2 Completed.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..7].to_vec(), 0, 6, true, false);

            // It is ok to collapse WFT1+WFT2, as there is no new event on WFT2.
            // WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (6, false));

            // WFTCompleted
            assert_eq!(next_check_peek2(&mut update, 7), (1, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(7), NextWFT::ReplayOver);
        }

        // 9. Up to WFT3 Started, no speculative Update pending.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..9].to_vec(), 0, 9, true, false);

            // It is ok to collapse WFT1+WFT2+WFT3 in this case, as there is no new event on WFT3.
            // FIXME: ... but this is inconsistent with case 4, where the WFTCompleted is part of the third vWFT. Are we ok with this?

            // WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (9, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(9), NextWFT::ReplayOver);
        }

        // 9a. Similar to 9, but WFT3 is a speculative WFT with a pending update.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..9].to_vec(), 0, 9, true, true);

            // WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (6, false));

            // Can't collapse because of speculative update affecting WFT3
            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 6), (3, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(9), NextWFT::ReplayOver);
        }

        // 10. Up to WFT3 Completed — same collapse.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..10].to_vec(), 0, 6, true, false);

            // It is ok to collapse WFT1+WFT2+WFT3 in this case, as there is no new event on WFT3.
            // FIXME: ... but this is inconsistent with case 4, where the WFTCompleted is part of the third vWFT. Are we ok with this?

            // WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (9, false));

            // WFTCompleted
            assert_eq!(next_check_peek2(&mut update, 9), (1, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(10), NextWFT::ReplayOver);
        }

        // 11. Similar to 10, but there's an UpdateAccepted affecting WFT3.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..11].to_vec(), 0, 9, true, false);

            // WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (6, false));

            // Can't collapse because of speculative update affecting WFT3
            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 6), (3, false));

            // Tail(WFTCompleted -> UpdateAccepted)
            assert_eq!(next_check_peek2(&mut update, 9), (2, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(11), NextWFT::ReplayOver);
        }

        // 18: Full history
        {
            let mut update = t.as_history_update();

            // WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (6, false));

            // Can't collapse because of speculative update affecting WFT3
            // WFTCompleted -> WFTScheduled -> WFTStarted
            assert_eq!(next_check_peek2(&mut update, 6), (3, false));

            // Complete(WFTCompleted -> UpdateAccepted -> UpdateCompleted -> TimerStarted -> TimerFired -> WFTScheduled -> WFTStarted)
            assert_eq!(next_check_peek2(&mut update, 9), (7, false));

            // Complete(WFTCompleted -> WorkflowExecutionCompleted)
            assert_eq!(next_check_peek2(&mut update, 16), (2, true));

            // ReplayOver
            assert_matches!(update.take_next_wft_sequence(18), NextWFT::ReplayOver);
        }
    }

    /// Empty WFT followed by WFT with update: the heuristic collapses WFT1+WFT2
    /// when UpdateAccepted is not yet visible. This is the known behavior that must
    /// be preserved for backward compatibility. When full history IS visible,
    /// the UpdateAccepted event breaks the sequence.
    ///
    /// IN THIS TEST, WE ALWAYS HAVE has_last_wft = false.
    #[test]
    fn empty_wft_then_update_heuristic_no_last_wft() {
        let t = build_empty_wft_then_update_history();
        let all_events = t.get_full_history_info().unwrap().into_events();

        // 3. Up to WFT1 Started.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..3].to_vec(), 0, 3, false, false);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> (unknown)

            // Can't decide because unknown could:
            // - be collapsable into WFT1
            // - contain an UpdateAccepted event pointing back to the first WFTStarted
            // - contain a WFTFailed event

            assert_matches!(update.take_next_wft_sequence(0), NextWFT::NeedFetch);
        }

        // 4. Up to WFT1 Completed.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..4].to_vec(), 0, 3, false, false);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> (unknown)

            // Can't decide because unknown could:
            // - be collapsable into WFT1
            // - contain an UpdateAccepted event pointing back to the first WFTStarted

            assert_matches!(update.take_next_wft_sequence(0), NextWFT::NeedFetch);
        }

        // 4a. Up to WFT1 Completed + a follow up command
        {
            let mut t = TestHistoryBuilder::from_history(all_events[..4].to_vec());
            t.add_timer_started("1".to_string());

            let events = t.get_full_history_info().unwrap().into_events().to_vec();
            let mut update = HistoryUpdate::new_from_events(events, 0, 3, false, false);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> TimerStarted -> (unknown)

            // It is safe to return vWFT ending at the first WFTStarted
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide because there are no more WFTStarted in buffer, but unknown could contain some
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 5. Up to WFT2 Scheduled.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..5].to_vec(), 0, 3, false, false);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> (unknown)

            // Can't decide because unknown could:
            // - be collapsable into WFT1+WFT2
            // - contain a WFTFailed event
            // - contain an UpdateAccepted event pointing back to the second WFTStarted

            assert_matches!(update.take_next_wft_sequence(0), NextWFT::NeedFetch);
        }

        // 5a. Up to WFT2 Scheduled + some inbound event
        {
            let mut t = TestHistoryBuilder::from_history(all_events[..5].to_vec());
            t.add_we_signaled("whee", vec![]);

            let events = t.get_full_history_info().unwrap().into_events().to_vec();
            let mut update = HistoryUpdate::new_from_events(events, 0, 3, false, false);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WeSignaled -> (unknown)

            // It is safe to return vWFT ending at the first WFTStarted.
            // There can't be any unknown passed the WeSignaled that would affect WFT1
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because there are no more WFTStarted in buffer, but unknown could contain some
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 6. Up to WFT2 Started.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..6].to_vec(), 0, 6, false, false);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> (unknown)

            // Can't decide because unknown could:
            // - allow or prevent collapsing WFT1+WFT2
            // - contain a WFTFailed event
            // - contain an UpdateAccepted event pointing back to the second WFTStarted

            assert_matches!(update.take_next_wft_sequence(0), NextWFT::NeedFetch);
        }

        // 6a. Up to WFT2 Started + WFTTimedOut.
        {
            let mut t = TestHistoryBuilder::from_history(all_events[..6].to_vec());
            t.add_workflow_task_timed_out();

            let events = t.get_full_history_info().unwrap().into_events().to_vec();
            let mut update = HistoryUpdate::new_from_events(events, 0, 3, false, false);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTFailed -> (unknown)


            // It is safe to return vWFT ending at the first WFTStarted.
            assert_eq!(next_check_peek2(&mut update, 0), (3, false));

            // Can't decide further because there are no more non-failed WFTStarted in buffer; unknown could contain some
            assert_matches!(update.take_next_wft_sequence(3), NextWFT::NeedFetch);
        }

        // 7. Up to WFT2 Completed.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..7].to_vec(), 0, 6, false, false);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> (unknown)

            // Can't decide because unknown could:
            // - allow or prevent collapsing WFT1+WFT2
            // - contain an UpdateAccepted event pointing back to the second WFTStarted

            assert_matches!(update.take_next_wft_sequence(0), NextWFT::NeedFetch);
        }

        // 7a. Up to WFT2 Completed + a follow up command.
        {
            let mut t = TestHistoryBuilder::from_history(all_events[..7].to_vec());
            t.add_timer_started("1".to_string());

            let events = t.get_full_history_info().unwrap().into_events().to_vec();
            let mut update = HistoryUpdate::new_from_events(events, 0, 3, false, false);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> TimerStarted -> (unknown)

            // It is safe to return vWFT ending at the second WFTStarted.
            assert_eq!(next_check_peek2(&mut update, 0), (6, false));

            assert_matches!(update.take_next_wft_sequence(6), NextWFT::NeedFetch);
        }

        // 9. Up to WFT3 Started.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..9].to_vec(), 0, 9, false, false);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> (unknown)

            // Can't decide because unknown could:
            // - allow or prevent collapsing WFT1+WFT2+WFT3
            // - contain a WFTFailed event
            // - contain an UpdateAccepted event pointing back to the second WFTStarted

            assert_matches!(update.take_next_wft_sequence(0), NextWFT::NeedFetch);
        }

        // 9a. Up to WFT3 Started + WFTTimedOut.
        {
            let mut t = TestHistoryBuilder::from_history(all_events[..9].to_vec());
            t.add_workflow_task_timed_out();

            let events = t.get_full_history_info().unwrap().into_events().to_vec();
            let mut update = HistoryUpdate::new_from_events(events, 0, 0, false, false);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTTimedOut -> (unknown)

            // It is safe to return vWFT ending at the second WFTStarted.
            assert_eq!(next_check_peek2(&mut update, 0), (6, false));

            // Can't decide further because there are no more non-failed WFTStarted in buffer; unknown could contain some
            assert_matches!(update.take_next_wft_sequence(6), NextWFT::NeedFetch);
        }

        // 10. Up to WFT3 Completed.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..10].to_vec(), 0, 9, false, false);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> (unknown)

            // Can't decide because unknown could:
            // - allow or prevent collapsing WFT1+WFT2+WFT3
            // - contain an UpdateAccepted event pointing back to the third WFTStarted

            assert_matches!(update.take_next_wft_sequence(0), NextWFT::NeedFetch);
        }

        // 11. Up to updateAccepted
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..11].to_vec(), 0, 9, false, false);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTAccepted -> (unknown)

            // First is safe because we know it can't collapse with WFT3 (because of UpdateAccepted)
            assert_eq!(next_check_peek2(&mut update, 0), (6, false));
            // Second is safe because we know we can't collapse past the UpdateAccepted ahead
            assert_eq!(next_check_peek2(&mut update, 6), (3, false));

            // Can't decide further because there are no more WFTStarted in buffer; unknown could contain some; UpdateAccepted is not part of any vWFT
            assert_matches!(update.take_next_wft_sequence(9), NextWFT::NeedFetch);
        }

        // 12. Up to TimerStarted
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..13].to_vec(), 0, 9, false, false);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTAccepted -> WFTCompleted -> TimerStarted -> (unknown)

            // First is safe because we know it can't collapse with WFT3 (because of UpdateAccepted)
            assert_eq!(next_check_peek2(&mut update, 0), (6, false));
            // Second is safe because we know we can't collapse past the UpdateAccepted ahead
            assert_eq!(next_check_peek2(&mut update, 6), (3, false));

            // Can't decide further because there are no more WFTStarted in buffer; unknown could contain some; UpdateAccepted is not part of any vWFT
            assert_matches!(update.take_next_wft_sequence(9), NextWFT::NeedFetch);
        }

        // 16. Up to WFT4 Started.
        {
            let mut update =
                HistoryUpdate::new_from_events(all_events[..16].to_vec(), 0, 9, false, false);

            // Buffer:
            //   WFEStarted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTScheduled -> WFTStarted -> WFTCompleted -> WFTAccepted -> WFTCompleted -> TimerStarted -> TimerFired -> WFTScheduled -> WFTStarted -> (unknown)

            // First is safe because we know it can't collapse with WFT3 (because of UpdateAccepted)
            assert_eq!(next_check_peek2(&mut update, 0), (6, false));
            // Second is safe because we know we can't collapse past the UpdateAccepted ahead
            assert_eq!(next_check_peek2(&mut update, 6), (3, false));

            // Can't decide further because WFT4 Started could be followed by a WFTFailure or noop WFT sequences.
            assert_matches!(update.take_next_wft_sequence(9), NextWFT::NeedFetch);
        }
    }

    fn build_heartbeat_then_commands_history() -> TestHistoryBuilder {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_full_wf_task(); // WFT2: has commands
        let timer_id = t.add_timer_started("1".to_string());
        t.add_timer_fired(timer_id, "1".to_string());
        t.add_full_wf_task(); // WFT3
        t
    }

    /// Heartbeat detected via heuristic: empty WFT followed by WFT with commands
    /// is collapsed into one sequence.
    #[test]
    fn heartbeat_heuristic_collapses() {
        let t = build_heartbeat_then_commands_history();

        let mut update = t.as_history_update();
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(seq.len(), 6, "WFT1+WFT2 should be collapsed via heuristic");
        assert_eq!(seq.last().unwrap().event_id, 6);
    }

    /// When there are pending speculative updates, the heartbeat heuristic must
    /// NOT collapse the last WFT in a heartbeat chain, because the update needs
    /// to be delivered in its own activation (matching the original execution).
    ///
    /// History:
    ///   Event 1:  WorkflowExecutionStarted
    ///   Event 2:  WFTScheduled  ─┐
    ///   Event 3:  WFTStarted    ─┤ WFT1 (heartbeat, empty)
    ///   Event 4:  WFTCompleted  ─┘
    ///   Event 5:  WFTScheduled  ─┐
    ///   Event 6:  WFTStarted    ─┤ WFT2 (current task, with pending update)
    ///
    /// Without speculative updates: WFT1+WFT2 would be collapsed.
    /// With speculative updates: WFT1 should be separate so WFT2 gets its own
    /// activation for the pending update.
    #[test]
    fn heartbeat_not_collapsed_when_speculative_updates_pending() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task(); // WFT1: events 2-4
        t.add_workflow_task_scheduled_and_started(); // WFT2: events 5-6
        let all_events = t.get_full_history_info().unwrap().into_events();

        // Without speculative updates: heartbeat collapsed as usual.
        {
            let (mut update, _) = HistoryUpdate::from_events(all_events.clone(), 0, 6, true, false);
            let seq = next_check_peek(&mut update, 0);
            assert_eq!(
                seq.len(),
                6,
                "Without speculative updates: WFT1+WFT2 collapsed"
            );
        }

        // With speculative updates: last heartbeat NOT collapsed.
        {
            let (mut update, _) = HistoryUpdate::from_events(all_events.clone(), 0, 6, true, true);
            let seq = next_check_peek(&mut update, 0);
            assert_eq!(
                seq.len(),
                3,
                "With speculative updates: WFT1 should be separate (3 events)"
            );
            assert_eq!(seq.last().unwrap().event_id, 3);

            let seq = next_check_peek(&mut update, 3);
            assert_eq!(
                seq.len(),
                3,
                "With speculative updates: WFT2 should be separate (3 events)"
            );
            assert_eq!(seq.last().unwrap().event_id, 6);
        }
    }

    /// Multiple heartbeats followed by a WFT with speculative updates: only the
    /// last heartbeat should be un-collapsed; earlier ones remain collapsed.
    ///
    /// History:
    ///   Event 1:  WorkflowExecutionStarted
    ///   Event 2-4:  WFT1 (heartbeat)
    ///   Event 5-7:  WFT2 (heartbeat)
    ///   Event 8-9:  WFT3 (WFTScheduled + WFTStarted, current task with update)
    #[test]
    fn multiple_heartbeats_only_last_uncollapsed_for_speculative_updates() {
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task(); // WFT1: events 2-4
        t.add_full_wf_task(); // WFT2: events 5-7
        t.add_workflow_task_scheduled_and_started(); // WFT3: events 8-9
        let all_events = t.get_full_history_info().unwrap().into_events();

        // With speculative updates: WFT1+WFT2 collapsed, WFT3 separate.
        let (mut update, _) = HistoryUpdate::from_events(all_events.clone(), 0, 9, true, true);
        let seq = next_check_peek(&mut update, 0);
        assert_eq!(
            seq.len(),
            6,
            "WFT1+WFT2 should be collapsed together (6 events)"
        );
        assert_eq!(seq.last().unwrap().event_id, 6);

        let seq = next_check_peek(&mut update, 6);
        assert_eq!(
            seq.len(),
            3,
            "WFT3 should be separate (3 events) for speculative update"
        );
        assert_eq!(seq.last().unwrap().event_id, 9);
    }
}
