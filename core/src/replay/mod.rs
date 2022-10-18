//! This module implements support for creating special core instances and workers which can be used
//! to replay canned histories. It should be used by Lang SDKs to provide replay capabilities to
//! users during testing.

use crate::{
    worker::client::{mocks::mock_manual_workflow_client, WorkerClient},
    Worker,
};
use futures::{FutureExt, Stream, StreamExt};
use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use temporal_sdk_core_protos::{
    coresdk::workflow_activation::remove_from_cache::EvictionReason,
    temporal::api::{
        common::v1::WorkflowExecution,
        history::v1::History,
        workflowservice::v1::{
            RespondWorkflowTaskCompletedResponse, RespondWorkflowTaskFailedResponse,
        },
    },
};
pub use temporal_sdk_core_protos::{
    default_wes_attribs, HistoryInfo, TestHistoryBuilder, DEFAULT_WORKFLOW_TYPE,
};
use tokio::sync::{mpsc, mpsc::UnboundedSender, Mutex as TokioMutex};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

/// A history which will be used during replay verification. Since histories do not include the
/// workflow id, it must be manually attached.
#[derive(Debug, Clone, derive_more::Constructor)]
pub struct HistoryForReplay {
    hist: History,
    workflow_id: String,
}

/// Allows lang to feed histories into the replayer one at a time. Simply drop the feeder to signal
/// to the worker that you're done and it should initiate shutdown.
pub struct HistoryFeeder {
    tx: mpsc::Sender<HistoryForReplay>,
}
/// The stream half of a [HistoryFeeder]
pub struct HistoryFeederStream {
    rcvr: mpsc::Receiver<HistoryForReplay>,
}

impl HistoryFeeder {
    /// Make a new history feeder, which will store at most `buffer_size` histories before `feed`
    /// blocks.
    ///
    /// Returns a feeder which will be used to feed in histories, and a stream you can pass to
    /// one of the replay worker init functions.
    pub fn new(buffer_size: usize) -> (Self, HistoryFeederStream) {
        let (tx, rcvr) = mpsc::channel(buffer_size);
        (Self { tx }, HistoryFeederStream { rcvr })
    }
    /// Feed a new history into the replayer, blocking if there is not room to accept another
    /// history.
    pub async fn feed(&self, history: HistoryForReplay) -> anyhow::Result<()> {
        self.tx.send(history).await?;
        Ok(())
    }
}

impl Stream for HistoryFeederStream {
    type Item = HistoryForReplay;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rcvr.poll_recv(cx)
    }
}

/// Create a mock client which can be used by a replay worker to serve up canned histories. It will
/// return the entire history in one workflow task. If a workflow task failure is sent to the mock,
/// it will send the complete response again.
///
/// Once it runs out of histories to return, it will serve up default responses after a 10s delay
pub(crate) fn mock_client_from_histories(historator: Historator) -> impl WorkerClient {
    let mut mg = mock_manual_workflow_client();

    let hist_allow_tx = historator.replay_done_tx.clone();
    let historator = Arc::new(TokioMutex::new(historator));

    mg.expect_poll_workflow_task().returning(move |_, _| {
        let historator = historator.clone();
        async move {
            let mut hlock = historator.lock().await;
            // Always wait for permission before dispatching the next task
            let _ = hlock.allow_stream.next().await;

            if let Some(history) = hlock.next().await {
                let hist_info = HistoryInfo::new_from_history(&history.hist, None).unwrap();
                let mut resp = hist_info.as_poll_wft_response();
                resp.workflow_execution = Some(WorkflowExecution {
                    workflow_id: history.workflow_id,
                    run_id: hist_info.orig_run_id().to_string(),
                });
                Ok(resp)
            } else {
                if let Some(wc) = hlock.worker_closer.get() {
                    wc.cancel();
                }
                Ok(Default::default())
            }
        }
        .boxed()
    });

    mg.expect_complete_workflow_task().returning(move |_| {
        async move { Ok(RespondWorkflowTaskCompletedResponse::default()) }.boxed()
    });
    mg.expect_fail_workflow_task().returning(move |_, _, _| {
        hist_allow_tx.send("Failed".to_string()).unwrap();
        async move { Ok(RespondWorkflowTaskFailedResponse::default()) }.boxed()
    });

    mg
}

pub(crate) struct Historator {
    iter: Pin<Box<dyn Stream<Item = HistoryForReplay> + Send>>,
    allow_stream: UnboundedReceiverStream<String>,
    worker_closer: Arc<OnceCell<CancellationToken>>,
    dat: Arc<Mutex<HistoratorDat>>,
    replay_done_tx: UnboundedSender<String>,
}
impl Historator {
    pub(crate) fn new(histories: impl Stream<Item = HistoryForReplay> + Send + 'static) -> Self {
        let dat = Arc::new(Mutex::new(HistoratorDat::default()));
        let (replay_done_tx, replay_done_rx) = mpsc::unbounded_channel();
        // Need to allow the first history item
        replay_done_tx.send("fake".to_string()).unwrap();
        Self {
            iter: Box::pin(histories.fuse()),
            allow_stream: UnboundedReceiverStream::new(replay_done_rx),
            worker_closer: Arc::new(OnceCell::new()),
            dat,
            replay_done_tx,
        }
    }

    /// Returns a callback that can be used as the post-activation hook for a worker to indicate
    /// we're ready to replay the next history, or whatever else.
    pub(crate) fn get_post_activate_hook(&self) -> impl Fn(&Worker, &str, usize) + Send + Sync {
        let dat = self.dat.clone();
        let done_tx = self.replay_done_tx.clone();
        move |worker, activated_run_id, last_processed_event| {
            // We can't hold the lock while evaluating the hook, or we'd deadlock.
            let last_event_in_hist = dat
                .lock()
                .run_id_to_last_event_num
                .get(activated_run_id)
                .cloned();
            if let Some(le) = last_event_in_hist {
                if last_processed_event >= le {
                    worker.request_wf_eviction(
                        activated_run_id,
                        "Always evict workflows after replay",
                        EvictionReason::LangRequested,
                    );
                    done_tx.send(activated_run_id.to_string()).unwrap();
                }
            }
        }
    }

    pub(crate) fn get_shutdown_setter(&self) -> impl FnOnce(CancellationToken) + 'static {
        let wc = self.worker_closer.clone();
        move |ct| {
            wc.set(ct).expect("Shutdown token must only be set once");
        }
    }
}

impl Stream for Historator {
    type Item = HistoryForReplay;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.iter.poll_next_unpin(cx) {
            Poll::Ready(Some(history)) => {
                let run_id = history
                    .hist
                    .extract_run_id_from_start()
                    .expect(
                        "Histories provided for replay must contain run ids in their workflow \
                                 execution started events",
                    )
                    .to_string();
                let last_event = history.hist.last_event_id();
                self.dat
                    .lock()
                    .run_id_to_last_event_num
                    .insert(run_id, last_event as usize);
                Poll::Ready(Some(history))
            }
            Poll::Ready(None) => {
                self.dat.lock().all_dispatched = true;
                Poll::Ready(None)
            }
            o => o,
        }
    }
}

#[derive(Default)]
struct HistoratorDat {
    run_id_to_last_event_num: HashMap<String, usize>,
    all_dispatched: bool,
}
