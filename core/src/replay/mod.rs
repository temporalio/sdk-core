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
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
};
use temporal_sdk_core_protos::temporal::api::{
    common::v1::WorkflowExecution,
    enums::v1::WorkflowTaskFailedCause,
    history::v1::History,
    workflowservice::v1::{
        RespondWorkflowTaskCompletedResponse, RespondWorkflowTaskFailedResponse,
    },
};
pub use temporal_sdk_core_protos::{
    default_wes_attribs, HistoryInfo, TestHistoryBuilder, DEFAULT_WORKFLOW_TYPE,
};
use tokio::sync::{mpsc, mpsc::UnboundedSender, Mutex as TokioMutex};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

/// Create a mock client which can be used by a replay worker to serve up canned histories. It will
/// return the entire history in one workflow task. If a workflow task failure is sent to the mock,
/// it will send the complete response again.
///
/// Once it runs out of histories to return, it will serve up default responses after a 10s delay
pub(crate) fn mock_client_from_histories(historator: Historator) -> impl WorkerClient {
    let mut mg = mock_manual_workflow_client();

    let current_hist = Arc::new(TokioMutex::new(None));
    let needs_redispatch = Arc::new(AtomicBool::new(false));
    let hist_allow_tx = historator.replay_done_tx.clone();
    let historator = Arc::new(TokioMutex::new(historator));
    let nd_fail_clone = needs_redispatch.clone();

    mg.expect_poll_workflow_task().returning(move |_, _| {
        let current_hist = current_hist.clone();
        let needs_redispatch = needs_redispatch.clone();
        let historator = historator.clone();
        info!("Polled");
        async move {
            let mut hlock = historator.lock().await;
            info!("Got historator lock");
            // Always wait for permission before dispatching the next task
            let last_run = hlock.allow_stream.next().await;
            info!("Allowing next, last run: {:?}", last_run);

            let mut cur_hist_info = current_hist.lock().await;
            info!("Got cur hist lock");
            let history = if needs_redispatch.swap(false, Ordering::AcqRel) {
                info!("Re-dispatching");
                &*cur_hist_info
            } else {
                let hist_rid = hlock.next().await;
                *cur_hist_info = hist_rid;
                &*cur_hist_info
            };
            if let Some(history) = history {
                info!("Dispatching history");
                let hist_info = HistoryInfo::new_from_history(history, None).unwrap();
                let mut resp = hist_info.as_poll_wft_response();
                resp.workflow_execution = Some(WorkflowExecution {
                    workflow_id: "fake_wf_id".to_string(),
                    run_id: hist_info.orig_run_id().to_string(),
                });
                Ok(resp)
            } else {
                warn!("Out of history yo");
                hlock.worker_closer.get().map(|wc| wc.cancel());
                Ok(Default::default())
            }
        }
        .boxed()
    });

    mg.expect_complete_workflow_task().returning(move |_| {
        async move { Ok(RespondWorkflowTaskCompletedResponse::default()) }.boxed()
    });
    mg.expect_fail_workflow_task()
        .returning(move |_, cause, _| {
            warn!("Should fail wft and re-dispatch");
            if matches!(cause, WorkflowTaskFailedCause::NonDeterministicError) {
                // TODO: Record all failures for final output
                warn!("Nondeterminism reported!");
            } else {
                nd_fail_clone.store(true, Ordering::Release);
            }
            // TODO: can get rid of bool and use channel instead??
            hist_allow_tx.send("Failed".to_string()).unwrap();
            async move { Ok(RespondWorkflowTaskFailedResponse::default()) }.boxed()
        });

    mg
}

pub(crate) struct Historator {
    iter: Pin<Box<dyn Stream<Item = History> + Send>>,
    allow_stream: UnboundedReceiverStream<String>,
    worker_closer: Arc<OnceCell<CancellationToken>>,
    dat: Arc<Mutex<HistoratorDat>>,
    replay_done_tx: UnboundedSender<String>,
}
impl Historator {
    pub(crate) fn new(
        // TODO: history + wfid to avoid making them up?
        histories: impl Stream<Item = History> + Send + 'static,
    ) -> Self {
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
        move |_worker, activated_run_id, last_processed_event| {
            // We can't hold the lock while evaluating the hook, or we'd deadlock.
            let last_event_in_hist = dat
                .lock()
                .run_id_to_last_event_num
                .get(activated_run_id)
                .cloned();
            if let Some(le) = last_event_in_hist {
                if last_processed_event >= le {
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
    type Item = History;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.iter.poll_next_unpin(cx) {
            Poll::Ready(Some(history)) => {
                let run_id = history
                    .extract_run_id_from_start()
                    .expect(
                        "Histories provided for replay must contain run ids in their workflow \
                                 execution started events",
                    )
                    .to_string();
                let last_event = history.last_event_id();
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
