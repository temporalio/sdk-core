//! This module implements support for creating special core instances and workers which can be used
//! to replay canned histories. It should be used by Lang SDKs to provide replay capabilities to
//! users during testing.

use crate::{
    worker::client::{mocks::mock_manual_workflow_client, WorkerClient},
    Worker,
};
use futures::FutureExt;
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    iter,
    sync::Arc,
    time::Duration,
};
use temporal_sdk_core_api::Worker as WorkerTrait;
use temporal_sdk_core_protos::temporal::api::{
    common::v1::WorkflowExecution,
    history::v1::History,
    workflowservice::v1::{
        RespondWorkflowTaskCompletedResponse, RespondWorkflowTaskFailedResponse,
    },
};
pub use temporal_sdk_core_protos::{
    default_wes_attribs, HistoryInfo, TestHistoryBuilder, DEFAULT_WORKFLOW_TYPE,
};
use tokio::sync::Notify;

/// Create a mock client which can be used by a replay worker to serve up canned histories. It will
/// return the entire history in one workflow task. If a workflow task failure is sent to the mock,
/// it will send the complete response again.
///
/// Once it runs out of histories to return, it will serve up default responses after a 10s delay
pub(crate) fn mock_client_from_histories(
    mut historator: Historator<impl Iterator<Item = History> + Send + 'static>,
) -> impl WorkerClient {
    let mut mg = mock_manual_workflow_client();

    let hook = historator.replay_complete_hook.clone();
    let current_hist = Arc::new(Mutex::new(historator.next()));
    let needs_dispatch = Arc::new(Mutex::new(true));
    let chi_clone = current_hist.clone();
    let nd_clone = needs_dispatch.clone();
    let nd_fail_clone = needs_dispatch.clone();
    let notifier = Arc::new(Notify::new());
    let noti_clone = notifier.clone();
    *hook.lock() = Box::new(move |_| {
        if let Some(h) = historator.next() {
            chi_clone.lock().replace(h);
            *nd_clone.lock() = true;
        }
        noti_clone.notify_waiters();
    });
    mg.expect_poll_workflow_task().returning(move |_, _| {
        let cur_hist_info = current_hist.lock();
        let needs_disp_bool = *needs_dispatch.lock();
        let history = match (cur_hist_info.as_ref(), needs_disp_bool) {
            (Some(h), true) => {
                *needs_dispatch.lock() = false;
                h
            }
            _ => {
                let noti = notifier.clone();
                return async move {
                    tokio::select! {
                        _ = noti.notified() => {}
                        _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                    }
                    Ok(Default::default())
                }
                .boxed();
            }
        };
        let hist_info = HistoryInfo::new_from_history(history, None).unwrap();
        async move {
            let mut resp = hist_info.as_poll_wft_response();
            resp.workflow_execution = Some(WorkflowExecution {
                workflow_id: "fake_wf_id".to_string(),
                run_id: hist_info.orig_run_id().to_string(),
            });
            Ok(resp)
        }
        .boxed()
    });

    mg.expect_complete_workflow_task().returning(move |_| {
        async move { Ok(RespondWorkflowTaskCompletedResponse::default()) }.boxed()
    });
    mg.expect_fail_workflow_task().returning(move |_, _, _| {
        *nd_fail_clone.lock() = true;
        async move { Ok(RespondWorkflowTaskFailedResponse::default()) }.boxed()
    });

    mg
}

pub(crate) struct Historator<I> {
    iter: iter::Fuse<I>,
    dat: Arc<Mutex<HistoratorDat>>,
    #[allow(clippy::type_complexity)] // Pshh clippy c'mon this is only a fraction of my power
    replay_complete_hook: Arc<Mutex<Box<dyn FnMut(&str) + Send>>>,
}
impl<I> Historator<I>
where
    I: Iterator<Item = History> + Send + 'static,
{
    pub(crate) fn new<II: IntoIterator<IntoIter = I> + 'static>(
        // TODO: history + wfid to avoid making them up?
        histories: II,
    ) -> Self {
        Self {
            iter: histories.into_iter().fuse(),
            dat: Arc::new(Mutex::new(HistoratorDat::default())),
            replay_complete_hook: Arc::new(Mutex::new(Box::new(|_| {}))),
        }
    }
}
impl<I> Historator<I> {
    /// Builds and returns a callback that can be used as the post-activation hook for a worker
    /// to trigger shutdown once all histories have completed replay
    pub(crate) fn make_post_activate_hook(&self) -> impl Fn(&Worker, &str, usize) + Send + Sync {
        let dat = self.dat.clone();
        let hook = self.replay_complete_hook.clone();
        let completed_ids = Mutex::new(HashSet::new());
        move |worker, activated_run_id, last_processed_event| {
            // We can't hold the lock while evaluating the hook, or we'd deadlock.
            let last_event_in_hist = dat
                .lock()
                .run_id_to_last_event_num
                .get(activated_run_id)
                .cloned();
            if let Some(le) = last_event_in_hist {
                if last_processed_event >= le {
                    completed_ids.lock().insert(activated_run_id.to_string());
                    (hook.lock())(activated_run_id);
                }
            }
            let datlock = dat.lock();
            if datlock.all_dispatched
                && completed_ids.lock().len() >= datlock.run_id_to_last_event_num.len()
            {
                info!("All histories have concluded replay, triggering worker shutdown");
                worker.initiate_shutdown();
            }
        }
    }
}
impl<I> Iterator for Historator<I>
where
    I: Iterator<Item = History>,
{
    type Item = History;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.iter.next();
        if let Some(ref history) = next {
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
        } else {
            self.dat.lock().all_dispatched = true;
        }
        next
    }
}

#[derive(Default)]
struct HistoratorDat {
    run_id_to_last_event_num: HashMap<String, usize>,
    all_dispatched: bool,
}
