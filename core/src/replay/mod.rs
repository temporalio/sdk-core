//! This module implements support for creating special core instances and workers which can be used
//! to replay canned histories. It should be used by Lang SDKs to provide replay capabilities to
//! users during testing.

use crate::worker::client::{mocks::mock_manual_workflow_client, WorkerClient};
use futures::FutureExt;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
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

/// Create a mock client which can be used by a replay worker to serve up canned history.
/// It will return the entire history in one workflow task, after that it will return default
/// responses (with a 10s wait). If a workflow task failure is sent to the mock, it will send
/// the complete response again.
pub(crate) fn mock_client_from_history(
    history: &History,
    task_queue: impl Into<String>,
) -> impl WorkerClient {
    let mut mg = mock_manual_workflow_client();

    let hist_info = HistoryInfo::new_from_history(history, None).unwrap();
    let wf = WorkflowExecution {
        workflow_id: "fake_wf_id".to_string(),
        run_id: hist_info.orig_run_id().to_string(),
    };

    let did_send = Arc::new(AtomicBool::new(false));
    let did_send_clone = did_send.clone();
    let tq = task_queue.into();
    mg.expect_poll_workflow_task().returning(move |_, _| {
        let hist_info = hist_info.clone();
        let wf = wf.clone();
        let did_send_clone = did_send_clone.clone();
        let tq = tq.clone();
        async move {
            if !did_send_clone.swap(true, Ordering::AcqRel) {
                let mut resp = hist_info.as_poll_wft_response(tq);
                resp.workflow_execution = Some(wf.clone());
                Ok(resp)
            } else {
                tokio::time::sleep(Duration::from_secs(10)).await;
                Ok(Default::default())
            }
        }
        .boxed()
    });

    mg.expect_complete_workflow_task()
        .returning(|_| async move { Ok(RespondWorkflowTaskCompletedResponse::default()) }.boxed());
    mg.expect_fail_workflow_task().returning(move |_, _, _| {
        // We'll need to re-send the history if WFT fails
        did_send.store(false, Ordering::Release);
        async move { Ok(RespondWorkflowTaskFailedResponse {}) }.boxed()
    });

    mg
}
