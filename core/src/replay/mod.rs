//! This module implements support for creating special core instances and workers which can be used
//! to replay canned histories. It should be used by Lang SDKs to provide replay capabilities to
//! users during testing.

use crate::{CoreInitOptionsBuilder, TelemetryOptions};
use futures::FutureExt;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use temporal_client::{
    mocks::{mock_gateway, mock_manual_gateway},
    ServerGatewayApis,
};
use temporal_sdk_core_api::{
    errors::{
        CompleteActivityError, CompleteWfError, PollActivityError, PollWfError,
        WorkerRegistrationError,
    },
    worker::WorkerConfig,
    CoreLog,
};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_task::ActivityTask, workflow_activation::WorkflowActivation,
        workflow_completion::WorkflowActivationCompletion, ActivityHeartbeat,
        ActivityTaskCompletion,
    },
    temporal::api::{
        common::v1::WorkflowExecution,
        history::v1::History,
        workflowservice::v1::{RespondWorkflowTaskFailedResponse, StartWorkflowExecutionResponse},
    },
};

pub use temporal_sdk_core_protos::{
    default_wes_attribs, HistoryInfo, TestHistoryBuilder, DEFAULT_WORKFLOW_TYPE,
};

// impl ReplayCore for ReplayCoreImpl {
//     fn make_replay_worker(
//         &self,
//         mut config: WorkerConfig,
//         history: &History,
//     ) -> Result<(), anyhow::Error> {
//         let mock_g = mock_gateway_from_history(history, config.task_queue.clone());
//         config.max_cached_workflows = 1;
//         config.max_concurrent_wft_polls = 1;
//         config.no_remote_activities = true;
//         self.inner
//             .register_replay_worker(config, Arc::new(mock_g), history)
//     }
// }

/// Create a mock gateway which can be used by a replay worker to serve up canned history.
/// It will return the entire history in one workflow task, after that it will return default
/// responses (with a 10s wait). If a workflow task failure is sent to the mock, it will send
/// the complete response again.
pub fn mock_gateway_from_history(
    history: &History,
    task_queue: impl Into<String>,
) -> impl ServerGatewayApis {
    let mut mg = mock_manual_gateway();

    let hist_info = HistoryInfo::new_from_history(history, None).unwrap();
    let wf = WorkflowExecution {
        workflow_id: "fake_wf_id".to_string(),
        run_id: hist_info.orig_run_id().to_string(),
    };

    let wf_clone = wf.clone();
    mg.expect_start_workflow().returning(move |_, _, _, _, _| {
        let wf_clone = wf_clone.clone();
        async move {
            Ok(StartWorkflowExecutionResponse {
                run_id: wf_clone.run_id.clone(),
            })
        }
        .boxed()
    });

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

    mg.expect_fail_workflow_task().returning(move |_, _, _| {
        // We'll need to re-send the history if WFT fails
        did_send.store(false, Ordering::Release);
        async move { Ok(RespondWorkflowTaskFailedResponse {}) }.boxed()
    });

    mg
}
