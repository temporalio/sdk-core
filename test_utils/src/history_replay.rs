mod history_builder;
mod history_info;

pub use history_builder::{default_wes_attribs, TestHistoryBuilder};
pub use history_info::HistoryInfo;

use crate::{mock_gateway, ServerGatewayApis, TEST_Q};
use temporal_sdk_core_protos::temporal::api::{
    common::v1::WorkflowExecution,
    history::v1::History,
    workflowservice::v1::{RespondWorkflowTaskFailedResponse, StartWorkflowExecutionResponse},
};

pub static DEFAULT_WORKFLOW_TYPE: &str = "not_specified";

pub fn mock_gateway_from_history(history: &History) -> impl ServerGatewayApis {
    let mut mg = mock_gateway();

    let hist_info = HistoryInfo::new_from_history(history, None).unwrap();
    let wf = WorkflowExecution {
        workflow_id: "fake_wf_id".to_string(),
        run_id: hist_info.orig_run_id().to_string(),
    };

    let wf_clone = wf.clone();
    mg.expect_start_workflow().returning(move |_, _, _, _, _| {
        Ok(StartWorkflowExecutionResponse {
            run_id: wf_clone.run_id.clone(),
        })
    });

    mg.expect_poll_workflow_task().returning(move |_, _| {
        let mut resp = hist_info.as_poll_wft_response(TEST_Q);
        resp.workflow_execution = Some(wf.clone());
        Ok(resp)
    });

    mg.expect_fail_workflow_task()
        .returning(|_, _, _| Ok(RespondWorkflowTaskFailedResponse {}));

    mg
}
