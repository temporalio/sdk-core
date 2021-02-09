type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

mod history_builder;
mod workflow_driver;

pub(crate) use history_builder::TestHistoryBuilder;
pub(super) use workflow_driver::{CommandSender, TestWFCommand, TestWorkflowDriver};

use crate::{
    pollers::MockServerGateway,
    protos::temporal::api::common::v1::WorkflowExecution,
    protos::temporal::api::history::v1::History,
    protos::temporal::api::workflowservice::v1::{
        PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedResponse,
    },
    CoreSDK,
};
use dashmap::DashMap;
use std::{collections::VecDeque, sync::Arc};
use tokio::runtime::Runtime;

/// Given identifiers for a workflow/run, and a test history builder, construct an instance of
/// the core SDK with a mock server gateway that will produce the responses as appropriate.
///
/// `response_batches` is used to control the fake [PollWorkflowTaskQueueResponse]s returned.
/// For each number in the input list, a fake response will be prepared which includes history
/// up to the workflow task with that number, as in [TestHistoryBuilder::get_history_info].
pub(crate) fn build_fake_core(
    wf_id: &str,
    run_id: &str,
    t: &mut TestHistoryBuilder,
    response_batches: &[usize],
) -> CoreSDK<MockServerGateway> {
    let wf = Some(WorkflowExecution {
        workflow_id: wf_id.to_string(),
        run_id: run_id.to_string(),
    });

    let responses: Vec<_> = response_batches
        .iter()
        .map(|to_task_num| {
            let batch = t.get_history_info(*to_task_num).unwrap().events;
            PollWorkflowTaskQueueResponse {
                history: Some(History { events: batch }),
                workflow_execution: wf.clone(),
                ..Default::default()
            }
        })
        .collect();

    let mut tasks = VecDeque::from(responses);
    let mut mock_gateway = MockServerGateway::new();
    mock_gateway
        .expect_poll_workflow_task()
        .returning(move |_| Ok(tasks.pop_front().unwrap()));
    // Response not really important here
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));

    let runtime = Runtime::new().unwrap();
    CoreSDK {
        runtime,
        server_gateway: Arc::new(mock_gateway),
        workflow_machines: DashMap::new(),
        workflow_task_tokens: DashMap::new(),
    }
}
