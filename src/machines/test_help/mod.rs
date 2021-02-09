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
pub(crate) fn build_fake_core(
    wfid: &str,
    run_id: &str,
    t: &mut TestHistoryBuilder,
) -> CoreSDK<MockServerGateway> {
    let events_first_batch = t.get_history_info(1).unwrap().events;
    let wf = Some(WorkflowExecution {
        workflow_id: wfid.to_string(),
        run_id: run_id.to_string(),
    });
    let first_response = PollWorkflowTaskQueueResponse {
        history: Some(History {
            events: events_first_batch,
        }),
        workflow_execution: wf.clone(),
        ..Default::default()
    };
    let events_second_batch = t.get_history_info(2).unwrap().events;
    let second_response = PollWorkflowTaskQueueResponse {
        history: Some(History {
            events: events_second_batch,
        }),
        workflow_execution: wf,
        ..Default::default()
    };
    let responses = vec![first_response, second_response];

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
    let core = CoreSDK {
        runtime,
        server_gateway: Arc::new(mock_gateway),
        workflow_machines: DashMap::new(),
        workflow_task_tokens: DashMap::new(),
    };
    core
}
