type Result<T, E = anyhow::Error> = std::result::Result<T, E>;

mod async_workflow_driver;
mod history_builder;

pub(super) use async_workflow_driver::{CommandSender, TestWorkflowDriver};
pub(crate) use history_builder::TestHistoryBuilder;

use crate::workflow::WorkflowConcurrencyManager;
use crate::{
    pollers::MockServerGatewayApis,
    protos::temporal::api::common::v1::WorkflowExecution,
    protos::temporal::api::history::v1::History,
    protos::temporal::api::workflowservice::v1::{
        PollWorkflowTaskQueueResponse, RespondWorkflowTaskCompletedResponse,
    },
    CoreSDK, PollTaskResponse, ServerGatewayApis,
};
use rand::{thread_rng, Rng};
use std::sync::atomic::AtomicBool;
use std::{collections::VecDeque, sync::Arc};
use tokio::runtime::Runtime;

pub(crate) type FakeCore = CoreSDK<MockServerGatewayApis>;

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
) -> FakeCore {
    let wf = Some(WorkflowExecution {
        workflow_id: wf_id.to_string(),
        run_id: run_id.to_string(),
    });

    let responses: Vec<_> = response_batches
        .iter()
        .map(|to_task_num| {
            let batch = t.get_history_info(*to_task_num).unwrap().events().to_vec();
            let task_token: [u8; 16] = thread_rng().gen();
            PollWorkflowTaskQueueResponse {
                history: Some(History { events: batch }),
                workflow_execution: wf.clone(),
                task_token: task_token.to_vec(),
                ..Default::default()
            }
        })
        .collect();

    let mut tasks = VecDeque::from(responses);
    let mut mock_gateway = MockServerGatewayApis::new();
    mock_gateway
        .expect_poll_task()
        .times(response_batches.len())
        .returning(move |_| Ok(PollTaskResponse::WorkflowTask(tasks.pop_front().unwrap())));
    // Response not really important here
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));

    fake_core_from_mock(mock_gateway)
}

pub(crate) fn fake_core_from_mock<MT>(mock_gateway: MT) -> CoreSDK<MT>
where
    MT: ServerGatewayApis,
{
    let runtime = Runtime::new().unwrap();
    CoreSDK {
        runtime,
        server_gateway: Arc::new(mock_gateway),
        workflow_machines: WorkflowConcurrencyManager::new(),
        workflow_task_tokens: Default::default(),
        pending_activations: Default::default(),
        shutdown_requested: AtomicBool::new(false),
    }
}
