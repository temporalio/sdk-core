//! Tests for SDK-level query handling

use crate::common::build_fake_sdk;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, future::poll_fn, task::Poll, time::Duration};
use temporalio_common::protos::{
    DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder,
    coresdk::workflow_commands::query_result,
    temporal::api::{
        common::v1::WorkflowType,
        enums::v1::{CommandType, EventType},
        history::v1::WorkflowExecutionStartedEventAttributes,
        query::v1::WorkflowQuery,
        taskqueue::v1::TaskQueue,
    },
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowContextView, WorkflowResult};
use temporalio_sdk_core::test_help::{
    MockPollCfg, ResponseType, hist_to_poll_resp, mock_worker_client,
};

/// A workflow that returns Pending on first poll and Ready on second poll.
/// Uses workflow state to track whether it has been polled before.
#[workflow]
#[derive(Default)]
struct CompleteOnSecondPollWf {
    polled_once: bool,
    value: u32,
}

#[workflow_methods]
impl CompleteOnSecondPollWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<u32> {
        poll_fn(|_| {
            if ctx.state(|s| s.polled_once) {
                Poll::Ready(())
            } else {
                ctx.state_mut(|s| s.polled_once = true);
                Poll::Pending
            }
        })
        .await;
        Ok(42)
    }

    #[query]
    fn get_value(&self, _ctx: &WorkflowContextView) -> u32 {
        self.value
    }
}

/// This test demonstrates a bug where the SDK advances the workflow future even when
/// receiving a query-only activation. When the workflow would complete on the next poll,
/// this causes both a query response AND a workflow completion command to be sent
/// together, which is invalid.
///
/// The error message from core when this happens is:
/// "Workflow completion had a legacy query response along with other commands.
/// This is not allowed and constitutes an error in the lang SDK."
#[tokio::test]
async fn query_only_activation_should_not_advance_workflow() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();

    let wfid = "query_only_test";

    let tasks = [
        hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(1)),
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(1));
            pr.query = Some(WorkflowQuery {
                query_type: "get_value".to_string(),
                query_args: None,
                header: None,
            });
            pr.history = Some(Default::default());
            pr
        },
    ];

    let mut mock_cfg = MockPollCfg::from_resp_batches(wfid, t, tasks, mock_worker_client());
    mock_cfg.num_expected_legacy_query_resps = 1;

    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(|wft| {
                let has_complete_cmd = wft
                    .commands
                    .iter()
                    .any(|c| c.command_type() == CommandType::CompleteWorkflowExecution);
                assert!(
                    !has_complete_cmd,
                    "First activation should not complete workflow! Commands: {:?}",
                    wft.commands
                );
            })
            .then(|wft| {
                let has_complete_cmd = wft
                    .commands
                    .iter()
                    .any(|c| c.command_type() == CommandType::CompleteWorkflowExecution);
                assert!(
                    !has_complete_cmd,
                    "Query-only activation should NOT cause workflow completion! Commands: {:?}",
                    wft.commands
                );
            });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_workflow::<CompleteOnSecondPollWf>();
    worker.run().await.unwrap();
}

/// Test that a query for a non-existent handler doesn't advance the workflow either.
#[tokio::test]
async fn nonexistent_query_should_not_advance_workflow() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();

    let wfid = "nonexistent_query_test";

    let tasks = [
        hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(1)),
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(1));
            pr.query = Some(WorkflowQuery {
                query_type: "__temporal_workflow_metadata".to_string(),
                query_args: None,
                header: None,
            });
            pr.history = Some(Default::default());
            pr
        },
    ];

    let mut mock_cfg = MockPollCfg::from_resp_batches(wfid, t, tasks, mock_worker_client());
    mock_cfg.num_expected_legacy_query_resps = 1;

    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(|wft| {
                let has_complete_cmd = wft
                    .commands
                    .iter()
                    .any(|c| c.command_type() == CommandType::CompleteWorkflowExecution);
                assert!(
                    !has_complete_cmd,
                    "First activation should not complete workflow! Commands: {:?}",
                    wft.commands
                );
            })
            .then(|wft| {
                let has_complete_cmd = wft
                    .commands
                    .iter()
                    .any(|c| c.command_type() == CommandType::CompleteWorkflowExecution);
                assert!(
                    !has_complete_cmd,
                    "Nonexistent query should NOT cause workflow completion! Commands: {:?}",
                    wft.commands
                );
            });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_workflow::<CompleteOnSecondPollWf>();
    worker.run().await.unwrap();
}

/// A workflow that increments a counter when started and when it receives a signal.
/// Used to test that non-legacy queries see state after the workflow has advanced.
#[workflow]
#[derive(Default)]
struct CounterWf {
    counter: u32,
    got_signal: bool,
}

#[workflow_methods]
impl CounterWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.state_mut(|s| s.counter += 1);
        ctx.wait_condition(|s| s.got_signal).await;
        ctx.state_mut(|s| s.counter += 1);
        Ok(())
    }

    #[signal]
    fn my_signal(&mut self, _ctx: &mut WorkflowContext<Self>) {
        self.got_signal = true;
    }

    #[query]
    fn get_counter(&self, _ctx: &WorkflowContextView) -> u32 {
        self.counter
    }
}

/// Non-legacy queries (in the `queries` field) come bundled with new history.
/// Core sends these queries in their own activation after the workflow has processed
/// the history, so queries should observe state AFTER the workflow has advanced.
#[tokio::test]
async fn non_legacy_query_should_see_state_after_workflow_advances() {
    let wfid = "non_legacy_query_state_test";

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled("my_signal", vec![]);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let tasks = [
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(1));
            pr.queries = HashMap::from([(
                "q1".to_string(),
                WorkflowQuery {
                    query_type: "get_counter".to_string(),
                    query_args: None,
                    header: None,
                },
            )]);
            pr
        },
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(2));
            pr.queries = HashMap::from([(
                "q2".to_string(),
                WorkflowQuery {
                    query_type: "get_counter".to_string(),
                    query_args: None,
                    header: None,
                },
            )]);
            pr
        },
    ];

    let mut mock_cfg = MockPollCfg::from_resp_batches(wfid, t, tasks, mock_worker_client());

    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(|wft| {
                assert_eq!(wft.query_responses.len(), 1);
                let query_resp = &wft.query_responses[0];
                assert_eq!(query_resp.query_id, "q1");

                match &query_resp.variant {
                    Some(query_result::Variant::Succeeded(success)) => {
                        let payload = success
                            .response
                            .as_ref()
                            .expect("Expected response payload");
                        let value: u32 =
                            serde_json::from_slice(&payload.data).expect("Expected u32 payload");
                        assert_eq!(
                            value, 1,
                            "After start, counter should be 1 but got {}",
                            value
                        );
                    }
                    other => panic!("Expected successful query response, got {:?}", other),
                }
            })
            .then(|wft| {
                assert_eq!(wft.query_responses.len(), 1);
                let query_resp = &wft.query_responses[0];
                assert_eq!(query_resp.query_id, "q2");

                match &query_resp.variant {
                    Some(query_result::Variant::Succeeded(success)) => {
                        let payload = success
                            .response
                            .as_ref()
                            .expect("Expected response payload");
                        let value: u32 =
                            serde_json::from_slice(&payload.data).expect("Expected u32 payload");
                        assert_eq!(
                            value, 2,
                            "After signal, counter should be 2 but got {}",
                            value
                        );
                    }
                    other => panic!("Expected successful query response, got {:?}", other),
                }
            });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_workflow::<CounterWf>();
    worker.run().await.unwrap();
}

/// Struct to capture WorkflowContextView information for testing.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct WorkflowInfo {
    workflow_id: String,
    workflow_type: String,
    task_queue: String,
    namespace: String,
    attempt: u32,
    first_execution_run_id: String,
}

impl<'a> From<&'a WorkflowContextView> for WorkflowInfo {
    fn from(ctx: &'a WorkflowContextView) -> Self {
        Self {
            workflow_id: ctx.workflow_id.clone(),
            workflow_type: ctx.workflow_type.clone(),
            task_queue: ctx.task_queue.clone(),
            namespace: ctx.namespace.clone(),
            attempt: ctx.attempt,
            first_execution_run_id: ctx.first_execution_run_id.clone(),
        }
    }
}

/// A workflow that captures context view information at initialization time.
#[workflow]
struct ContextViewWf;

#[workflow_methods]
impl ContextViewWf {
    #[init]
    fn new(_ctx: &WorkflowContextView) -> Self {
        Self
    }

    #[run(name = "context_view_wf")]
    async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }

    #[query]
    fn get_info(&self, ctx: &WorkflowContextView) -> WorkflowInfo {
        ctx.into()
    }
}

/// Test that WorkflowContextView contains the correct workflow information.
#[tokio::test]
async fn query_returns_workflow_context_view_info() {
    const WFID: &str = "context_view_test_wf";
    const FIRST_RUN_ID: &str = "first-run-id-12345";

    let mut t = TestHistoryBuilder::default();
    t.add(WorkflowExecutionStartedEventAttributes {
        workflow_type: Some(WorkflowType {
            name: "context_view_wf".to_string(),
        }),
        task_queue: Some(TaskQueue {
            name: "test-task-queue".to_string(),
            ..Default::default()
        }),
        first_execution_run_id: FIRST_RUN_ID.to_string(),
        attempt: 3,
        workflow_task_timeout: Some(Duration::from_secs(5).try_into().unwrap()),
        ..Default::default()
    });
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let tasks = [{
        let mut pr = hist_to_poll_resp(&t, WFID.to_owned(), ResponseType::ToTaskNum(1));
        pr.queries = HashMap::from([(
            "q1".to_string(),
            WorkflowQuery {
                query_type: "get_info".to_string(),
                query_args: None,
                header: None,
            },
        )]);
        pr
    }];

    let mut mock_cfg = MockPollCfg::from_resp_batches(WFID, t, tasks, mock_worker_client());

    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(|wft| {
            assert_eq!(wft.query_responses.len(), 1);
            let query_resp = &wft.query_responses[0];
            assert_eq!(query_resp.query_id, "q1");

            match &query_resp.variant {
                Some(query_result::Variant::Succeeded(success)) => {
                    let payload = success
                        .response
                        .as_ref()
                        .expect("Expected response payload");
                    let info: WorkflowInfo =
                        serde_json::from_slice(&payload.data).expect("Expected WorkflowInfo");

                    assert_eq!(info.workflow_id, WFID);
                    assert_eq!(info.workflow_type, "context_view_wf");
                    // task_queue comes from worker config, not workflow history
                    assert_eq!(info.namespace, "default");
                    assert_eq!(info.attempt, 3);
                    assert_eq!(info.first_execution_run_id, FIRST_RUN_ID);
                }
                other => panic!("Expected successful query response, got {:?}", other),
            }
        });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_workflow::<ContextViewWf>();
    worker.run().await.unwrap();
}
