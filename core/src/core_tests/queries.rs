use crate::test_help::{
    canned_histories, hist_to_poll_resp, mock_worker, MocksHolder, ResponseType, TEST_Q,
};
use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};
use temporal_client::mocks::mock_gateway;
use temporal_sdk_core_api::Worker as WorkerTrait;
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{workflow_activation_job, WorkflowActivationJob},
        workflow_commands::{CompleteWorkflowExecution, QueryResult, QuerySuccess},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        common::v1::Payload,
        failure::v1::Failure,
        history::v1::History,
        query::v1::WorkflowQuery,
        workflowservice::v1::{
            RespondQueryTaskCompletedResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
};
use temporal_sdk_core_test_utils::start_timer_cmd;

#[rstest::rstest]
#[case::with_history(true)]
#[case::without_history(false)]
#[tokio::test]
async fn legacy_query(#[case] include_history: bool) {
    let wfid = "fake_wf_id";
    let query_resp = "response";
    let t = canned_histories::single_timer("1");
    let mut header = HashMap::new();
    header.insert("head".to_string(), Payload::from(b"er"));
    let tasks = VecDeque::from(vec![
        hist_to_poll_resp(&t, wfid.to_owned(), 1.into(), TEST_Q.to_string()),
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), 1.into(), TEST_Q.to_string());
            pr.query = Some(WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
                header: Some(header.into()),
            });
            if !include_history {
                pr.history = Some(History { events: vec![] });
            }
            pr
        },
        hist_to_poll_resp(&t, wfid.to_owned(), 2.into(), TEST_Q.to_string()),
    ]);
    let mut mock_gateway = mock_gateway();
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_gateway
        .expect_respond_legacy_query()
        .times(1)
        .returning(move |_, _| Ok(RespondQueryTaskCompletedResponse::default()));

    let mut mock = MocksHolder::from_gateway_with_responses(mock_gateway, tasks, vec![]);
    if !include_history {
        mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    }
    let worker = mock_worker(mock);

    let first_wft = || async {
        let task = worker.poll_workflow_activation().await.unwrap();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                task.run_id,
                start_timer_cmd(1, Duration::from_secs(1)),
            ))
            .await
            .unwrap();
    };
    let clear_eviction = || async {
        let t = worker.poll_workflow_activation().await.unwrap();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(t.run_id))
            .await
            .unwrap();
    };

    first_wft().await;

    if include_history {
        clear_eviction().await;
        first_wft().await;
    }

    let task = worker.poll_workflow_activation().await.unwrap();
    // Poll again, and we end up getting a `query` field query response
    let query = assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
        }] => q
    );
    assert_eq!(query.headers.get("head").unwrap(), &b"er".into());
    // Complete the query
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            QueryResult {
                query_id: query.query_id.clone(),
                variant: Some(
                    QuerySuccess {
                        response: Some(query_resp.into()),
                    }
                    .into(),
                ),
            }
            .into(),
        ))
        .await
        .unwrap();

    if include_history {
        clear_eviction().await;
        first_wft().await;
    }

    let task = worker.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }]
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            task.run_id,
            vec![CompleteWorkflowExecution { result: None }.into()],
        ))
        .await
        .unwrap();
    worker.shutdown().await;
}

#[rstest::rstest]
#[case::one_query(1)]
#[case::multiple_queries(3)]
#[tokio::test]
async fn new_queries(#[case] num_queries: usize) {
    let wfid = "fake_wf_id";
    let query_resp = "response";
    let t = canned_histories::single_timer("1");
    let mut header = HashMap::new();
    header.insert("head".to_string(), Payload::from(b"er"));
    let tasks = VecDeque::from(vec![
        hist_to_poll_resp(&t, wfid.to_owned(), 1.into(), TEST_Q.to_string()),
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), 2.into(), TEST_Q.to_string());
            pr.queries = HashMap::new();
            for i in 1..=num_queries {
                pr.queries.insert(
                    format!("q{}", i),
                    WorkflowQuery {
                        query_type: "query-type".to_string(),
                        query_args: Some(b"hi".into()),
                        header: Some(header.clone().into()),
                    },
                );
            }
            pr
        },
    ]);
    let mut mock_gateway = mock_gateway();
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_gateway.expect_respond_legacy_query().times(0);

    let mut mock = MocksHolder::from_gateway_with_responses(mock_gateway, tasks, vec![]);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs[0],
        WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }
    );
    for i in 1..=num_queries {
        assert_matches!(
            task.jobs[i],
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(ref q)),
            } => {
                assert_eq!(q.headers.get("head").unwrap(), &b"er".into());
            }
        );
    }
    let mut qresults: Vec<_> = (1..=num_queries)
        .map(|i| {
            QueryResult {
                query_id: format!("q{}", i),
                variant: Some(
                    QuerySuccess {
                        response: Some(query_resp.into()),
                    }
                    .into(),
                ),
            }
            .into()
        })
        .collect();
    qresults.push(CompleteWorkflowExecution { result: None }.into());
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        qresults,
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn legacy_query_failure_on_wft_failure() {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("1");
    let tasks = VecDeque::from(vec![
        hist_to_poll_resp(&t, wfid.to_owned(), 1.into(), TEST_Q.to_string()),
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), 1.into(), TEST_Q.to_string());
            pr.query = Some(WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
                header: None,
            });
            pr.history = Some(History { events: vec![] });
            pr
        },
        hist_to_poll_resp(&t, wfid.to_owned(), 2.into(), TEST_Q.to_string()),
    ]);
    let mut mock_gateway = mock_gateway();
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_gateway
        .expect_respond_legacy_query()
        .times(1)
        .returning(move |_, _| Ok(RespondQueryTaskCompletedResponse::default()));

    let mut mock = MocksHolder::from_gateway_with_responses(mock_gateway, tasks, vec![]);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    // Poll again, and we end up getting a `query` field query response
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
        }] => q
    );
    // Fail wft which should result in query being failed
    core.complete_workflow_activation(WorkflowActivationCompletion::fail(
        task.run_id,
        Failure {
            message: "Ahh i broke".to_string(),
            ..Default::default()
        },
    ))
    .await
    .unwrap();

    core.shutdown().await;
}

#[rstest::rstest]
#[tokio::test]
async fn legacy_query_after_complete(#[values(false, true)] full_history: bool) {
    let wfid = "fake_wf_id";
    let t = if full_history {
        canned_histories::single_timer_wf_completes("1")
    } else {
        let mut t = canned_histories::single_timer("1");
        t.add_workflow_task_completed();
        t
    };
    let query_with_hist_task = {
        let mut pr = hist_to_poll_resp(
            &t,
            wfid.to_owned(),
            ResponseType::AllHistory,
            TEST_Q.to_string(),
        );
        pr.query = Some(WorkflowQuery {
            query_type: "query-type".to_string(),
            query_args: Some(b"hi".into()),
            header: None,
        });
        pr
    };
    let tasks = VecDeque::from(vec![
        hist_to_poll_resp(
            &t,
            wfid.to_owned(),
            ResponseType::AllHistory,
            TEST_Q.to_string(),
        ),
        query_with_hist_task.clone(),
        query_with_hist_task,
    ]);
    let mut mock_gateway = mock_gateway();
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_gateway
        .expect_respond_legacy_query()
        .times(2)
        .returning(move |_, _| Ok(RespondQueryTaskCompletedResponse::default()));

    let mut mock = MocksHolder::from_gateway_with_responses(mock_gateway, tasks, vec![]);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();

    // We should get queries two times
    for _ in 1..=2 {
        let task = core.poll_workflow_activation().await.unwrap();
        let query = assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
            }] => q
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            QueryResult {
                query_id: query.query_id.clone(),
                variant: Some(
                    QuerySuccess {
                        response: Some("whatever".into()),
                    }
                    .into(),
                ),
            }
            .into(),
        ))
        .await
        .unwrap();
    }

    core.shutdown().await;
}
