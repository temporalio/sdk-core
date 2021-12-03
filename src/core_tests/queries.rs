use crate::{
    pollers::MockServerGatewayApis,
    test_help::{
        canned_histories, hist_to_poll_resp, mock_core, MocksHolder, ResponseType, TEST_Q,
    },
    Core,
};
use std::collections::{HashMap, VecDeque};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{wf_activation_job, WfActivationJob},
        workflow_commands::{CompleteWorkflowExecution, QueryResult, QuerySuccess, StartTimer},
        workflow_completion::WfActivationCompletion,
    },
    temporal::api::{
        failure::v1::Failure,
        history::v1::History,
        query::v1::WorkflowQuery,
        workflowservice::v1::{
            RespondQueryTaskCompletedResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
};

#[rstest::rstest]
#[case::with_history(true)]
#[case::without_history(false)]
#[tokio::test]
async fn legacy_query(#[case] include_history: bool) {
    let wfid = "fake_wf_id";
    let query_resp = "response";
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
            if !include_history {
                pr.history = Some(History { events: vec![] });
            }
            pr
        },
        hist_to_poll_resp(&t, wfid.to_owned(), 2.into(), TEST_Q.to_string()),
    ]);
    let mut mock_gateway = MockServerGatewayApis::new();
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_gateway
        .expect_respond_legacy_query()
        .times(1)
        .returning(move |_, _| Ok(RespondQueryTaskCompletedResponse::default()));

    let mut mock = MocksHolder::from_gateway_with_responses(mock_gateway, tasks, vec![]);
    if !include_history {
        mock.worker_cfg(TEST_Q, |wc| wc.max_cached_workflows = 10);
    }
    let core = mock_core(mock);

    let first_wft = || async {
        let task = core.poll_workflow_activation(TEST_Q).await.unwrap();
        core.complete_workflow_activation(WfActivationCompletion::from_cmd(
            TEST_Q,
            task.run_id,
            StartTimer {
                seq: 1,
                ..Default::default()
            }
            .into(),
        ))
        .await
        .unwrap();
    };
    let clear_eviction = || async {
        let t = core.poll_workflow_activation(TEST_Q).await.unwrap();
        core.complete_workflow_activation(WfActivationCompletion::empty(TEST_Q, t.run_id))
            .await
            .unwrap();
    };

    first_wft().await;

    if include_history {
        clear_eviction().await;
        first_wft().await;
    }

    let task = core.poll_workflow_activation(TEST_Q).await.unwrap();
    // Poll again, and we end up getting a `query` field query response
    let query = assert_matches!(
        task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::QueryWorkflow(q)),
        }] => q
    );
    // Complete the query
    core.complete_workflow_activation(WfActivationCompletion::from_cmd(
        TEST_Q,
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

    let task = core.poll_workflow_activation(TEST_Q).await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_workflow_activation(WfActivationCompletion::from_cmds(
        TEST_Q,
        task.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[rstest::rstest]
#[case::one_query(1)]
#[case::multiple_queries(3)]
#[tokio::test]
async fn new_queries(#[case] num_queries: usize) {
    let wfid = "fake_wf_id";
    let query_resp = "response";
    let t = canned_histories::single_timer("1");
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
                        header: None,
                    },
                );
            }
            pr
        },
    ]);
    let mut mock_gateway = MockServerGatewayApis::new();
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_gateway.expect_respond_legacy_query().times(0);

    let mut mock = MocksHolder::from_gateway_with_responses(mock_gateway, tasks, vec![]);
    mock.worker_cfg(TEST_Q, |wc| wc.max_cached_workflows = 10);
    let core = mock_core(mock);

    let task = core.poll_workflow_activation(TEST_Q).await.unwrap();
    core.complete_workflow_activation(WfActivationCompletion::from_cmd(
        TEST_Q,
        task.run_id,
        StartTimer {
            seq: 1,
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation(TEST_Q).await.unwrap();
    assert_matches!(
        task.jobs[0],
        WfActivationJob {
            variant: Some(wf_activation_job::Variant::FireTimer(_)),
        }
    );
    for i in 1..=num_queries {
        assert_matches!(
            task.jobs[i],
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::QueryWorkflow(_)),
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
    core.complete_workflow_activation(WfActivationCompletion::from_cmds(
        TEST_Q,
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
    let mut mock_gateway = MockServerGatewayApis::new();
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_gateway
        .expect_respond_legacy_query()
        .times(1)
        .returning(move |_, _| Ok(RespondQueryTaskCompletedResponse::default()));

    let mut mock = MocksHolder::from_gateway_with_responses(mock_gateway, tasks, vec![]);
    mock.worker_cfg(TEST_Q, |wc| wc.max_cached_workflows = 10);
    let core = mock_core(mock);

    let task = core.poll_workflow_activation(TEST_Q).await.unwrap();
    core.complete_workflow_activation(WfActivationCompletion::from_cmd(
        TEST_Q,
        task.run_id,
        StartTimer {
            seq: 1,
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation(TEST_Q).await.unwrap();
    // Poll again, and we end up getting a `query` field query response
    assert_matches!(
        task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::QueryWorkflow(q)),
        }] => q
    );
    // Fail wft which should result in query being failed
    core.complete_workflow_activation(WfActivationCompletion::fail(
        TEST_Q,
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

#[tokio::test]
async fn legacy_query_with_full_history_after_complete() {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer_wf_completes("1");
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
    let mut mock_gateway = MockServerGatewayApis::new();
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
    mock_gateway
        .expect_respond_legacy_query()
        .times(2)
        .returning(move |_, _| Ok(RespondQueryTaskCompletedResponse::default()));

    let mut mock = MocksHolder::from_gateway_with_responses(mock_gateway, tasks, vec![]);
    mock.worker_cfg(TEST_Q, |wc| wc.max_cached_workflows = 10);
    let core = mock_core(mock);

    let task = core.poll_workflow_activation(TEST_Q).await.unwrap();
    core.complete_workflow_activation(WfActivationCompletion::from_cmd(
        TEST_Q,
        task.run_id,
        StartTimer {
            seq: 1,
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation(TEST_Q).await.unwrap();
    core.complete_workflow_activation(WfActivationCompletion::from_cmds(
        TEST_Q,
        task.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();

    // We should get queries two times
    for _ in 1..=2 {
        let task = core.poll_workflow_activation(TEST_Q).await.unwrap();
        let query = assert_matches!(
            task.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::QueryWorkflow(q)),
            }] => q
        );
        core.complete_workflow_activation(WfActivationCompletion::from_cmd(
            TEST_Q,
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
