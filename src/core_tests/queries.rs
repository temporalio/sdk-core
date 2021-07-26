use std::collections::{HashMap, VecDeque};

use crate::{
    pollers::MockServerGatewayApis,
    protos::{
        coresdk::{
            common::UserCodeFailure,
            workflow_activation::{wf_activation_job, WfActivationJob},
            workflow_commands::{CompleteWorkflowExecution, QueryResult, QuerySuccess, StartTimer},
            workflow_completion::WfActivationCompletion,
        },
        temporal::api::history::v1::History,
        temporal::api::query::v1::WorkflowQuery,
        temporal::api::workflowservice::v1::{
            RespondQueryTaskCompletedResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
    test_help::{canned_histories, hist_to_poll_resp, mock_core_with_opts, MocksHolder, TEST_Q},
    Core, CoreInitOptionsBuilder,
};

#[rstest::rstest]
#[case::with_history(true)]
#[case::without_history(false)]
#[tokio::test]
async fn legacy_query(#[case] include_history: bool) {
    let wfid = "fake_wf_id";
    let query_resp = "response";
    let t = canned_histories::single_timer("fake_timer");
    let tasks = VecDeque::from(vec![
        hist_to_poll_resp(&t, wfid.to_owned(), 1.into(), TEST_Q.to_string()),
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), 1.into(), TEST_Q.to_string());
            pr.query = Some(WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
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

    let mut opts = CoreInitOptionsBuilder::default();
    if !include_history {
        opts.max_cached_workflows(10_usize);
    }
    let core = mock_core_with_opts(
        MocksHolder::from_gateway_with_responses(mock_gateway, tasks, vec![].into()),
        opts,
    );

    let first_wft = || async {
        let task = core.poll_workflow_task(TEST_Q).await.unwrap();
        core.complete_workflow_task(WfActivationCompletion::from_cmd(
            StartTimer {
                timer_id: "fake_timer".to_string(),
                ..Default::default()
            }
            .into(),
            task.run_id,
        ))
        .await
        .unwrap();
    };
    let clear_eviction = || async {
        let t = core.poll_workflow_task(TEST_Q).await.unwrap();
        core.complete_workflow_task(WfActivationCompletion::empty(t.run_id))
            .await
            .unwrap();
        first_wft().await;
    };

    first_wft().await;

    if include_history {
        clear_eviction().await
    }

    let task = core.poll_workflow_task(TEST_Q).await.unwrap();
    // Poll again, and we end up getting a `query` field query response
    let query = assert_matches!(
        task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::QueryWorkflow(q)),
        }] => q
    );
    // Complete the query
    core.complete_workflow_task(WfActivationCompletion::from_cmd(
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
        task.run_id,
    ))
    .await
    .unwrap();

    if include_history {
        clear_eviction().await
    }

    let task = core.poll_workflow_task(TEST_Q).await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.run_id,
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
    let t = canned_histories::single_timer("fake_timer");
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

    let mut opts = CoreInitOptionsBuilder::default();
    opts.max_cached_workflows(10_usize);
    let core = mock_core_with_opts(
        MocksHolder::from_gateway_with_responses(mock_gateway, tasks, vec![].into()),
        opts,
    );

    let task = core.poll_workflow_task(TEST_Q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmd(
        StartTimer {
            timer_id: "fake_timer".to_string(),
            ..Default::default()
        }
        .into(),
        task.run_id,
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_task(TEST_Q).await.unwrap();
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
    core.complete_workflow_task(WfActivationCompletion::from_cmds(qresults, task.run_id))
        .await
        .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn legacy_query_failure_on_wft_failure() {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("fake_timer");
    let tasks = VecDeque::from(vec![
        hist_to_poll_resp(&t, wfid.to_owned(), 1.into(), TEST_Q.to_string()),
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), 1.into(), TEST_Q.to_string());
            pr.query = Some(WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
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

    let mut opts = CoreInitOptionsBuilder::default();
    opts.max_cached_workflows(10_usize);
    let core = mock_core_with_opts(
        MocksHolder::from_gateway_with_responses(mock_gateway, tasks, vec![].into()),
        opts,
    );

    let task = core.poll_workflow_task(TEST_Q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmd(
        StartTimer {
            timer_id: "fake_timer".to_string(),
            ..Default::default()
        }
        .into(),
        task.run_id,
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_task(TEST_Q).await.unwrap();
    // Poll again, and we end up getting a `query` field query response
    assert_matches!(
        task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::QueryWorkflow(q)),
        }] => q
    );
    // Fail wft which should result in query being failed
    core.complete_workflow_task(WfActivationCompletion::fail(
        task.run_id,
        UserCodeFailure {
            message: "Ahh i broke".to_string(),
            ..Default::default()
        },
    ))
    .await
    .unwrap();

    core.shutdown().await;
}
