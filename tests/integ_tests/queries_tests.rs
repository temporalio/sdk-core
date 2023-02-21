use assert_matches::assert_matches;
use futures::{prelude::stream::FuturesUnordered, FutureExt, StreamExt};
use std::time::Duration;
use temporal_client::WorkflowClientTrait;
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{workflow_activation_job, WorkflowActivationJob},
        workflow_commands::{QueryResult, QuerySuccess, StartTimer},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{failure::v1::Failure, query::v1::WorkflowQuery},
};
use temporal_sdk_core_test_utils::{
    drain_pollers_and_shutdown, init_core_and_create_wf, WorkerTestHelpers,
};

#[tokio::test]
async fn simple_query_legacy() {
    let query_resp = b"response";
    let mut starter = init_core_and_create_wf("simple_query_legacy").await;
    let core = starter.get_worker().await;
    let workflow_id = starter.get_task_queue().to_string();
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id.clone(),
        vec![
            StartTimer {
                seq: 0,
                start_to_fire_timeout: Some(prost_dur!(from_millis(500))),
            }
            .into(),
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(prost_dur!(from_secs(3))),
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    // Query after timer should have fired and there should be new WFT
    let query_fut = async {
        starter
            .get_client()
            .await
            .query_workflow_execution(
                workflow_id,
                task.run_id.to_string(),
                WorkflowQuery {
                    query_type: "myquery".to_string(),
                    query_args: Some(b"hi".into()),
                    header: None,
                },
            )
            .await
            .unwrap()
    };
    let workflow_completions_future = async {
        // Give query a beat to get going
        tokio::time::sleep(Duration::from_millis(400)).await;
        // This poll *should* have the `queries` field populated, but doesn't, seemingly due to
        // a server bug. So, complete the WF task of the first timer firing with empty commands
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::FireTimer(_)),
            }]
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            task.run_id,
            vec![],
        ))
        .await
        .unwrap();
        let task = core.poll_workflow_activation().await.unwrap();
        // Poll again, and we end up getting a `query` field query response
        let query = assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
            }] => q
        );
        // Complete the query
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
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
        // Finish the workflow
        let task = core.poll_workflow_activation().await.unwrap();
        core.complete_execution(&task.run_id).await;
    };
    let (q_resp, _) = tokio::join!(query_fut, workflow_completions_future);
    // Ensure query response is as expected
    assert_eq!(&q_resp.unwrap()[0].data, query_resp);
}

#[rstest]
#[case::no_eviction(false)]
#[case::with_eviction(true)]
#[tokio::test]
async fn query_after_execution_complete(#[case] do_evict: bool) {
    let query_resp = b"response";
    let mut starter =
        init_core_and_create_wf(&format!("query_after_execution_complete-{do_evict}")).await;
    let core = &starter.get_worker().await;
    let workflow_id = &starter.get_task_queue().to_string();

    let do_workflow = |go_until_query: bool| async move {
        loop {
            let task = core.poll_workflow_activation().await.unwrap();

            // When we see the query, handle it.
            if go_until_query {
                if let [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::QueryWorkflow(query)),
                }] = task.jobs.as_slice()
                {
                    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
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
                    break "".to_string();
                }
            }

            if matches!(
                task.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
                }]
            ) {
                core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
                    .await
                    .unwrap();
                continue;
            }
            if matches!(
                task.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::StartWorkflow(_)),
                }]
            ) {
                core.complete_timer(&task.run_id, 1, Duration::from_millis(500))
                    .await;
            } else {
                core.complete_execution(&task.run_id).await;
            }
            if !go_until_query {
                break task.run_id;
            }
        }
    };

    let run_id = &do_workflow(false).await;
    assert!(!run_id.is_empty());

    if do_evict {
        core.request_workflow_eviction(run_id);
    }
    // Spam some queries (sending multiple queries after WF closed covers a possible path where
    // we could screw-up re-applying the final WFT)
    let mut query_futs = FuturesUnordered::new();
    for _ in 0..3 {
        let gw = starter.get_client().await.clone();
        let query_fut = async move {
            let q_resp = gw
                .query_workflow_execution(
                    workflow_id.to_string(),
                    run_id.to_string(),
                    WorkflowQuery {
                        query_type: "myquery".to_string(),
                        query_args: Some(b"hi".into()),
                        header: None,
                    },
                )
                .await
                .unwrap();
            // Ensure query response is as expected
            assert_eq!(q_resp.unwrap()[0].data, query_resp);
        };

        query_futs.push(query_fut.boxed());
        query_futs.push(do_workflow(true).map(|_| ()).boxed());
    }
    while query_futs.next().await.is_some() {}
    drain_pollers_and_shutdown(core).await;
}

#[tokio::test]
async fn fail_legacy_query() {
    let query_err = "oh no broken";
    let mut starter = init_core_and_create_wf("fail_legacy_query").await;
    let core = starter.get_worker().await;
    let workflow_id = starter.get_task_queue().to_string();
    let task = core.poll_workflow_activation().await.unwrap();
    let t1_resp = vec![
        StartTimer {
            seq: 1,
            start_to_fire_timeout: Some(prost_dur!(from_millis(500))),
        }
        .into(),
        StartTimer {
            seq: 2,
            start_to_fire_timeout: Some(prost_dur!(from_secs(3))),
        }
        .into(),
    ];
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id.clone(),
        t1_resp.clone(),
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    // Query after timer should have fired and there should be new WFT
    let query_fut = async {
        starter
            .get_client()
            .await
            .query_workflow_execution(
                workflow_id.to_string(),
                task.run_id.to_string(),
                WorkflowQuery {
                    query_type: "myquery".to_string(),
                    query_args: Some(b"hi".into()),
                    header: None,
                },
            )
            .await
            .unwrap_err()
    };
    let workflow_completions_future = async {
        // Give query a beat to get going
        tokio::time::sleep(Duration::from_millis(400)).await;
        // This poll *should* have the `queries` field populated, but doesn't, seemingly due to
        // a server bug. So, complete the WF task of the first timer firing with empty commands
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::FireTimer(_)),
            }]
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            task.run_id,
            vec![],
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
        // Fail this task
        core.complete_workflow_activation(WorkflowActivationCompletion::fail(
            task.run_id,
            Failure {
                message: query_err.to_string(),
                ..Default::default()
            },
        ))
        .await
        .unwrap();
        // Finish the workflow (handling cache removal)
        let task = core.poll_workflow_activation().await.unwrap();
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
            .await
            .unwrap();
        let task = core.poll_workflow_activation().await.unwrap();
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            task.run_id,
            t1_resp.clone(),
        ))
        .await
        .unwrap();
        let task = core.poll_workflow_activation().await.unwrap();
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
            .await
            .unwrap();
        let task = core.poll_workflow_activation().await.unwrap();
        core.complete_execution(&task.run_id).await;
    };
    let (q_resp, _) = tokio::join!(query_fut, workflow_completions_future);
    // Ensure query response is a failure and has the right message
    assert_eq!(q_resp.message(), query_err);
}
