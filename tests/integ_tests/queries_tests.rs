use assert_matches::assert_matches;
use std::time::Duration;
use temporal_sdk_core::protos::coresdk::{
    workflow_activation::{wf_activation_job, WfActivationJob},
    workflow_commands::{CompleteWorkflowExecution, QueryResult, QuerySuccess, StartTimer},
    workflow_completion::WfActivationCompletion,
};
use temporal_sdk_core::protos::temporal::api::query::v1::WorkflowQuery;
use temporal_sdk_core::tracing_init;
use test_utils::{init_core_and_create_wf, with_gw, CoreWfStarter, GwApi};

#[tokio::test]
async fn simple_query_legacy() {
    tracing_init();
    let workflow_id = "simple_query";
    let query_resp = b"response";
    let (core, task_q) = init_core_and_create_wf(workflow_id).await;
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![
            StartTimer {
                timer_id: "timer-1".to_string(),
                start_to_fire_timeout: Some(Duration::from_millis(500).into()),
            }
            .into(),
            StartTimer {
                timer_id: "timer-2".to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(3).into()),
            }
            .into(),
        ],
        task.run_id.clone(),
    ))
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    // Query after timer should have fired and there should be new WFT
    let query_fut = with_gw(core.as_ref(), |gw: GwApi| async move {
        gw.query_workflow_execution(
            workflow_id.to_string(),
            task.run_id.to_string(),
            WorkflowQuery {
                query_type: "myquery".to_string(),
                query_args: Some(b"hi".into()),
            },
        )
        .await
        .unwrap()
    });
    let workflow_completions_future = async {
        // Give query a beat to get going
        tokio::time::sleep(Duration::from_millis(400)).await;
        // This poll *should* have the `queries` field populated, but doesn't, seemingly due to
        // a server bug. So, complete the WF task of the first timer firing with empty commands
        let task = core.poll_workflow_task(&task_q).await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(_)),
            }]
        );
        core.complete_workflow_task(WfActivationCompletion::from_cmds(vec![], task.run_id))
            .await
            .unwrap();
        let task = core.poll_workflow_task(&task_q).await.unwrap();
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
        // Finish the workflow
        let task = core.poll_workflow_task(&task_q).await.unwrap();
        core.complete_workflow_task(WfActivationCompletion::from_cmds(
            vec![CompleteWorkflowExecution { result: None }.into()],
            task.run_id,
        ))
        .await
        .unwrap();
    };
    let (q_resp, _) = tokio::join!(query_fut, workflow_completions_future);
    // Ensure query response is as expected
    assert_eq!(&q_resp.unwrap()[0].data, query_resp);
}

#[tokio::test]
async fn queries_in_wf_task() {
    tracing_init();
    let workflow_id = "queries_in_wf_task";
    let q1_resp = b"query_1_resp";
    let q2_resp = b"query_2_resp";
    let mut wf_starter = CoreWfStarter::new(workflow_id);
    // Easiest way I discovered to reliably trigger new query path is with a WFT timeout
    wf_starter.wft_timeout(Duration::from_secs(1));
    let core = wf_starter.get_core().await;
    let task_q = wf_starter.get_task_queue();
    wf_starter.start_wf().await;

    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![StartTimer {
            timer_id: "timer-1".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(500).into()),
        }
        .into()],
        task.run_id.clone(),
    ))
    .await
    .unwrap();
    // Poll for a task we will time out
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    dbg!(&task);
    tokio::time::sleep(Duration::from_secs(2)).await;

    let run_id = task.run_id.to_string();
    let q1_fut = with_gw(core.as_ref(), |gw: GwApi| async move {
        gw.query_workflow_execution(
            workflow_id.to_string(),
            run_id,
            WorkflowQuery {
                query_type: "query_1".to_string(),
                query_args: Some(b"hi 1".into()),
            },
        )
        .await
        .unwrap()
    });
    let run_id = task.run_id.to_string();
    let q2_fut = with_gw(core.as_ref(), |gw: GwApi| async move {
        gw.query_workflow_execution(
            workflow_id.to_string(),
            run_id,
            WorkflowQuery {
                query_type: "query_2".to_string(),
                query_args: Some(b"hi 2".into()),
            },
        )
        .await
        .unwrap()
    });
    let workflow_completions_future = async {
        // Complete now-timed-out task (add a new timer)
        core.complete_workflow_task(WfActivationCompletion::from_cmds(vec![], task.run_id))
            .await
            .unwrap();
        let mut seen_q1 = false;
        let mut seen_q2 = false;
        while !seen_q1 || !seen_q2 {
            dbg!("Polling");
            let task = core.poll_workflow_task(&task_q).await.unwrap();
            dbg!("Past second poll");
            // There should be a query job (really, there should be both... server only sends one?)
            let query = assert_matches!(
                task.jobs.as_slice(),
                [WfActivationJob {
                    variant: Some(wf_activation_job::Variant::QueryWorkflow(q)),
                }] => q
            );
            let resp = if query.query_id == "query_1" {
                seen_q1 = true;
                q1_resp
            } else {
                seen_q2 = true;
                q2_resp
            };
            // Complete the query
            core.complete_workflow_task(WfActivationCompletion::from_cmds(
                vec![
                    QueryResult {
                        query_id: query.query_id.clone(),
                        variant: Some(
                            QuerySuccess {
                                response: Some(resp.into()),
                            }
                            .into(),
                        ),
                    }
                    .into(),
                    // Also start a timer to avoid stalling out -- bug where query can be dropped
                    StartTimer {
                        timer_id: "timer-2".to_string(),
                        start_to_fire_timeout: Some(Duration::from_millis(500).into()),
                    }
                    .into(),
                ],
                task.run_id,
            ))
            .await
            .unwrap();
        }
        // Finish the workflow
        let task = core.poll_workflow_task(&task_q).await.unwrap();
        core.complete_workflow_task(WfActivationCompletion::from_cmds(
            vec![CompleteWorkflowExecution { result: None }.into()],
            task.run_id,
        ))
        .await
        .unwrap();
    };
    let (q1_res, q2_res, _) = tokio::join!(q1_fut, q2_fut, workflow_completions_future);
    // Ensure query responses are as expected
    assert_eq!(&q1_res.unwrap()[0].data, q1_resp);
    assert_eq!(&q2_res.unwrap()[0].data, q2_resp);
}
