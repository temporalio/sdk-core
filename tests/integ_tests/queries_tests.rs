use assert_matches::assert_matches;
use std::time::Duration;
use temporal_sdk_core::protos::coresdk::{
    workflow_activation::{wf_activation_job, WfActivationJob},
    workflow_commands::{CompleteWorkflowExecution, QueryResult, QuerySuccess, StartTimer},
    workflow_completion::WfActivationCompletion,
};
use temporal_sdk_core::protos::temporal::api::query::v1::WorkflowQuery;
use temporal_sdk_core::tracing_init;
use test_utils::{init_core_and_create_wf, with_gw, GwApi};

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
