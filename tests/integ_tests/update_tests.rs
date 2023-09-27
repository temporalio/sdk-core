use assert_matches::assert_matches;
use temporal_client::WorkflowClientTrait;
use temporal_sdk_core_protos::coresdk::workflow_completion::WorkflowActivationCompletion;
use temporal_sdk_core_test_utils::{init_core_and_create_wf, WorkerTestHelpers};
use tokio::join;

#[tokio::test]
async fn update_workflow() {
    let mut starter = init_core_and_create_wf("update_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue().to_string();

    let update_id = "some_update";
    let res = core.poll_workflow_activation().await.unwrap();
    // Task is completed with no commands
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id.clone(),
        vec![],
    ))
    .await
    .unwrap();

    // Send the update to the server
    let update_task = async {
        client
            .update_workflow_execution(
                workflow_id.to_string(),
                res.run_id.to_string(),
                update_id.to_string(),
                Some("hi".into()),
            )
            .await
            .unwrap();
    };

    let processing_task = async {
        let mut res = core.poll_workflow_activation().await.unwrap();
        dbg!(&res);
        // assert_matches!(
        //     res.jobs.as_slice(),
        //     [WorkflowActivationJob {
        //         variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        //     },]
        // );
        // core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        //     res.run_id,
        //     vec![],
        // ))
        // .await
        // .unwrap();
        core.complete_execution(&res.run_id).await;
    };
    join!(update_task, processing_task);
}
