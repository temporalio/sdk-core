use assert_matches::assert_matches;
use std::time::Duration;
use temporal_sdk_core::ServerGatewayApis;
use temporal_sdk_core_api::errors::{PollActivityError, PollWfError};
use temporal_sdk_core_protos::{
    coresdk::{workflow_commands::StartTimer, workflow_completion::WorkflowActivationCompletion},
    temporal::api::workflowservice::v1::PollWorkflowTaskQueueResponse,
};
use temporal_sdk_core_test_utils::{
    history_from_proto_binary, history_replay::mock_gateway_from_history,
    init_core_replay_preloaded, CoreTestHelpers,
};
use tokio::join;

#[tokio::test]
async fn timer_workflow_replay() {
    let (core, task_q) = init_core_replay_preloaded(
        "fail_wf_task",
        &history_from_proto_binary("histories/fail_wf_task.bin")
            .await
            .unwrap(),
    );
    let task = core.poll_workflow_activation(&task_q).await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        &task_q,
        task.run_id,
        vec![StartTimer {
            seq: 0,
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
        }
        .into()],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation(&task_q).await.unwrap();
    // Verify that an in-progress poll is interrupted by completion finishing processing history
    let act_poll_fut = async {
        assert_matches!(
            core.poll_activity_task(&task_q).await,
            Err(PollActivityError::ShutDown)
        );
    };
    let poll_fut = async {
        assert_matches!(
            core.poll_workflow_activation(&task_q).await,
            Err(PollWfError::ShutDown)
        );
    };
    let complete_fut = async {
        core.complete_execution(&task_q, &task.run_id).await;
    };
    join!(act_poll_fut, poll_fut, complete_fut);

    // Subsequent polls should still return shutdown
    assert_matches!(
        core.poll_workflow_activation(&task_q).await,
        Err(PollWfError::ShutDown)
    );

    core.shutdown().await;
}

// Regression test to verify mock replayers don't interfere with each other
#[tokio::test]
async fn two_cores_replay() {
    let hist = history_from_proto_binary("histories/fail_wf_task.bin")
        .await
        .unwrap();

    let mock_1 = mock_gateway_from_history(&hist);
    let mock_2 = mock_gateway_from_history(&hist);
    assert_ne!(
        mock_1
            .poll_workflow_task("a".to_string(), false)
            .await
            .unwrap(),
        PollWorkflowTaskQueueResponse::default()
    );
    assert_ne!(
        mock_2
            .poll_workflow_task("b".to_string(), false)
            .await
            .unwrap(),
        PollWorkflowTaskQueueResponse::default()
    );
}
