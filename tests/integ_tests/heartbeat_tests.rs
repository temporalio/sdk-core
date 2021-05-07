use assert_matches::assert_matches;
use integ_test_helpers::{init_core_and_create_wf, schedule_activity_cmd};
use std::time::Duration;
use temporal_sdk_core::protos::coresdk::{
    activity_result::{self, activity_result as act_res, ActivityResult},
    activity_task::activity_task as act_task,
    common::Payload,
    workflow_activation::{wf_activation_job, ResolveActivity, WfActivationJob},
    workflow_commands::{ActivityCancellationType, CompleteWorkflowExecution},
    workflow_completion::WfActivationCompletion,
    ActivityHeartbeat, ActivityTaskCompletion,
};
use temporal_sdk_core::IntoCompletion;
use tokio::time::sleep;

#[tokio::test]
async fn activity_heartbeat() {
    let (core, task_q) = init_core_and_create_wf("activity_heartbeat").await;
    let activity_id = "act-1";
    let task = core.poll_workflow_task().await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_task(
        schedule_activity_cmd(
            &task_q,
            activity_id,
            ActivityCancellationType::TryCancel,
            Duration::from_secs(60),
            Duration::from_secs(1),
        )
        .into_completion(task.task_token),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(
        task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    // Heartbeat timeout is set to 1 second, this loop is going to send heartbeat every 100ms.
    // Activity shouldn't timeout since we are sending heartbeats regularly, however if we didn't
    // send heartbeats activity would have timed out as it takes 2 sec to execute this loop.
    for _ in 0u8..20 {
        sleep(Duration::from_millis(100)).await;
        core.record_activity_heartbeat(ActivityHeartbeat {
            task_token: task.task_token.clone(),
            details: vec![],
        });
    }

    let response_payload = Payload {
        data: b"hello ".to_vec(),
        metadata: Default::default(),
    };
    // Complete activity successfully.
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        result: Some(ActivityResult::ok(response_payload.clone())),
    })
    .await
    .unwrap();
    // Poll workflow task and verify that activity has succeeded.
    let task = core.poll_workflow_task().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(
                    ResolveActivity {activity_id: a_id, result: Some(ActivityResult{
                    status: Some(act_res::Status::Completed(activity_result::Success{result: Some(r)})),
                     ..})}
                )),
            },
        ] => {
            assert_eq!(a_id, activity_id);
            assert_eq!(r, &response_payload);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap()
}
