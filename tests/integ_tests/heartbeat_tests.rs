use assert_matches::assert_matches;
use std::time::Duration;
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{
            self, activity_resolution as act_res, ActivityExecutionResult, ActivityResolution,
        },
        activity_task::activity_task,
        workflow_activation::{workflow_activation_job, ResolveActivity, WorkflowActivationJob},
        workflow_commands::{ActivityCancellationType, ScheduleActivity},
        workflow_completion::WorkflowActivationCompletion,
        ActivityHeartbeat, ActivityTaskCompletion, IntoCompletion,
    },
    temporal::api::common::v1::{Payload, RetryPolicy},
};
use temporal_sdk_core_test_utils::{
    init_core_and_create_wf, schedule_activity_cmd, WorkerTestHelpers,
};
use tokio::time::sleep;

#[tokio::test]
async fn activity_heartbeat() {
    let mut starter = init_core_and_create_wf("activity_heartbeat").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_activation(
        schedule_activity_cmd(
            0,
            task_q,
            activity_id,
            ActivityCancellationType::TryCancel,
            Duration::from_secs(60),
            Duration::from_secs(1),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(
        task.variant,
        Some(activity_task::Variant::Start(start_activity)) => {
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
        result: Some(ActivityExecutionResult::ok(response_payload.clone())),
    })
    .await
    .unwrap();
    // Poll workflow task and verify that activity has succeeded.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {seq, result: Some(ActivityResolution {
                    status: Some(act_res::Status::Completed(activity_result::Success{result: Some(r)})),
                     ..})}
                )),
            },
        ] => {
            assert_eq!(*seq, 0);
            assert_eq!(r, &response_payload);
        }
    );
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn many_act_fails_with_heartbeats() {
    let mut starter = init_core_and_create_wf("many_act_fails_with_heartbeats").await;
    let core = starter.get_worker().await;
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        ScheduleActivity {
            seq: 0,
            activity_id: activity_id.to_string(),
            activity_type: "test_act".to_string(),
            task_queue: starter.get_task_queue().to_string(),
            start_to_close_timeout: Some(Duration::from_secs(30).into()),
            retry_policy: Some(RetryPolicy {
                initial_interval: Some(Duration::from_millis(10).into()),
                backoff_coefficient: 1.0,
                maximum_attempts: 4,
                ..Default::default()
            }),
            heartbeat_timeout: Some(Duration::from_secs(1).into()),
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();

    // Multiple times, poll for the activity, heartbeat, and then immediately fail
    // Poll activity and verify that it's been scheduled with correct parameters
    for i in 0u8..=3 {
        let task = core.poll_activity_task().await.unwrap();
        let start_t = assert_matches!(task.variant, Some(activity_task::Variant::Start(s)) => s);

        core.record_activity_heartbeat(ActivityHeartbeat {
            task_token: task.task_token.clone(),
            details: vec![[i].into()],
        });

        let compl = if i == 3 {
            // Verify last hb was recorded
            assert_eq!(start_t.heartbeat_details, [[2].into()]);
            ActivityTaskCompletion {
                task_token: task.task_token,
                result: Some(ActivityExecutionResult::ok("passed".into())),
            }
        } else {
            if i != 0 {
                assert_eq!(start_t.heartbeat_details, [[i - 1].into()]);
            }
            ActivityTaskCompletion {
                task_token: task.task_token,
                result: Some(ActivityExecutionResult::fail(format!("Die on {i}").into())),
            }
        };
        core.complete_activity_task(compl).await.unwrap();
    }
    let task = core.poll_workflow_activation().await.unwrap();

    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(
                ResolveActivity {
                    result: Some(ActivityResolution {
                        status: Some(act_res::Status::Completed(activity_result::Success { .. })),
                        ..
                    }),
                    ..
                }
            )),
        },]
    );
    core.complete_execution(&task.run_id).await;
    core.shutdown().await;
}
