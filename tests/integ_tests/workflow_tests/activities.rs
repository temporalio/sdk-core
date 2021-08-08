use assert_matches::assert_matches;
use std::time::Duration;
use temporal_sdk_core::{
    protos::coresdk::{
        activity_result::{self, activity_result as act_res, ActivityResult},
        activity_task::activity_task as act_task,
        common::Payload,
        workflow_activation::{wf_activation_job, FireTimer, ResolveActivity, WfActivationJob},
        workflow_commands::{ActivityCancellationType, RequestCancelActivity, StartTimer},
        workflow_completion::WfActivationCompletion,
        ActivityTaskCompletion,
    },
    protos::temporal::api::common::v1::ActivityType,
    protos::temporal::api::enums::v1::RetryState,
    protos::temporal::api::failure::v1::{failure::FailureInfo, ActivityFailureInfo, Failure},
    IntoCompletion,
};
use test_utils::{init_core_and_create_wf, schedule_activity_cmd, CoreTestHelpers};
use tokio::time::sleep;

#[tokio::test]
async fn activity_workflow() {
    let (core, task_q) = init_core_and_create_wf("activity_workflow").await;
    let activity_id = "act-1";
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_task(
        schedule_activity_cmd(
            &task_q,
            activity_id,
            ActivityCancellationType::TryCancel,
            Duration::from_secs(60),
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters
    let task = core.poll_activity_task(&task_q).await.unwrap();
    assert_matches!(
        task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    let response_payload = Payload {
        data: b"hello ".to_vec(),
        metadata: Default::default(),
    };
    // Complete activity successfully.
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        task_queue: task_q.to_string(),
        result: Some(ActivityResult::ok(response_payload.clone())),
    })
    .await
    .unwrap();
    // Poll workflow task and verify that activity has succeeded.
    let task = core.poll_workflow_task(&task_q).await.unwrap();
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
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_non_retryable_failure() {
    let (core, task_q) = init_core_and_create_wf("activity_non_retryable_failure").await;
    let activity_id = "act-1";
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_task(
        schedule_activity_cmd(
            &task_q,
            activity_id,
            ActivityCancellationType::TryCancel,
            Duration::from_secs(60),
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters
    let task = core.poll_activity_task(&task_q).await.unwrap();
    assert_matches!(
        task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    // Fail activity with non-retryable error
    let failure = Failure::application_failure("activity failed".to_string(), true);
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        task_queue: task_q.to_string(),
        result: Some(ActivityResult {
            status: Some(activity_result::activity_result::Status::Failed(
                activity_result::Failure {
                    failure: Some(failure.clone()),
                },
            )),
        }),
    })
    .await
    .unwrap();
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(
                    ResolveActivity {activity_id: a_id, result: Some(ActivityResult{
                    status: Some(act_res::Status::Failed(activity_result::Failure{
                        failure: Some(f),
                    }))})}
                )),
            },
        ] => {
            assert_eq!(a_id, activity_id);
            assert_eq!(f, &Failure{
                message: "Activity task failed".to_owned(),
                cause: Some(Box::new(failure)),
                failure_info: Some(FailureInfo::ActivityFailureInfo(ActivityFailureInfo{
                    activity_id: "act-1".to_owned(),
                    activity_type: Some(ActivityType {
                        name: "test_activity".to_owned(),
                    }),
                    scheduled_event_id: 5,
                    started_event_id: 6,
                    identity: "integ_tester".to_owned(),
                    retry_state: RetryState::NonRetryableFailure as i32,
                })),
                ..Default::default()
            });
        }
    );
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_retry() {
    let (core, task_q) = init_core_and_create_wf("activity_retry").await;
    let activity_id = "act-1";
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_task(
        schedule_activity_cmd(
            &task_q,
            activity_id,
            ActivityCancellationType::TryCancel,
            Duration::from_secs(60),
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity 1st time
    let task = core.poll_activity_task(&task_q).await.unwrap();
    assert_matches!(
        task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    // Fail activity with retryable error
    let failure = Failure::application_failure("activity failed".to_string(), false);
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        task_queue: task_q.to_string(),
        result: Some(ActivityResult {
            status: Some(activity_result::activity_result::Status::Failed(
                activity_result::Failure {
                    failure: Some(failure),
                },
            )),
        }),
    })
    .await
    .unwrap();
    // Poll 2nd time
    let task = core.poll_activity_task(&task_q).await.unwrap();
    assert_matches!(
        task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    // Complete activity successfully
    let response_payload = Payload {
        data: b"hello ".to_vec(),
        metadata: Default::default(),
    };
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        task_queue: task_q.to_string(),
        result: Some(ActivityResult::ok(response_payload.clone())),
    })
    .await
    .unwrap();
    // Poll workflow task and verify activity has succeeded.
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(
                    ResolveActivity {activity_id: a_id, result: Some(ActivityResult{
                    status: Some(act_res::Status::Completed(activity_result::Success{result: Some(r)}))})}
                )),
            },
        ] => {
            assert_eq!(a_id, activity_id);
            assert_eq!(r, &response_payload);
        }
    );
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_cancellation_try_cancel() {
    let (core, task_q) = init_core_and_create_wf("activity_cancellation_try_cancel").await;
    let activity_id = "act-1";
    let timer_id = "timer-1";
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_task(
        vec![
            schedule_activity_cmd(
                &task_q,
                activity_id,
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            StartTimer {
                timer_id: timer_id.to_owned(),
                start_to_fire_timeout: Some(Duration::from_millis(50).into()),
            }
            .into(),
        ]
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters, we don't expect to
    // complete it in this test as activity is try-cancelled.
    let activity_task = core.poll_activity_task(&task_q).await.unwrap();
    assert_matches!(
        activity_task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(
                    FireTimer { timer_id: t_id }
                )),
            },
        ] => {
            assert_eq!(t_id, timer_id);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![RequestCancelActivity {
            activity_id: activity_id.to_owned(),
        }
        .into()],
        task.run_id,
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_cancellation_plus_complete_doesnt_double_resolve() {
    let (core, task_q) =
        init_core_and_create_wf("activity_cancellation_plus_complete_doesnt_double_resolve").await;
    let activity_id = "act-1";
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_task(
        vec![
            schedule_activity_cmd(
                &task_q,
                activity_id,
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            StartTimer {
                timer_id: "timer1".to_owned(),
                start_to_fire_timeout: Some(Duration::from_millis(50).into()),
            }
            .into(),
        ]
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    let activity_task = core.poll_activity_task(&task_q).await.unwrap();
    assert_matches!(activity_task.variant, Some(act_task::Variant::Start(_)));
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![RequestCancelActivity {
            activity_id: activity_id.to_owned(),
        }
        .into()],
        task.run_id,
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    // Should get cancel task
    assert_matches!(
        task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::ResolveActivity(
                ResolveActivity {
                    result: Some(ActivityResult {
                        status: Some(activity_result::activity_result::Status::Canceled(_))
                    }),
                    ..
                }
            )),
        }]
    );
    // We need to complete the wf task to send the activity cancel command to the server, so start
    // another short timer
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![StartTimer {
            timer_id: "timer2".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(100).into()),
        }
        .into()],
        task.run_id,
    ))
    .await
    .unwrap();
    // Now say the activity completes anyways
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: activity_task.task_token,
        task_queue: task_q.to_string(),
        result: Some(ActivityResult {
            status: Some(
                activity_result::Success {
                    result: Some(vec![1].into()),
                }
                .into(),
            ),
        }),
    })
    .await
    .unwrap();
    // Ensure we do not get a wakeup with the activity being resolved completed, and instead get
    // the timer fired event (also wait for timer to fire)
    sleep(Duration::from_secs(1)).await;
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn started_activity_timeout() {
    let (core, task_q) = init_core_and_create_wf("started_activity_timeout").await;
    let activity_id = "act-1";
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    // Complete workflow task and schedule activity that times out in 1 second.
    core.complete_workflow_task(
        schedule_activity_cmd(
            &task_q,
            activity_id,
            ActivityCancellationType::TryCancel,
            Duration::from_secs(1),
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters, we don't expect to
    // complete it in this test as activity is timed out after 1 second.
    let activity_task = core.poll_activity_task(&task_q).await.unwrap();
    assert_matches!(
        activity_task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        activity_id: a_id,
                        result: Some(ActivityResult{
                            status: Some(
                                act_res::Status::Failed(
                                    activity_result::Failure{failure: Some(_)}
                                )
                            ),
                            ..
                        })
                    }
                )),
            },
        ] => {
            assert_eq!(a_id, activity_id);
        }
    );
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_cancellation_wait_cancellation_completed() {
    let (core, task_q) =
        init_core_and_create_wf("activity_cancellation_wait_cancellation_completed").await;
    let activity_id = "act-1";
    let timer_id = "timer-1";
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_task(
        vec![
            schedule_activity_cmd(
                &task_q,
                activity_id,
                ActivityCancellationType::WaitCancellationCompleted,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            StartTimer {
                timer_id: timer_id.to_owned(),
                start_to_fire_timeout: Some(Duration::from_millis(50).into()),
            }
            .into(),
        ]
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters, we don't expect to
    // complete it in this test as activity is wait-cancelled.
    let activity_task = core.poll_activity_task(&task_q).await.unwrap();
    assert_matches!(
        activity_task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(
                    FireTimer { timer_id: t_id }
                )),
            },
        ] => {
            assert_eq!(t_id, timer_id);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![RequestCancelActivity {
            activity_id: activity_id.to_owned(),
        }
        .into()],
        task.run_id,
    ))
    .await
    .unwrap();
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: activity_task.task_token,
        task_queue: task_q.to_string(),
        result: Some(ActivityResult {
            status: Some(activity_result::activity_result::Status::Canceled(
                activity_result::Cancelation { details: None },
            )),
        }),
    })
    .await
    .unwrap();
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_cancellation_abandon() {
    let (core, task_q) = init_core_and_create_wf("activity_cancellation_abandon").await;
    let activity_id = "act-1";
    let timer_id = "timer-1";
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_task(
        vec![
            schedule_activity_cmd(
                &task_q,
                activity_id,
                ActivityCancellationType::Abandon,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            StartTimer {
                timer_id: timer_id.to_owned(),
                start_to_fire_timeout: Some(Duration::from_millis(50).into()),
            }
            .into(),
        ]
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters, we don't expect to
    // complete it in this test as activity is abandoned.
    let activity_task = core.poll_activity_task(&task_q).await.unwrap();
    assert_matches!(
        activity_task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(
                    FireTimer { timer_id: t_id }
                )),
            },
        ] => {
            assert_eq!(t_id, timer_id);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![RequestCancelActivity {
            activity_id: activity_id.to_owned(),
        }
        .into()],
        task.run_id,
    ))
    .await
    .unwrap();
    // Poll workflow task expecting that activation has been created by the state machine
    // immediately after the cancellation request.
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_execution(&task.run_id).await;
}
