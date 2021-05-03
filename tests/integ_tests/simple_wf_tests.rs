use crate::integ_tests::{
    create_workflow, create_workflow_custom_timeout, get_integ_core, get_integ_server_options,
    with_gw, GwApi, NAMESPACE,
};
use assert_matches::assert_matches;
use futures::{channel::mpsc::UnboundedReceiver, future, SinkExt, StreamExt};
use rand::{self, Rng};
use std::{collections::HashMap, sync::Arc, time::Duration};
use temporal_sdk_core::{
    protos::coresdk::{
        activity_result::{self, activity_result as act_res, ActivityResult},
        activity_task::activity_task as act_task,
        common::{Payload, UserCodeFailure},
        workflow_activation::{
            wf_activation_job, FireTimer, ResolveActivity, StartWorkflow, WfActivation,
            WfActivationJob,
        },
        workflow_commands::{
            ActivityCancellationType, CancelTimer, CompleteWorkflowExecution,
            FailWorkflowExecution, RequestCancelActivity, ScheduleActivity, StartTimer,
        },
        workflow_completion::WfActivationCompletion,
        ActivityHeartbeat, ActivityTaskCompletion,
    },
    Core, CoreInitOptions, PollWfError,
};
use tokio::time::sleep;

// TODO: These tests can get broken permanently if they break one time and the server is not
//  restarted, because pulling from the same task queue produces tasks for the previous failed
//  workflows. Fix that.

// TODO: We should also get expected histories for these tests and confirm that the history
//   at the end matches.

#[tokio::test]
async fn timer_workflow() {
    let task_q = "timer_workflow";
    let core = get_integ_core(task_q).await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let timer_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![StartTimer {
            timer_id,
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn activity_workflow() {
    let mut rng = rand::thread_rng();
    let task_q_salt: u32 = rng.gen();
    let task_q = &format!("activity_workflow_{}", task_q_salt.to_string());
    let core = get_integ_core(task_q).await;
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let activity_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task().await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_task(schedule_activity_cmd(
        task_q,
        &activity_id,
        ActivityCancellationType::TryCancel,
        task,
        Duration::from_secs(60),
        Duration::from_secs(60),
    ))
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
            assert_eq!(a_id, &activity_id);
            assert_eq!(r, &response_payload);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap()
}

#[tokio::test]
async fn activity_non_retryable_failure() {
    let mut rng = rand::thread_rng();
    let task_q_salt: u32 = rng.gen();
    let task_q = &format!("activity_failed_workflow_{}", task_q_salt.to_string());
    let core = get_integ_core(task_q).await;
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let activity_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task().await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_task(schedule_activity_cmd(
        task_q,
        &activity_id,
        ActivityCancellationType::TryCancel,
        task,
        Duration::from_secs(60),
        Duration::from_secs(60),
    ))
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
    // Fail activity with non-retryable error
    let failure = UserCodeFailure {
        message: "activity failed".to_string(),
        non_retryable: true,
        ..Default::default()
    };
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
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
    let task = core.poll_workflow_task().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(
                    ResolveActivity {activity_id: a_id, result: Some(ActivityResult{
                    status: Some(act_res::Status::Failed(activity_result::Failure{failure: Some(f)}))})}
                )),
            },
        ] => {
            assert_eq!(a_id, &activity_id);
            assert_eq!(f, &failure);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap()
}

#[tokio::test]
async fn activity_retry() {
    let mut rng = rand::thread_rng();
    let task_q_salt: u32 = rng.gen();
    let task_q = &format!("activity_failed_workflow_{}", task_q_salt.to_string());
    let core = get_integ_core(task_q).await;
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let activity_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task().await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_task(schedule_activity_cmd(
        task_q,
        &activity_id,
        ActivityCancellationType::TryCancel,
        task,
        Duration::from_secs(60),
        Duration::from_secs(60),
    ))
    .await
    .unwrap();
    // Poll activity 1st time
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(
        task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    // Fail activity with retryable error
    let failure = UserCodeFailure {
        message: "activity failed".to_string(),
        non_retryable: false,
        ..Default::default()
    };
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
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
    let task = core.poll_activity_task().await.unwrap();
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
        result: Some(ActivityResult::ok(response_payload.clone())),
    })
    .await
    .unwrap();
    // Poll workflow task and verify activity has succeeded.
    let task = core.poll_workflow_task().await.unwrap();
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
            assert_eq!(a_id, &activity_id);
            assert_eq!(r, &response_payload);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap()
}

#[tokio::test]
async fn activity_heartbeat() {
    let mut rng = rand::thread_rng();
    let task_q_salt: u32 = rng.gen();
    let task_q = &format!("activity_workflow_{}", task_q_salt.to_string());
    let core = get_integ_core(task_q).await;
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let activity_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task().await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_task(schedule_activity_cmd(
        task_q,
        &activity_id,
        ActivityCancellationType::TryCancel,
        task,
        Duration::from_secs(60),
        Duration::from_secs(1),
    ))
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
    // Activity shouldn't timeout since we are sending heartbeats regularly, however if we didn't send
    // heartbeats activity would have timed out as it takes 2 sec to execute this loop.
    for _ in 0u8..20 {
        sleep(Duration::from_millis(100)).await;
        core.record_activity_heartbeat(ActivityHeartbeat {
            task_token: task.task_token.clone(),
            details: vec![],
        })
        .await
        .unwrap();
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
            assert_eq!(a_id, &activity_id);
            assert_eq!(r, &response_payload);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap()
}

fn schedule_activity_cmd(
    task_q: &str,
    activity_id: &str,
    cancellation_type: ActivityCancellationType,
    task: WfActivation,
    activity_timeout: Duration,
    heartbeat_timeout: Duration,
) -> WfActivationCompletion {
    WfActivationCompletion::ok_from_cmds(
        vec![ScheduleActivity {
            activity_id: activity_id.to_string(),
            activity_type: "test_activity".to_string(),
            namespace: NAMESPACE.to_owned(),
            task_queue: task_q.to_owned(),
            schedule_to_start_timeout: Some(activity_timeout.into()),
            start_to_close_timeout: Some(activity_timeout.into()),
            schedule_to_close_timeout: Some(activity_timeout.into()),
            heartbeat_timeout: Some(heartbeat_timeout.into()),
            cancellation_type: cancellation_type as i32,
            ..Default::default()
        }
        .into()],
        task.task_token,
    )
}

pub fn schedule_activity_and_timer_cmds(
    task_q: &str,
    activity_id: &str,
    timer_id: &str,
    cancellation_type: ActivityCancellationType,
    task: WfActivation,
    activity_timeout: Duration,
    timer_delay: Duration,
) -> WfActivationCompletion {
    WfActivationCompletion::ok_from_cmds(
        vec![
            ScheduleActivity {
                activity_id: activity_id.to_string(),
                activity_type: "test_activity".to_string(),
                namespace: NAMESPACE.to_owned(),
                task_queue: task_q.to_owned(),
                schedule_to_start_timeout: Some(activity_timeout.into()),
                start_to_close_timeout: Some(activity_timeout.into()),
                schedule_to_close_timeout: Some(activity_timeout.into()),
                heartbeat_timeout: Some(activity_timeout.into()),
                cancellation_type: cancellation_type as i32,
                ..Default::default()
            }
            .into(),
            StartTimer {
                timer_id: timer_id.to_string(),
                start_to_fire_timeout: Some(timer_delay.into()),
            }
            .into(),
        ],
        task.task_token,
    )
}

#[tokio::test]
async fn activity_cancellation_try_cancel() {
    let mut rng = rand::thread_rng();
    let task_q_salt: u32 = rng.gen();
    let task_q = &format!("activity_cancelled_workflow_{}", task_q_salt.to_string());
    let core = get_integ_core(task_q).await;
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let activity_id: String = rng.gen::<u32>().to_string();
    let timer_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task().await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_task(schedule_activity_and_timer_cmds(
        task_q,
        &activity_id,
        &timer_id,
        ActivityCancellationType::TryCancel,
        task,
        Duration::from_secs(60),
        Duration::from_millis(50),
    ))
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters, we don't expect to
    // complete it in this test as activity is try-cancelled.
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(
        activity_task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_task().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(
                    FireTimer { timer_id: t_id }
                )),
            },
        ] => {
            assert_eq!(t_id, &timer_id);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![RequestCancelActivity {
            activity_id,
            ..Default::default()
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn activity_cancellation_plus_complete_doesnt_double_resolve() {
    let mut rng = rand::thread_rng();
    let task_q_salt: u32 = rng.gen();
    let task_q = &format!(
        "activity_cancellation_plus_complete_doesnt_double_resolve_{}",
        task_q_salt.to_string()
    );
    let core = get_integ_core(task_q).await;
    let activity_id = "activity_id";
    create_workflow(&core, task_q, "wfid", None).await;
    let task = core.poll_workflow_task().await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_task(schedule_activity_and_timer_cmds(
        task_q,
        activity_id,
        "timer_id",
        ActivityCancellationType::TryCancel,
        task,
        Duration::from_secs(60),
        Duration::from_millis(50),
    ))
    .await
    .unwrap();
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(activity_task.variant, Some(act_task::Variant::Start(_)));
    dbg!("Got activity task");
    let task = core.poll_workflow_task().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![RequestCancelActivity {
            activity_id: activity_id.to_owned(),
            ..Default::default()
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();
    dbg!("Completed w/ cancel");
    let task = core.poll_workflow_task().await.unwrap();
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
    dbg!("Got cancel task");
    // We need to complete the wf task to send the activity cancel command to the server, so start
    // another short timer
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![StartTimer {
            timer_id: "timer2".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(100).into()),
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();
    // Now say the activity completes anyways
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: activity_task.task_token,
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
    dbg!("Completed AT");
    // Ensure we do not get a wakeup with the activity being resolved completed, and instead get
    // the timer fired event (also wait for timer to fire)
    sleep(Duration::from_secs(1)).await;
    let task = core.poll_workflow_task().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn started_activity_timeout() {
    let mut rng = rand::thread_rng();
    let task_q_salt: u32 = rng.gen();
    let task_q = &format!("activity_cancelled_workflow_{}", task_q_salt.to_string());
    let core = get_integ_core(task_q).await;
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let activity_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task().await.unwrap();
    // Complete workflow task and schedule activity that times out in 1 second.
    core.complete_workflow_task(schedule_activity_cmd(
        task_q,
        &activity_id,
        ActivityCancellationType::TryCancel,
        task,
        Duration::from_secs(1),
        Duration::from_secs(60),
    ))
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters, we don't expect to
    // complete it in this test as activity is timed out after 1 second.
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(
        activity_task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    let task = core.poll_workflow_task().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(
                    ResolveActivity {activity_id: a_id, result: Some(ActivityResult{
                    status: Some(act_res::Status::Failed(activity_result::Failure{failure: Some(_)})),
                     ..})}
                )),
            },
        ] => {
            assert_eq!(a_id, &activity_id);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn activity_cancellation_wait_cancellation_completed() {
    let mut rng = rand::thread_rng();
    let task_q_salt: u32 = rng.gen();
    let task_q = &format!("activity_cancelled_workflow_{}", task_q_salt.to_string());
    let core = get_integ_core(task_q).await;
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let activity_id: String = rng.gen::<u32>().to_string();
    let timer_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task().await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_task(schedule_activity_and_timer_cmds(
        task_q,
        &activity_id,
        &timer_id,
        ActivityCancellationType::WaitCancellationCompleted,
        task,
        Duration::from_secs(60),
        Duration::from_millis(50),
    ))
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters, we don't expect to
    // complete it in this test as activity is wait-cancelled.
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(
        activity_task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_task().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(
                    FireTimer { timer_id: t_id }
                )),
            },
        ] => {
            assert_eq!(t_id, &timer_id);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![RequestCancelActivity {
            activity_id,
            ..Default::default()
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: activity_task.task_token,
        result: Some(ActivityResult {
            status: Some(activity_result::activity_result::Status::Canceled(
                activity_result::Cancelation { details: None },
            )),
        }),
    })
    .await
    .unwrap();
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn activity_cancellation_abandon() {
    let mut rng = rand::thread_rng();
    let task_q_salt: u32 = rng.gen();
    let task_q = &format!("activity_cancelled_workflow_{}", task_q_salt.to_string());
    let core = get_integ_core(task_q).await;
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let activity_id: String = rng.gen::<u32>().to_string();
    let timer_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task().await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_task(schedule_activity_and_timer_cmds(
        task_q,
        &activity_id,
        &timer_id,
        ActivityCancellationType::Abandon,
        task,
        Duration::from_secs(60),
        Duration::from_millis(50),
    ))
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters, we don't expect to
    // complete it in this test as activity is abandoned.
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(
        activity_task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, "test_activity".to_string())
        }
    );
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_task().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(
                    FireTimer { timer_id: t_id }
                )),
            },
        ] => {
            assert_eq!(t_id, &timer_id);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![RequestCancelActivity {
            activity_id,
            ..Default::default()
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();
    // Poll workflow task expecting that activation has been created by the state machine
    // immediately after the cancellation request.
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn parallel_timer_workflow() {
    let task_q = "parallel_timer_workflow";
    let core = get_integ_core(task_q).await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let timer_id = "timer 1".to_string();
    let timer_2_id = "timer 2".to_string();
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![
            StartTimer {
                timer_id: timer_id.clone(),
                start_to_fire_timeout: Some(Duration::from_millis(50).into()),
            }
            .into(),
            StartTimer {
                timer_id: timer_2_id.clone(),
                start_to_fire_timeout: Some(Duration::from_millis(100).into()),
            }
            .into(),
        ],
        task.task_token,
    ))
    .await
    .unwrap();
    // Wait long enough for both timers to complete. Server seems to be a bit weird about actually
    // sending both of these in one go, so we need to wait longer than you would expect.
    std::thread::sleep(Duration::from_millis(1500));
    let task = core.poll_workflow_task().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(
                    FireTimer { timer_id: t1_id }
                )),
            },
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(
                    FireTimer { timer_id: t2_id }
                )),
            }
        ] => {
            assert_eq!(t1_id, &timer_id);
            assert_eq!(t2_id, &timer_2_id);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn timer_cancel_workflow() {
    let task_q = "timer_cancel_workflow";
    let core = get_integ_core(task_q).await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let timer_id = "wait_timer";
    let cancel_timer_id = "cancel_timer";
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![
            StartTimer {
                timer_id: timer_id.to_string(),
                start_to_fire_timeout: Some(Duration::from_millis(50).into()),
            }
            .into(),
            StartTimer {
                timer_id: cancel_timer_id.to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(10).into()),
            }
            .into(),
        ],
        task.task_token,
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![
            CancelTimer {
                timer_id: cancel_timer_id.to_string(),
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn timer_immediate_cancel_workflow() {
    let task_q = "timer_immediate_cancel_workflow";
    let core = get_integ_core(task_q).await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let cancel_timer_id = "cancel_timer";
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![
            StartTimer {
                timer_id: cancel_timer_id.to_string(),
                ..Default::default()
            }
            .into(),
            CancelTimer {
                timer_id: cancel_timer_id.to_string(),
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn parallel_workflows_same_queue() {
    let task_q = "parallel_workflows_same_queue";
    let core = get_integ_core(task_q).await;
    let num_workflows = 25;

    let run_ids: Vec<_> =
        future::join_all((0..num_workflows).map(|i| {
            let core = &core;
            async move {
                create_workflow(core, task_q, &format!("wf-id-{}", i), Some("wf-type-1")).await
            }
        }))
        .await;

    let mut send_chans = HashMap::new();

    async fn wf_task(core: Arc<dyn Core>, mut task_chan: UnboundedReceiver<WfActivation>) {
        let task = task_chan.next().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::StartWorkflow(
                    StartWorkflow {
                        workflow_type,
                        ..
                    }
                )),
            }] => assert_eq!(&workflow_type, &"wf-type-1")
        );
        core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
            vec![StartTimer {
                timer_id: "timer".to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(1).into()),
            }
            .into()],
            task.task_token,
        ))
        .await
        .unwrap();
        let task = task_chan.next().await.unwrap();
        core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
            vec![CompleteWorkflowExecution { result: None }.into()],
            task.task_token,
        ))
        .await
        .unwrap();
    }

    let core = Arc::new(core);
    let handles: Vec<_> = run_ids
        .iter()
        .map(|run_id| {
            let (tx, rx) = futures::channel::mpsc::unbounded();
            send_chans.insert(run_id.clone(), tx);
            let core_c = core.clone();
            tokio::spawn(wf_task(core_c, rx))
        })
        .collect();

    for _ in 0..num_workflows * 2 {
        let task = core.poll_workflow_task().await.unwrap();
        send_chans
            .get(&task.run_id)
            .unwrap()
            .send(task)
            .await
            .unwrap();
    }

    for handle in handles {
        handle.await.unwrap()
    }
}

// Ideally this would be a unit test, but returning a pending future with mockall bloats the mock
// code a bunch and just isn't worth it. Do it when https://github.com/asomers/mockall/issues/189 is
// fixed.
#[tokio::test]
async fn shutdown_aborts_actively_blocked_poll() {
    let task_q = "shutdown_aborts_actively_blocked_poll";
    let core = Arc::new(get_integ_core(task_q).await);
    // Begin the poll, and request shutdown from another thread after a small period of time.
    let tcore = core.clone();
    let handle = tokio::spawn(async move {
        std::thread::sleep(Duration::from_millis(100));
        tcore.shutdown().await;
    });
    assert_matches!(
        core.poll_workflow_task().await.unwrap_err(),
        PollWfError::ShutDown
    );
    handle.await.unwrap();
    // Ensure double-shutdown doesn't explode
    core.shutdown().await;
    assert_matches!(
        core.poll_workflow_task().await.unwrap_err(),
        PollWfError::ShutDown
    );
}

#[tokio::test]
async fn fail_wf_task() {
    let task_q = "fail_wf_task";
    let core = get_integ_core(task_q).await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;

    // Start with a timer
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![StartTimer {
            timer_id: "best-timer".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(200).into()),
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();

    // Allow timer to fire
    std::thread::sleep(Duration::from_millis(500));

    // Then break for whatever reason
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::fail(
        task.task_token,
        UserCodeFailure {
            message: "I did an oopsie".to_string(),
            ..Default::default()
        },
    ))
    .await
    .unwrap();

    // The server will want to retry the task. This time we finish the workflow -- but we need
    // to poll a couple of times as there will be more than one required workflow activation.
    let task = core.poll_workflow_task().await.unwrap();
    // The first poll response will tell us to evict
    assert_matches!(
        task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    // So poll again
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![StartTimer {
            timer_id: "best-timer".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(200).into()),
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn fail_workflow_execution() {
    let task_q = "fail_workflow_execution";
    let core = get_integ_core(task_q).await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;
    let timer_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![StartTimer {
            timer_id,
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![FailWorkflowExecution {
            failure: Some(UserCodeFailure {
                message: "I'm ded".to_string(),
                ..Default::default()
            }),
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn signal_workflow() {
    let task_q = "signal_workflow";
    let core = get_integ_core(task_q).await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;

    let signal_id_1 = "signal1";
    let signal_id_2 = "signal2";
    let res = core.poll_workflow_task().await.unwrap();
    // Task is completed with no commands
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![],
        res.task_token.clone(),
    ))
    .await
    .unwrap();

    // Send the signals to the server
    with_gw(&core, |gw: GwApi| async move {
        gw.signal_workflow_execution(
            workflow_id.to_string(),
            res.run_id.to_string(),
            signal_id_1.to_string(),
            None,
        )
        .await
        .unwrap();
        gw.signal_workflow_execution(
            workflow_id.to_string(),
            res.run_id.to_string(),
            signal_id_2.to_string(),
            None,
        )
        .await
        .unwrap();
    })
    .await;

    let res = core.poll_workflow_task().await.unwrap();
    assert_matches!(
        res.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
            },
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
            }
        ]
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        res.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn signal_workflow_signal_not_handled_on_workflow_completion() {
    let task_q = "signal_workflow_signal_not_handled_on_workflow_completion";
    let core = get_integ_core(task_q).await;
    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow(&core, task_q, &workflow_id.to_string(), None).await;

    let signal_id_1 = "signal1";
    let res = core.poll_workflow_task().await.unwrap();
    // Task is completed with a timer
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![StartTimer {
            timer_id: "sometimer".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(10).into()),
        }
        .into()],
        res.task_token,
    ))
    .await
    .unwrap();

    // Poll before sending the signal - we should have the timer job
    let res = core.poll_workflow_task().await.unwrap();
    assert_matches!(
        res.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::FireTimer(_)),
        }]
    );

    let task_token = res.task_token.clone();
    // Send the signals to the server
    with_gw(&core, |gw: GwApi| async move {
        gw.signal_workflow_execution(
            workflow_id.to_string(),
            res.run_id.to_string(),
            signal_id_1.to_string(),
            None,
        )
        .await
        .unwrap();
    })
    .await;

    // Send completion - not having seen a poll response with a signal in it yet (unhandled command
    // error will be silenced)
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task_token,
    ))
    .await
    .unwrap();

    // We should get a new task with the signal
    let res = core.poll_workflow_task().await.unwrap();
    assert_matches!(
        res.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        res.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn wft_timeout_doesnt_create_unsolvable_autocomplete() {
    let task_q = "wft_timeout_doesnt_create_unsolvable_autocomplete";
    let activity_id = "act-1";
    let signal_at_start = "at-start";
    let signal_at_complete = "at-complete";
    let gateway_opts = get_integ_server_options(task_q);
    let core = temporal_sdk_core::init(CoreInitOptions {
        gateway_opts,
        // Eviction needs to be on in this test
        evict_after_pending_cleared: true,
        max_outstanding_workflow_tasks: 5,
        max_outstanding_activities: 5,
    })
    .await
    .unwrap();

    // Set up some helpers for polling and completing
    let poll_sched_act = || async {
        let wf_task = core.poll_workflow_task().await.unwrap();
        core.complete_workflow_task(schedule_activity_cmd(
            task_q,
            activity_id,
            ActivityCancellationType::TryCancel,
            wf_task.clone(),
            Duration::from_secs(1),
            Duration::from_secs(60),
        ))
        .await
        .unwrap();
        wf_task
    };
    let poll_sched_act_poll = || async {
        poll_sched_act().await;
        let wf_task = core.poll_workflow_task().await.unwrap();
        assert_matches!(
            wf_task.jobs.as_slice(),
            [
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
                },
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::ResolveActivity(_)),
                },
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
                }
            ]
        );
        wf_task
    };

    let mut rng = rand::thread_rng();
    let workflow_id: u32 = rng.gen();
    create_workflow_custom_timeout(
        &core,
        task_q,
        &workflow_id.to_string(),
        // Use a short task timeout
        Duration::from_secs(1),
    )
    .await;

    // Poll and schedule the activity
    let wf_task = poll_sched_act().await;
    // Before polling for a task again, we start and complete the activity and send the
    // corresponding signals.
    let ac_task = core.poll_activity_task().await.unwrap();
    let rid = wf_task.run_id.clone();
    // Send the signals to the server -- sometimes this happens too fast
    sleep(Duration::from_millis(200)).await;
    with_gw(&core, |gw: GwApi| async move {
        gw.signal_workflow_execution(
            workflow_id.to_string(),
            rid,
            signal_at_start.to_string(),
            None,
        )
        .await
        .unwrap();
    })
    .await;
    // Complete activity successfully.
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: ac_task.task_token,
        result: Some(ActivityResult::ok(Default::default())),
    })
    .await
    .unwrap();
    let rid = wf_task.run_id.clone();
    with_gw(&core, |gw: GwApi| async move {
        gw.signal_workflow_execution(
            workflow_id.to_string(),
            rid,
            signal_at_complete.to_string(),
            None,
        )
        .await
        .unwrap();
    })
    .await;
    // Now poll again, it will be an eviction b/c non-sticky mode.
    let wf_task = core.poll_workflow_task().await.unwrap();
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    // Start from the beginning
    let wf_task = poll_sched_act_poll().await;
    // Time out this time
    sleep(Duration::from_secs(2)).await;
    // Poll again, which should not have any work to do and spin, until the complete goes through.
    // Which will be rejected with not found, producing an eviction.
    let (wf_task, _) = tokio::join!(async { core.poll_workflow_task().await.unwrap() }, async {
        sleep(Duration::from_millis(500)).await;
        // Reply to the first one, finally
        core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
            vec![CompleteWorkflowExecution { result: None }.into()],
            wf_task.task_token,
        ))
        .await
        .unwrap();
    });
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    // Do it all over again, without timing out this time
    let wf_task = poll_sched_act_poll().await;
    core.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        wf_task.task_token,
    ))
    .await
    .unwrap();
}
