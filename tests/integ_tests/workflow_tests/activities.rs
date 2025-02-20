use crate::integ_tests::activity_functions::echo;
use anyhow::anyhow;
use assert_matches::assert_matches;
use futures_util::future::join_all;
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use temporal_client::{WfClientExt, WorkflowClientTrait, WorkflowExecutionResult, WorkflowOptions};
use temporal_sdk::{
    ActContext, ActExitValue, ActivityError, ActivityOptions, CancellableFuture, WfContext,
    WfExitValue, WorkflowResult,
};
use temporal_sdk_core_protos::{
    DEFAULT_ACTIVITY_TYPE, TaskToken,
    coresdk::{
        ActivityHeartbeat, ActivityTaskCompletion, AsJsonPayloadExt, FromJsonPayloadExt,
        IntoCompletion,
        activity_result::{
            self, ActivityExecutionResult, ActivityResolution, activity_resolution as act_res,
        },
        activity_task::activity_task as act_task,
        workflow_activation::{
            FireTimer, ResolveActivity, WorkflowActivationJob, workflow_activation_job,
        },
        workflow_commands::{
            ActivityCancellationType, RequestCancelActivity, ScheduleActivity, StartTimer,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        common::v1::{ActivityType, Payload, Payloads, RetryPolicy},
        enums::v1::RetryState,
        failure::v1::{ActivityFailureInfo, Failure, failure::FailureInfo},
    },
};
use temporal_sdk_core_test_utils::{
    CoreWfStarter, WorkerTestHelpers, drain_pollers_and_shutdown, init_core_and_create_wf,
    schedule_activity_cmd,
};
use tokio::{join, sync::Semaphore, time::sleep};

pub(crate) async fn one_activity_wf(ctx: WfContext) -> WorkflowResult<()> {
    ctx.activity(ActivityOptions {
        activity_type: "echo_activity".to_string(),
        start_to_close_timeout: Some(Duration::from_secs(5)),
        input: "hi!".as_json_payload().expect("serializes fine"),
        ..Default::default()
    })
    .await;
    Ok(().into())
}

#[tokio::test]
async fn one_activity_only() {
    let wf_name = "one_activity";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    worker.register_wf(wf_name.to_owned(), one_activity_wf);
    worker.register_activity("echo_activity", echo);

    let run_id = worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let handle = client.get_untyped_workflow_handle(wf_name, run_id);
    let res = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    assert_matches!(res, WorkflowExecutionResult::Succeeded(_));
}

#[tokio::test]
async fn activity_workflow() {
    let mut starter = init_core_and_create_wf("activity_workflow").await;
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
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled with correct parameters
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(
        task.variant,
        Some(act_task::Variant::Start(start_activity)) => {
            assert_eq!(start_activity.activity_type, DEFAULT_ACTIVITY_TYPE.to_string())
        }
    );
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
                    ResolveActivity {seq, result: Some(ActivityResolution{
                      status: Some(
                        act_res::Status::Completed(activity_result::Success{result: Some(r)})),
                        ..
                    }), ..}
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
async fn activity_non_retryable_failure() {
    let mut starter = init_core_and_create_wf("activity_non_retryable_failure").await;
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
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(task.variant, Some(act_task::Variant::Start(_)));
    // Fail activity with non-retryable error
    let failure = Failure::application_failure("activity failed".to_string(), true);
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        result: Some(ActivityExecutionResult::fail(failure.clone())),
    })
    .await
    .unwrap();
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {seq, result: Some(ActivityResolution{
                    status: Some(act_res::Status::Failed(activity_result::Failure{
                        failure: Some(f),
                    }))}),..}
                )),
            },
        ] => {
            assert_eq!(*seq, 0);
            assert_eq!(f, &Failure{
                message: "Activity task failed".to_owned(),
                cause: Some(Box::new(failure)),
                failure_info: Some(FailureInfo::ActivityFailureInfo(ActivityFailureInfo{
                    activity_id: "act-1".to_owned(),
                    activity_type: Some(ActivityType {
                        name: DEFAULT_ACTIVITY_TYPE.to_owned(),
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
async fn activity_non_retryable_failure_with_error() {
    let mut starter = init_core_and_create_wf("activity_non_retryable_failure").await;
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
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(task.variant, Some(act_task::Variant::Start(_)));
    // Fail activity with non-retryable error
    let failure = Failure::application_failure_from_error(anyhow!("activity failed"), true);
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        result: Some(ActivityExecutionResult::fail(failure.clone())),
    })
    .await
    .unwrap();
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {seq, result: Some(ActivityResolution{
                    status: Some(act_res::Status::Failed(activity_result::Failure{
                        failure: Some(f),
                    }))}),..}
                )),
            },
        ] => {
            assert_eq!(*seq, 0);
            assert_eq!(f, &Failure{
                message: "Activity task failed".to_owned(),
                cause: Some(Box::new(failure)),
                failure_info: Some(FailureInfo::ActivityFailureInfo(ActivityFailureInfo{
                    activity_id: "act-1".to_owned(),
                    activity_type: Some(ActivityType {
                        name: DEFAULT_ACTIVITY_TYPE.to_owned(),
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
    let mut starter = init_core_and_create_wf("activity_retry").await;
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
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity 1st time
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(task.variant, Some(act_task::Variant::Start(_)));
    // Fail activity with retryable error
    let failure = Failure::application_failure("activity failed".to_string(), false);
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        result: Some(ActivityExecutionResult::fail(failure)),
    })
    .await
    .unwrap();
    // Poll 2nd time
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(task.variant, Some(act_task::Variant::Start(_)));
    // Complete activity successfully
    let response_payload = Payload {
        data: b"hello ".to_vec(),
        metadata: Default::default(),
    };
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token,
        result: Some(ActivityExecutionResult::ok(response_payload.clone())),
    })
    .await
    .unwrap();
    // Poll workflow task and verify activity has succeeded.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {seq, result: Some(ActivityResolution{
                    status: Some(act_res::Status::Completed(activity_result::Success{result: Some(r)}))}),..}
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
async fn activity_cancellation_try_cancel() {
    let mut starter = init_core_and_create_wf("activity_cancellation_try_cancel").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_activation(
        vec![
            schedule_activity_cmd(
                0,
                task_q,
                activity_id,
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(prost_dur!(from_millis(50))),
            }
            .into(),
        ]
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled, we don't expect to complete it in this
    // test as activity is try-cancelled.
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(activity_task.variant, Some(act_task::Variant::Start(_)));
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::FireTimer(
                    FireTimer { seq }
                )),
            },
        ] => {
            assert_eq!(*seq, 1);
        }
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![RequestCancelActivity { seq: 0 }.into()],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_cancellation_plus_complete_doesnt_double_resolve() {
    let mut starter =
        init_core_and_create_wf("activity_cancellation_plus_complete_doesnt_double_resolve").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_activation(
        vec![
            schedule_activity_cmd(
                0,
                task_q,
                activity_id,
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(prost_dur!(from_millis(50))),
            }
            .into(),
        ]
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(activity_task.variant, Some(act_task::Variant::Start(_)));
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![RequestCancelActivity { seq: 0 }.into()],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    // Should get cancel task
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveActivity(
                ResolveActivity {
                    result: Some(ActivityResolution {
                        status: Some(act_res::Status::Cancelled(_))
                    }),
                    ..
                }
            )),
        }]
    );
    // We need to complete the wf task to send the activity cancel command to the server, so start
    // another short timer
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            StartTimer {
                seq: 2,
                start_to_fire_timeout: Some(prost_dur!(from_millis(100))),
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    // Now say the activity completes anyways
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: activity_task.task_token,
        result: Some(ActivityExecutionResult::ok([1].into())),
    })
    .await
    .unwrap();
    // Ensure we do not get a wakeup with the activity being resolved completed, and instead get
    // the timer fired event (also wait for timer to fire)
    sleep(Duration::from_secs(1)).await;
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn started_activity_timeout() {
    let mut starter = init_core_and_create_wf("started_activity_timeout").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity that times out in 1 second.
    core.complete_workflow_activation(
        schedule_activity_cmd(
            0,
            task_q,
            activity_id,
            ActivityCancellationType::TryCancel,
            Duration::from_secs(1),
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled, we don't expect to complete it in this
    // test as activity is timed out after 1 second.
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(activity_task.variant, Some(act_task::Variant::Start(_)));
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        seq,
                        result: Some(ActivityResolution{
                            status: Some(
                                act_res::Status::Failed(
                                    activity_result::Failure{failure: Some(_)}
                                )
                            ),
                            ..
                        }), ..
                    }
                )),
            },
        ] => {
            assert_eq!(*seq, 0);
        }
    );
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_cancellation_wait_cancellation_completed() {
    let mut starter =
        init_core_and_create_wf("activity_cancellation_wait_cancellation_completed").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_activation(
        vec![
            schedule_activity_cmd(
                0,
                task_q,
                activity_id,
                ActivityCancellationType::WaitCancellationCompleted,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(prost_dur!(from_millis(50))),
            }
            .into(),
        ]
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled, we don't expect to complete it in this
    // test as activity is wait-cancelled.
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(activity_task.variant, Some(act_task::Variant::Start(_)));
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::FireTimer(
                    FireTimer { seq }
                )),
            },
        ] => {
            assert_eq!(*seq, 1);
        }
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![RequestCancelActivity { seq: 0 }.into()],
    ))
    .await
    .unwrap();
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: activity_task.task_token,
        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn activity_cancellation_abandon() {
    let mut starter = init_core_and_create_wf("activity_cancellation_abandon").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_activation(
        vec![
            schedule_activity_cmd(
                0,
                task_q,
                activity_id,
                ActivityCancellationType::Abandon,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(prost_dur!(from_millis(50))),
            }
            .into(),
        ]
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled, we don't expect to complete it in this
    // test as activity is abandoned.
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(activity_task.variant, Some(act_task::Variant::Start(_)));
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::FireTimer(
                    FireTimer { seq }
                )),
            },
        ] => {
            assert_eq!(*seq, 1);
        }
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![RequestCancelActivity { seq: 0 }.into()],
    ))
    .await
    .unwrap();
    // Poll workflow task expecting that activation has been created by the state machine
    // immediately after the cancellation request.
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn async_activity_completion_workflow() {
    let mut starter = init_core_and_create_wf("async_activity_workflow").await;
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
            Duration::from_secs(60),
        )
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(task.variant, Some(act_task::Variant::Start(_)));
    let response_payload = Payload {
        data: b"hello ".to_vec(),
        metadata: Default::default(),
    };
    // Complete activity asynchronously.
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token.clone(),
        result: Some(ActivityExecutionResult::will_complete_async()),
    })
    .await
    .unwrap();
    starter
        .get_client()
        .await
        .complete_activity_task(
            task.task_token.into(),
            Some(Payloads {
                payloads: vec![response_payload.clone()],
            }),
        )
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
                     ..}), ..}
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
async fn activity_cancelled_after_heartbeat_times_out() {
    let mut starter = init_core_and_create_wf("activity_cancelled_after_heartbeat_times_out").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue().to_string();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        ScheduleActivity {
            seq: 0,
            activity_id: activity_id.to_string(),
            activity_type: "dontcare".to_string(),
            task_queue: task_q.clone(),
            schedule_to_close_timeout: Some(prost_dur!(from_secs(10))),
            heartbeat_timeout: Some(prost_dur!(from_secs(1))),
            retry_policy: Some(RetryPolicy {
                maximum_attempts: 2,
                initial_interval: Some(prost_dur!(from_secs(5))),
                ..Default::default()
            }),
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled
    let task = core.poll_activity_task().await.unwrap();
    assert_matches!(task.variant, Some(act_task::Variant::Start(_)));
    // Delay the heartbeat
    sleep(Duration::from_secs(2)).await;
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: task.task_token.clone(),
        details: vec![],
    });

    // Verify activity got cancelled
    let cancel_task = core.poll_activity_task().await.unwrap();
    assert_matches!(cancel_task.variant, Some(act_task::Variant::Cancel(_)));
    assert_eq!(cancel_task.task_token, task.task_token.clone());

    // Complete activity with cancelled result
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: task.task_token.clone(),
        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();

    // Verify shutdown completes
    drain_pollers_and_shutdown(&core).await;
    // Cleanup just in case
    starter
        .get_client()
        .await
        .terminate_workflow_execution(task_q, None)
        .await
        .unwrap();
}

#[tokio::test]
async fn one_activity_abandon_cancelled_before_started() {
    let wf_name = "one_activity_abandon_cancelled_before_started";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let act_fut = ctx.activity(ActivityOptions {
            activity_type: "echo_activity".to_string(),
            start_to_close_timeout: Some(Duration::from_secs(5)),
            input: "hi!".as_json_payload().expect("serializes fine"),
            cancellation_type: ActivityCancellationType::Abandon,
            ..Default::default()
        });
        act_fut.cancel(&ctx);
        act_fut.await;
        Ok(().into())
    });
    worker.register_activity(
        "echo_activity",
        |_ctx: ActContext, echo_me: String| async move {
            sleep(Duration::from_secs(2)).await;
            Ok(echo_me)
        },
    );

    let run_id = worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let handle = client.get_untyped_workflow_handle(wf_name, run_id);
    let res = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    assert_matches!(res, WorkflowExecutionResult::Succeeded(_));
}

#[tokio::test]
async fn one_activity_abandon_cancelled_after_complete() {
    let wf_name = "one_activity_abandon_cancelled_after_complete";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let act_fut = ctx.activity(ActivityOptions {
            activity_type: "echo_activity".to_string(),
            start_to_close_timeout: Some(Duration::from_secs(5)),
            input: "hi!".as_json_payload().expect("serializes fine"),
            cancellation_type: ActivityCancellationType::Abandon,
            ..Default::default()
        });
        ctx.timer(Duration::from_secs(1)).await;
        act_fut.cancel(&ctx);
        ctx.timer(Duration::from_secs(3)).await;
        act_fut.await;
        Ok(().into())
    });
    worker.register_activity(
        "echo_activity",
        |_ctx: ActContext, echo_me: String| async move {
            sleep(Duration::from_secs(2)).await;
            Ok(echo_me)
        },
    );

    let run_id = worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let handle = client.get_untyped_workflow_handle(wf_name, run_id);
    let res = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    assert_matches!(res, WorkflowExecutionResult::Succeeded(_));
}

#[tokio::test]
async fn it_can_complete_async() {
    use std::sync::Arc;
    use tokio::sync::Mutex;

    let wf_name = "it_can_complete_async".to_owned();
    let mut starter = CoreWfStarter::new(&wf_name);
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    let async_response = "agence";
    let shared_token: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));
    worker.register_wf(wf_name.clone(), move |ctx: WfContext| async move {
        let activity_resolution = ctx
            .activity(ActivityOptions {
                activity_type: "complete_async_activity".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                start_to_close_timeout: Some(Duration::from_secs(30)),
                ..Default::default()
            })
            .await;

        let res = match activity_resolution.status {
            Some(act_res::Status::Completed(activity_result::Success { result })) => result
                .map(|p| String::from_json_payload(&p).unwrap())
                .unwrap(),
            _ => panic!("activity task failed {activity_resolution:?}"),
        };

        assert_eq!(&res, async_response);
        Ok(().into())
    });

    let shared_token_ref = shared_token.clone();
    worker.register_activity(
        "complete_async_activity",
        move |ctx: ActContext, _: String| {
            let shared_token_ref = shared_token_ref.clone();
            async move {
                // set the `activity_task_token`
                let activity_info = ctx.get_info();
                let task_token = &activity_info.task_token;
                let mut shared = shared_token_ref.lock().await;
                *shared = Some(task_token.clone());
                Ok::<ActExitValue<()>, _>(ActExitValue::WillCompleteAsync)
            }
        },
    );

    let shared_token_ref2 = shared_token.clone();
    tokio::spawn(async move {
        loop {
            let mut shared = shared_token_ref2.lock().await;
            let maybe_token = shared.take();

            if let Some(task_token) = maybe_token {
                client
                    .complete_activity_task(
                        TaskToken(task_token),
                        Some(async_response.as_json_payload().unwrap().into()),
                    )
                    .await
                    .unwrap();
                return;
            }
        }
    });

    let _run_id = worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn graceful_shutdown() {
    let wf_name = "graceful_shutdown";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .worker_config
        .graceful_shutdown_period(Some(Duration::from_millis(500)));
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let act_futs = (1..=10).map(|_| {
            ctx.activity(ActivityOptions {
                activity_type: "sleeper".to_string(),
                start_to_close_timeout: Some(Duration::from_secs(5)),
                retry_policy: Some(RetryPolicy {
                    maximum_attempts: 1,
                    ..Default::default()
                }),
                cancellation_type: ActivityCancellationType::WaitCancellationCompleted,
                input: "hi".as_json_payload().unwrap(),
                ..Default::default()
            })
        });
        join_all(act_futs).await;
        Ok(().into())
    });
    static ACTS_STARTED: Semaphore = Semaphore::const_new(0);
    static ACTS_DONE: Semaphore = Semaphore::const_new(0);
    worker.register_activity("sleeper", |ctx: ActContext, _: String| async move {
        ACTS_STARTED.add_permits(1);
        // just wait to be cancelled
        ctx.cancelled().await;
        ACTS_DONE.add_permits(1);
        Result::<(), _>::Err(ActivityError::cancelled())
    });

    worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    let handle = worker.inner_mut().shutdown_handle();
    let shutdowner = async {
        // Wait for all acts to be started before initiating shutdown
        let _ = ACTS_STARTED.acquire_many(10).await;
        handle();
        // Kill workflow once all acts are cancelled. This also ensures we actually see all the
        // cancels, otherwise run_until_done will hang since the workflow won't complete.
        let _ = ACTS_DONE.acquire_many(10).await;
        client
            .terminate_workflow_execution(wf_name.to_owned(), None)
            .await
            .unwrap();
    };
    let runner = async {
        worker.run_until_done().await.unwrap();
    };
    join!(shutdowner, runner);
}

#[tokio::test]
async fn activity_can_be_cancelled_by_local_timeout() {
    let wf_name = "activity_can_be_cancelled_by_local_timeout";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .worker_config
        .local_timeout_buffer_for_activities(Duration::from_secs(0));
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let res = ctx
            .activity(ActivityOptions {
                activity_type: "echo_activity".to_string(),
                start_to_close_timeout: Some(Duration::from_secs(1)),
                input: "hi!".as_json_payload().expect("serializes fine"),
                retry_policy: Some(RetryPolicy {
                    maximum_attempts: 1,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await;
        assert!(res.timed_out().is_some());
        Ok(().into())
    });
    static WAS_CANCELLED: AtomicBool = AtomicBool::new(false);
    worker.register_activity(
        "echo_activity",
        |ctx: ActContext, echo_me: String| async move {
            // Doesn't heartbeat
            ctx.cancelled().await;
            WAS_CANCELLED.store(true, Ordering::Relaxed);
            Ok(echo_me)
        },
    );

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
    assert!(WAS_CANCELLED.load(Ordering::Relaxed));
}

#[tokio::test]
#[ignore] // Runs forever, used to manually attempt to repro spurious activity completion rpc errs
async fn long_activity_timeout_repro() {
    let wf_name = "long_activity_timeout_repro";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .worker_config
        .local_timeout_buffer_for_activities(Duration::from_secs(0));
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let mut iter = 1;
        loop {
            let res = ctx
                .activity(ActivityOptions {
                    activity_type: "echo_activity".to_string(),
                    start_to_close_timeout: Some(Duration::from_secs(1)),
                    input: "hi!".as_json_payload().expect("serializes fine"),
                    retry_policy: Some(RetryPolicy {
                        maximum_attempts: 1,
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .await;
            assert!(res.completed_ok());
            ctx.timer(Duration::from_secs(60 * 3)).await;
            iter += 1;
            if iter > 5000 {
                return Ok(WfExitValue::<()>::continue_as_new(Default::default()));
            }
        }
    });
    worker.register_activity("echo_activity", echo);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}
