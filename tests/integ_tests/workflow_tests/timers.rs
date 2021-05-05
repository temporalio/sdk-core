use crate::integ_tests::init_core_and_create_wf;
use assert_matches::assert_matches;
use std::time::Duration;
use temporal_sdk_core::protos::coresdk::{
    activity_task::activity_task as act_task,
    workflow_activation::{wf_activation_job, FireTimer, WfActivation, WfActivationJob},
    workflow_commands::{CancelTimer, CompleteWorkflowExecution, StartTimer},
    workflow_completion::WfActivationCompletion,
};

#[tokio::test]
async fn timer_workflow() {
    let (core, _) = init_core_and_create_wf("timer_workflow").await;
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![StartTimer {
            timer_id: "timer-1".to_string(),
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
        }
        .into()],
        task.task_token,
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn timer_cancel_workflow() {
    let (core, _) = init_core_and_create_wf("timer_cancel_workflow").await;
    let timer_id = "wait_timer";
    let cancel_timer_id = "cancel_timer";
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
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
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
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
    let (core, _) = init_core_and_create_wf("timer_immediate_cancel_workflow").await;
    let cancel_timer_id = "cancel_timer";
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
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
async fn parallel_timer_workflow() {
    let (core, _) = init_core_and_create_wf("parallel_timer_workflow").await;
    let timer_id = "timer 1".to_string();
    let timer_2_id = "timer 2".to_string();
    let task = core.poll_workflow_task().await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
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
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.task_token,
    ))
    .await
    .unwrap();
}
