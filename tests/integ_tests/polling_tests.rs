use crate::integ_tests::{
    create_workflow, get_integ_core, get_integ_server_options,
    simple_wf_tests::schedule_activity_and_timer_cmds,
};
use assert_matches::assert_matches;
use crossbeam::channel::{unbounded, RecvTimeoutError};
use rand::Rng;
use std::{sync::Arc, time::Duration};
use temporal_sdk_core::protos::coresdk::{
    activity_task::activity_task as act_task,
    workflow_activation::{wf_activation_job, FireTimer, WfActivationJob},
    workflow_commands::{
        ActivityCancellationType, CompleteWorkflowExecution, RequestCancelActivity,
    },
    workflow_completion::WfActivationCompletion,
};
use temporal_sdk_core::{tracing_init, Core, CoreInitOptions};
use tokio::time::timeout;

#[tokio::test]
async fn out_of_order_completion_doesnt_hang() {
    tracing_init();

    let mut rng = rand::thread_rng();
    let task_q_salt: u32 = rng.gen();
    let task_q = format!("activity_cancelled_workflow_{}", task_q_salt.to_string());
    let core = Arc::new(get_integ_core(&task_q).await);
    let workflow_id: u32 = rng.gen();
    create_workflow(&*core, &task_q, &workflow_id.to_string(), None).await;
    let activity_id: String = rng.gen::<u32>().to_string();
    let timer_id: String = rng.gen::<u32>().to_string();
    let task = core.poll_workflow_task().await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_task(schedule_activity_and_timer_cmds(
        &task_q,
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

    // Start polling again *before* we complete the WFT
    let cc = core.clone();
    let jh = tokio::spawn(async move {
        // We want to fail the test if this takes too long -- we should not hit long poll timeout
        let task = timeout(Duration::from_secs(1), cc.poll_workflow_task())
            .await
            .expect("Poll should come back right away")
            .unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        cc.complete_workflow_task(WfActivationCompletion::ok_from_cmds(
            vec![CompleteWorkflowExecution { result: None }.into()],
            task.task_token,
        ))
        .await
        .unwrap();
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    // Then complete the (last) WFT with a request to cancel the AT, which should produce a
    // pending activation, unblocking the (already started) poll
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

    jh.await.unwrap();
}

#[tokio::test]
async fn long_poll_timeout_is_retried() {
    let mut gateway_opts = get_integ_server_options("some_task_q");
    // Server whines unless long poll > 2 seconds
    gateway_opts.long_poll_timeout = Duration::from_secs(3);
    let core = temporal_sdk_core::init(CoreInitOptions {
        gateway_opts,
        evict_after_pending_cleared: false,
        max_outstanding_workflow_tasks: 1,
        max_outstanding_activities: 1,
    })
    .await
    .unwrap();
    // Should block for more than 3 seconds, since we internally retry long poll
    let (tx, rx) = unbounded();
    tokio::spawn(async move {
        core.poll_workflow_task().await.unwrap();
        tx.send(())
    });
    let err = rx.recv_timeout(Duration::from_secs(4)).unwrap_err();
    assert_matches!(err, RecvTimeoutError::Timeout);
}
