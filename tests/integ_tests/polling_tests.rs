use assert_matches::assert_matches;
use crossbeam::channel::{unbounded, RecvTimeoutError};
use std::time::Duration;
use temporal_sdk_core::protos::coresdk::{
    activity_task::activity_task as act_task,
    workflow_activation::{wf_activation_job, FireTimer, WfActivationJob},
    workflow_commands::{ActivityCancellationType, RequestCancelActivity, StartTimer},
    workflow_completion::WfActivationCompletion,
};
use temporal_sdk_core::{Core, CoreInitOptionsBuilder, IntoCompletion};
use test_utils::{
    get_integ_server_options, init_core_and_create_wf, schedule_activity_cmd, CoreTestHelpers,
};
use tokio::time::timeout;

#[tokio::test]
async fn out_of_order_completion_doesnt_hang() {
    let (core, task_q) = init_core_and_create_wf("out_of_order_completion_doesnt_hang").await;
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

    // Start polling again *before* we complete the WFT
    let cc = core.clone();
    let jh = tokio::spawn(async move {
        // We want to fail the test if this takes too long -- we should not hit long poll timeout
        let task = timeout(Duration::from_secs(1), cc.poll_workflow_task(&task_q))
            .await
            .expect("Poll should come back right away")
            .unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        cc.complete_execution(&task.run_id).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    // Then complete the (last) WFT with a request to cancel the AT, which should produce a
    // pending activation, unblocking the (already started) poll
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![RequestCancelActivity {
            activity_id: activity_id.to_string(),
            ..Default::default()
        }
        .into()],
        task.run_id,
    ))
    .await
    .unwrap();

    jh.await.unwrap();
}

#[tokio::test]
async fn long_poll_timeout_is_retried() {
    let mut gateway_opts = get_integ_server_options();
    // Server whines unless long poll > 2 seconds
    gateway_opts.long_poll_timeout = Duration::from_secs(3);
    let core = temporal_sdk_core::init(
        CoreInitOptionsBuilder::default()
            .gateway_opts(gateway_opts)
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    // Should block for more than 3 seconds, since we internally retry long poll
    let (tx, rx) = unbounded();
    tokio::spawn(async move {
        core.poll_workflow_task("doesnt_matter").await.unwrap();
        tx.send(())
    });
    let err = rx.recv_timeout(Duration::from_secs(4)).unwrap_err();
    assert_matches!(err, RecvTimeoutError::Timeout);
}
