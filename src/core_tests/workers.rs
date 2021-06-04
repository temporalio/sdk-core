use crate::machines::test_help::TEST_Q;
use crate::{
    machines::test_help::{build_fake_core, build_multihist_mock_sg, mock_core, FakeWfResponses},
    protos::coresdk::workflow_activation::wf_activation_job,
    protos::coresdk::workflow_commands::{
        ActivityCancellationType, RequestCancelActivity, ScheduleActivity,
    },
    protos::coresdk::workflow_completion::WfActivationCompletion,
    test_help::canned_histories,
    Core, PollWfError, WorkerConfigBuilder,
};

#[tokio::test]
async fn multi_workers() {
    // Make histories for 5 different workflows on 5 different task queues
    let hists = (0..5).into_iter().map(|i| {
        let wf_id = format!("fake-wf-{}", i);
        let hist = canned_histories::single_timer("fake_timer");
        FakeWfResponses {
            wf_id,
            hist,
            response_batches: vec![1, 2],
            task_q: format!("q-{}", i),
        }
    });
    let mock = build_multihist_mock_sg(hists, false, None);

    // This will register a pointless worker for `TEST_Q` as well, but, nothing will happen with it.
    let core = &mock_core(mock.sg);
    for i in 0..5 {
        core.register_worker(
            WorkerConfigBuilder::default()
                .task_queue(format!("q-{}", i))
                .build()
                .unwrap(),
        )
        .await;
    }

    for i in 0..5 {
        let tq = format!("q-{}", i);
        let res = core.poll_workflow_task(&tq).await.unwrap();
        assert_matches!(
            res.jobs[0].variant,
            Some(wf_activation_job::Variant::StartWorkflow(_))
        );
    }
    core.shutdown().await;
}

#[tokio::test]
async fn no_worker_for_queue_error_returned_properly() {
    let t = canned_histories::single_timer("fake_timer");
    // Empty batches array to specify 0 calls to poll expectation
    let core = build_fake_core("fake_wf_id", t, &[]);

    let fake_q = "not a registered queue";
    let res = core.inner.poll_workflow_task(&fake_q).await.unwrap_err();
    assert_matches!(res, PollWfError::NoWorkerForQueue(err_q) => err_q == fake_q);
    core.inner.shutdown().await;
}

#[tokio::test]
async fn worker_double_register_replaces_old_worker() {
    let t = canned_histories::single_timer("fake_timer");
    let core = build_fake_core("fake_wf_id", t, &[1]);
    // Replace the existing TEST_Q worker
    core.inner
        .register_worker(
            WorkerConfigBuilder::default()
                .task_queue(TEST_Q)
                .max_outstanding_workflow_tasks(2_usize)
                .build()
                .unwrap(),
        )
        .await;

    // If the old worker wasn't replaced properly this would hang forever
    core.inner.poll_workflow_task(TEST_Q).await.unwrap();
    core.inner.shutdown().await;
}

// Here we're making sure that when a pending activity is generated for a workflow on one task
// queue that it doesn't "leak" over and get returned when lang polls for a different task queue
#[tokio::test]
async fn pending_activities_only_returned_for_their_queue() {
    let act_id = "act-1";
    let histmaker = |qname: String| {
        let hist = canned_histories::cancel_scheduled_activity(act_id, "sig-1");
        FakeWfResponses {
            wf_id: "wf1".to_string(),
            hist,
            response_batches: vec![1, 2],
            task_q: qname,
        }
    };
    let hist_q_1 = histmaker("q-1".to_string());
    let hist_q_2 = histmaker("q-2".to_string());
    let mock = build_multihist_mock_sg(vec![hist_q_1, hist_q_2], false, None);
    let core = &mock_core(mock.sg);
    for i in 1..=2 {
        core.register_worker(
            WorkerConfigBuilder::default()
                .task_queue(format!("q-{}", i))
                .build()
                .unwrap(),
        )
        .await;
    }

    // Create a pending activation by cancelling a try-cancel activity
    let res = core.poll_workflow_task("q-1").await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![ScheduleActivity {
            activity_id: act_id.to_string(),
            cancellation_type: ActivityCancellationType::TryCancel as i32,
            ..Default::default()
        }
        .into()],
        res.run_id,
    ))
    .await
    .unwrap();
    let res = core.poll_workflow_task("q-1").await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![RequestCancelActivity {
            activity_id: act_id.to_string(),
            ..Default::default()
        }
        .into()],
        res.run_id,
    ))
    .await
    .unwrap();
    // Poll on the other task queue, verifying we get start workflow
    let res = core.poll_workflow_task("q-2").await.unwrap();
    assert_matches!(
        res.jobs[0].variant,
        Some(wf_activation_job::Variant::StartWorkflow(_))
    );
}
