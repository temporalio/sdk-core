use crate::{
    machines::test_help::{
        build_fake_core, build_multihist_mock_sg, hist_to_poll_resp, mock_core,
        mock_core_with_opts_no_workers, FakeWfResponses, TEST_Q,
    },
    pollers::MockManualGateway,
    protos::coresdk::workflow_activation::wf_activation_job,
    protos::coresdk::workflow_commands::{
        ActivityCancellationType, CompleteWorkflowExecution, RequestCancelActivity,
        ScheduleActivity, StartTimer,
    },
    protos::coresdk::workflow_completion::WfActivationCompletion,
    test_help::canned_histories,
    Core, CoreInitOptionsBuilder, CoreSDK, PollWfError, ServerGatewayApis, WorkerConfigBuilder,
};
use futures::FutureExt;
use rstest::{fixture, rstest};
use tokio::sync::watch;

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
        .await
        .unwrap();
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
async fn worker_double_register_is_err() {
    let t = canned_histories::single_timer("fake_timer");
    let core = build_fake_core("fake_wf_id", t, &[]);
    assert!(core
        .inner
        .register_worker(
            WorkerConfigBuilder::default()
                .task_queue(TEST_Q)
                .max_outstanding_workflow_tasks(2_usize)
                .build()
                .unwrap(),
        )
        .await
        .is_err());
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
        .await
        .unwrap();
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

#[tokio::test]
async fn nonexistent_worker_poll_returns_not_registered() {
    let core =
        mock_core_with_opts_no_workers(MockManualGateway::new(), CoreInitOptionsBuilder::default());
    assert_matches!(
        core.poll_workflow_task(TEST_Q).await.unwrap_err(),
        PollWfError::NoWorkerForQueue(_)
    );
}

#[tokio::test]
async fn after_shutdown_of_worker_get_shutdown_err() {
    let t = canned_histories::single_timer("fake_timer");
    let core = build_fake_core("fake_wf_id", t, &[1]);
    let res = core.inner.poll_workflow_task(TEST_Q).await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    core.inner.shutdown_worker(TEST_Q).await;
    assert_matches!(
        core.inner.poll_workflow_task(TEST_Q).await.unwrap_err(),
        PollWfError::ShutDown
    );
}

#[tokio::test]
async fn after_shutdown_of_worker_can_be_reregistered() {
    let t = canned_histories::single_timer("fake_timer");
    let core = build_fake_core("fake_wf_id", t, &[1]);
    let res = core.inner.poll_workflow_task(TEST_Q).await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    core.inner.shutdown_worker(TEST_Q).await;
    core.inner
        .register_worker(
            WorkerConfigBuilder::default()
                .task_queue(TEST_Q)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn shutdown_worker_can_complete_pending_activation() {
    let t = canned_histories::single_timer("fake_timer");
    let core = build_fake_core("fake_wf_id", t, &[2]);
    let res = core.inner.poll_workflow_task(TEST_Q).await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    // Complete the timer, will queue PA
    core.inner
        .complete_workflow_task(WfActivationCompletion::from_cmds(
            vec![StartTimer {
                timer_id: "fake_timer".to_string(),
                ..Default::default()
            }
            .into()],
            res.run_id,
        ))
        .await
        .unwrap();

    core.inner.shutdown_worker(TEST_Q).await;
    let res = core.inner.poll_workflow_task(TEST_Q).await.unwrap();
    // The timer fires
    assert_eq!(res.jobs.len(), 1);
    core.inner
        .complete_workflow_task(WfActivationCompletion::from_cmds(
            vec![CompleteWorkflowExecution::default().into()],
            res.run_id,
        ))
        .await
        .unwrap();
    // Since non-sticky, one more activation for eviction
    core.inner.poll_workflow_task(TEST_Q).await.unwrap();
    // Now it's shut down
    assert_matches!(
        core.inner.poll_workflow_task(TEST_Q).await.unwrap_err(),
        PollWfError::ShutDown
    );
}

#[fixture]
fn worker_shutdown() -> (
    CoreSDK<impl ServerGatewayApis + Send + Sync + 'static>,
    watch::Sender<bool>,
) {
    let mut mock_gateway = MockManualGateway::new();
    let (tx, rx) = watch::channel(false);
    mock_gateway
        .expect_poll_workflow_task()
        .returning(move |_| {
            let mut rx = rx.clone();
            async move {
                let t = canned_histories::single_timer("fake_timer");
                // Don't resolve polls until worker shuts down
                rx.changed().await.unwrap();
                Ok(hist_to_poll_resp(
                    &t,
                    "wf".to_string(),
                    100,
                    TEST_Q.to_string(),
                ))
            }
            .boxed()
        });
    (mock_core(mock_gateway), tx)
}

#[rstest]
#[tokio::test]
async fn worker_shutdown_during_poll_doesnt_deadlock(
    worker_shutdown: (
        CoreSDK<impl ServerGatewayApis + Send + Sync + 'static>,
        watch::Sender<bool>,
    ),
) {
    let (core, tx) = worker_shutdown;
    let pollfut = core.poll_workflow_task(TEST_Q);
    let shutdownfut = async {
        core.shutdown_worker(TEST_Q).await;
        tx.send(true).unwrap();
    };
    let (pollres, _) = tokio::join!(pollfut, shutdownfut);
    assert_matches!(pollres.unwrap_err(), PollWfError::ShutDown);
    core.shutdown().await;
}

#[rstest]
#[tokio::test]
async fn worker_shutdown_during_multiple_poll_doesnt_deadlock(
    worker_shutdown: (
        CoreSDK<impl ServerGatewayApis + Send + Sync + 'static>,
        watch::Sender<bool>,
    ),
) {
    let (core, tx) = worker_shutdown;
    core.register_worker(
        WorkerConfigBuilder::default()
            .task_queue("q2")
            .build()
            .unwrap(),
    )
    .await
    .unwrap();

    let pollfut = core.poll_workflow_task(TEST_Q);
    let poll2fut = core.poll_workflow_task("q2");
    let shutdownfut = async {
        core.shutdown_worker(TEST_Q).await;
        tx.send(true).unwrap();
    };
    let (pollres, poll2res, _) = tokio::join!(pollfut, poll2fut, shutdownfut);
    assert_matches!(pollres.unwrap_err(), PollWfError::ShutDown);
    // Worker 2 poll should not be an error
    poll2res.unwrap();
    core.shutdown().await;
}
