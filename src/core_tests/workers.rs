use crate::{
    errors::PollWfError::TonicError,
    pollers::{BoxedWFPoller, MockManualGateway, MockManualPoller, MockServerGatewayApis},
    protos::coresdk::{
        workflow_activation::wf_activation_job,
        workflow_commands::{
            ActivityCancellationType, CompleteWorkflowExecution, RequestCancelActivity,
            ScheduleActivity, StartTimer,
        },
        workflow_completion::WfActivationCompletion,
    },
    test_help::{
        build_fake_core, build_multihist_mock_sg, canned_histories, hist_to_poll_resp, mock_core,
        mock_core_with_opts_no_workers, register_mock_workers, single_hist_mock_sg,
        FakeWfResponses, MocksHolder, TEST_Q,
    },
    Core, CoreInitOptionsBuilder, CoreSDK, PollWfError, ServerGatewayApis, WorkerConfigBuilder,
};
use futures::FutureExt;
use rstest::{fixture, rstest};
use std::collections::HashMap;
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

    let core = &mock_core(mock);

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
    let core = &mock_core(mock);

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
    let mut core = build_fake_core("fake_wf_id", t.clone(), &[1]);
    let res = core.inner.poll_workflow_task(TEST_Q).await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    // Need to recreate mock to re-register worker
    let mocks = single_hist_mock_sg("fake_wf_id", t, &[1, 2], MockServerGatewayApis::new(), true);
    core.inner.shutdown_worker(TEST_Q).await;
    register_mock_workers(&mut core.inner, mocks.mock_pollers);
    // The mock doesn't understand about shutdowns, but all we need to do is make sure the poller
    // gets hit (the mock) -- it will return no work here since there is still an outstanding WFT,
    // which is persistent across the recreated worker.
    assert_matches!(
        core.inner.poll_workflow_task(TEST_Q).await.unwrap_err(),
        TonicError(_)
    );
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
fn worker_shutdown() -> (CoreSDK<MockServerGatewayApis>, watch::Sender<bool>) {
    let (tx, rx) = watch::channel(false);

    let mut mock_pollers = HashMap::new();
    for i in 1..=2 {
        let tq = format!("q{}", i);
        let mut mock_poller = MockManualPoller::new();
        mock_poller.expect_notify_shutdown().return_const(());
        mock_poller
            .expect_shutdown_box()
            .returning(|| async {}.boxed());
        let rx = rx.clone();
        let tqc = tq.clone();
        mock_poller.expect_poll().returning(move || {
            let mut rx = rx.clone();
            let tqc = tqc.clone();
            async move {
                let t = canned_histories::single_timer("fake_timer");
                // Don't resolve polls until worker shuts down
                rx.changed().await.unwrap();
                Some(Ok(hist_to_poll_resp(&t, "wf".to_string(), 100, tqc)))
            }
            .boxed()
        });
        mock_pollers.insert(tq, (Box::new(mock_poller) as BoxedWFPoller, None));
    }
    (
        mock_core(MocksHolder::from_mock_pollers(
            MockServerGatewayApis::new(),
            mock_pollers,
        )),
        tx,
    )
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
    let pollfut = core.poll_workflow_task("q1");
    let shutdownfut = async {
        core.shutdown_worker("q1").await;
        // Either the send works and unblocks the poll or the poll future is dropped before actually
        // polling -- either way things worked OK
        let _ = tx.send(true);
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

    let pollfut = core.poll_workflow_task("q1");
    let poll2fut = core.poll_workflow_task("q2");
    let shutdownfut = async {
        core.shutdown_worker("q1").await;
        let _ = tx.send(true);
    };
    let (pollres, poll2res, _) = tokio::join!(pollfut, poll2fut, shutdownfut);
    assert_matches!(pollres.unwrap_err(), PollWfError::ShutDown);
    // Worker 2 poll should not be an error
    poll2res.unwrap();
    core.shutdown().await;
}
