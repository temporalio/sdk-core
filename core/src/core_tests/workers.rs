use crate::{
    test_help::{
        build_fake_core, build_mock_pollers, build_multihist_mock_sg, canned_histories,
        hist_to_poll_resp, mock_core, mock_core_with_opts_no_workers, mock_gateway,
        mock_manual_poller, register_mock_workers, single_hist_mock_sg, FakeWfResponses,
        MockPollCfg, MockWorker, MocksHolder, ResponseType, TEST_Q,
    },
    Core, CoreInitOptionsBuilder, CoreSDK, PollActivityError, PollWfError, WorkerConfigBuilder,
};
use futures::FutureExt;
use rstest::{fixture, rstest};
use std::{cell::RefCell, time::Duration};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::workflow_activation_job,
        workflow_commands::{
            workflow_command, ActivityCancellationType, CompleteWorkflowExecution,
            RequestCancelActivity, ScheduleActivity, StartTimer,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::workflowservice::v1::RespondWorkflowTaskCompletedResponse,
};
use temporal_sdk_core_test_utils::{mock_manual_gateway, start_timer_cmd};
use tokio::sync::{watch, Barrier};

#[tokio::test]
async fn multi_workers() {
    // Make histories for 5 different workflows on 5 different task queues
    let hists = (0..5).into_iter().map(|i| {
        let wf_id = format!("fake-wf-{}", i);
        let hist = canned_histories::single_timer("1");
        FakeWfResponses {
            wf_id,
            hist,
            response_batches: vec![1.into(), 2.into()],
            task_q: format!("q-{}", i),
        }
    });
    let mock = build_multihist_mock_sg(hists, false, None);

    let core = &mock_core(mock);

    for i in 0..5 {
        let tq = format!("q-{}", i);
        let res = core.poll_workflow_activation(&tq).await.unwrap();
        assert_matches!(
            res.jobs[0].variant,
            Some(workflow_activation_job::Variant::StartWorkflow(_))
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(tq, res.run_id))
            .await
            .unwrap();
    }
    core.shutdown().await;
}

#[tokio::test]
async fn no_worker_for_queue_error_returned_properly() {
    let t = canned_histories::single_timer("1");
    // Empty batches to specify 0 calls to poll expectation
    let core = build_fake_core("fake_wf_id", t, Vec::<ResponseType>::new());

    core.shutdown_worker(TEST_Q).await;
    let res = core.poll_workflow_activation(TEST_Q).await.unwrap_err();
    assert_matches!(res, PollWfError::ShutDown);
    core.shutdown().await;
}

#[tokio::test]
async fn shutdown_worker_stays_shutdown_not_no_worker() {
    let t = canned_histories::single_timer("1");
    // Empty batches to specify 0 calls to poll expectation
    let core = build_fake_core("fake_wf_id", t, Vec::<ResponseType>::new());

    let fake_q = "not a registered queue";
    let res = core.poll_workflow_activation(fake_q).await.unwrap_err();
    assert_matches!(res, PollWfError::NoWorkerForQueue(err_q) => err_q == fake_q);
    core.shutdown().await;
}

#[tokio::test]
async fn worker_double_register_is_err() {
    let t = canned_histories::single_timer("1");
    let core = build_fake_core("fake_wf_id", t, Vec::<ResponseType>::new());
    assert!(core
        .register_worker(
            WorkerConfigBuilder::default()
                .task_queue(TEST_Q)
                .max_outstanding_workflow_tasks(2_usize)
                .build()
                .unwrap(),
        )
        .is_err());
}

// Here we're making sure that when a pending activity is generated for a workflow on one task
// queue that it doesn't "leak" over and get returned when lang polls for a different task queue
#[tokio::test]
async fn pending_activities_only_returned_for_their_queue() {
    let act_id = 1;
    let histmaker = |qname: String| {
        let hist =
            canned_histories::cancel_scheduled_activity(act_id.to_string().as_str(), "sig-1");
        FakeWfResponses {
            wf_id: "wf1".to_string(),
            hist,
            response_batches: vec![1.into(), 2.into()],
            task_q: qname,
        }
    };
    let hist_q_1 = histmaker("q-1".to_string());
    let hist_q_2 = histmaker("q-2".to_string());
    let mock = build_multihist_mock_sg(vec![hist_q_1, hist_q_2], false, None);
    let core = &mock_core(mock);

    // Create a pending activation by cancelling a try-cancel activity
    let res = core.poll_workflow_activation("q-1").await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        "q-1",
        res.run_id,
        vec![ScheduleActivity {
            seq: act_id,
            activity_id: act_id.to_string(),
            cancellation_type: ActivityCancellationType::TryCancel as i32,
            ..Default::default()
        }
        .into()],
    ))
    .await
    .unwrap();
    let res = core.poll_workflow_activation("q-1").await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        "q-1",
        res.run_id,
        vec![RequestCancelActivity { seq: act_id }.into()],
    ))
    .await
    .unwrap();
    // Poll on the other task queue, verifying we get start workflow
    let res = core.poll_workflow_activation("q-2").await.unwrap();
    assert_matches!(
        res.jobs[0].variant,
        Some(workflow_activation_job::Variant::StartWorkflow(_))
    );
}

#[tokio::test]
async fn nonexistent_worker_poll_returns_not_registered() {
    let core =
        mock_core_with_opts_no_workers(mock_manual_gateway(), CoreInitOptionsBuilder::default());
    assert_matches!(
        core.poll_workflow_activation(TEST_Q).await.unwrap_err(),
        PollWfError::NoWorkerForQueue(_)
    );
}

#[tokio::test]
async fn after_shutdown_of_worker_get_shutdown_err() {
    let t = canned_histories::single_timer("1");
    let core = build_fake_core("fake_wf_id", t, &[1]);
    let res = core.poll_workflow_activation(TEST_Q).await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    let run_id = res.run_id;

    tokio::join!(core.shutdown_worker(TEST_Q), async {
        // Need to complete task for shutdown to finish
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            TEST_Q,
            run_id.clone(),
            workflow_command::Variant::StartTimer(StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(Duration::from_secs(1).into()),
            }),
        ))
        .await
        .unwrap();
        // Since non-sticky, one more activation for eviction
        let res = core.poll_workflow_activation(TEST_Q).await.unwrap();
        assert_matches!(
            res.jobs[0].variant,
            Some(workflow_activation_job::Variant::RemoveFromCache(_))
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(
            TEST_Q,
            run_id.clone(),
        ))
        .await
        .unwrap();
        assert_matches!(
            core.poll_workflow_activation(TEST_Q).await.unwrap_err(),
            PollWfError::ShutDown
        );
    });
}

#[tokio::test]
async fn after_shutdown_of_worker_can_be_reregistered() {
    let t = canned_histories::single_timer("1");
    let mut core = build_fake_core("fake_wf_id", t.clone(), &[1]);
    let res = core.poll_workflow_activation(TEST_Q).await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(TEST_Q, res.run_id))
        .await
        .unwrap();
    core.shutdown_worker(TEST_Q).await;
    // Need to recreate mock to re-register worker
    let mocks = single_hist_mock_sg("fake_wf_id", t, &[1], mock_gateway(), true);
    let pollers = mocks.take_pollers().into_values();
    register_mock_workers(&mut core, pollers);
    // Worker is replaced and the different mock returns a new wft
    let res = core.poll_workflow_activation(TEST_Q).await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(TEST_Q, res.run_id))
        .await
        .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn shutdown_worker_can_complete_pending_activation() {
    let t = canned_histories::single_timer("1");
    let core = build_fake_core("fake_wf_id", t, &[2]);
    let res = core.poll_workflow_activation(TEST_Q).await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    // Complete the timer, will queue PA
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        TEST_Q,
        res.run_id,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    ))
    .await
    .unwrap();

    tokio::join!(core.shutdown_worker(TEST_Q), async {
        let res = core.poll_workflow_activation(TEST_Q).await.unwrap();
        // The timer fires
        assert_eq!(res.jobs.len(), 1);
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            TEST_Q,
            res.run_id,
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();
        // Since non-sticky, one more activation for eviction
        core.poll_workflow_activation(TEST_Q).await.unwrap();
        // Now it's shut down
        assert_matches!(
            core.poll_workflow_activation(TEST_Q).await.unwrap_err(),
            PollWfError::ShutDown
        );
    });
}

#[fixture]
fn worker_shutdown() -> (CoreSDK, watch::Sender<bool>) {
    let (tx, rx) = watch::channel(false);

    let mut mock_pollers = vec![];
    for i in 1..=2 {
        let tq = format!("q{}", i);
        let mut mock_poller = mock_manual_poller();
        let rx = rx.clone();
        let tqc = tq.clone();
        mock_poller.expect_poll().returning(move || {
            let mut rx = rx.clone();
            let tqc = tqc.clone();
            async move {
                let t = canned_histories::single_timer("1");
                // Don't resolve polls until worker shuts down
                rx.changed().await.unwrap();
                Some(Ok(hist_to_poll_resp(
                    &t,
                    "wf".to_string(),
                    ResponseType::ToTaskNum(1),
                    tqc,
                )))
            }
            .boxed()
        });
        mock_pollers.push(MockWorker::new(&tq, Box::from(mock_poller)));
    }
    let mut mock_gateway = mock_gateway();
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
    (
        mock_core(MocksHolder::from_mock_workers(mock_gateway, mock_pollers)),
        tx,
    )
}

#[rstest]
#[tokio::test]
async fn worker_shutdown_during_poll_doesnt_deadlock(
    worker_shutdown: (CoreSDK, watch::Sender<bool>),
) {
    let (core, tx) = worker_shutdown;
    let pollfut = core.poll_workflow_activation("q1");
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
    worker_shutdown: (CoreSDK, watch::Sender<bool>),
) {
    let (core, tx) = worker_shutdown;

    let pollfut = async {
        assert_matches!(
            core.poll_workflow_activation("q1").await.unwrap_err(),
            PollWfError::ShutDown
        );
    };
    let poll2fut = async {
        let res = core.poll_workflow_activation("q2").await.unwrap();
        core.complete_workflow_activation(WorkflowActivationCompletion::empty("q2", res.run_id))
            .await
            .unwrap();
    };
    let shutdownfut = async {
        core.shutdown_worker("q1").await;
        // Will allow both workers to proceed
        let _ = tx.send(true);
    };
    tokio::join!(pollfut, poll2fut, shutdownfut);
    core.shutdown().await;
}

#[rstest]
#[tokio::test]
async fn can_shutdown_local_act_only_worker_when_act_polling(
    #[values(true, false)] whole_core_shutdown: bool,
) {
    let t = canned_histories::single_timer("1");
    let mock = mock_gateway();
    let mh = MockPollCfg::from_resp_batches("fakeid", t, [1], mock);
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(TEST_Q, |w| {
        w.no_remote_activities = true;
    });
    let core = mock_core(mock);
    let barrier = Barrier::new(2);

    tokio::join!(
        async {
            barrier.wait().await;
            if whole_core_shutdown {
                core.shutdown().await;
            } else {
                core.shutdown_worker(TEST_Q).await;
            }
        },
        async {
            let res = core.poll_workflow_activation(TEST_Q).await.unwrap();
            // Complete so there's no outstanding WFT and shutdown can finish
            core.complete_workflow_activation(WorkflowActivationCompletion::empty(
                TEST_Q, res.run_id,
            ))
            .await
            .unwrap();
            barrier.wait().await;
            assert_matches!(
                core.poll_activity_task(TEST_Q).await.unwrap_err(),
                PollActivityError::ShutDown
            );
        }
    );
}

#[tokio::test]
async fn complete_with_task_not_found_during_shutdwn() {
    let t = canned_histories::single_timer("1");
    let mut mock = mock_gateway();
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(|_| Err(tonic::Status::not_found("Workflow task not found.")));
    let mh = MockPollCfg::from_resp_batches("fakeid", t, [1], mock);
    let core = mock_core(build_mock_pollers(mh));

    let res = core.poll_workflow_activation(TEST_Q).await.unwrap();
    assert_eq!(res.jobs.len(), 1);

    let complete_order = RefCell::new(vec![]);
    // Initiate shutdown before completing the activation
    let shutdown_fut = async {
        core.shutdown().await;
        complete_order.borrow_mut().push(3);
    };
    let poll_fut = async {
        // This should *not* return shutdown, but instead should do nothing until the complete
        // goes through, at which point it will return the eviction.
        let res = core.poll_workflow_activation(TEST_Q).await.unwrap();
        assert_matches!(
            res.jobs[0].variant,
            Some(workflow_activation_job::Variant::RemoveFromCache(_))
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(TEST_Q, res.run_id))
            .await
            .unwrap();
        complete_order.borrow_mut().push(2);
    };
    let complete_fut = async {
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            TEST_Q,
            res.run_id,
            vec![start_timer_cmd(1, Duration::from_secs(1))],
        ))
        .await
        .unwrap();
        complete_order.borrow_mut().push(1);
    };
    tokio::join!(shutdown_fut, poll_fut, complete_fut);
    assert_eq!(&complete_order.into_inner(), &[1, 2, 3])
}
