use crate::{
    test_help::{
        build_fake_worker, build_mock_pollers, canned_histories, hist_to_poll_resp,
        mock_manual_poller, mock_worker, MockPollCfg, MockWorker, MocksHolder, ResponseType,
    },
    PollActivityError, PollWfError,
};
use futures::FutureExt;

use crate::test_help::TEST_Q;
use std::{cell::RefCell, time::Duration};
use temporal_client::mocks::mock_gateway;
use temporal_sdk_core_api::Worker;
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::workflow_activation_job,
        workflow_commands::{workflow_command, CompleteWorkflowExecution, StartTimer},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::workflowservice::v1::RespondWorkflowTaskCompletedResponse,
};
use temporal_sdk_core_test_utils::start_timer_cmd;
use tokio::sync::{watch, Barrier};

#[tokio::test]
async fn multi_workers() {
    // TODO: Turn this into a test with multiple independent workers
    // Make histories for 5 different workflows on 5 different task queues
    // let hists = (0..5).into_iter().map(|i| {
    //     let wf_id = format!("fake-wf-{}", i);
    //     let hist = canned_histories::single_timer("1");
    //     FakeWfResponses {
    //         wf_id,
    //         hist,
    //         response_batches: vec![1.into(), 2.into()],
    //         task_q: format!("q-{}", i),
    //     }
    // });
    // let mock = build_multihist_mock_sg(hists, false, None);
    //
    // let core = &mock_worker(mock);
    //
    // for i in 0..5 {
    //     let tq = format!("q-{}", i);
    //     let res = core.poll_workflow_activation().await.unwrap();
    //     assert_matches!(
    //         res.jobs[0].variant,
    //         Some(workflow_activation_job::Variant::StartWorkflow(_))
    //     );
    //     core.complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id))
    //         .await
    //         .unwrap();
    // }
    // core.shutdown().await;
}

#[tokio::test]
async fn shutdown_worker_stays_shutdown_not_no_worker() {
    let t = canned_histories::single_timer("1");
    // Empty batches to specify 0 calls to poll expectation
    let core = build_fake_worker("fake_wf_id", t, Vec::<ResponseType>::new());

    let fake_q = "not a registered queue";
    let res = core.poll_workflow_activation().await.unwrap_err();
    assert_matches!(res, PollWfError::NoWorkerForQueue(err_q) => err_q == fake_q);
    core.shutdown().await;
}

#[tokio::test]
async fn after_shutdown_of_worker_get_shutdown_err() {
    let t = canned_histories::single_timer("1");
    let worker = build_fake_worker("fake_wf_id", t, &[1]);
    let res = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    let run_id = res.run_id;

    tokio::join!(worker.shutdown(), async {
        // Need to complete task for shutdown to finish
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                run_id.clone(),
                workflow_command::Variant::StartTimer(StartTimer {
                    seq: 1,
                    start_to_fire_timeout: Some(Duration::from_secs(1).into()),
                }),
            ))
            .await
            .unwrap();
        // Since non-sticky, one more activation for eviction
        let res = worker.poll_workflow_activation().await.unwrap();
        assert_matches!(
            res.jobs[0].variant,
            Some(workflow_activation_job::Variant::RemoveFromCache(_))
        );
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(run_id.clone()))
            .await
            .unwrap();
        assert_matches!(
            worker.poll_workflow_activation().await.unwrap_err(),
            PollWfError::ShutDown
        );
    });
}

#[tokio::test]
async fn shutdown_worker_can_complete_pending_activation() {
    let t = canned_histories::single_timer("1");
    let worker = build_fake_worker("fake_wf_id", t, &[2]);
    let res = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    // Complete the timer, will queue PA
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![start_timer_cmd(1, Duration::from_secs(1))],
        ))
        .await
        .unwrap();

    tokio::join!(worker.shutdown(), async {
        let res = worker.poll_workflow_activation().await.unwrap();
        // The timer fires
        assert_eq!(res.jobs.len(), 1);
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                res.run_id,
                vec![CompleteWorkflowExecution::default().into()],
            ))
            .await
            .unwrap();
        // Since non-sticky, one more activation for eviction
        worker.poll_workflow_activation().await.unwrap();
        // Now it's shut down
        assert_matches!(
            worker.poll_workflow_activation().await.unwrap_err(),
            PollWfError::ShutDown
        );
    });
}

#[tokio::test]
async fn worker_shutdown_during_poll_doesnt_deadlock() {
    let (tx, rx) = watch::channel(false);
    let mut mock_poller = mock_manual_poller();
    let rx = rx.clone();
    mock_poller.expect_poll().returning(move || {
        let mut rx = rx.clone();
        async move {
            let t = canned_histories::single_timer("1");
            // Don't resolve polls until worker shuts down
            rx.changed().await.unwrap();
            Some(Ok(hist_to_poll_resp(
                &t,
                "wf".to_string(),
                ResponseType::ToTaskNum(1),
                TEST_Q,
            )))
        }
        .boxed()
    });
    let mw = MockWorker::default();
    let mut mock_gateway = mock_gateway();
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
    let worker = mock_worker(MocksHolder::from_mock_worker(mock_gateway, mw));
    let pollfut = worker.poll_workflow_activation();
    let shutdownfut = async {
        worker.shutdown().await;
        // Either the send works and unblocks the poll or the poll future is dropped before actually
        // polling -- either way things worked OK
        let _ = tx.send(true);
    };
    let (pollres, _) = tokio::join!(pollfut, shutdownfut);
    assert_matches!(pollres.unwrap_err(), PollWfError::ShutDown);
    worker.shutdown().await;
}

#[tokio::test]
async fn can_shutdown_local_act_only_worker_when_act_polling() {
    let t = canned_histories::single_timer("1");
    let mock = mock_gateway();
    let mh = MockPollCfg::from_resp_batches("fakeid", t, [1], mock);
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|w| {
        w.no_remote_activities = true;
    });
    let worker = mock_worker(mock);
    let barrier = Barrier::new(2);

    tokio::join!(
        async {
            barrier.wait().await;
            worker.shutdown().await;
        },
        async {
            let res = worker.poll_workflow_activation().await.unwrap();
            // Complete so there's no outstanding WFT and shutdown can finish
            worker
                .complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id))
                .await
                .unwrap();
            barrier.wait().await;
            assert_matches!(
                worker.poll_activity_task().await.unwrap_err(),
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
    let core = mock_worker(build_mock_pollers(mh));

    let res = core.poll_workflow_activation().await.unwrap();
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
        let res = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            res.jobs[0].variant,
            Some(workflow_activation_job::Variant::RemoveFromCache(_))
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id))
            .await
            .unwrap();
        complete_order.borrow_mut().push(2);
    };
    let complete_fut = async {
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
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
