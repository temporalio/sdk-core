mod activity_tasks;
mod child_workflows;
mod determinism;
mod local_activities;
mod queries;
mod replay_flag;
mod workers;
mod workflow_cancels;
mod workflow_tasks;

use crate::{
    errors::{PollActivityError, PollWfError},
    test_help::{build_mock_pollers, canned_histories, mock_worker, test_worker_cfg, MockPollCfg},
    worker::client::mocks::{mock_manual_workflow_client, mock_workflow_client},
    Worker,
};
use futures::FutureExt;
use std::time::Duration;
use temporal_sdk_core_api::Worker as WorkerTrait;
use temporal_sdk_core_protos::coresdk::workflow_completion::WorkflowActivationCompletion;
use tokio::{sync::Barrier, time::sleep};

#[tokio::test]
async fn after_shutdown_server_is_not_polled() {
    let t = canned_histories::single_timer("fake_timer");
    let mh = MockPollCfg::from_resp_batches("fake_wf_id", t, [1], mock_workflow_client());
    let mut mock = build_mock_pollers(mh);
    // Just so we don't have to deal w/ cache overflow
    mock.worker_cfg(|cfg| cfg.max_cached_workflows = 1);
    let worker = mock_worker(mock);

    let res = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id))
        .await
        .unwrap();
    worker.shutdown().await;
    assert_matches!(
        worker.poll_workflow_activation().await.unwrap_err(),
        PollWfError::ShutDown
    );
    worker.finalize_shutdown().await;
}

// Better than cloning a billion arcs...
lazy_static::lazy_static! {
    static ref BARR: Barrier = Barrier::new(3);
}
#[tokio::test]
async fn shutdown_interrupts_both_polls() {
    let mut mock_client = mock_manual_workflow_client();
    mock_client
        .expect_poll_activity_task()
        .times(1)
        .returning(move |_, _| {
            async move {
                BARR.wait().await;
                sleep(Duration::from_secs(1)).await;
                Ok(Default::default())
            }
            .boxed()
        });
    mock_client
        .expect_poll_workflow_task()
        .times(1)
        .returning(move |_, _| {
            async move {
                BARR.wait().await;
                sleep(Duration::from_secs(1)).await;
                Ok(Default::default())
            }
            .boxed()
        });

    let worker = Worker::new_test(
        test_worker_cfg()
            // Need only 1 concurrent pollers for mock expectations to work here
            .max_concurrent_wft_polls(1_usize)
            .max_concurrent_at_polls(1_usize)
            .build()
            .unwrap(),
        mock_client,
    );
    tokio::join! {
        async {
            assert_matches!(worker.poll_activity_task().await.unwrap_err(),
                            PollActivityError::ShutDown);
        },
        async {
            assert_matches!(worker.poll_workflow_activation().await.unwrap_err(),
                            PollWfError::ShutDown);
        },
        async {
            // Give polling a bit to get stuck, then shutdown
            BARR.wait().await;
            worker.shutdown().await;
        }
    };
}
