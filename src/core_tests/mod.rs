mod activity_tasks;
mod queries;
mod replay_flag;
mod workers;
mod workflow_tasks;

use crate::{
    errors::{PollActivityError, PollWfError},
    pollers::MockManualGateway,
    protos::temporal::api::workflowservice::v1::PollActivityTaskQueueResponse,
    test_help::{build_fake_core, canned_histories, fake_sg_opts, hist_to_poll_resp, TEST_Q},
    Core, CoreInitOptionsBuilder, CoreSDK, WorkerConfigBuilder,
};
use futures::FutureExt;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn after_shutdown_server_is_not_polled() {
    let t = canned_histories::single_timer("fake_timer");
    let core = build_fake_core("fake_wf_id", t, &[1]);
    let res = core.inner.poll_workflow_task(TEST_Q).await.unwrap();
    assert_eq!(res.jobs.len(), 1);

    core.inner.shutdown().await;
    assert_matches!(
        core.inner.poll_workflow_task(TEST_Q).await.unwrap_err(),
        PollWfError::ShutDown
    );
}

#[tokio::test]
async fn shutdown_interrupts_both_polls() {
    let mut mock_gateway = MockManualGateway::new();
    mock_gateway
        .expect_poll_activity_task()
        .times(1)
        .returning(move |_| {
            async move {
                sleep(Duration::from_secs(1)).await;
                Ok(PollActivityTaskQueueResponse {
                    task_token: vec![1],
                    heartbeat_timeout: Some(Duration::from_secs(1).into()),
                    ..Default::default()
                })
            }
            .boxed()
        });
    mock_gateway
        .expect_poll_workflow_task()
        .times(1)
        .returning(move |_| {
            async move {
                let t = canned_histories::single_timer("hi");
                sleep(Duration::from_secs(1)).await;
                Ok(hist_to_poll_resp(
                    &t,
                    "wf".to_string(),
                    100,
                    TEST_Q.to_string(),
                ))
            }
            .boxed()
        });

    let core = CoreSDK::new(
        mock_gateway,
        CoreInitOptionsBuilder::default()
            .gateway_opts(fake_sg_opts())
            .build()
            .unwrap(),
    );
    core.register_worker(
        WorkerConfigBuilder::default()
            .task_queue(TEST_Q)
            // Need only 1 concurrent pollers for mock expectations to work here
            .max_concurrent_wft_polls(1usize)
            .max_concurrent_at_polls(1usize)
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    tokio::join! {
        async {
            assert_matches!(core.poll_activity_task(TEST_Q).await.unwrap_err(),
                            PollActivityError::ShutDown);
        },
        async {
            assert_matches!(core.poll_workflow_task(TEST_Q).await.unwrap_err(),
                            PollWfError::ShutDown);
        },
        async {
            // Give polling a bit to get stuck, then shutdown
            sleep(Duration::from_millis(200)).await;
            core.shutdown().await;
        }
    };
}
