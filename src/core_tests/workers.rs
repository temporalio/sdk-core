use crate::{
    machines::test_help::{build_fake_core, build_multihist_mock_sg, mock_core, FakeWfResponses},
    protos::coresdk::workflow_activation::wf_activation_job,
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
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("fake_timer");
    // Empty batches array to specify 0 calls to poll expectation
    let core = build_fake_core(wfid, t, &[]);

    let fake_q = "not a registered queue";
    let res = core.inner.poll_workflow_task(&fake_q).await.unwrap_err();
    assert_matches!(res, PollWfError::NoWorkerForQueue(err_q) => err_q == fake_q);
    core.inner.shutdown().await;
}
