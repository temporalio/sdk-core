use crate::{
    machines::test_help::{build_multihist_mock_sg, fake_sg_opts, FakeWfResponses},
    protos::coresdk::workflow_activation::wf_activation_job,
    test_help::canned_histories,
    Core, CoreInitOptionsBuilder, CoreSDK, WorkerConfigBuilder,
};
use std::collections::HashSet;

#[tokio::test]
async fn multi_workers() {
    let mut must_see_queues = HashSet::new();
    // Make histories for 5 different workflows on 5 different task queues
    let hists = (0..5).into_iter().map(|i| {
        let wf_id = format!("fake-wf-{}", i);
        let hist = canned_histories::single_timer("fake_timer");
        let queue_name = format!("q-{}", i);
        must_see_queues.insert(queue_name.clone());
        FakeWfResponses {
            wf_id,
            hist,
            response_batches: vec![1, 2],
            task_q: queue_name,
        }
    });
    let mock = build_multihist_mock_sg(hists, false, None);

    let core = CoreSDK::new(
        mock.sg,
        CoreInitOptionsBuilder::default()
            .gateway_opts(fake_sg_opts())
            .build()
            .unwrap(),
    );
    for queue_name in must_see_queues.iter() {
        core.register_worker(
            WorkerConfigBuilder::default()
                .task_queue(queue_name.clone())
                .build()
                .unwrap(),
        )
        .await;
    }

    for _ in 0..5 {
        let res = core.poll_workflow_task().await.unwrap();
        assert!(must_see_queues.remove(&res.task_queue));
        assert_matches!(
            res.jobs[0].variant,
            Some(wf_activation_job::Variant::StartWorkflow(_))
        );
    }
    core.shutdown().await;
}
