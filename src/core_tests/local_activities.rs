use crate::{
    pollers::MockServerGatewayApis,
    prototype_rust_sdk::{LocalActivityOptions, TestRustWorker, WfContext},
    test_help::{
        build_mock_pollers, mock_core, MockPollCfg, ResponseType, TestHistoryBuilder,
        DEFAULT_WORKFLOW_TYPE, TEST_Q,
    },
};
use std::{sync::Arc, time::Duration};
use temporal_sdk_core_protos::{coresdk::AsJsonPayloadExt, temporal::api::enums::v1::EventType};

/// This test verifies that when replaying we are able to resolve local activities whose data we
/// don't see until after the workflow issues the command
#[tokio::test]
async fn local_act_two_wfts_before_marker() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
    t.add_full_wf_task();
    t.add_local_activity_result_marker(1, "1", b"echo".into());
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = MockServerGatewayApis::new();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::AllHistory], mock);
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(TEST_Q, |cfg| {
        cfg.max_cached_workflows = 2;
    });
    let core = mock_core(mock);
    let mut worker = TestRustWorker::new(Arc::new(core), TEST_Q.to_string(), None);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |mut ctx: WfContext| async move {
            let la = ctx.local_activity(LocalActivityOptions {
                activity_type: "echo_activity".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                ..Default::default()
            });
            dbg!("B4 timer");
            ctx.timer(Duration::from_secs(1)).await;
            dbg!("HERE!");
            la.await;
            Ok(().into())
        },
    );
    worker
        .submit_wf(wf_id.to_owned(), DEFAULT_WORKFLOW_TYPE.to_owned(), vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

// pub async fn local_act_fanout_wf(mut ctx: WfContext) -> WorkflowResult<()> {
//     let las: Vec<_> = (1..=50)
//         .map(|i| {
//             ctx.local_activity(LocalActivityOptions {
//                 activity_type: "echo_activity".to_string(),
//                 input: format!("Hi {}", i)
//                     .as_json_payload()
//                     .expect("serializes fine"),
//                 ..Default::default()
//             })
//         })
//         .collect();
//     ctx.timer(Duration::from_secs(1)).await;
//     join_all(las).await;
//     Ok(().into())
// }
//
// #[tokio::test]
// async fn local_act_max_concurrent_respected() {
//     let mut t = TestHistoryBuilder::default();
//     t.add_by_type(EventType::WorkflowExecutionStarted);
//     t.add_full_wf_task();
//     let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
//     t.add_full_wf_task();
//     for i in 1..=50 {
//         t.add_local_activity_result_marker(i, &i.to_string(), b"echo".into());
//     }
//     t.add_timer_fired(timer_started_event_id, "1".to_string());
//     t.add_full_wf_task();
//     t.add_workflow_execution_completed();
//
//     let wf_id = "fakeid";
//     let mock = MockServerGatewayApis::new();
//     let mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::AllHistory], mock);
//     let mock = build_mock_pollers(mh);
//     let core = mock_core(mock);
//     let mut worker = TestRustWorker::new(Arc::new(core), TEST_Q.to_string(), None);
//
//     worker.register_wf(DEFAULT_WORKFLOW_TYPE.to_owned(), local_act_fanout_wf);
//     worker
//         .submit_wf(wf_id.to_owned(), DEFAULT_WORKFLOW_TYPE.to_owned(), vec![])
//         .await
//         .unwrap();
//     worker.run_until_done().await.unwrap();
// }
