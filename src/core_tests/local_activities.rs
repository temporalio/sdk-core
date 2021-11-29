use crate::{
    pollers::MockServerGatewayApis,
    prototype_rust_sdk::{LocalActivityOptions, TestRustWorker, WfContext, WorkflowResult},
    test_help::{
        build_mock_pollers, history_builder::default_wes_attribs, mock_core, MockPollCfg,
        ResponseType, TestHistoryBuilder, DEFAULT_WORKFLOW_TYPE, TEST_Q,
    },
    Core,
};
use futures::future::join_all;
use std::{sync::Arc, time::Duration};
use temporal_sdk_core_protos::{coresdk::AsJsonPayloadExt, temporal::api::enums::v1::EventType};
use tokio::sync::Barrier;

async fn echo(e: String) -> String {
    e
}

/// This test verifies that when replaying we are able to resolve local activities whose data we
/// don't see until after the workflow issues the command
#[rstest::rstest]
#[case::replay(true)]
#[case::not_replay(false)]
#[tokio::test]
async fn local_act_two_wfts_before_marker(#[case] replay: bool) {
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
    let resps = if replay {
        vec![ResponseType::AllHistory]
    } else {
        vec![1.into(), 2.into(), ResponseType::AllHistory]
    };
    let mh = MockPollCfg::from_resp_batches(wf_id, t, resps, mock);
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(TEST_Q, |cfg| cfg.max_cached_workflows = 2);
    let core = mock_core(mock);
    let mut worker = TestRustWorker::new(Arc::new(core), TEST_Q.to_string(), None);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |mut ctx: WfContext| async move {
            let la = ctx.local_activity(LocalActivityOptions {
                activity_type: "echo".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                ..Default::default()
            });
            ctx.timer(Duration::from_secs(1)).await;
            la.await;
            Ok(().into())
        },
    );
    worker.register_activity("echo", echo);
    worker
        .submit_wf(wf_id.to_owned(), DEFAULT_WORKFLOW_TYPE.to_owned(), vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

pub async fn local_act_fanout_wf(mut ctx: WfContext) -> WorkflowResult<()> {
    let las: Vec<_> = (1..=50)
        .map(|i| {
            ctx.local_activity(LocalActivityOptions {
                activity_type: "echo".to_string(),
                input: format!("Hi {}", i)
                    .as_json_payload()
                    .expect("serializes fine"),
                ..Default::default()
            })
        })
        .collect();
    ctx.timer(Duration::from_secs(1)).await;
    join_all(las).await;
    Ok(().into())
}

#[tokio::test]
async fn local_act_many_concurrent() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
    t.add_full_wf_task();
    for i in 1..=50 {
        t.add_local_activity_result_marker(i, &i.to_string(), b"echo".into());
    }
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = MockServerGatewayApis::new();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 3], mock);
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(TEST_Q, |wc| wc.max_outstanding_local_activities = 1);
    let core = mock_core(mock);
    let mut worker = TestRustWorker::new(Arc::new(core), TEST_Q.to_string(), None);

    worker.register_wf(DEFAULT_WORKFLOW_TYPE.to_owned(), local_act_fanout_wf);
    worker.register_activity("echo", |str: String| async move { str });
    worker
        .submit_wf(wf_id.to_owned(), DEFAULT_WORKFLOW_TYPE.to_owned(), vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

/// Verifies that local activities which take more than a workflow task timeout will cause
/// us to issue additional (empty) WFT completions with the force flag on, thus preventing timeout
/// of WFT while the local activity continues to execute.
///
/// The test with shutdown verifies if we call shutdown while the local activity is running that
/// shutdown does not complete until it's finished.
#[rstest::rstest]
#[case::with_shutdown(true)]
#[case::normal_complete(false)]
#[tokio::test]
async fn local_act_heartbeat(#[case] shutdown_middle: bool) {
    let mut t = TestHistoryBuilder::default();
    let wft_timeout = Duration::from_millis(200);
    let mut wes_short_wft_timeout = default_wes_attribs();
    wes_short_wft_timeout.workflow_task_timeout = Some(wft_timeout.into());
    t.add(
        EventType::WorkflowExecutionStarted,
        wes_short_wft_timeout.into(),
    );
    t.add_full_wf_task();
    // Task created by WFT heartbeat
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = MockServerGatewayApis::new();
    // Allow returning incomplete history more than once, as the wft timeout can be timing sensitive
    // and might poll an extra time
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 2, 2, 2], mock);
    mh.enforce_correct_number_of_polls = false;
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(TEST_Q, |wc| wc.max_cached_workflows = 1);
    let core = Arc::new(mock_core(mock));
    let mut worker = TestRustWorker::new(core.clone(), TEST_Q.to_string(), None);
    let shutdown_barr: &'static Barrier = Box::leak(Box::new(Barrier::new(2)));

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |mut ctx: WfContext| async move {
            ctx.local_activity(LocalActivityOptions {
                activity_type: "echo".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                ..Default::default()
            })
            .await;
            Ok(().into())
        },
    );
    worker.register_activity("echo", move |str: String| async move {
        if shutdown_middle {
            shutdown_barr.wait().await;
        }
        // Take slightly more than two workflow tasks
        tokio::time::sleep(wft_timeout.mul_f32(2.2)).await;
        str
    });
    worker
        .submit_wf(wf_id.to_owned(), DEFAULT_WORKFLOW_TYPE.to_owned(), vec![])
        .await
        .unwrap();
    let (_, runres) = tokio::join!(
        async {
            if shutdown_middle {
                shutdown_barr.wait().await;
                core.shutdown().await;
            }
        },
        worker.run_until_done()
    );
    runres.unwrap();
}
