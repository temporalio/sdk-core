use crate::{
    pollers::MockServerGatewayApis,
    prototype_rust_sdk::{LocalActivityOptions, TestRustWorker, WfContext, WorkflowResult},
    test_help::{
        build_mock_pollers, history_builder::default_wes_attribs, mock_core, MockPollCfg,
        ResponseType, TestHistoryBuilder, DEFAULT_WORKFLOW_TYPE, TEST_Q,
    },
    Core,
};
use anyhow::anyhow;
use futures::future::join_all;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use temporal_sdk_core_protos::{
    coresdk::{common::RetryPolicy, AsJsonPayloadExt},
    temporal::api::{enums::v1::EventType, failure::v1::Failure},
};
use tokio::sync::Barrier;

async fn echo(e: String) -> anyhow::Result<String> {
    Ok(e)
}

/// This test verifies that when replaying we are able to resolve local activities whose data we
/// don't see until after the workflow issues the command
#[rstest::rstest]
#[case::replay(true, true)]
#[case::not_replay(false, true)]
#[case::replay_cache_off(true, false)]
#[case::not_replay_cache_off(false, false)]
#[tokio::test]
async fn local_act_two_wfts_before_marker(#[case] replay: bool, #[case] cached: bool) {
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
    if cached {
        mock.worker_cfg(TEST_Q, |cfg| cfg.max_cached_workflows = 1);
    }
    let core = mock_core(mock);
    let mut worker = TestRustWorker::new(Arc::new(core), TEST_Q.to_string(), None);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
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

pub async fn local_act_fanout_wf(ctx: WfContext) -> WorkflowResult<()> {
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
    let mock = build_mock_pollers(mh);
    let core = mock_core(mock);
    let mut worker = TestRustWorker::new(Arc::new(core), TEST_Q.to_string(), None);

    worker.register_wf(DEFAULT_WORKFLOW_TYPE.to_owned(), local_act_fanout_wf);
    worker.register_activity("echo", |str: String| async move { Ok(str) });
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
        |ctx: WfContext| async move {
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
        Ok(str)
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

#[rstest::rstest]
#[case::retry_then_pass(true)]
#[case::retry_until_fail(false)]
#[tokio::test]
async fn local_act_fail_and_retry(#[case] eventually_pass: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = MockServerGatewayApis::new();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [1], mock);
    let mock = build_mock_pollers(mh);
    let core = mock_core(mock);
    let mut worker = TestRustWorker::new(Arc::new(core), TEST_Q.to_string(), None);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        move |ctx: WfContext| async move {
            let la_res = ctx
                .local_activity(LocalActivityOptions {
                    activity_type: "echo".to_string(),
                    input: "hi".as_json_payload().expect("serializes fine"),
                    retry_policy: RetryPolicy {
                        initial_interval: Some(Duration::from_millis(50).into()),
                        backoff_coefficient: 1.2,
                        maximum_interval: None,
                        maximum_attempts: 5,
                        non_retryable_error_types: vec![],
                    },
                    ..Default::default()
                })
                .await;
            if eventually_pass {
                assert!(la_res.completed_ok())
            } else {
                assert!(la_res.failed())
            }
            Ok(().into())
        },
    );
    let attempts: &'static _ = Box::leak(Box::new(AtomicUsize::new(0)));
    worker.register_activity("echo", move |_: String| async move {
        // Succeed on 3rd attempt (which is ==2 since fetch_add returns prev val)
        if 2 == attempts.fetch_add(1, Ordering::Relaxed) && eventually_pass {
            Ok(())
        } else {
            Err(anyhow!("Oh no I failed!"))
        }
    });
    worker
        .submit_wf(wf_id.to_owned(), DEFAULT_WORKFLOW_TYPE.to_owned(), vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let expected_attempts = if eventually_pass { 3 } else { 5 };
    assert_eq!(expected_attempts, attempts.load(Ordering::Relaxed));
}

#[tokio::test]
async fn local_act_retry_long_backoff_uses_timer() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_local_activity_fail_marker(
        1,
        "1",
        Failure::application_failure("la failed".to_string(), false),
    );
    let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_local_activity_fail_marker(
        2,
        "2",
        Failure::application_failure("la failed".to_string(), false),
    );
    let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = MockServerGatewayApis::new();
    let mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [1.into(), 2.into(), ResponseType::AllHistory],
        mock,
    );
    let mut mock = build_mock_pollers(mh);
    // TODO: Probably test w/o cache too -
    mock.worker_cfg(TEST_Q, |w| w.max_cached_workflows = 1);
    let core = mock_core(mock);
    let mut worker = TestRustWorker::new(Arc::new(core), TEST_Q.to_string(), None);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            let la_res = ctx
                .local_activity(LocalActivityOptions {
                    activity_type: "echo".to_string(),
                    input: "hi".as_json_payload().expect("serializes fine"),
                    retry_policy: RetryPolicy {
                        initial_interval: Some(Duration::from_millis(65).into()),
                        // This will make the second backoff 65 seconds, plenty to use timer
                        backoff_coefficient: 1_000.,
                        maximum_interval: Some(Duration::from_secs(600).into()),
                        maximum_attempts: 3,
                        non_retryable_error_types: vec![],
                    },
                    ..Default::default()
                })
                .await;
            assert!(la_res.failed());
            // Extra timer just to have an extra workflow task which we can return full history for
            ctx.timer(Duration::from_secs(1)).await;
            Ok(().into())
        },
    );
    worker.register_activity("echo", move |_: String| async move {
        Result::<(), _>::Err(anyhow!("Oh no I failed!"))
    });
    worker
        .submit_wf(wf_id.to_owned(), DEFAULT_WORKFLOW_TYPE.to_owned(), vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}
