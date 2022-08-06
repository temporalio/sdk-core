use crate::{
    prost_dur,
    replay::{default_wes_attribs, TestHistoryBuilder, DEFAULT_WORKFLOW_TYPE},
    test_help::{
        hist_to_poll_resp, mock_sdk, mock_sdk_cfg, mock_worker, single_hist_mock_sg, MockPollCfg,
        ResponseType, TEST_Q,
    },
    worker::client::mocks::mock_workflow_client,
};
use anyhow::anyhow;
use futures::{future::join_all, FutureExt};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use temporal_client::WorkflowOptions;
use temporal_sdk::{ActContext, LocalActivityOptions, WfContext, WorkflowResult};
use temporal_sdk_core_api::Worker;
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::ActivityExecutionResult,
        workflow_activation::{workflow_activation_job, WorkflowActivationJob},
        workflow_commands::{ActivityCancellationType, QueryResult, QuerySuccess},
        workflow_completion::WorkflowActivationCompletion,
        ActivityTaskCompletion, AsJsonPayloadExt,
    },
    temporal::api::{
        common::v1::RetryPolicy, enums::v1::EventType, failure::v1::Failure,
        query::v1::WorkflowQuery,
    },
};
use temporal_sdk_core_test_utils::{schedule_local_activity_cmd, WorkerTestHelpers};
use tokio::sync::Barrier;

async fn echo(_ctx: ActContext, e: String) -> anyhow::Result<String> {
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
    let mock = mock_workflow_client();
    let resps = if replay {
        vec![ResponseType::AllHistory]
    } else {
        vec![1.into(), 2.into(), ResponseType::AllHistory]
    };
    let mh = MockPollCfg::from_resp_batches(wf_id, t, resps, mock);
    let mut worker = mock_sdk_cfg(mh, |cfg| {
        if cached {
            cfg.max_cached_workflows = 1;
        }
    });

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
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
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
    let mock = mock_workflow_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 3], mock);
    let mut worker = mock_sdk(mh);

    worker.register_wf(DEFAULT_WORKFLOW_TYPE.to_owned(), local_act_fanout_wf);
    worker.register_activity("echo", echo);
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
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
    wes_short_wft_timeout.workflow_task_timeout = Some(wft_timeout.try_into().unwrap());
    t.add(
        EventType::WorkflowExecutionStarted,
        wes_short_wft_timeout.into(),
    );
    t.add_full_wf_task();
    // Task created by WFT heartbeat
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_workflow_client();
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 2, 2], mock);
    mh.enforce_correct_number_of_polls = false;
    let mut worker = mock_sdk_cfg(mh, |wc| {
        wc.max_cached_workflows = 1;
        wc.max_outstanding_workflow_tasks = 1;
    });
    let core = worker.core_worker.clone();

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
    worker.register_activity("echo", move |_ctx: ActContext, str: String| async move {
        if shutdown_middle {
            shutdown_barr.wait().await;
        }
        // Take slightly more than two workflow tasks
        tokio::time::sleep(wft_timeout.mul_f32(2.2)).await;
        Ok(str)
    });
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
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
    let mock = mock_workflow_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [1], mock);
    let mut worker = mock_sdk(mh);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        move |ctx: WfContext| async move {
            let la_res = ctx
                .local_activity(LocalActivityOptions {
                    activity_type: "echo".to_string(),
                    input: "hi".as_json_payload().expect("serializes fine"),
                    retry_policy: RetryPolicy {
                        initial_interval: Some(prost_dur!(from_millis(50))),
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
    worker.register_activity("echo", move |_ctx: ActContext, _: String| async move {
        // Succeed on 3rd attempt (which is ==2 since fetch_add returns prev val)
        if 2 == attempts.fetch_add(1, Ordering::Relaxed) && eventually_pass {
            Ok(())
        } else {
            Err(anyhow!("Oh no I failed!"))
        }
    });
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
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
    let mock = mock_workflow_client();
    let mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [1.into(), 2.into(), ResponseType::AllHistory],
        mock,
    );
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            let la_res = ctx
                .local_activity(LocalActivityOptions {
                    activity_type: "echo".to_string(),
                    input: "hi".as_json_payload().expect("serializes fine"),
                    retry_policy: RetryPolicy {
                        initial_interval: Some(prost_dur!(from_millis(65))),
                        // This will make the second backoff 65 seconds, plenty to use timer
                        backoff_coefficient: 1_000.,
                        maximum_interval: Some(prost_dur!(from_secs(600))),
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
    worker.register_activity("echo", move |_ctx: ActContext, _: String| async move {
        Result::<(), _>::Err(anyhow!("Oh no I failed!"))
    });
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn local_act_null_result() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_local_activity_marker(1, "1", None, None, None);
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_workflow_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::AllHistory], mock);
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            ctx.local_activity(LocalActivityOptions {
                activity_type: "nullres".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                ..Default::default()
            })
            .await;
            Ok(().into())
        },
    );
    worker.register_activity("nullres", |_ctx: ActContext, _: String| async { Ok(()) });
    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn query_during_wft_heartbeat_doesnt_accidentally_fail_to_continue_heartbeat() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    let mut wes_short_wft_timeout = default_wes_attribs();
    wes_short_wft_timeout.workflow_task_timeout = Some(prost_dur!(from_millis(200)));
    t.add(
        EventType::WorkflowExecutionStarted,
        wes_short_wft_timeout.into(),
    );
    t.add_full_wf_task();
    // get query here
    t.add_full_wf_task();
    t.add_local_activity_marker(1, "1", None, None, None);
    t.add_workflow_execution_completed();

    let query_with_hist_task = {
        let mut pr = hist_to_poll_resp(&t, wfid, ResponseType::ToTaskNum(1), TEST_Q);
        pr.queries = HashMap::new();
        pr.queries.insert(
            "the-query".to_string(),
            WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
                header: None,
            },
        );
        pr
    };
    let after_la_resolved = Arc::new(Barrier::new(2));
    let poll_barr = after_la_resolved.clone();
    let tasks = [
        query_with_hist_task,
        hist_to_poll_resp(
            &t,
            wfid,
            ResponseType::UntilResolved(
                async move {
                    poll_barr.wait().await;
                }
                .boxed(),
                3,
            ),
            TEST_Q,
        ),
    ];
    let mock = mock_workflow_client();
    let mut mock = single_hist_mock_sg(wfid, t, tasks, mock, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let barrier = Barrier::new(2);

    let wf_fut = async {
        let task = core.poll_workflow_activation().await.unwrap();
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            schedule_local_activity_cmd(
                1,
                "act-id",
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
            ),
        ))
        .await
        .unwrap();
        let task = core.poll_workflow_activation().await.unwrap();
        // Get query, and complete it
        let query = assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
            }] => q
        );
        // Now complete the LA
        barrier.wait().await;
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            QueryResult {
                query_id: query.query_id.clone(),
                variant: Some(
                    QuerySuccess {
                        response: Some("whatever".into()),
                    }
                    .into(),
                ),
            }
            .into(),
        ))
        .await
        .unwrap();
        // Activation with it resolving:
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        core.complete_execution(&task.run_id).await;
    };
    let act_fut = async {
        let act_task = core.poll_activity_task().await.unwrap();
        barrier.wait().await;
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
        after_la_resolved.wait().await;
    };

    tokio::join!(wf_fut, act_fut);
}
