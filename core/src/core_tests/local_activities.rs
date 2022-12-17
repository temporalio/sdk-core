use crate::{
    prost_dur,
    replay::{default_wes_attribs, TestHistoryBuilder, DEFAULT_WORKFLOW_TYPE},
    test_help::{
        hist_to_poll_resp, mock_sdk, mock_sdk_cfg, mock_worker, single_hist_mock_sg, MockPollCfg,
        ResponseType,
    },
    worker::{client::mocks::mock_workflow_client, LEGACY_QUERY_ID},
};
use anyhow::anyhow;
use futures::{future::join_all, FutureExt};
use std::{
    collections::HashMap,
    ops::Sub,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime},
};
use temporal_client::WorkflowOptions;
use temporal_sdk::{
    ActContext, ActivityCancelledError, LocalActivityOptions, WfContext, WorkflowResult,
};
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
        common::v1::RetryPolicy,
        enums::v1::{EventType, TimeoutType, WorkflowTaskFailedCause},
        failure::v1::Failure,
        history::v1::History,
        query::v1::WorkflowQuery,
    },
};
use temporal_sdk_core_test_utils::{
    schedule_local_activity_cmd, start_timer_cmd, WorkerTestHelpers,
};
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
    crate::telemetry::test_telem_console();
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
    t.add_wfe_started_with_wft_timeout(wft_timeout);
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
    t.add_local_activity_marker(1, "1", None, None, |_| {});
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
async fn local_act_command_immediately_follows_la_marker() {
    // This repro only works both when cache is off, and there is at least one heartbeat wft
    // before the marker & next command are recorded.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_full_wf_task();
    t.add_local_activity_result_marker(1, "1", "done".into());
    t.add_get_event_id(EventType::TimerStarted, None);
    t.add_full_wf_task();

    let wf_id = "fakeid";
    let mock = mock_workflow_client();
    // Bug only repros when seeing history up to third wft
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [3], mock);
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 0);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            ctx.local_activity(LocalActivityOptions {
                activity_type: "nullres".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                ..Default::default()
            })
            .await;
            ctx.timer(Duration::from_secs(1)).await;
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
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    t.add_full_wf_task();
    // get query here
    t.add_full_wf_task();
    t.add_local_activity_result_marker(1, "1", "done".into());
    t.add_workflow_execution_completed();

    let query_with_hist_task = {
        let mut pr = hist_to_poll_resp(&t, wfid, ResponseType::ToTaskNum(1));
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

#[rstest::rstest]
#[case::impossible_query_in_task(true)]
#[case::real_history(false)]
#[tokio::test]
async fn la_resolve_during_legacy_query_does_not_combine(#[case] impossible_query_in_task: bool) {
    // Ensures we do not send an activation with a legacy query and any other work, which should
    // never happen, but there was an issue where an LA resolving could trigger that.
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    let wes_short_wft_timeout = default_wes_attribs();
    t.add(
        EventType::WorkflowExecutionStarted,
        wes_short_wft_timeout.into(),
    );
    // Since we don't send queries with start workflow, need one workflow task of something else
    // b/c we want to get an activation with a job and a nonlegacy query
    t.add_full_wf_task();
    let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
    t.add_timer_fired(timer_started_event_id, "1".to_string());

    // nonlegacy query got here & LA started here
    t.add_full_wf_task();
    // legacy query got here, at the same time that the LA is resolved
    t.add_local_activity_result_marker(1, "1", "whatever".into());
    t.add_workflow_execution_completed();

    let barr = Arc::new(Barrier::new(2));
    let barr_c = barr.clone();

    let tasks = [
        hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(1)),
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::OneTask(2));
            pr.queries = HashMap::new();
            pr.queries.insert(
                "q1".to_string(),
                WorkflowQuery {
                    query_type: "query-type".to_string(),
                    query_args: Some(b"hi".into()),
                    header: None,
                },
            );
            pr
        },
        {
            let mut pr = hist_to_poll_resp(
                &t,
                wfid.to_owned(),
                ResponseType::UntilResolved(
                    async move {
                        barr_c.wait().await;
                        // This sleep is the only not-incredibly-invasive way to ensure the LA
                        // resolves & updates machines before we process this task
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    .boxed(),
                    2,
                ),
            );
            // Strip history, we need to look like we hit the cache
            pr.history = Some(History { events: vec![] });
            // In the nonsense server response case, we attach a legacy query, otherwise this
            // response looks like a normal response to a forced WFT heartbeat.
            if impossible_query_in_task {
                pr.query = Some(WorkflowQuery {
                    query_type: "query-type".to_string(),
                    query_args: Some(b"hi".into()),
                    header: None,
                });
            }
            pr
        },
    ];
    let mut mock = mock_workflow_client();
    if impossible_query_in_task {
        mock.expect_respond_legacy_query()
            .times(1)
            .returning(move |_, _| Ok(Default::default()));
    }
    let mut mock = single_hist_mock_sg(wfid, t, tasks, mock, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let wf_fut = async {
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            &[WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::StartWorkflow(_)),
            },]
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            start_timer_cmd(1, Duration::from_secs(1)),
        ))
        .await
        .unwrap();
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            &[
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::FireTimer(_)),
                },
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::QueryWorkflow(_)),
                }
            ]
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            task.run_id,
            vec![
                schedule_local_activity_cmd(
                    1,
                    "act-id",
                    ActivityCancellationType::TryCancel,
                    Duration::from_secs(60),
                ),
                QueryResult {
                    query_id: "q1".to_string(),
                    variant: Some(
                        QuerySuccess {
                            response: Some("whatev".into()),
                        }
                        .into(),
                    ),
                }
                .into(),
            ],
        ))
        .await
        .unwrap();
        barr.wait().await;
        let task = core.poll_workflow_activation().await.unwrap();
        // The next task needs to be resolve, since the LA is completed immediately
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        // Complete workflow
        core.complete_execution(&task.run_id).await;
        if impossible_query_in_task {
            // finish last query
            let task = core.poll_workflow_activation().await.unwrap();
            core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                task.run_id,
                vec![QueryResult {
                    query_id: LEGACY_QUERY_ID.to_string(),
                    variant: Some(
                        QuerySuccess {
                            response: Some("whatev".into()),
                        }
                        .into(),
                    ),
                }
                .into()],
            ))
            .await
            .unwrap();
        }
    };
    let act_fut = async {
        let act_task = core.poll_activity_task().await.unwrap();
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    };

    tokio::join!(wf_fut, act_fut);
    core.shutdown().await;
}

#[tokio::test]
async fn test_schedule_to_start_timeout() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();

    let wf_id = "fakeid";
    let mock = mock_workflow_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::ToTaskNum(1)], mock);
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            let la_res = ctx
                .local_activity(LocalActivityOptions {
                    activity_type: "echo".to_string(),
                    input: "hi".as_json_payload().expect("serializes fine"),
                    // Impossibly small timeout so we timeout in the queue
                    schedule_to_start_timeout: prost_dur!(from_nanos(1)),
                    ..Default::default()
                })
                .await;
            assert_eq!(la_res.timed_out(), Some(TimeoutType::ScheduleToStart));
            Ok(().into())
        },
    );
    worker.register_activity(
        "echo",
        move |_ctx: ActContext, _: String| async move { Ok(()) },
    );
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

#[rstest::rstest]
#[case::sched_to_start(true)]
#[case::sched_to_close(false)]
#[tokio::test]
async fn test_schedule_to_start_timeout_not_based_on_original_time(
    #[case] is_sched_to_start: bool,
) {
    // We used to carry over the schedule time of LAs from the "original" schedule time if these LAs
    // created newly after backing off across a timer. That was a mistake, since schedule-to-start
    // timeouts should apply to when the new attempt was scheduled. This test verifies:
    // * we don't time out on s-t-s timeouts because of that, when the param is true.
    // * we do properly time out on s-t-c timeouts when the param is false

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let orig_sched = SystemTime::now().sub(Duration::from_secs(60 * 20));
    t.add_local_activity_marker(
        1,
        "1",
        None,
        Some(Failure::application_failure("la failed".to_string(), false)),
        |deets| {
            // Really old schedule time, which should _not_ count against schedule_to_start
            deets.original_schedule_time = Some(orig_sched.into());
            // Backoff value must be present since we're simulating timer backoff
            deets.backoff = Some(prost_dur!(from_secs(100)));
        },
    );
    let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_workflow_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::AllHistory], mock);
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    let schedule_to_close_timeout = Some(if is_sched_to_start {
        // This 60 minute timeout will not have elapsed according to the original
        // schedule time in the history.
        Duration::from_secs(60 * 60)
    } else {
        // This 10 minute timeout will have already elapsed
        Duration::from_secs(10 * 60)
    });

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
                    schedule_to_start_timeout: Some(Duration::from_secs(60)),
                    schedule_to_close_timeout,
                    ..Default::default()
                })
                .await;
            if is_sched_to_start {
                assert!(la_res.completed_ok());
            } else {
                assert_eq!(la_res.timed_out(), Some(TimeoutType::ScheduleToClose));
            }
            Ok(().into())
        },
    );
    worker.register_activity(
        "echo",
        move |_ctx: ActContext, _: String| async move { Ok(()) },
    );
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
async fn wft_failure_cancels_running_las() {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    t.add_full_wf_task();
    let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_workflow_client();
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2], mock);
    mh.num_expected_fails = 1;
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            let la_handle = ctx.local_activity(LocalActivityOptions {
                activity_type: "echo".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                ..Default::default()
            });
            tokio::join!(
                async {
                    ctx.timer(Duration::from_secs(1)).await;
                    panic!("ahhh I'm failing wft")
                },
                la_handle
            );
            Ok(().into())
        },
    );
    worker.register_activity("echo", move |ctx: ActContext, _: String| async move {
        let res = tokio::time::timeout(Duration::from_millis(500), ctx.cancelled()).await;
        if res.is_err() {
            panic!("Activity must be cancelled!!!!");
        }
        Result::<(), _>::Err(ActivityCancelledError::default().into())
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
async fn resolved_las_not_recorded_if_wft_fails_many_times() {
    crate::telemetry::test_telem_console();
    // We shouldn't record any LA results if the workflow activation is repeatedly failing. There
    // was an issue that, because we stop reporting WFT failures after 2 tries, this meant the WFT
    // was not marked as "completed" and the WFT could accidentally be replied to with LA results.
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::Unspecified,
        Default::default(),
    );
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_workflow_client();
    let mut mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [1.into(), ResponseType::AllHistory, ResponseType::AllHistory],
        mock,
    );
    mh.num_expected_fails = 2;
    mh.completion_asserts = Some(Box::new(|_| {
        panic!("should never successfully complete a WFT");
    }));
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            ctx.local_activity(LocalActivityOptions {
                activity_type: "echo".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                ..Default::default()
            })
            .await;
            panic!("Oh nooooo")
        },
    );
    worker.register_activity(
        "echo",
        move |_: ActContext, _: String| async move { Ok(()) },
    );
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
