use crate::common::{CoreWfStarter, WorkflowHandleExt, mock_sdk, mock_sdk_cfg};
use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};
use temporalio_client::WorkflowOptions;
use temporalio_common::{
    protos::{
        DEFAULT_ACTIVITY_TYPE, TestHistoryBuilder, canned_histories,
        coresdk::AsJsonPayloadExt,
        temporal::api::{
            enums::v1::{EventType, WorkflowTaskFailedCause},
            failure::v1::Failure,
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_sdk::{
    ActContext, ActivityOptions, ChildWorkflowOptions, LocalActivityOptions, WfContext,
    WorkflowResult,
};
use temporalio_sdk_core::{
    replay::DEFAULT_WORKFLOW_TYPE,
    test_help::{CoreInternalFlags, MockPollCfg, ResponseType, mock_worker_client},
};

static RUN_CT: AtomicUsize = AtomicUsize::new(1);

pub(crate) async fn timer_wf_nondeterministic(ctx: WfContext) -> WorkflowResult<()> {
    let run_ct = RUN_CT.fetch_add(1, Ordering::Relaxed);

    match run_ct {
        1 | 3 => {
            // If we have not run yet or are on the third attempt, schedule a timer
            ctx.timer(Duration::from_secs(1)).await;
            if run_ct == 1 {
                // on first attempt we need to blow up after the timer fires so we will replay
                panic!("dying on purpose");
            }
        }
        2 => {
            // On the second attempt we should cause a nondeterminism error
            ctx.activity(ActivityOptions {
                activity_type: "whatever".to_string(),
                ..Default::default()
            })
            .await;
        }
        _ => panic!("Ran too many times"),
    }
    Ok(().into())
}

#[tokio::test]
async fn test_determinism_error_then_recovers() {
    let wf_name = "test_determinism_error_then_recovers";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    worker.register_wf(wf_name.to_owned(), timer_wf_nondeterministic);
    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
    // 4 because we still add on the 3rd and final attempt
    assert_eq!(RUN_CT.load(Ordering::Relaxed), 4);
}

#[tokio::test]
async fn task_fail_causes_replay_unset_too_soon() {
    let wf_name = "task_fail_causes_replay_unset_too_soon";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;

    static DID_FAIL: AtomicBool = AtomicBool::new(false);
    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| async move {
        if DID_FAIL.load(Ordering::Relaxed) {
            assert!(ctx.is_replaying());
        }
        ctx.activity(ActivityOptions {
            activity_type: "echo".to_string(),
            input: "hi!".as_json_payload().expect("serializes fine"),
            start_to_close_timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        })
        .await;
        if !DID_FAIL.load(Ordering::Relaxed) {
            DID_FAIL.store(true, Ordering::Relaxed);
            panic!("Die on purpose");
        }
        Ok(().into())
    });
    worker.register_activity("echo", |_ctx: ActContext, echo_me: String| async move {
        Ok(echo_me)
    });

    let handle = starter.start_with_worker(wf_name, &mut worker).await;

    worker.run_until_done().await.unwrap();
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

async fn timer_wf_fails_once(ctx: WfContext) -> WorkflowResult<()> {
    static DID_FAIL: AtomicBool = AtomicBool::new(false);

    ctx.timer(Duration::from_secs(1)).await;
    if DID_FAIL
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        panic!("Ahh");
    }
    Ok(().into())
}

/// Verifies that workflow panics (which in this case the Rust SDK turns into workflow activation
/// failures) are turned into unspecified WFT failures.
#[tokio::test]
async fn test_panic_wf_task_rejected_properly() {
    let wf_id = "fakeid";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let t = canned_histories::workflow_fails_with_failure_after_timer("1");
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 2], mock);
    // We should see one wft failure which has the default cause, since panics don't have a defined
    // type.
    mh.num_expected_fails = 1;
    mh.expect_fail_wft_matcher = Box::new(|_, cause, _| {
        matches!(
            cause,
            WorkflowTaskFailedCause::WorkflowWorkerUnhandledFailure
        )
    });
    let mut worker = mock_sdk(mh);

    worker.register_wf(wf_type.to_owned(), timer_wf_fails_once);
    worker
        .submit_wf(
            wf_id.to_owned(),
            wf_type.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

/// Verifies nondeterministic behavior in workflows results in automatic WFT failure with the
/// appropriate nondeterminism cause.
#[rstest::rstest]
#[case::with_cache(true)]
#[case::without_cache(false)]
#[tokio::test]
async fn test_wf_task_rejected_properly_due_to_nondeterminism(#[case] use_cache: bool) {
    let wf_id = "fakeid";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let t = canned_histories::single_timer_wf_completes("1");
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        // Two polls are needed, since the first will fail
        [ResponseType::AllHistory, ResponseType::AllHistory],
        mock,
    );
    // We should see one wft failure which has nondeterminism cause
    mh.num_expected_fails = 1;
    mh.expect_fail_wft_matcher =
        Box::new(|_, cause, _| matches!(cause, WorkflowTaskFailedCause::NonDeterministicError));
    let mut worker = mock_sdk_cfg(mh, |cfg| {
        if use_cache {
            cfg.max_cached_workflows = 2;
        }
    });

    let started_count: &'static _ = Box::leak(Box::new(AtomicUsize::new(0)));
    worker.register_wf(wf_type.to_owned(), move |ctx: WfContext| async move {
        // The workflow is replaying all of history, so the when it schedules an extra timer it
        // should not have, it causes a nondeterminism error.
        if started_count.fetch_add(1, Ordering::Relaxed) == 0 {
            ctx.timer(Duration::from_secs(1)).await;
        }
        ctx.timer(Duration::from_secs(1)).await;
        Ok(().into())
    });

    worker
        .submit_wf(
            wf_id.to_owned(),
            wf_type.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    // Started count is two since we start, restart once due to error, then we unblock the real
    // timer and proceed without restarting
    assert_eq!(2, started_count.load(Ordering::Relaxed));
}

#[rstest::rstest]
#[tokio::test]
async fn activity_id_or_type_change_is_nondeterministic(
    #[values(true, false)] use_cache: bool,
    #[values(true, false)] id_change: bool,
    #[values(true, false)] local_act: bool,
) {
    let wf_id = "fakeid";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let mut t: TestHistoryBuilder = if local_act {
        canned_histories::single_local_activity("1")
    } else {
        canned_histories::single_activity("1")
    };
    t.set_flags_first_wft(&[CoreInternalFlags::IdAndTypeDeterminismChecks as u32], &[]);
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        // Two polls are needed, since the first will fail
        [ResponseType::AllHistory, ResponseType::AllHistory],
        mock,
    );
    // We should see one wft failure which has nondeterminism cause
    mh.num_expected_fails = 1;
    mh.expect_fail_wft_matcher = Box::new(move |_, cause, f| {
        let should_contain = if id_change {
            "does not match activity id"
        } else {
            "does not match activity type"
        };
        matches!(cause, WorkflowTaskFailedCause::NonDeterministicError)
            && matches!(f, Some(Failure {
                message,
                ..
            }) if message.contains(should_contain))
    });
    let mut worker = mock_sdk_cfg(mh, |cfg| {
        if use_cache {
            cfg.max_cached_workflows = 2;
        }
    });

    worker.register_wf(wf_type.to_owned(), move |ctx: WfContext| async move {
        if local_act {
            ctx.local_activity(if id_change {
                LocalActivityOptions {
                    activity_id: Some("I'm bad and wrong!".to_string()),
                    activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
                    ..Default::default()
                }
            } else {
                LocalActivityOptions {
                    activity_type: "not the default act type".to_string(),
                    ..Default::default()
                }
            })
            .await;
        } else {
            ctx.activity(if id_change {
                ActivityOptions {
                    activity_id: Some("I'm bad and wrong!".to_string()),
                    activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
                    ..Default::default()
                }
            } else {
                ActivityOptions {
                    activity_type: "not the default act type".to_string(),
                    ..Default::default()
                }
            })
            .await;
        }
        Ok(().into())
    });

    worker
        .submit_wf(
            wf_id.to_owned(),
            wf_type.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn child_wf_id_or_type_change_is_nondeterministic(
    #[values(true, false)] use_cache: bool,
    #[values(true, false)] id_change: bool,
) {
    let wf_id = "fakeid";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let mut t = canned_histories::single_child_workflow("1");
    t.set_flags_first_wft(&[CoreInternalFlags::IdAndTypeDeterminismChecks as u32], &[]);
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        // Two polls are needed, since the first will fail
        [ResponseType::AllHistory, ResponseType::AllHistory],
        mock,
    );
    // We should see one wft failure which has nondeterminism cause
    mh.num_expected_fails = 1;
    mh.expect_fail_wft_matcher = Box::new(move |_, cause, f| {
        let should_contain = if id_change {
            "does not match child workflow id"
        } else {
            "does not match child workflow type"
        };
        matches!(cause, WorkflowTaskFailedCause::NonDeterministicError)
            && matches!(f, Some(Failure {
                message,
                ..
            }) if message.contains(should_contain))
    });
    let mut worker = mock_sdk_cfg(mh, |cfg| {
        if use_cache {
            cfg.max_cached_workflows = 2;
        }
    });

    worker.register_wf(wf_type.to_owned(), move |ctx: WfContext| async move {
        ctx.child_workflow(if id_change {
            ChildWorkflowOptions {
                workflow_id: "I'm bad and wrong!".to_string(),
                workflow_type: DEFAULT_ACTIVITY_TYPE.to_string(),
                ..Default::default()
            }
        } else {
            ChildWorkflowOptions {
                workflow_id: "1".to_string(),
                workflow_type: "not the child wf type".to_string(),
                ..Default::default()
            }
        })
        .start(&ctx)
        .await;
        Ok(().into())
    });

    worker
        .submit_wf(
            wf_id.to_owned(),
            wf_type.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

/// Repros a situation where if, upon completing a task there is some internal error which causes
/// us to want to auto-fail the workflow task while there is also an outstanding eviction, the wf
/// would get evicted but then try to send some info down the completion channel afterward, causing
/// a panic.
#[tokio::test]
async fn repro_channel_missing_because_nondeterminism() {
    for _ in 1..50 {
        let wf_id = "fakeid";
        let wf_type = DEFAULT_WORKFLOW_TYPE;
        let mut t = TestHistoryBuilder::default();
        t.add_by_type(EventType::WorkflowExecutionStarted);
        t.add_full_wf_task();
        t.add_has_change_marker("patch-1", false);
        let _ts = t.add_by_type(EventType::TimerStarted);
        t.add_workflow_task_scheduled_and_started();

        let mock = mock_worker_client();
        let mut mh =
            MockPollCfg::from_resp_batches(wf_id, t, [1.into(), ResponseType::AllHistory], mock);
        mh.num_expected_fails = 1;
        let mut worker = mock_sdk_cfg(mh, |cfg| {
            cfg.max_cached_workflows = 2;
            cfg.ignore_evicts_on_shutdown = false;
        });

        worker.register_wf(wf_type.to_owned(), move |ctx: WfContext| async move {
            ctx.patched("wrongid");
            ctx.timer(Duration::from_secs(1)).await;
            Ok(().into())
        });

        worker
            .submit_wf(
                wf_id.to_owned(),
                wf_type.to_owned(),
                vec![],
                WorkflowOptions::default(),
            )
            .await
            .unwrap();
        worker.run_until_done().await.unwrap();
    }
}
