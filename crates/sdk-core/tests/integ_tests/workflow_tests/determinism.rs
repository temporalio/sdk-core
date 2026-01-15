use crate::common::{
    CoreWfStarter, WorkflowHandleExt, activity_functions::StdActivities, mock_sdk, mock_sdk_cfg,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_client::WorkflowOptions;
use temporalio_common::{
    protos::{
        TestHistoryBuilder, canned_histories,
        coresdk::AsJsonPayloadExt,
        temporal::api::{
            enums::v1::{EventType, WorkflowTaskFailedCause},
            failure::v1::Failure,
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ChildWorkflowOptions, LocalActivityOptions, WorkflowContext, WorkflowResult,
};
use temporalio_sdk_core::{
    replay::DEFAULT_WORKFLOW_TYPE,
    test_help::{CoreInternalFlags, MockPollCfg, ResponseType, mock_worker_client},
};

#[workflow]
pub(crate) struct TimerWfNondeterministic {
    run_ct: Arc<AtomicUsize>,
}

#[workflow_methods(factory_only)]
impl TimerWfNondeterministic {
    #[run]
    pub(crate) async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        let run_ct = self.run_ct.fetch_add(1, Ordering::Relaxed);

        match run_ct {
            1 | 3 => {
                ctx.timer(Duration::from_secs(1)).await;
                if run_ct == 1 {
                    panic!("dying on purpose");
                }
            }
            2 => {
                ctx.start_activity(StdActivities::default, (), ActivityOptions::default())?
                    .await;
            }
            _ => panic!("Ran too many times"),
        }
        Ok(().into())
    }
}

#[tokio::test]
async fn test_determinism_error_then_recovers() {
    let wf_name = "test_determinism_error_then_recovers";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    let run_ct = Arc::new(AtomicUsize::new(1));
    let run_ct_clone = run_ct.clone();
    worker.register_workflow_with_factory(move || TimerWfNondeterministic {
        run_ct: run_ct_clone.clone(),
    });
    worker
        .submit_workflow(
            TimerWfNondeterministic::run,
            starter.get_task_queue(),
            (),
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    assert_eq!(run_ct.load(Ordering::Relaxed), 4);
}

#[workflow]
struct TaskFailReplayWf {
    did_fail: Arc<AtomicBool>,
}

#[workflow_methods(factory_only)]
impl TaskFailReplayWf {
    #[run]
    async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        if self.did_fail.load(Ordering::Relaxed) {
            assert!(ctx.is_replaying());
        }
        ctx.start_activity(
            StdActivities::echo,
            "hi!".to_string(),
            ActivityOptions {
                start_to_close_timeout: Some(Duration::from_secs(2)),
                ..Default::default()
            },
        )
        .unwrap()
        .await;
        if !self.did_fail.load(Ordering::Relaxed) {
            self.did_fail.store(true, Ordering::Relaxed);
            panic!("Die on purpose");
        }
        Ok(().into())
    }
}

#[tokio::test]
async fn task_fail_causes_replay_unset_too_soon() {
    let wf_name = "task_fail_causes_replay_unset_too_soon";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;

    let did_fail = Arc::new(AtomicBool::new(false));
    let did_fail_clone = did_fail.clone();
    worker.register_workflow_with_factory(move || TaskFailReplayWf {
        did_fail: did_fail_clone.clone(),
    });

    let handle = worker
        .submit_workflow(
            TaskFailReplayWf::run,
            starter.get_task_queue(),
            (),
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[workflow]
struct TimerWfFailsOnce {
    did_fail: Arc<AtomicBool>,
}

#[workflow_methods(factory_only)]
impl TimerWfFailsOnce {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        ctx.timer(Duration::from_secs(1)).await;
        if self
            .did_fail
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            panic!("Ahh");
        }
        Ok(().into())
    }
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

    let did_fail = Arc::new(AtomicBool::new(false));
    worker.register_workflow_with_factory(move || TimerWfFailsOnce {
        did_fail: did_fail.clone(),
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

#[workflow]
struct NondeterministicTimerWf {
    started_count: Arc<AtomicUsize>,
}

#[workflow_methods(factory_only)]
impl NondeterministicTimerWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        if self.started_count.fetch_add(1, Ordering::Relaxed) == 0 {
            ctx.timer(Duration::from_secs(1)).await;
        }
        ctx.timer(Duration::from_secs(1)).await;
        Ok(().into())
    }
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
        [ResponseType::AllHistory, ResponseType::AllHistory],
        mock,
    );
    mh.num_expected_fails = 1;
    mh.expect_fail_wft_matcher =
        Box::new(|_, cause, _| matches!(cause, WorkflowTaskFailedCause::NonDeterministicError));
    let mut worker = mock_sdk_cfg(mh, |cfg| {
        if use_cache {
            cfg.max_cached_workflows = 2;
        }
    });

    let started_count = Arc::new(AtomicUsize::new(0));
    let count_clone = started_count.clone();
    worker.register_workflow_with_factory(move || NondeterministicTimerWf {
        started_count: count_clone.clone(),
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
    assert_eq!(2, started_count.load(Ordering::Relaxed));
}

#[workflow]
#[derive(Default)]
struct ActivityIdOrTypeChangeWf;

#[workflow_methods]
impl ActivityIdOrTypeChangeWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(
        &mut self,
        ctx: &mut WorkflowContext,
        (id_change, local_act): (bool, bool),
    ) -> WorkflowResult<()> {
        if local_act {
            if id_change {
                ctx.start_local_activity(
                    StdActivities::default,
                    (),
                    LocalActivityOptions {
                        activity_id: Some("I'm bad and wrong!".to_string()),
                        ..Default::default()
                    },
                )?
                .await;
            } else {
                ctx.start_local_activity(StdActivities::no_op, (), Default::default())?
                    .await;
            }
        } else if id_change {
            ctx.start_activity(
                StdActivities::default,
                (),
                ActivityOptions {
                    activity_id: Some("I'm bad and wrong!".to_string()),
                    ..Default::default()
                },
            )?
            .await;
        } else {
            ctx.start_activity(StdActivities::no_op, (), ActivityOptions::default())?
                .await;
        }
        Ok(().into())
    }
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
    t.set_wf_input((id_change, local_act).as_json_payload().unwrap());
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [ResponseType::AllHistory, ResponseType::AllHistory],
        mock,
    );
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
    worker.register_workflow::<ActivityIdOrTypeChangeWf>();

    worker
        .submit_wf(
            wf_id.to_owned(),
            wf_type.to_owned(),
            vec![(id_change, local_act).as_json_payload().unwrap()],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct ChildWfIdOrTypeChangeWf;

#[workflow_methods]
impl ChildWfIdOrTypeChangeWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(&mut self, ctx: &mut WorkflowContext, id_change: bool) -> WorkflowResult<()> {
        ctx.child_workflow(if id_change {
            ChildWorkflowOptions {
                workflow_id: "I'm bad and wrong!".to_string(),
                workflow_type: "child".to_string(),
                ..Default::default()
            }
        } else {
            ChildWorkflowOptions {
                workflow_id: "1".to_string(),
                workflow_type: "not the child wf type".to_string(),
                ..Default::default()
            }
        })
        .start(ctx)
        .await;
        Ok(().into())
    }
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
    t.set_wf_input(id_change.as_json_payload().unwrap());
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [ResponseType::AllHistory, ResponseType::AllHistory],
        mock,
    );
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

    worker.register_workflow::<ChildWfIdOrTypeChangeWf>();

    worker
        .submit_wf(
            wf_id.to_owned(),
            wf_type.to_owned(),
            vec![id_change.as_json_payload().unwrap()],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct ReproChannelMissingWf;

#[workflow_methods]
impl ReproChannelMissingWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        ctx.patched("wrongid");
        ctx.timer(Duration::from_secs(1)).await;
        Ok(().into())
    }
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

        worker.register_workflow::<ReproChannelMissingWf>();

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
