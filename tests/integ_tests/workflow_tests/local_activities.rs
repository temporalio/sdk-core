use anyhow::anyhow;
use futures::future::join_all;
use futures_util::stream::{FuturesUnordered, StreamExt};
use std::{
    sync::atomic::{AtomicU8, Ordering},
    time::Duration,
};
use temporal_client::{WorkflowClientTrait, WorkflowOptions};
use temporal_sdk::{
    interceptors::WorkerInterceptor, ActContext, ActivityCancelledError, ActivityOptions,
    CancellableFuture, LocalActivityOptions, WfContext, WorkflowResult,
};
use temporal_sdk_core::replay::HistoryForReplay;
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_commands::ActivityCancellationType,
        workflow_completion::WorkflowActivationCompletion, AsJsonPayloadExt,
    },
    temporal::api::{common::v1::RetryPolicy, enums::v1::TimeoutType},
    TestHistoryBuilder,
};
use temporal_sdk_core_test_utils::{
    history_from_proto_binary, init_integ_telem, replay_sdk_worker, CoreWfStarter,
};
use tokio_util::sync::CancellationToken;

pub async fn echo(_ctx: ActContext, e: String) -> anyhow::Result<String> {
    Ok(e)
}

pub async fn one_local_activity_wf(ctx: WfContext) -> WorkflowResult<()> {
    let initial_workflow_time = ctx.workflow_time().expect("Workflow time should be set");
    ctx.local_activity(LocalActivityOptions {
        activity_type: "echo_activity".to_string(),
        input: "hi!".as_json_payload().expect("serializes fine"),
        ..Default::default()
    })
    .await;
    // Verify LA execution advances the clock
    assert!(initial_workflow_time < ctx.workflow_time().unwrap());
    Ok(().into())
}

#[tokio::test]
async fn one_local_activity() {
    let wf_name = "one_local_activity";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), one_local_activity_wf);
    worker.register_activity("echo_activity", echo);

    worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

pub async fn local_act_concurrent_with_timer_wf(ctx: WfContext) -> WorkflowResult<()> {
    let la = ctx.local_activity(LocalActivityOptions {
        activity_type: "echo_activity".to_string(),
        input: "hi!".as_json_payload().expect("serializes fine"),
        ..Default::default()
    });
    let timer = ctx.timer(Duration::from_secs(1));
    tokio::join!(la, timer);
    Ok(().into())
}

#[tokio::test]
async fn local_act_concurrent_with_timer() {
    let wf_name = "local_act_concurrent_with_timer";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), local_act_concurrent_with_timer_wf);
    worker.register_activity("echo_activity", echo);

    worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

pub async fn local_act_then_timer_then_wait(ctx: WfContext) -> WorkflowResult<()> {
    let la = ctx.local_activity(LocalActivityOptions {
        activity_type: "echo_activity".to_string(),
        input: "hi!".as_json_payload().expect("serializes fine"),
        ..Default::default()
    });
    ctx.timer(Duration::from_secs(1)).await;
    let res = la.await;
    assert!(res.completed_ok());
    Ok(().into())
}

#[tokio::test]
async fn local_act_then_timer_then_wait_result() {
    let wf_name = "local_act_then_timer_then_wait_result";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), local_act_then_timer_then_wait);
    worker.register_activity("echo_activity", echo);

    worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn long_running_local_act_with_timer() {
    let wf_name = "long_running_local_act_with_timer";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.wft_timeout(Duration::from_secs(1));
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), local_act_then_timer_then_wait);
    worker.register_activity("echo_activity", |_ctx: ActContext, str: String| async {
        tokio::time::sleep(Duration::from_secs(4)).await;
        Ok(str)
    });

    worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
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
                activity_type: "echo_activity".to_string(),
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
async fn local_act_fanout() {
    let wf_name = "local_act_fanout";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.max_local_at(1);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), local_act_fanout_wf);
    worker.register_activity("echo_activity", echo);

    worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn local_act_retry_timer_backoff() {
    let wf_name = "local_act_retry_timer_backoff";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let res = ctx
            .local_activity(LocalActivityOptions {
                activity_type: "echo".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                retry_policy: RetryPolicy {
                    initial_interval: Some(prost_dur!(from_micros(15))),
                    // We want two local backoffs that are short. Third backoff will use timer
                    backoff_coefficient: 1_000.,
                    maximum_interval: Some(prost_dur!(from_millis(1500))),
                    maximum_attempts: 4,
                    non_retryable_error_types: vec![],
                },
                timer_backoff_threshold: Some(Duration::from_secs(1)),
                ..Default::default()
            })
            .await;
        assert!(res.failed());
        Ok(().into())
    });
    worker.register_activity("echo", |_: ActContext, _: String| async {
        Result::<(), _>::Err(anyhow!("Oh no I failed!"))
    });

    let run_id = worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    starter
        .fetch_history_and_replay(wf_name, run_id, worker.inner_mut())
        .await
        .unwrap();
}

#[rstest::rstest]
#[case::wait(ActivityCancellationType::WaitCancellationCompleted)]
#[case::try_cancel(ActivityCancellationType::TryCancel)]
#[case::abandon(ActivityCancellationType::Abandon)]
#[tokio::test]
async fn cancel_immediate(#[case] cancel_type: ActivityCancellationType) {
    let wf_name = format!("cancel_immediate_{:?}", cancel_type);
    let mut starter = CoreWfStarter::new(&wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(&wf_name, move |ctx: WfContext| async move {
        let la = ctx.local_activity(LocalActivityOptions {
            activity_type: "echo".to_string(),
            input: "hi".as_json_payload().expect("serializes fine"),
            cancel_type,
            ..Default::default()
        });
        la.cancel(&ctx);
        let resolution = la.await;
        assert!(resolution.cancelled());
        Ok(().into())
    });

    // If we don't use this, we'd hang on shutdown for abandon cancel modes.
    let manual_cancel = CancellationToken::new();
    let manual_cancel_act = manual_cancel.clone();

    worker.register_activity("echo", move |ctx: ActContext, _: String| {
        let manual_cancel_act = manual_cancel_act.clone();
        async move {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(10)) => {},
                _ = ctx.cancelled() => {
                    return Err(anyhow!(ActivityCancelledError::default()))
                }
                _ = manual_cancel_act.cancelled() => {}
            }
            Ok(())
        }
    });

    worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker
        .run_until_done_intercepted(Some(LACancellerInterceptor {
            token: manual_cancel,
        }))
        .await
        .unwrap();
}

struct LACancellerInterceptor {
    token: CancellationToken,
}
#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for LACancellerInterceptor {
    async fn on_workflow_activation_completion(&self, _: &WorkflowActivationCompletion) {}
    fn on_shutdown(&self, _: &temporal_sdk::Worker) {
        self.token.cancel()
    }
}

#[rstest::rstest]
#[case::while_running(None)]
#[case::while_backing_off(Some(Duration::from_millis(1500)))]
#[case::while_backing_off_locally(Some(Duration::from_millis(150)))]
#[tokio::test]
async fn cancel_after_act_starts(
    #[case] cancel_on_backoff: Option<Duration>,
    #[values(
        ActivityCancellationType::WaitCancellationCompleted,
        ActivityCancellationType::TryCancel,
        ActivityCancellationType::Abandon
    )]
    cancel_type: ActivityCancellationType,
) {
    let wf_name = format!(
        "cancel_after_act_starts_timer_{:?}_{:?}",
        cancel_on_backoff, cancel_type
    );
    let mut starter = CoreWfStarter::new(&wf_name);
    starter.wft_timeout(Duration::from_secs(1));
    let mut worker = starter.worker().await;
    let bo_dur = cancel_on_backoff.unwrap_or_else(|| Duration::from_secs(1));
    worker.register_wf(&wf_name, move |ctx: WfContext| async move {
        let la = ctx.local_activity(LocalActivityOptions {
            activity_type: "echo".to_string(),
            input: "hi".as_json_payload().expect("serializes fine"),
            retry_policy: RetryPolicy {
                initial_interval: Some(bo_dur.try_into().unwrap()),
                backoff_coefficient: 1.,
                maximum_interval: Some(bo_dur.try_into().unwrap()),
                // Retry forever until cancelled
                ..Default::default()
            },
            timer_backoff_threshold: Some(Duration::from_secs(1)),
            cancel_type,
            ..Default::default()
        });
        ctx.timer(Duration::from_secs(1)).await;
        // Note that this cancel can't go through for *two* WF tasks, because we do a full heartbeat
        // before the timer (LA hasn't resolved), and then the timer fired event won't appear in
        // history until *after* the next WFT because we force generated it when we sent the timer
        // command.
        la.cancel(&ctx);
        // This extra timer is here to ensure the presence of another WF task doesn't mess up
        // resolving the LA with cancel on replay
        ctx.timer(Duration::from_secs(1)).await;
        let resolution = la.await;
        assert!(resolution.cancelled());
        Ok(().into())
    });

    // If we don't use this, we'd hang on shutdown for abandon cancel modes.
    let manual_cancel = CancellationToken::new();
    let manual_cancel_act = manual_cancel.clone();

    worker.register_activity("echo", move |ctx: ActContext, _: String| {
        let manual_cancel_act = manual_cancel_act.clone();
        async move {
            if cancel_on_backoff.is_some() {
                if ctx.is_cancelled() {
                    return Err(anyhow!(ActivityCancelledError::default()));
                }
                // Just fail constantly so we get stuck on the backoff timer
                return Err(anyhow!("Oh no I failed!"));
            } else {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(100)) => {},
                    _ = ctx.cancelled() => {
                        return Err(anyhow!(ActivityCancelledError::default()))
                    }
                    _ = manual_cancel_act.cancelled() => {
                        return Ok(())
                    }
                }
            }
            Err(anyhow!("Oh no I failed!"))
        }
    });

    worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker
        .run_until_done_intercepted(Some(LACancellerInterceptor {
            token: manual_cancel,
        }))
        .await
        .unwrap();
}

#[rstest::rstest]
#[case::schedule(true)]
#[case::start(false)]
#[tokio::test]
async fn x_to_close_timeout(#[case] is_schedule: bool) {
    let wf_name = format!(
        "{}_to_close_timeout",
        if is_schedule { "schedule" } else { "start" }
    );
    let mut starter = CoreWfStarter::new(&wf_name);
    let mut worker = starter.worker().await;
    let (sched, start) = if is_schedule {
        (Some(Duration::from_secs(2)), None)
    } else {
        (None, Some(Duration::from_secs(2)))
    };
    let timeout_type = if is_schedule {
        TimeoutType::ScheduleToClose
    } else {
        TimeoutType::StartToClose
    };

    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| async move {
        let res = ctx
            .local_activity(LocalActivityOptions {
                activity_type: "echo".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                retry_policy: RetryPolicy {
                    initial_interval: Some(prost_dur!(from_micros(15))),
                    backoff_coefficient: 1_000.,
                    maximum_interval: Some(prost_dur!(from_millis(1500))),
                    maximum_attempts: 4,
                    non_retryable_error_types: vec![],
                },
                timer_backoff_threshold: Some(Duration::from_secs(1)),
                schedule_to_close_timeout: sched,
                start_to_close_timeout: start,
                ..Default::default()
            })
            .await;
        assert_eq!(res.timed_out(), Some(timeout_type));
        Ok(().into())
    });
    worker.register_activity("echo", |ctx: ActContext, _: String| async move {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(100)) => {},
            _ = ctx.cancelled() => {
                return Err(anyhow!(ActivityCancelledError::default()))
            }
        };
        Ok(())
    });

    worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[rstest::rstest]
#[case::cached(true)]
#[case::not_cached(false)]
#[tokio::test]
async fn schedule_to_close_timeout_across_timer_backoff(#[case] cached: bool) {
    let wf_name = format!(
        "schedule_to_close_timeout_across_timer_backoff_{}",
        if cached { "cached" } else { "not_cached" }
    );
    let mut starter = CoreWfStarter::new(&wf_name);
    if !cached {
        starter.max_cached_workflows(0);
    }
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let res = ctx
            .local_activity(LocalActivityOptions {
                activity_type: "echo".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                retry_policy: RetryPolicy {
                    initial_interval: Some(prost_dur!(from_millis(15))),
                    backoff_coefficient: 1_000.,
                    maximum_interval: Some(prost_dur!(from_millis(1000))),
                    maximum_attempts: 40,
                    non_retryable_error_types: vec![],
                },
                timer_backoff_threshold: Some(Duration::from_millis(500)),
                schedule_to_close_timeout: Some(Duration::from_secs(2)),
                ..Default::default()
            })
            .await;
        assert_eq!(res.timed_out(), Some(TimeoutType::ScheduleToClose));
        Ok(().into())
    });
    let num_attempts: &'static _ = Box::leak(Box::new(AtomicU8::new(0)));
    worker.register_activity("echo", move |_: ActContext, _: String| async {
        num_attempts.fetch_add(1, Ordering::Relaxed);
        Result::<(), _>::Err(anyhow!("Oh no I failed!"))
    });

    worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    // 3 attempts b/c first backoff is very small, then the next 2 attempts take at least 2 seconds
    // b/c of timer backoff.
    assert_eq!(3, num_attempts.load(Ordering::Relaxed));
}

#[rstest::rstest]
#[tokio::test]
async fn eviction_wont_make_local_act_get_dropped(#[values(true, false)] short_wft_timeout: bool) {
    let wf_name = format!(
        "eviction_wont_make_local_act_get_dropped_{}",
        short_wft_timeout
    );
    let mut starter = CoreWfStarter::new(&wf_name);
    starter.max_cached_workflows(0);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), local_act_then_timer_then_wait);
    worker.register_activity("echo_activity", |_ctx: ActContext, str: String| async {
        tokio::time::sleep(Duration::from_secs(4)).await;
        Ok(str)
    });

    let opts = if short_wft_timeout {
        WorkflowOptions {
            task_timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        }
    } else {
        Default::default()
    };
    worker
        .submit_wf(wf_name.to_owned(), wf_name.to_owned(), vec![], opts)
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn timer_backoff_concurrent_with_non_timer_backoff() {
    let wf_name = "timer_backoff_concurrent_with_non_timer_backoff";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let r1 = ctx.local_activity(LocalActivityOptions {
            activity_type: "echo".to_string(),
            input: "hi".as_json_payload().expect("serializes fine"),
            retry_policy: RetryPolicy {
                initial_interval: Some(prost_dur!(from_micros(15))),
                backoff_coefficient: 1_000.,
                maximum_interval: Some(prost_dur!(from_millis(1500))),
                maximum_attempts: 4,
                non_retryable_error_types: vec![],
            },
            timer_backoff_threshold: Some(Duration::from_secs(1)),
            ..Default::default()
        });
        let r2 = ctx.local_activity(LocalActivityOptions {
            activity_type: "echo".to_string(),
            input: "hi".as_json_payload().expect("serializes fine"),
            retry_policy: RetryPolicy {
                initial_interval: Some(prost_dur!(from_millis(15))),
                backoff_coefficient: 10.,
                maximum_interval: Some(prost_dur!(from_millis(1500))),
                maximum_attempts: 4,
                non_retryable_error_types: vec![],
            },
            timer_backoff_threshold: Some(Duration::from_secs(10)),
            ..Default::default()
        });
        let (r1, r2) = tokio::join!(r1, r2);
        assert!(r1.failed());
        assert!(r2.failed());
        Ok(().into())
    });
    worker.register_activity("echo", |_: ActContext, _: String| async {
        Result::<(), _>::Err(anyhow!("Oh no I failed!"))
    });

    worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn repro_nondeterminism_with_timer_bug() {
    let wf_name = "repro_nondeterminism_with_timer_bug";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;

    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let t1 = ctx.timer(Duration::from_secs(30));
        let r1 = ctx.local_activity(LocalActivityOptions {
            activity_type: "delay".to_string(),
            input: "hi".as_json_payload().expect("serializes fine"),
            retry_policy: RetryPolicy {
                initial_interval: Some(prost_dur!(from_micros(15))),
                backoff_coefficient: 1_000.,
                maximum_interval: Some(prost_dur!(from_millis(1500))),
                maximum_attempts: 4,
                non_retryable_error_types: vec![],
            },
            timer_backoff_threshold: Some(Duration::from_secs(1)),
            ..Default::default()
        });
        tokio::pin!(t1);
        tokio::select! {
            _ = &mut t1 => {},
            _ = r1 => {
                t1.cancel(&ctx);
            },
        };
        ctx.timer(Duration::from_secs(1)).await;
        Ok(().into())
    });
    worker.register_activity("delay", |_: ActContext, _: String| async {
        tokio::time::sleep(Duration::from_secs(2)).await;
        Ok(())
    });

    let run_id = worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    starter
        .fetch_history_and_replay(wf_name, run_id, worker.inner_mut())
        .await
        .unwrap();
}

async fn la_problem_workflow(ctx: WfContext) -> WorkflowResult<()> {
    ctx.local_activity(LocalActivityOptions {
        activity_type: "delay".to_string(),
        input: "hi".as_json_payload().expect("serializes fine"),
        retry_policy: RetryPolicy {
            initial_interval: Some(prost_dur!(from_micros(15))),
            backoff_coefficient: 1_000.,
            maximum_interval: Some(prost_dur!(from_millis(1500))),
            maximum_attempts: 4,
            non_retryable_error_types: vec![],
        },
        timer_backoff_threshold: Some(Duration::from_secs(1)),
        ..Default::default()
    })
    .await;
    ctx.activity(ActivityOptions {
        activity_type: "delay".to_string(),
        start_to_close_timeout: Some(Duration::from_secs(20)),
        input: "hi!".as_json_payload().expect("serializes fine"),
        ..Default::default()
    })
    .await;
    Ok(().into())
}

// Expensive to run - worth enabling on a stress/regression pipeline.
#[ignore]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evict_while_la_running_no_interference() {
    let wf_name = "evict_while_la_running_no_interference";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.max_local_at(20);
    starter.max_cached_workflows(20);
    let mut worker = starter.worker().await;

    worker.register_wf(wf_name.to_owned(), la_problem_workflow);
    worker.register_activity("delay", |_: ActContext, _: String| async {
        tokio::time::sleep(Duration::from_secs(15)).await;
        Ok(())
    });

    let client = starter.get_client().await;
    let subfs = FuturesUnordered::new();
    for i in 1..100 {
        let wf_id = format!("{}-{}", wf_name, i);
        let run_id = worker
            .submit_wf(
                &wf_id,
                wf_name.to_owned(),
                vec![],
                WorkflowOptions::default(),
            )
            .await
            .unwrap();
        let cw = worker.core_worker.clone();
        let client = client.clone();
        subfs.push(async move {
            // Evict the workflow
            tokio::time::sleep(Duration::from_secs(1)).await;
            cw.request_workflow_eviction(&run_id);
            // Wake up workflow by sending signal
            client
                .signal_workflow_execution(
                    wf_id,
                    run_id.clone(),
                    "whaatever".to_string(),
                    None,
                    None,
                )
                .await
                .unwrap();
        });
    }
    let runf = async {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(subfs.collect::<Vec<_>>(), runf);
}

#[rstest::rstest]
#[tokio::test]
async fn weird_la_nondeterminism_repro(#[values(true, false)] fix_hist: bool) {
    init_integ_telem();
    let mut hist = history_from_proto_binary(
        "histories/evict_while_la_running_no_interference-85_history.bin",
    )
    .await
    .unwrap();
    if fix_hist {
        // Replace broken ending with accurate ending
        hist.events.truncate(20);
        let mut thb = TestHistoryBuilder::from_history(hist.events);
        thb.add_workflow_task_completed();
        thb.add_workflow_execution_completed();
        hist = thb.get_full_history_info().unwrap().into();
    }

    let mut worker = replay_sdk_worker([HistoryForReplay::new(hist, "fake".to_owned())]);
    worker.register_wf(
        "evict_while_la_running_no_interference",
        la_problem_workflow,
    );
    worker.register_activity("delay", |_: ActContext, _: String| async {
        tokio::time::sleep(Duration::from_secs(15)).await;
        Ok(())
    });
    worker.run().await.unwrap();
}

#[tokio::test]
async fn second_weird_la_nondeterminism_repro() {
    init_integ_telem();
    let mut hist = history_from_proto_binary(
        "histories/evict_while_la_running_no_interference-23_history.bin",
    )
    .await
    .unwrap();
    // Chop off uninteresting ending
    hist.events.truncate(24);
    let mut thb = TestHistoryBuilder::from_history(hist.events);
    // thb.add_workflow_task_completed();
    thb.add_workflow_execution_completed();
    hist = thb.get_full_history_info().unwrap().into();

    let mut worker = replay_sdk_worker([HistoryForReplay::new(hist, "fake".to_owned())]);
    worker.register_wf(
        "evict_while_la_running_no_interference",
        la_problem_workflow,
    );
    worker.register_activity("delay", |_: ActContext, _: String| async {
        tokio::time::sleep(Duration::from_secs(15)).await;
        Ok(())
    });
    worker.run().await.unwrap();
}
