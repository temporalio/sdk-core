use crate::common::{
    ActivationAssertionsInterceptor, CoreWfStarter, WorkflowHandleExt, build_fake_sdk,
    history_from_proto_binary, init_core_replay_preloaded, mock_sdk, mock_sdk_cfg,
    replay_sdk_worker, workflows::la_problem_workflow,
};
use anyhow::anyhow;
use crossbeam_queue::SegQueue;
use futures_util::{FutureExt, future::join_all};
use rstest::Context;
use std::{
    collections::HashMap,
    ops::Sub,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};
use temporalio_client::{WfClientExt, WorkflowClientTrait, WorkflowOptions};
use temporalio_common::{
    Worker,
    errors::PollError,
    protos::{
        DEFAULT_ACTIVITY_TYPE, canned_histories,
        coresdk::{
            ActivityTaskCompletion, AsJsonPayloadExt, FromJsonPayloadExt, IntoPayloadsExt,
            activity_result::ActivityExecutionResult,
            workflow_activation::{WorkflowActivationJob, workflow_activation_job},
            workflow_commands::{
                ActivityCancellationType, ScheduleLocalActivity, workflow_command::Variant,
            },
            workflow_completion,
            workflow_completion::{WorkflowActivationCompletion, workflow_activation_completion},
        },
        temporal::api::{
            command::v1::{RecordMarkerCommandAttributes, command},
            common::v1::RetryPolicy,
            enums::v1::{
                CommandType, EventType, TimeoutType, UpdateWorkflowExecutionLifecycleStage,
                WorkflowTaskFailedCause,
            },
            failure::v1::{Failure, failure::FailureInfo},
            history::v1::history_event::Attributes::MarkerRecordedEventAttributes,
            query::v1::WorkflowQuery,
            update::v1::WaitPolicy,
        },
        test_utils::{query_ok, schedule_local_activity_cmd, start_timer_cmd},
    },
};
use temporalio_sdk::{
    ActContext, ActivityError, ActivityOptions, CancellableFuture, LocalActivityOptions,
    UpdateContext, WfContext, WorkflowFunction, WorkflowResult,
    interceptors::{FailOnNondeterminismInterceptor, WorkerInterceptor},
};
use temporalio_sdk_core::{
    prost_dur,
    replay::{DEFAULT_WORKFLOW_TYPE, HistoryForReplay, TestHistoryBuilder, default_wes_attribs},
    test_help::{
        LEGACY_QUERY_ID, MockPollCfg, ResponseType, WorkerExt, WorkerTestHelpers,
        build_mock_pollers, hist_to_poll_resp, mock_worker, mock_worker_client,
        single_hist_mock_sg,
    },
};
use tokio::{join, select, sync::Barrier};
use tokio_util::sync::CancellationToken;

pub(crate) async fn one_local_activity_wf(ctx: WfContext) -> WorkflowResult<()> {
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

    let handle = starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

pub(crate) async fn local_act_concurrent_with_timer_wf(ctx: WfContext) -> WorkflowResult<()> {
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

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

pub(crate) async fn local_act_then_timer_then_wait(ctx: WfContext) -> WorkflowResult<()> {
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

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn long_running_local_act_with_timer() {
    let wf_name = "long_running_local_act_with_timer";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), local_act_then_timer_then_wait);
    worker.register_activity("echo_activity", |_ctx: ActContext, str: String| async {
        tokio::time::sleep(Duration::from_secs(4)).await;
        Ok(str)
    });

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

pub(crate) async fn local_act_fanout_wf(ctx: WfContext) -> WorkflowResult<()> {
    let las: Vec<_> = (1..=50)
        .map(|i| {
            ctx.local_activity(LocalActivityOptions {
                activity_type: "echo_activity".to_string(),
                input: format!("Hi {i}")
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
    starter
        .worker_config
        .max_outstanding_local_activities(1_usize);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), local_act_fanout_wf);
    worker.register_activity("echo_activity", echo);

    starter.start_with_worker(wf_name, &mut worker).await;
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
        Result::<(), _>::Err(anyhow!("Oh no I failed!").into())
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
    let client = starter.get_client().await;
    let handle = client.get_untyped_workflow_handle(wf_name, run_id);
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[rstest::rstest]
#[case::wait(ActivityCancellationType::WaitCancellationCompleted)]
#[case::try_cancel(ActivityCancellationType::TryCancel)]
#[case::abandon(ActivityCancellationType::Abandon)]
#[tokio::test]
async fn cancel_immediate(#[case] cancel_type: ActivityCancellationType) {
    let wf_name = format!("cancel_immediate_{cancel_type:?}");
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
                    return Err(ActivityError::cancelled())
                }
                _ = manual_cancel_act.cancelled() => {}
            }
            Ok(())
        }
    });

    starter.start_with_worker(wf_name, &mut worker).await;
    worker
        .run_until_done_intercepted(Some(LACancellerInterceptor {
            cancel_on_workflow_completed: false,
            token: manual_cancel,
        }))
        .await
        .unwrap();
}

struct LACancellerInterceptor {
    token: CancellationToken,
    cancel_on_workflow_completed: bool,
}
#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for LACancellerInterceptor {
    async fn on_workflow_activation_completion(&self, completion: &WorkflowActivationCompletion) {
        if !self.cancel_on_workflow_completed {
            return;
        }
        if let Some(workflow_activation_completion::Status::Successful(
            workflow_completion::Success { commands, .. },
        )) = completion.status.as_ref()
            && let Some(&Variant::CompleteWorkflowExecution(_)) =
                commands.last().and_then(|v| v.variant.as_ref())
        {
            self.token.cancel();
        }
    }
    fn on_shutdown(&self, _: &temporalio_sdk::Worker) {
        if !self.cancel_on_workflow_completed {
            self.token.cancel()
        }
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
    let wf_name = format!("cancel_after_act_starts_{cancel_on_backoff:?}_{cancel_type:?}");
    let mut starter = CoreWfStarter::new(&wf_name);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
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
                    return Err(ActivityError::cancelled());
                }
                // Just fail constantly so we get stuck on the backoff timer
                return Err(anyhow!("Oh no I failed!").into());
            } else {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(100)) => {},
                    _ = ctx.cancelled() => {
                        return Err(ActivityError::cancelled())
                    }
                    _ = manual_cancel_act.cancelled() => {
                        return Ok(())
                    }
                }
            }
            Err(anyhow!("Oh no I failed!").into())
        }
    });

    starter.start_with_worker(&wf_name, &mut worker).await;
    worker
        .run_until_done_intercepted(Some(LACancellerInterceptor {
            token: manual_cancel,
            // Only needed for this one case since the activity is not drained and prevents worker from shutting down.
            cancel_on_workflow_completed: matches!(cancel_type, ActivityCancellationType::Abandon)
                && cancel_on_backoff.is_none(),
        }))
        .await
        .unwrap();
    starter.shutdown().await;
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
                return Err(ActivityError::cancelled())
            }
        };
        Ok(())
    });

    starter.start_with_worker(wf_name, &mut worker).await;
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
        starter.worker_config.max_cached_workflows(0_usize);
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
        Result::<(), _>::Err(anyhow!("Oh no I failed!").into())
    });

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
    // 3 attempts b/c first backoff is very small, then the next 2 attempts take at least 2 seconds
    // b/c of timer backoff.
    assert_eq!(3, num_attempts.load(Ordering::Relaxed));
}

#[rstest::rstest]
#[tokio::test]
async fn eviction_wont_make_local_act_get_dropped(#[values(true, false)] short_wft_timeout: bool) {
    let wf_name = format!("eviction_wont_make_local_act_get_dropped_{short_wft_timeout}");
    let mut starter = CoreWfStarter::new(&wf_name);
    starter.worker_config.max_cached_workflows(0_usize);
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
        Result::<(), _>::Err(anyhow!("Oh no I failed!").into())
    });

    starter.start_with_worker(wf_name, &mut worker).await;
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
        }
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
    let client = starter.get_client().await;
    let handle = client.get_untyped_workflow_handle(wf_name, run_id);
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn weird_la_nondeterminism_repro(#[values(true, false)] fix_hist: bool) {
    let mut hist =
        history_from_proto_binary("evict_while_la_running_no_interference-85_history.bin")
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
    let mut hist =
        history_from_proto_binary("evict_while_la_running_no_interference-23_history.bin")
            .await
            .unwrap();
    // Chop off uninteresting ending
    hist.events.truncate(24);
    let mut thb = TestHistoryBuilder::from_history(hist.events);
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

#[tokio::test]
async fn third_weird_la_nondeterminism_repro() {
    let mut hist =
        history_from_proto_binary("evict_while_la_running_no_interference-16_history.bin")
            .await
            .unwrap();
    let mut thb = TestHistoryBuilder::from_history(hist.events);
    thb.add_workflow_task_scheduled_and_started();
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

/// This test demonstrates why it's important to send LA resolutions last within a job.
/// If we were to (during replay) scan ahead, see the marker, and resolve the LA before the
/// activity cancellation, that would be wrong because, during execution, the LA resolution is
/// always going to take _longer_ than the instantaneous cancel effect.
///
/// This affect applies regardless of how you choose to interleave cancellations and LAs. Ultimately
/// all cancellations will happen at once (in the order they are submitted) while the LA executions
/// are queued (because this all happens synchronously in the workflow machines). If you were to
/// _wait_ on an LA, and then cancel something else, and then run another LA, such that all commands
/// happened in the same workflow task, it would _still_ be fine to sort LA jobs last _within_ the
/// 2 activations that would necessarily entail (because, one you wait on the LA result, control
/// will be yielded and it will take another activation to unblock that LA).
#[tokio::test]
async fn la_resolve_same_time_as_other_cancel() {
    let wf_name = "la_resolve_same_time_as_other_cancel";
    let mut starter = CoreWfStarter::new(wf_name);
    // The activity won't get a chance to receive the cancel so make sure we still exit fast
    starter
        .worker_config
        .graceful_shutdown_period(Duration::from_millis(100));
    let mut worker = starter.worker().await;

    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let normal_act = ctx.activity(ActivityOptions {
            activity_type: "delay".to_string(),
            input: 9000.as_json_payload().expect("serializes fine"),
            cancellation_type: ActivityCancellationType::TryCancel,
            start_to_close_timeout: Some(Duration::from_secs(9000)),
            ..Default::default()
        });
        // Make new task
        ctx.timer(Duration::from_millis(1)).await;

        // Start LA and cancel the activity at the same time
        let local_act = ctx.local_activity(LocalActivityOptions {
            activity_type: "delay".to_string(),
            input: 100.as_json_payload().expect("serializes fine"),
            ..Default::default()
        });
        normal_act.cancel(&ctx);
        // Race them, starting a timer if LA completes first
        tokio::select! {
            biased;
            _ = normal_act => {},
            _ = local_act => {
                ctx.timer(Duration::from_millis(1)).await;
            },
        }
        Ok(().into())
    });
    worker.register_activity("delay", |ctx: ActContext, wait_time: u64| async move {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(wait_time)) => {}
            _ = ctx.cancelled() => {}
        }
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
    let client = starter.get_client().await;
    let handle = client.get_untyped_workflow_handle(wf_name, run_id);
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[rstest::rstest]
#[case(200, 0)]
#[case(200, 2000)]
#[case(2000, 0)]
#[case(2000, 2000)]
#[tokio::test]
async fn long_local_activity_with_update(
    #[context] ctx: Context,
    #[case] update_interval_ms: u64,
    #[case] update_inner_timer: u64,
) {
    let wf_name = format!("{}-{}", ctx.name, ctx.case.unwrap());
    let mut starter = CoreWfStarter::new(&wf_name);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;

    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| async move {
        let update_counter = Arc::new(AtomicUsize::new(1));
        let uc = update_counter.clone();
        ctx.update_handler(
            "update",
            |_: &_, _: ()| Ok(()),
            move |u: UpdateContext, _: ()| {
                let uc = uc.clone();
                async move {
                    if update_inner_timer != 0 {
                        u.wf_ctx
                            .timer(Duration::from_millis(update_inner_timer))
                            .await;
                    }
                    uc.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                }
            },
        );
        ctx.local_activity(LocalActivityOptions {
            activity_type: "delay".to_string(),
            input: "hi".as_json_payload().expect("serializes fine"),
            ..Default::default()
        })
        .await;
        update_counter.load(Ordering::Relaxed);
        Ok(().into())
    });
    worker.register_activity("delay", |_: ActContext, _: String| async {
        tokio::time::sleep(Duration::from_secs(6)).await;
        Ok(())
    });

    let handle = starter
        .start_with_worker(wf_name.clone(), &mut worker)
        .await;

    let wf_id = starter.get_task_queue().to_string();
    let update = async {
        loop {
            tokio::time::sleep(Duration::from_millis(update_interval_ms)).await;
            let _ = client
                .update_workflow_execution(
                    wf_id.clone(),
                    "".to_string(),
                    "update".to_string(),
                    WaitPolicy {
                        lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
                    },
                    [().as_json_payload().unwrap()].into_payloads(),
                )
                .await;
        }
    };
    let runner = async {
        worker.run_until_done().await.unwrap();
    };
    tokio::select!(_ = update => {}, _ = runner => {});
    let res = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap()
        .unwrap_success();
    let replay_res = handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
    assert_eq!(res[0], replay_res.unwrap());

    // Load histories from pre-fix version and ensure compat
    let replay_worker = init_core_replay_preloaded(
        starter.get_task_queue(),
        [HistoryForReplay::new(
            history_from_proto_binary(&format!("{wf_name}_history.bin"))
                .await
                .unwrap(),
            "fake".to_owned(),
        )],
    );
    let inner_worker = worker.inner_mut();
    inner_worker.with_new_core_worker(replay_worker);
    inner_worker.set_worker_interceptor(FailOnNondeterminismInterceptor {});
    inner_worker.run().await.unwrap();
}

#[tokio::test]
async fn local_activity_with_heartbeat_only_causes_one_wakeup() {
    let wf_name = "local_activity_with_heartbeat_only_causes_one_wakeup";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    let mut worker = starter.worker().await;

    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| async move {
        let mut wakeup_counter = 1;
        let la_resolved = AtomicBool::new(false);
        tokio::join!(
            async {
                ctx.local_activity(LocalActivityOptions {
                    activity_type: "delay".to_string(),
                    input: "hi".as_json_payload().expect("serializes fine"),
                    ..Default::default()
                })
                .await;
                la_resolved.store(true, Ordering::Relaxed);
            },
            async {
                ctx.wait_condition(|| {
                    wakeup_counter += 1;
                    la_resolved.load(Ordering::Relaxed)
                })
                .await;
            }
        );
        Ok(().into())
    });
    worker.register_activity("delay", |_: ActContext, _: String| async {
        tokio::time::sleep(Duration::from_secs(6)).await;
        Ok(())
    });

    let handle = starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
    let res = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap()
        .unwrap_success();
    let replay_res = handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
    assert_eq!(res[0], replay_res.unwrap());
}

pub(crate) async fn local_activity_with_summary_wf(ctx: WfContext) -> WorkflowResult<()> {
    ctx.local_activity(LocalActivityOptions {
        activity_type: "echo_activity".to_string(),
        input: "hi!".as_json_payload().expect("serializes fine"),
        summary: Some("Echo summary".to_string()),
        ..Default::default()
    })
    .await;
    Ok(().into())
}

#[tokio::test]
async fn local_activity_with_summary() {
    let wf_name = "local_activity_with_summary";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), local_activity_with_summary_wf);
    worker.register_activity("echo_activity", echo);

    let handle = starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();

    let la_events = starter
        .get_history()
        .await
        .events
        .into_iter()
        .filter(|e| match e.attributes {
            Some(MarkerRecordedEventAttributes(ref a)) => a.marker_name == "core_local_activity",
            _ => false,
        })
        .collect::<Vec<_>>();
    assert_eq!(la_events.len(), 1);
    let summary = la_events[0]
        .user_metadata
        .as_ref()
        .expect("metadata missing from local activity marker")
        .summary
        .as_ref()
        .expect("summary missing from local activity marker");
    assert_eq!(
        "Echo summary",
        String::from_json_payload(summary).expect("failed to parse summary")
    );
}

async fn echo(_ctx: ActContext, e: String) -> Result<String, ActivityError> {
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
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_full_wf_task();
    t.add_local_activity_result_marker(1, "1", b"echo".into());
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
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
                activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                ..Default::default()
            });
            ctx.timer(Duration::from_secs(1)).await;
            la.await;
            Ok(().into())
        },
    );
    worker.register_activity(DEFAULT_ACTIVITY_TYPE, echo);
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
async fn local_act_many_concurrent() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_full_wf_task();
    for i in 1..=50 {
        t.add_local_activity_result_marker(i, &i.to_string(), b"echo".into());
    }
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 3], mock);
    let mut worker = mock_sdk(mh);

    worker.register_wf(DEFAULT_WORKFLOW_TYPE.to_owned(), local_act_fanout_wf);
    worker.register_activity("echo_activity", echo);
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
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 2, 2], mock);
    mh.enforce_correct_number_of_polls = false;
    let mut worker = mock_sdk_cfg(mh, |wc| {
        wc.max_cached_workflows = 1;
        wc.max_outstanding_workflow_tasks = Some(1);
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
    let mock = mock_worker_client();
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
            Err(anyhow!("Oh no I failed!").into())
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
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_local_activity_fail_marker(
        2,
        "2",
        Failure::application_failure("la failed".to_string(), false),
    );
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
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
                    activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
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
    worker.register_activity(
        DEFAULT_ACTIVITY_TYPE,
        move |_ctx: ActContext, _: String| async move {
            Result::<(), _>::Err(anyhow!("Oh no I failed!").into())
        },
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
async fn local_act_null_result() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_local_activity_marker(1, "1", None, None, |_| {});
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
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
    t.add_by_type(EventType::TimerStarted);
    t.add_full_wf_task();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
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
    let mock = mock_worker_client();
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
                "1",
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
            query_ok(&query.query_id, "whatev"),
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
    t.add(default_wes_attribs());
    // Since we don't send queries with start workflow, need one workflow task of something else
    // b/c we want to get an activation with a job and a nonlegacy query
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());

    // nonlegacy query got here & LA started here
    // then next task is incremental w/ legacy query (for impossible query case)
    t.add_full_wf_task();

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
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::ToTaskNum(2));
            // Strip beginning of history so the only events are WFT sched/started, we need to look
            // like we hit the cache
            {
                let h = pr.history.as_mut().unwrap();
                h.events = h.events.split_off(6);
            }
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
    let mut mock = mock_worker_client();
    if impossible_query_in_task {
        mock.expect_respond_legacy_query()
            .times(1)
            .returning(move |_, _| Ok(Default::default()));
    }
    let mut mock = single_hist_mock_sg(wfid, t, tasks, mock, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let taskmap = mock.outstanding_task_map.clone().unwrap();
    let core = mock_worker(mock);

    let wf_fut = async {
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            &[WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
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
            &[WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::FireTimer(_)),
            },]
        );
        // We want to make sure the weird-looking query gets received while we're working on other
        // stuff, so that we don't see the workflow complete and choose to evict.
        taskmap.release_run(&task.run_id);
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
        // The next task needs to be resolve, since the LA is completed immediately
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        // Complete workflow
        core.complete_execution(&task.run_id).await;

        // Now we will get the query
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            &[WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(ref q)),
            }]
            if q.query_id == "q1"
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            query_ok("q1", "whatev"),
        ))
        .await
        .unwrap();

        if impossible_query_in_task {
            // finish last query
            let task = core.poll_workflow_activation().await.unwrap();
            core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                task.run_id,
                query_ok(LEGACY_QUERY_ID, "whatev"),
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

    join!(wf_fut, act_fut);
    core.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn test_schedule_to_start_timeout() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
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
            let rfail = la_res.unwrap_failure();
            assert_matches!(
                rfail.failure_info,
                Some(FailureInfo::ActivityFailureInfo(_))
            );
            assert_matches!(
                rfail.cause.unwrap().failure_info,
                Some(FailureInfo::TimeoutFailureInfo(_))
            );
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
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
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

#[rstest::rstest]
#[tokio::test]
async fn start_to_close_timeout_allows_retries(#[values(true, false)] la_completes: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    if la_completes {
        t.add_local_activity_marker(1, "1", Some("hi".into()), None, |_| {});
    } else {
        t.add_local_activity_marker(
            1,
            "1",
            None,
            Some(Failure::timeout(TimeoutType::StartToClose)),
            |_| {},
        );
    }
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [ResponseType::ToTaskNum(1), ResponseType::AllHistory],
        mock,
    );
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        move |ctx: WfContext| async move {
            let la_res = ctx
                .local_activity(LocalActivityOptions {
                    activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
                    input: "hi".as_json_payload().expect("serializes fine"),
                    retry_policy: RetryPolicy {
                        initial_interval: Some(prost_dur!(from_millis(20))),
                        backoff_coefficient: 1.0,
                        maximum_interval: None,
                        maximum_attempts: 5,
                        non_retryable_error_types: vec![],
                    },
                    start_to_close_timeout: Some(prost_dur!(from_millis(25))),
                    ..Default::default()
                })
                .await;
            if la_completes {
                assert!(la_res.completed_ok());
            } else {
                assert_eq!(la_res.timed_out(), Some(TimeoutType::StartToClose));
            }
            Ok(().into())
        },
    );
    let attempts: &'static _ = Box::leak(Box::new(AtomicUsize::new(0)));
    let cancels: &'static _ = Box::leak(Box::new(AtomicUsize::new(0)));
    worker.register_activity(
        DEFAULT_ACTIVITY_TYPE,
        move |ctx: ActContext, _: String| async move {
            // Timeout the first 4 attempts, or all of them if we intend to fail
            if attempts.fetch_add(1, Ordering::AcqRel) < 4 || !la_completes {
                select! {
                    _ = tokio::time::sleep(Duration::from_millis(100)) => (),
                    _ = ctx.cancelled() => {
                        cancels.fetch_add(1, Ordering::AcqRel);
                        return Err(ActivityError::cancelled());
                    }
                }
            }
            Ok(())
        },
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
    // Activity should have been attempted all 5 times
    assert_eq!(attempts.load(Ordering::Acquire), 5);
    let num_cancels = if la_completes { 4 } else { 5 };
    assert_eq!(cancels.load(Ordering::Acquire), num_cancels);
}

#[tokio::test]
async fn wft_failure_cancels_running_las() {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2], mock);
    mh.num_expected_fails = 1;
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            let la_handle = ctx.local_activity(LocalActivityOptions {
                activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
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
    worker.register_activity(
        DEFAULT_ACTIVITY_TYPE,
        move |ctx: ActContext, _: String| async move {
            let res = tokio::time::timeout(Duration::from_millis(500), ctx.cancelled()).await;
            if res.is_err() {
                panic!("Activity must be cancelled!!!!");
            }
            Result::<(), _>::Err(ActivityError::cancelled())
        },
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
async fn resolved_las_not_recorded_if_wft_fails_many_times() {
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
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [1.into(), ResponseType::AllHistory, ResponseType::AllHistory],
        mock,
    );
    mh.num_expected_fails = 2;
    mh.num_expected_completions = Some(0.into());
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    #[allow(unreachable_code)]
    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        WorkflowFunction::new::<_, _, ()>(|ctx: WfContext| async move {
            ctx.local_activity(LocalActivityOptions {
                activity_type: "echo".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                ..Default::default()
            })
            .await;
            panic!()
        }),
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

#[tokio::test]
async fn local_act_records_nonfirst_attempts_ok() {
    let mut t = TestHistoryBuilder::default();
    let wft_timeout = Duration::from_millis(200);
    t.add_wfe_started_with_wft_timeout(wft_timeout);
    t.add_full_wf_task();
    t.add_full_wf_task();
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mut mh = MockPollCfg::from_resp_batches(wf_id, t, [1, 2, 3], mock);
    let nonfirst_counts = Arc::new(SegQueue::new());
    let nfc_c = nonfirst_counts.clone();
    mh.completion_mock_fn = Some(Box::new(move |c| {
        nfc_c.push(
            c.metering_metadata
                .nonfirst_local_activity_execution_attempts,
        );
        Ok(Default::default())
    }));
    let mut worker = mock_sdk_cfg(mh, |wc| {
        wc.max_cached_workflows = 1;
        wc.max_outstanding_workflow_tasks = Some(1);
    });

    worker.register_wf(
        DEFAULT_WORKFLOW_TYPE.to_owned(),
        |ctx: WfContext| async move {
            ctx.local_activity(LocalActivityOptions {
                activity_type: "echo".to_string(),
                input: "hi".as_json_payload().expect("serializes fine"),
                retry_policy: RetryPolicy {
                    initial_interval: Some(prost_dur!(from_millis(10))),
                    backoff_coefficient: 1.0,
                    maximum_interval: None,
                    maximum_attempts: 0,
                    non_retryable_error_types: vec![],
                },
                ..Default::default()
            })
            .await;
            Ok(().into())
        },
    );
    worker.register_activity("echo", move |_ctx: ActContext, _: String| async move {
        Result::<(), _>::Err(anyhow!("I fail").into())
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
    // 3 workflow tasks
    assert_eq!(nonfirst_counts.len(), 3);
    // First task's non-first count should, of course, be 0
    assert_eq!(nonfirst_counts.pop().unwrap(), 0);
    // Next two, some nonzero amount which could vary based on test load
    assert!(nonfirst_counts.pop().unwrap() > 0);
    assert!(nonfirst_counts.pop().unwrap() > 0);
}

#[tokio::test]
async fn local_activities_can_be_delivered_during_shutdown() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_workflow_task_scheduled_and_started();

    let mock = mock_worker_client();
    let mut mock = single_hist_mock_sg(
        wfid,
        t,
        [ResponseType::ToTaskNum(1), ResponseType::AllHistory],
        mock,
        true,
    );
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    // Initiate shutdown once we have the WF activation, but before replying that we want to do an
    // LA
    core.initiate_shutdown();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        ScheduleLocalActivity {
            seq: 1,
            activity_id: "1".to_string(),
            activity_type: "test_act".to_string(),
            start_to_close_timeout: Some(prost_dur!(from_secs(30))),
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();

    let wf_poller = async { core.poll_workflow_activation().await };

    let at_poller = async {
        let act_task = core.poll_activity_task().await.unwrap();
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
        core.poll_activity_task().await
    };

    let (wf_r, act_r) = join!(wf_poller, at_poller);
    assert_matches!(wf_r.unwrap_err(), PollError::ShutDown);
    assert_matches!(act_r.unwrap_err(), PollError::ShutDown);
}

#[tokio::test]
async fn queries_can_be_received_while_heartbeating() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    t.add_full_wf_task();
    t.add_full_wf_task();
    t.add_full_wf_task();

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
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), ResponseType::OneTask(3));
            pr.query = Some(WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
                header: None,
            });
            pr
        },
    ];
    let mut mock = mock_worker_client();
    mock.expect_respond_legacy_query()
        .times(1)
        .returning(move |_, _| Ok(Default::default()));
    let mut mock = single_hist_mock_sg(wfid, t, tasks, mock, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        &[WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        },]
    );
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
    assert_matches!(
        task.jobs.as_slice(),
        &[WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(ref q)),
        }]
        if q.query_id == "q1"
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        query_ok("q1", "whatev"),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        &[WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::QueryWorkflow(ref q)),
        }]
        if q.query_id == LEGACY_QUERY_ID
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        query_ok(LEGACY_QUERY_ID, "whatev"),
    ))
    .await
    .unwrap();

    // Handle the activity so we can shut down cleanly
    let act_task = core.poll_activity_task().await.unwrap();
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act_task.task_token,
        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();

    core.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn local_activity_after_wf_complete_is_discarded() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(200));
    t.add_full_wf_task();
    t.add_workflow_task_scheduled_and_started();

    let mock = mock_worker_client();
    let mut mock_cfg = MockPollCfg::from_resp_batches(
        wfid,
        t,
        [ResponseType::ToTaskNum(1), ResponseType::ToTaskNum(2)],
        mock,
    );
    mock_cfg.make_poll_stream_interminable = true;
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 0);
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 2);
                assert_eq!(wft.commands[0].command_type(), CommandType::RecordMarker);
                assert_eq!(
                    wft.commands[1].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            });
    });
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 1;
        wc.ignore_evicts_on_shutdown = false;
    });
    let core = mock_worker(mock);

    let barr = Barrier::new(2);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            ScheduleLocalActivity {
                seq: 1,
                activity_id: "1".to_string(),
                activity_type: "test_act".to_string(),
                start_to_close_timeout: Some(prost_dur!(from_secs(30))),
                ..Default::default()
            }
            .into(),
            ScheduleLocalActivity {
                seq: 2,
                activity_id: "2".to_string(),
                activity_type: "test_act".to_string(),
                start_to_close_timeout: Some(prost_dur!(from_secs(30))),
                ..Default::default()
            }
            .into(),
        ],
    ))
    .await
    .unwrap();

    let wf_poller = async {
        let task = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        barr.wait().await;
        core.complete_execution(&task.run_id).await;
    };

    let at_poller = async {
        let act_task = core.poll_activity_task().await.unwrap();
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
        let act_task = core.poll_activity_task().await.unwrap();
        barr.wait().await;
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![2].into())),
        })
        .await
        .unwrap();
    };

    join!(wf_poller, at_poller);
    core.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn local_act_retry_explicit_delay() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
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
                        backoff_coefficient: 1.0,
                        maximum_attempts: 5,
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await;
            assert!(la_res.completed_ok());
            Ok(().into())
        },
    );
    let attempts: &'static _ = Box::leak(Box::new(AtomicUsize::new(0)));
    worker.register_activity("echo", move |_ctx: ActContext, _: String| async move {
        // Succeed on 3rd attempt (which is ==2 since fetch_add returns prev val)
        let last_attempt = attempts.fetch_add(1, Ordering::Relaxed);
        if 0 == last_attempt {
            Err(ActivityError::Retryable {
                source: anyhow!("Explicit backoff error"),
                explicit_delay: Some(Duration::from_millis(300)),
            })
        } else if 2 == last_attempt {
            Ok(())
        } else {
            Err(anyhow!("Oh no I failed!").into())
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
    let start = Instant::now();
    worker.run_until_done().await.unwrap();
    let expected_attempts = 3;
    assert_eq!(expected_attempts, attempts.load(Ordering::Relaxed));
    // There will be one 300ms backoff and one 50s backoff, so things should take at least that long
    assert!(start.elapsed() > Duration::from_millis(350));
}

async fn la_wf(ctx: WfContext) -> WorkflowResult<()> {
    ctx.local_activity(LocalActivityOptions {
        activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
        input: ().as_json_payload().unwrap(),
        retry_policy: RetryPolicy {
            maximum_attempts: 1,
            ..Default::default()
        },
        ..Default::default()
    })
    .await;
    Ok(().into())
}

#[rstest]
#[case::incremental(false, true)]
#[case::replay(true, true)]
#[case::incremental_fail(false, false)]
#[case::replay_fail(true, false)]
#[tokio::test]
async fn one_la_success(#[case] replay: bool, #[case] completes_ok: bool) {
    let activity_id = "1";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    if completes_ok {
        t.add_local_activity_result_marker(1, activity_id, b"hi".into());
    } else {
        t.add_local_activity_fail_marker(
            1,
            activity_id,
            Failure::application_failure("I failed".to_string(), false),
        );
    }
    t.add_workflow_task_scheduled_and_started();

    let mut mock_cfg = if replay {
        MockPollCfg::from_resps(t, [ResponseType::AllHistory])
    } else {
        MockPollCfg::from_hist_builder(t)
    };
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            let commands = &wft.commands;
            if !replay {
                assert_eq!(commands.len(), 2);
                assert_eq!(commands[0].command_type(), CommandType::RecordMarker);
                if completes_ok {
                    assert_matches!(
                        commands[0].attributes.as_ref().unwrap(),
                        command::Attributes::RecordMarkerCommandAttributes(
                            RecordMarkerCommandAttributes { failure: None, .. }
                        )
                    );
                } else {
                    assert_matches!(
                        commands[0].attributes.as_ref().unwrap(),
                        command::Attributes::RecordMarkerCommandAttributes(
                            RecordMarkerCommandAttributes {
                                failure: Some(_),
                                ..
                            }
                        )
                    );
                }
                assert_eq!(
                    commands[1].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            } else {
                assert_eq!(commands.len(), 1);
                assert_matches!(
                    commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            }
        });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, la_wf);
    worker.register_activity(
        DEFAULT_ACTIVITY_TYPE,
        move |_ctx: ActContext, _: ()| async move {
            if replay {
                panic!("Should not be invoked on replay");
            }
            if completes_ok {
                Ok("hi")
            } else {
                Err(anyhow!("Oh no I failed!").into())
            }
        },
    );
    worker.run().await.unwrap();
}

async fn two_la_wf(ctx: WfContext) -> WorkflowResult<()> {
    ctx.local_activity(LocalActivityOptions {
        activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
        input: ().as_json_payload().unwrap(),
        ..Default::default()
    })
    .await;
    ctx.local_activity(LocalActivityOptions {
        activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
        input: ().as_json_payload().unwrap(),
        ..Default::default()
    })
    .await;
    Ok(().into())
}

async fn two_la_wf_parallel(ctx: WfContext) -> WorkflowResult<()> {
    tokio::join!(
        ctx.local_activity(LocalActivityOptions {
            activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
            input: ().as_json_payload().unwrap(),
            ..Default::default()
        }),
        ctx.local_activity(LocalActivityOptions {
            activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
            input: ().as_json_payload().unwrap(),
            ..Default::default()
        })
    );
    Ok(().into())
}

#[rstest]
#[tokio::test]
async fn two_sequential_las(
    #[values(true, false)] replay: bool,
    #[values(true, false)] parallel: bool,
) {
    let t = canned_histories::two_local_activities_one_wft(parallel);
    let mut mock_cfg = if replay {
        MockPollCfg::from_resps(t, [ResponseType::AllHistory])
    } else {
        MockPollCfg::from_hist_builder(t)
    };

    let mut aai = ActivationAssertionsInterceptor::default();
    let first_act_ts_seconds: &'static _ = Box::leak(Box::new(AtomicI64::new(-1)));
    aai.then(|a| {
        first_act_ts_seconds.store(a.timestamp.as_ref().unwrap().seconds, Ordering::Relaxed)
    });
    // Verify LAs advance time (they take 1s as defined in the canned history)
    aai.then(move |a| {
        if !parallel {
            assert_matches!(
                a.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
                }] => assert_eq!(ra.seq, 1)
            );
        } else {
            assert_matches!(
                a.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
                 }, WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(ra2))
                }] => {assert_eq!(ra.seq, 1); assert_eq!(ra2.seq, 2)}
            );
        }
        if replay {
            assert!(
                a.timestamp.as_ref().unwrap().seconds
                    > first_act_ts_seconds.load(Ordering::Relaxed)
            )
        }
    });
    if !parallel {
        aai.then(move |a| {
            assert_matches!(
                a.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
                }] => assert_eq!(ra.seq, 2)
            );
            if replay {
                assert!(
                    a.timestamp.as_ref().unwrap().seconds
                        >= first_act_ts_seconds.load(Ordering::Relaxed) + 2
                )
            }
        });
    }

    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            let commands = &wft.commands;
            if !replay {
                assert_eq!(commands.len(), 3);
                assert_eq!(commands[0].command_type(), CommandType::RecordMarker);
                assert_eq!(commands[1].command_type(), CommandType::RecordMarker);
                assert_matches!(
                    commands[2].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            } else {
                assert_eq!(commands.len(), 1);
                assert_matches!(
                    commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            }
        });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.set_worker_interceptor(aai);
    if parallel {
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, two_la_wf_parallel);
    } else {
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, two_la_wf);
    }
    worker.register_activity(
        DEFAULT_ACTIVITY_TYPE,
        move |_ctx: ActContext, _: ()| async move { Ok("Resolved") },
    );
    worker.run().await.unwrap();
}

async fn la_timer_la(ctx: WfContext) -> WorkflowResult<()> {
    ctx.local_activity(LocalActivityOptions {
        activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
        input: ().as_json_payload().unwrap(),
        ..Default::default()
    })
    .await;
    ctx.timer(Duration::from_secs(5)).await;
    ctx.local_activity(LocalActivityOptions {
        activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
        input: ().as_json_payload().unwrap(),
        ..Default::default()
    })
    .await;
    Ok(().into())
}

#[rstest]
#[case::incremental(false)]
#[case::replay(true)]
#[tokio::test]
async fn las_separated_by_timer(#[case] replay: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_local_activity_result_marker(1, "1", b"hi".into());
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_local_activity_result_marker(2, "2", b"hi2".into());
    t.add_workflow_task_scheduled_and_started();
    let mut mock_cfg = if replay {
        MockPollCfg::from_resps(t, [ResponseType::AllHistory])
    } else {
        MockPollCfg::from_hist_builder(t)
    };

    let mut aai = ActivationAssertionsInterceptor::default();
    aai.skip_one()
        .then(|a| {
            assert_matches!(
                a.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(ra))
                }] => assert_eq!(ra.seq, 1)
            );
        })
        .then(|a| {
            assert_matches!(
                a.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::FireTimer(_))
                }]
            );
        });

    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        if replay {
            asserts.then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type,
                    CommandType::CompleteWorkflowExecution as i32
                );
            });
        } else {
            asserts
                .then(|wft| {
                    let commands = &wft.commands;
                    assert_eq!(commands.len(), 2);
                    assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
                    assert_eq!(commands[1].command_type, CommandType::StartTimer as i32);
                })
                .then(|wft| {
                    let commands = &wft.commands;
                    assert_eq!(commands.len(), 2);
                    assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
                    assert_eq!(
                        commands[1].command_type,
                        CommandType::CompleteWorkflowExecution as i32
                    );
                });
        }
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.set_worker_interceptor(aai);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, la_timer_la);
    worker.register_activity(
        DEFAULT_ACTIVITY_TYPE,
        move |_ctx: ActContext, _: ()| async move { Ok("Resolved") },
    );
    worker.run().await.unwrap();
}

#[tokio::test]
async fn one_la_heartbeating_wft_failure_still_executes() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    // Heartbeats
    t.add_full_wf_task();
    // fails a wft for some reason
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(
        WorkflowTaskFailedCause::NonDeterministicError,
        Default::default(),
    );
    t.add_workflow_task_scheduled_and_started();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            assert_eq!(wft.commands.len(), 2);
            assert_eq!(wft.commands[0].command_type(), CommandType::RecordMarker);
            assert_matches!(
                wft.commands[1].command_type(),
                CommandType::CompleteWorkflowExecution
            );
        });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, la_wf);
    worker.register_activity(
        DEFAULT_ACTIVITY_TYPE,
        move |_ctx: ActContext, _: ()| async move { Ok("Resolved") },
    );
    worker.run().await.unwrap();
}

#[rstest]
#[tokio::test]
async fn immediate_cancel(
    #[values(
        ActivityCancellationType::WaitCancellationCompleted,
        ActivityCancellationType::TryCancel,
        ActivityCancellationType::Abandon
    )]
    cancel_type: ActivityCancellationType,
) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(|wft| {
            assert_eq!(wft.commands.len(), 2);
            // We record the cancel marker
            assert_eq!(wft.commands[0].command_type(), CommandType::RecordMarker);
            assert_matches!(
                wft.commands[1].command_type(),
                CommandType::CompleteWorkflowExecution
            );
        });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, move |ctx: WfContext| async move {
        let la = ctx.local_activity(LocalActivityOptions {
            cancel_type,
            ..Default::default()
        });
        la.cancel(&ctx);
        la.await;
        Ok(().into())
    });
    // Explicitly don't register an activity, since we shouldn't need to run one.
    worker.run().await.unwrap();
}

#[rstest]
#[case::incremental(false)]
#[case::replay(true)]
#[tokio::test]
async fn cancel_after_act_starts_canned(
    #[case] replay: bool,
    #[values(
        ActivityCancellationType::WaitCancellationCompleted,
        ActivityCancellationType::TryCancel,
        ActivityCancellationType::Abandon
    )]
    cancel_type: ActivityCancellationType,
) {
    let mut t = TestHistoryBuilder::default();
    t.add_wfe_started_with_wft_timeout(Duration::from_millis(100));
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    // This extra workflow task serves to prevent looking ahead and pre-resolving during
    // wait-cancel.
    // TODO: including this on non wait-cancel seems to cause double-send of
    //   marker recorded cmd
    if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
        t.add_full_wf_task();
    }
    if cancel_type != ActivityCancellationType::WaitCancellationCompleted {
        // With non-wait cancels, the cancel is immediate
        t.add_local_activity_cancel_marker(1, "1");
    }
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
        // With wait cancels, the cancel marker is not recorded until activity reports.
        t.add_local_activity_cancel_marker(1, "1");
    }
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_cfg = if replay {
        MockPollCfg::from_resps(t, [ResponseType::AllHistory])
    } else {
        MockPollCfg::from_hist_builder(t)
    };
    let allow_cancel_barr = CancellationToken::new();
    let allow_cancel_barr_clone = allow_cancel_barr.clone();

    if !replay {
        mock_cfg.completion_asserts_from_expectations(|mut asserts| {
            asserts
                .then(move |wft| {
                    assert_eq!(wft.commands.len(), 1);
                    assert_eq!(wft.commands[0].command_type, CommandType::StartTimer as i32);
                })
                .then(move |wft| {
                    let commands = &wft.commands;
                    if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
                        assert_eq!(commands.len(), 1);
                        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
                    } else {
                        // Try-cancel/abandon immediately recordsmarker (when not replaying)
                        assert_eq!(commands.len(), 2);
                        assert_eq!(commands[0].command_type, CommandType::RecordMarker as i32);
                        assert_eq!(commands[1].command_type, CommandType::StartTimer as i32);
                    }
                    // Allow the wait-cancel to actually cancel
                    allow_cancel_barr.cancel();
                })
                .then(move |wft| {
                    let commands = &wft.commands;
                    if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
                        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
                        assert_eq!(commands[1].command_type, CommandType::RecordMarker as i32);
                    } else {
                        assert_eq!(
                            commands[0].command_type,
                            CommandType::CompleteWorkflowExecution as i32
                        );
                    }
                });
        });
    }

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, move |ctx: WfContext| async move {
        let la = ctx.local_activity(LocalActivityOptions {
            cancel_type,
            input: ().as_json_payload().unwrap(),
            activity_type: DEFAULT_ACTIVITY_TYPE.to_string(),
            ..Default::default()
        });
        ctx.timer(Duration::from_secs(1)).await;
        la.cancel(&ctx);
        // This extra timer is here to ensure the presence of another WF task doesn't mess up
        // resolving the LA with cancel on replay
        ctx.timer(Duration::from_secs(1)).await;
        let resolution = la.await;
        assert!(resolution.cancelled());
        let rfail = resolution.unwrap_failure();
        assert_matches!(
            rfail.failure_info,
            Some(FailureInfo::ActivityFailureInfo(_))
        );
        assert_matches!(
            rfail.cause.unwrap().failure_info,
            Some(FailureInfo::CanceledFailureInfo(_))
        );
        Ok(().into())
    });
    worker.register_activity(DEFAULT_ACTIVITY_TYPE, move |ctx: ActContext, _: ()| {
        let allow_cancel_barr_clone = allow_cancel_barr_clone.clone();
        async move {
            if cancel_type == ActivityCancellationType::WaitCancellationCompleted {
                ctx.cancelled().await;
            }
            allow_cancel_barr_clone.cancelled().await;
            Result::<(), _>::Err(ActivityError::cancelled())
        }
    });
    worker.run().await.unwrap();
}
