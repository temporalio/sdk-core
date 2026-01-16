use crate::common::{
    ActivationAssertionsInterceptor, CoreWfStarter, WorkflowHandleExt,
    activity_functions::StdActivities, build_fake_sdk, history_from_proto_binary,
    init_core_replay_preloaded, mock_sdk, mock_sdk_cfg, replay_sdk_worker,
    workflows::LaProblemWorkflow,
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
use temporalio_common::protos::{
    DEFAULT_ACTIVITY_TYPE, canned_histories,
    coresdk::{
        ActivityTaskCompletion, AsJsonPayloadExt, FromJsonPayloadExt, IntoPayloadsExt,
        activity_result::ActivityExecutionResult,
        workflow_activation::{WorkflowActivationJob, workflow_activation_job},
        workflow_commands::{
            ActivityCancellationType, ScheduleLocalActivity, workflow_command::Variant,
        },
        workflow_completion::{self, WorkflowActivationCompletion, workflow_activation_completion},
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
};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, CancellableFuture, LocalActivityOptions, UpdateContext, WorkflowContext,
    WorkflowResult,
    activities::{ActivityContext, ActivityError},
    interceptors::{FailOnNondeterminismInterceptor, WorkerInterceptor},
};
use temporalio_sdk_core::{
    PollError, TunerHolder, prost_dur,
    replay::{DEFAULT_WORKFLOW_TYPE, HistoryForReplay, TestHistoryBuilder, default_wes_attribs},
    test_help::{
        LEGACY_QUERY_ID, MockPollCfg, ResponseType, WorkerExt, WorkerTestHelpers,
        build_mock_pollers, hist_to_poll_resp, mock_worker, mock_worker_client,
        single_hist_mock_sg,
    },
};
use tokio::{join, select, sync::Barrier};
use tokio_util::sync::CancellationToken;

#[workflow]
#[derive(Default)]
pub(crate) struct OneLocalActivityWf;

#[workflow_methods]
impl OneLocalActivityWf {
    #[run]
    pub(crate) async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        let initial_workflow_time = ctx.workflow_time().expect("Workflow time should be set");
        ctx.start_local_activity(
            StdActivities::echo,
            "hi!".to_string(),
            LocalActivityOptions::default(),
        )?
        .await;
        assert!(initial_workflow_time < ctx.workflow_time().unwrap());
        Ok(().into())
    }
}

#[tokio::test]
async fn one_local_activity() {
    let wf_name = "one_local_activity";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    worker.register_workflow::<OneLocalActivityWf>();

    let handle = worker
        .submit_workflow(
            OneLocalActivityWf::run,
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
#[derive(Default)]
pub(crate) struct LocalActConcurrentWithTimerWf;

#[workflow_methods]
impl LocalActConcurrentWithTimerWf {
    #[run]
    pub(crate) async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        let la = ctx.start_local_activity(
            StdActivities::echo,
            "hi!".to_string(),
            LocalActivityOptions::default(),
        )?;
        let timer = ctx.timer(Duration::from_secs(1));
        tokio::join!(la, timer);
        Ok(().into())
    }
}

#[tokio::test]
async fn local_act_concurrent_with_timer() {
    let wf_name = "local_act_concurrent_with_timer";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    worker.register_workflow::<LocalActConcurrentWithTimerWf>();

    worker
        .submit_workflow(
            LocalActConcurrentWithTimerWf::run,
            starter.get_task_queue(),
            (),
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct LocalActThenTimerThenWaitResult;

#[workflow_methods]
impl LocalActThenTimerThenWaitResult {
    #[run]
    pub(crate) async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        let la = ctx.start_local_activity(
            StdActivities::echo,
            "hi!".to_string(),
            LocalActivityOptions::default(),
        )?;
        ctx.timer(Duration::from_secs(1)).await;
        let res = la.await;
        assert!(res.completed_ok());
        Ok(().into())
    }
}

#[tokio::test]
async fn local_act_then_timer_then_wait_result() {
    let wf_name = "local_act_then_timer_then_wait_result";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    worker.register_workflow::<LocalActThenTimerThenWaitResult>();

    worker
        .submit_workflow(
            LocalActThenTimerThenWaitResult::run,
            starter.get_task_queue(),
            (),
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
pub(crate) struct LocalActThenTimerThenWait;

#[workflow_methods]
impl LocalActThenTimerThenWait {
    #[run]
    pub(crate) async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        let la = ctx.start_local_activity(
            StdActivities::delay,
            Duration::from_secs(4),
            LocalActivityOptions::default(),
        )?;
        ctx.timer(Duration::from_secs(1)).await;
        let res = la.await;
        assert!(res.completed_ok());
        Ok(().into())
    }
}

#[tokio::test]
async fn long_running_local_act_with_timer() {
    let wf_name = "long_running_local_act_with_timer";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    worker.register_workflow::<LocalActThenTimerThenWait>();

    worker
        .submit_workflow(
            LocalActThenTimerThenWait::run,
            starter.get_task_queue(),
            (),
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
pub(crate) struct LocalActFanoutWf;

#[workflow_methods]
impl LocalActFanoutWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    pub(crate) async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        let las: Vec<_> = (1..=50)
            .map(|i| {
                ctx.start_local_activity(StdActivities::echo, format!("Hi {i}"), Default::default())
                    .expect("serializes fine")
            })
            .collect();
        ctx.timer(Duration::from_secs(1)).await;
        join_all(las).await;
        Ok(().into())
    }
}

#[tokio::test]
async fn local_act_fanout() {
    let wf_name = "local_act_fanout";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(5, 1, 1, 1));
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    worker.register_workflow::<LocalActFanoutWf>();

    worker
        .submit_workflow(
            LocalActFanoutWf::run,
            starter.get_task_queue(),
            (),
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct LocalActRetryTimerBackoff;

#[workflow_methods]
impl LocalActRetryTimerBackoff {
    #[run]
    pub(crate) async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        let res = ctx
            .start_local_activity(
                StdActivities::always_fail,
                (),
                LocalActivityOptions {
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
                },
            )?
            .await;
        assert!(res.failed());
        Ok(().into())
    }
}

#[tokio::test]
async fn local_act_retry_timer_backoff() {
    let wf_name = "local_act_retry_timer_backoff";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    worker.register_workflow::<LocalActRetryTimerBackoff>();

    let handle = worker
        .submit_workflow(
            LocalActRetryTimerBackoff::run,
            wf_name,
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

#[rstest::rstest]
#[case::wait(ActivityCancellationType::WaitCancellationCompleted)]
#[case::try_cancel(ActivityCancellationType::TryCancel)]
#[case::abandon(ActivityCancellationType::Abandon)]
#[tokio::test]
async fn cancel_immediate(#[case] cancel_type: ActivityCancellationType) {
    let wf_name = format!("cancel_immediate_{cancel_type:?}");
    // If we don't use this, we'd hang on shutdown for abandon cancel modes.
    let manual_cancel = CancellationToken::new();
    let mut starter = CoreWfStarter::new(&wf_name);

    struct EchoWithManualCancel {
        manual_cancel: CancellationToken,
    }
    #[activities]
    impl EchoWithManualCancel {
        #[activity]
        async fn echo(
            self: Arc<Self>,
            ctx: ActivityContext,
            _: String,
        ) -> Result<(), ActivityError> {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                _ = ctx.cancelled() => {
                    return Err(ActivityError::cancelled())
                }
                _ = self.manual_cancel.cancelled() => {}
            }
            Ok(())
        }
    }

    starter
        .sdk_config
        .register_activities(EchoWithManualCancel {
            manual_cancel: manual_cancel.clone(),
        });

    #[workflow]
    struct CancelImmediate {
        cancel_type: ActivityCancellationType,
    }

    #[workflow_methods]
    impl CancelImmediate {
        #[init]
        fn new(_ctx: &WorkflowContext, cancel_type: ActivityCancellationType) -> Self {
            Self { cancel_type }
        }

        #[run]
        pub(crate) async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            let la = ctx.start_local_activity(
                EchoWithManualCancel::echo,
                "hi".to_string(),
                LocalActivityOptions {
                    cancel_type: self.cancel_type,
                    ..Default::default()
                },
            )?;
            la.cancel(ctx);
            let resolution = la.await;
            assert!(resolution.cancelled());
            Ok(().into())
        }
    }

    let mut worker = starter.worker().await;
    worker.register_workflow::<CancelImmediate>();

    worker
        .submit_workflow(
            CancelImmediate::run,
            wf_name,
            cancel_type,
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
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
    // If we don't use this, we'd hang on shutdown for abandon cancel modes.
    let manual_cancel = CancellationToken::new();
    let mut starter = CoreWfStarter::new(&wf_name);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));

    struct EchoWithManualCancelAndBackoff {
        manual_cancel: CancellationToken,
        cancel_on_backoff: Option<CancellationToken>,
    }
    #[activities]
    impl EchoWithManualCancelAndBackoff {
        #[activity]
        async fn echo(
            self: Arc<Self>,
            ctx: ActivityContext,
            _: String,
        ) -> Result<(), ActivityError> {
            if self.cancel_on_backoff.is_some() {
                if ctx.is_cancelled() {
                    return Err(ActivityError::cancelled());
                }
                // Just fail constantly so we get stuck on the backoff timer
                return Err(anyhow!("Oh no I failed!").into());
            } else {
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(100)) => {}
                    _ = ctx.cancelled() => {
                        return Err(ActivityError::cancelled())
                    }
                    _ = self.manual_cancel.cancelled() => {
                        return Ok(())
                    }
                }
            }
            Err(anyhow!("Oh no I failed!").into())
        }
    }

    starter
        .sdk_config
        .register_activities(EchoWithManualCancelAndBackoff {
            manual_cancel: manual_cancel.clone(),
            cancel_on_backoff: if cancel_on_backoff.is_some() {
                Some(CancellationToken::new())
            } else {
                None
            },
        });
    #[workflow]
    #[derive(Default)]
    struct CancelAfterActStartsWf;

    #[workflow_methods]
    impl CancelAfterActStartsWf {
        #[run]
        async fn run(
            &mut self,
            ctx: &mut WorkflowContext,
            (bo_dur, cancel_type): (Duration, ActivityCancellationType),
        ) -> WorkflowResult<()> {
            let la = ctx.start_local_activity(
                EchoWithManualCancelAndBackoff::echo,
                "hi".to_string(),
                LocalActivityOptions {
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
                },
            )?;
            ctx.timer(Duration::from_secs(1)).await;
            // Note that this cancel can't go through for *two* WF tasks, because we do a full heartbeat
            // before the timer (LA hasn't resolved), and then the timer fired event won't appear in
            // history until *after* the next WFT because we force generated it when we sent the timer
            // command.
            la.cancel(ctx);
            // This extra timer is here to ensure the presence of another WF task doesn't mess up
            // resolving the LA with cancel on replay
            ctx.timer(Duration::from_secs(1)).await;
            let resolution = la.await;
            assert!(resolution.cancelled());
            Ok(().into())
        }
    }

    let mut worker = starter.worker().await;
    let bo_dur = cancel_on_backoff.unwrap_or_else(|| Duration::from_secs(1));
    worker.register_workflow::<CancelAfterActStartsWf>();

    worker
        .submit_workflow(
            CancelAfterActStartsWf::run,
            &wf_name,
            (bo_dur, cancel_type),
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
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

    struct LongRunningWithCancellation;
    #[activities]
    impl LongRunningWithCancellation {
        #[activity]
        async fn go(ctx: ActivityContext) -> Result<(), ActivityError> {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(100)) => {}
                _ = ctx.cancelled() => {
                    return Err(ActivityError::cancelled())
                }
            }
            Ok(())
        }
    }

    starter
        .sdk_config
        .register_activities(LongRunningWithCancellation);
    #[workflow]
    #[derive(Default)]
    struct XToCloseTimeoutWf;

    #[workflow_methods]
    impl XToCloseTimeoutWf {
        #[run]
        async fn run(
            &mut self,
            ctx: &mut WorkflowContext,
            (sched, start, timeout_type): (Option<Duration>, Option<Duration>, i32),
        ) -> WorkflowResult<()> {
            let res = ctx
                .start_local_activity(
                    LongRunningWithCancellation::go,
                    (),
                    LocalActivityOptions {
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
                    },
                )?
                .await;
            assert_eq!(
                res.timed_out(),
                Some(TimeoutType::try_from(timeout_type).unwrap())
            );
            Ok(().into())
        }
    }

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
    worker.register_workflow::<XToCloseTimeoutWf>();

    worker
        .submit_workflow(
            XToCloseTimeoutWf::run,
            &wf_name,
            (sched, start, timeout_type as i32),
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
        starter.sdk_config.max_cached_workflows = 0_usize;
    }

    #[workflow]
    #[derive(Default)]
    struct ScheduleToCloseTimeoutAcrossTimerBackoff;

    #[workflow_methods]
    impl ScheduleToCloseTimeoutAcrossTimerBackoff {
        #[run]
        pub(crate) async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            let res = ctx
                .start_local_activity(
                    FailWithAtomicCounter::go,
                    "hi".to_string(),
                    LocalActivityOptions {
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
                    },
                )?
                .await;
            assert_eq!(res.timed_out(), Some(TimeoutType::ScheduleToClose));
            Ok(().into())
        }
    }

    let mut worker = starter.worker().await;
    worker.register_workflow::<ScheduleToCloseTimeoutAcrossTimerBackoff>();

    let num_attempts = Arc::new(AtomicU8::new(0));

    struct FailWithAtomicCounter {
        counter: Arc<AtomicU8>,
    }
    #[activities]
    impl FailWithAtomicCounter {
        #[activity]
        async fn go(self: Arc<Self>, _: ActivityContext, _: String) -> Result<(), ActivityError> {
            self.counter.fetch_add(1, Ordering::Relaxed);
            Err(anyhow!("Oh no I failed!").into())
        }
    }

    worker.register_activities(FailWithAtomicCounter {
        counter: num_attempts.clone(),
    });

    worker
        .submit_workflow(
            ScheduleToCloseTimeoutAcrossTimerBackoff::run,
            starter.get_task_queue(),
            (),
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
    let wf_name = format!("eviction_wont_make_local_act_get_dropped_{short_wft_timeout}");
    let mut starter = CoreWfStarter::new(&wf_name);
    starter.sdk_config.max_cached_workflows = 0_usize;
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    worker.register_workflow::<LocalActThenTimerThenWait>();

    let opts = if short_wft_timeout {
        WorkflowOptions {
            task_timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        }
    } else {
        Default::default()
    };
    worker
        .submit_workflow(LocalActThenTimerThenWait::run, wf_name, (), opts)
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn timer_backoff_concurrent_with_non_timer_backoff() {
    let wf_name = "timer_backoff_concurrent_with_non_timer_backoff";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);

    #[workflow]
    #[derive(Default)]
    struct TimerBackoffConcurrentWf;

    #[workflow_methods]
    impl TimerBackoffConcurrentWf {
        #[run]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            let r1 = ctx.start_local_activity(
                StdActivities::always_fail,
                (),
                LocalActivityOptions {
                    retry_policy: RetryPolicy {
                        initial_interval: Some(prost_dur!(from_micros(15))),
                        backoff_coefficient: 1_000.,
                        maximum_interval: Some(prost_dur!(from_millis(1500))),
                        maximum_attempts: 4,
                        non_retryable_error_types: vec![],
                    },
                    timer_backoff_threshold: Some(Duration::from_secs(1)),
                    ..Default::default()
                },
            )?;
            let r2 = ctx.start_local_activity(
                StdActivities::always_fail,
                (),
                LocalActivityOptions {
                    retry_policy: RetryPolicy {
                        initial_interval: Some(prost_dur!(from_millis(15))),
                        backoff_coefficient: 10.,
                        maximum_interval: Some(prost_dur!(from_millis(1500))),
                        maximum_attempts: 4,
                        non_retryable_error_types: vec![],
                    },
                    timer_backoff_threshold: Some(Duration::from_secs(10)),
                    ..Default::default()
                },
            )?;
            let (r1, r2) = tokio::join!(r1, r2);
            assert!(r1.failed());
            assert!(r2.failed());
            Ok(().into())
        }
    }

    let mut worker = starter.worker().await;
    worker.register_workflow::<TimerBackoffConcurrentWf>();

    worker
        .submit_workflow(
            TimerBackoffConcurrentWf::run,
            wf_name,
            (),
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
    starter.sdk_config.register_activities(StdActivities);

    #[workflow]
    #[derive(Default)]
    struct ReproNondeterminismWithTimerBugWf;

    #[workflow_methods]
    impl ReproNondeterminismWithTimerBugWf {
        #[run]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            let t1 = ctx.timer(Duration::from_secs(30));
            let r1 = ctx.start_local_activity(
                StdActivities::delay,
                Duration::from_secs(2),
                LocalActivityOptions {
                    retry_policy: RetryPolicy {
                        initial_interval: Some(prost_dur!(from_micros(15))),
                        backoff_coefficient: 1_000.,
                        maximum_interval: Some(prost_dur!(from_millis(1500))),
                        maximum_attempts: 4,
                        non_retryable_error_types: vec![],
                    },
                    timer_backoff_threshold: Some(Duration::from_secs(1)),
                    ..Default::default()
                },
            )?;
            tokio::pin!(t1);
            tokio::select! {
                _ = &mut t1 => {},
                _ = r1 => {
                    t1.cancel(ctx);
                },
            }
            ctx.timer(Duration::from_secs(1)).await;
            Ok(().into())
        }
    }

    let mut worker = starter.worker().await;
    worker.register_workflow::<ReproNondeterminismWithTimerBugWf>();

    let handle = worker
        .submit_workflow(
            ReproNondeterminismWithTimerBugWf::run,
            wf_name,
            (),
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let client = starter.get_client().await;
    let handle = client.get_untyped_workflow_handle(wf_name, handle.run_id().unwrap());
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
    worker.register_workflow::<LaProblemWorkflow>();
    worker.register_activities(StdActivities);
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
    worker.register_workflow::<LaProblemWorkflow>();
    worker.register_activities(StdActivities);
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
    worker.register_workflow::<LaProblemWorkflow>();
    worker.register_activities(StdActivities);
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

    struct DelayWithCancellation;
    #[activities]
    impl DelayWithCancellation {
        #[activity]
        async fn delay(ctx: ActivityContext, dur: Duration) -> Result<(), ActivityError> {
            tokio::select! {
                _ = tokio::time::sleep(dur) => {}
                _ = ctx.cancelled() => {}
            }
            Ok(())
        }
    }

    starter
        .sdk_config
        .register_activities(DelayWithCancellation);
    // The activity won't get a chance to receive the cancel so make sure we still exit fast
    starter.sdk_config.graceful_shutdown_period = Some(Duration::from_millis(100));

    #[workflow]
    #[derive(Default)]
    struct LaResolveSameTimeAsOtherCancelWf;

    #[workflow_methods]
    impl LaResolveSameTimeAsOtherCancelWf {
        #[run]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            let normal_act = ctx
                .start_activity(
                    DelayWithCancellation::delay,
                    Duration::from_secs(9),
                    ActivityOptions {
                        cancellation_type: ActivityCancellationType::TryCancel,
                        start_to_close_timeout: Some(Duration::from_secs(9000)),
                        ..Default::default()
                    },
                )
                .unwrap();
            // Make new task
            ctx.timer(Duration::from_millis(1)).await;

            // Start LA and cancel the activity at the same time
            let local_act = ctx.start_local_activity(
                DelayWithCancellation::delay,
                Duration::from_millis(100),
                LocalActivityOptions {
                    ..Default::default()
                },
            )?;
            normal_act.cancel(ctx);
            // Race them, starting a timer if LA completes first
            tokio::select! {
                biased;
                _ = normal_act => {},
                _ = local_act => {
                    ctx.timer(Duration::from_millis(1)).await;
                },
            }
            Ok(().into())
        }
    }

    let mut worker = starter.worker().await;
    worker.register_workflow::<LaResolveSameTimeAsOtherCancelWf>();

    let handle = worker
        .submit_workflow(
            LaResolveSameTimeAsOtherCancelWf::run,
            wf_name,
            (),
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let client = starter.get_client().await;
    let handle = client.get_untyped_workflow_handle(wf_name, handle.run_id().unwrap());
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
    starter.sdk_config.register_activities(StdActivities);

    #[workflow]
    struct LongLocalActivityWithUpdateWf {
        update_counter: Arc<AtomicUsize>,
    }

    #[workflow_methods(factory_only)]
    impl LongLocalActivityWithUpdateWf {
        #[run]
        async fn run(
            &mut self,
            ctx: &mut WorkflowContext,
            update_inner_timer: u64,
        ) -> WorkflowResult<usize> {
            let uc = self.update_counter.clone();
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
            ctx.start_local_activity(
                StdActivities::delay,
                Duration::from_secs(6),
                LocalActivityOptions::default(),
            )?
            .await;
            Ok(self.update_counter.load(Ordering::Relaxed).into())
        }
    }

    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    worker.register_workflow_with_factory(move || LongLocalActivityWithUpdateWf {
        update_counter: Arc::new(AtomicUsize::new(1)),
    });

    let handle = worker
        .submit_workflow(
            LongLocalActivityWithUpdateWf::run,
            &wf_name,
            update_inner_timer,
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

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
        .unwrap();
    let replay_res = handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
    assert_eq!(
        res.unwrap_success(),
        usize::from_json_payload(&replay_res.unwrap()).unwrap()
    );

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
    inner_worker.with_new_core_worker(Arc::new(replay_worker));
    inner_worker.set_worker_interceptor(FailOnNondeterminismInterceptor {});
    inner_worker.run().await.unwrap();
}

#[tokio::test]
async fn local_activity_with_heartbeat_only_causes_one_wakeup() {
    let wf_name = "local_activity_with_heartbeat_only_causes_one_wakeup";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    starter.sdk_config.register_activities(StdActivities);

    #[workflow]
    #[derive(Default)]
    struct LocalActivityWithHeartbeatOnlyCausesOneWakeupWf;

    #[workflow_methods]
    impl LocalActivityWithHeartbeatOnlyCausesOneWakeupWf {
        #[run]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            let mut wakeup_counter = 1;
            let la_resolved = AtomicBool::new(false);
            tokio::join!(
                async {
                    ctx.start_local_activity(
                        StdActivities::delay,
                        Duration::from_secs(6),
                        LocalActivityOptions::default(),
                    )
                    .unwrap()
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
        }
    }

    let mut worker = starter.worker().await;
    worker.register_workflow::<LocalActivityWithHeartbeatOnlyCausesOneWakeupWf>();

    let handle = worker
        .submit_workflow(
            LocalActivityWithHeartbeatOnlyCausesOneWakeupWf::run,
            wf_name,
            (),
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    handle
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}

#[workflow]
#[derive(Default)]
pub(crate) struct LocalActivityWithSummaryWf;

#[workflow_methods]
impl LocalActivityWithSummaryWf {
    #[run(name = "local_activity_with_summary")]
    pub(crate) async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        ctx.start_local_activity(
            StdActivities::echo,
            "hi".to_string(),
            LocalActivityOptions {
                summary: Some("Echo summary".to_string()),
                ..Default::default()
            },
        )?
        .await;
        Ok(().into())
    }
}

#[tokio::test]
async fn local_activity_with_summary() {
    let wf_name = "local_activity_with_summary";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    worker.register_workflow::<LocalActivityWithSummaryWf>();

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

    #[workflow]
    #[derive(Default)]
    struct LocalActTwoWftsBeforeMarkerWf;

    #[workflow_methods]
    impl LocalActTwoWftsBeforeMarkerWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            let la = ctx.start_local_activity(StdActivities::default, (), Default::default())?;
            ctx.timer(Duration::from_secs(1)).await;
            la.await;
            Ok(().into())
        }
    }

    worker.register_workflow::<LocalActTwoWftsBeforeMarkerWf>();
    worker.register_activities(StdActivities);
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

    worker.register_workflow::<LocalActFanoutWf>();
    worker.register_activities(StdActivities);
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
    let core = worker.core_worker();

    let shutdown_barr = Arc::new(Barrier::new(2));

    #[workflow]
    #[derive(Default)]
    struct LocalActHeartbeatWf;

    #[workflow_methods]
    impl LocalActHeartbeatWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            dbg!("dafuq");
            ctx.start_local_activity(
                EchoWithConditionalBarrier::echo,
                "hi".to_string(),
                LocalActivityOptions::default(),
            )?
            .await;
            Ok(().into())
        }
    }

    worker.register_workflow::<LocalActHeartbeatWf>();

    struct EchoWithConditionalBarrier {
        shutdown_barr: Option<Arc<Barrier>>,
        wft_timeout: Duration,
    }
    #[activities]
    impl EchoWithConditionalBarrier {
        #[activity]
        async fn echo(
            self: Arc<Self>,
            _: ActivityContext,
            str: String,
        ) -> Result<String, ActivityError> {
            dbg!("Running activity");
            if let Some(barr) = &self.shutdown_barr {
                barr.wait().await;
            }
            // Take slightly more than two workflow tasks
            tokio::time::sleep(self.wft_timeout.mul_f32(2.2)).await;
            Ok(str)
        }
    }

    worker.register_activities(EchoWithConditionalBarrier {
        shutdown_barr: if shutdown_middle {
            Some(shutdown_barr.clone())
        } else {
            None
        },
        wft_timeout,
    });
    let (_, runres) = tokio::join!(
        async {
            if shutdown_middle {
                shutdown_barr.wait().await;
                dbg!("Past barrier");
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
    t.set_wf_input(eventually_pass.as_json_payload().unwrap());
    t.add_workflow_task_scheduled_and_started();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [1], mock);
    let mut worker = mock_sdk(mh);

    #[workflow]
    #[derive(Default)]
    struct LocalActFailAndRetryWf;

    #[workflow_methods]
    impl LocalActFailAndRetryWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(
            &mut self,
            ctx: &mut WorkflowContext,
            eventually_pass: bool,
        ) -> WorkflowResult<()> {
            let la_res = ctx
                .start_local_activity(
                    EventuallyPassingActivity::echo,
                    "hi".to_string(),
                    LocalActivityOptions {
                        retry_policy: RetryPolicy {
                            initial_interval: Some(prost_dur!(from_millis(50))),
                            backoff_coefficient: 1.2,
                            maximum_interval: None,
                            maximum_attempts: 5,
                            non_retryable_error_types: vec![],
                        },
                        ..Default::default()
                    },
                )?
                .await;
            if eventually_pass {
                assert!(la_res.completed_ok())
            } else {
                assert!(la_res.failed())
            }
            Ok(().into())
        }
    }

    worker.register_workflow::<LocalActFailAndRetryWf>();
    let attempts = Arc::new(AtomicUsize::new(0));

    struct EventuallyPassingActivity {
        attempts: Arc<AtomicUsize>,
        eventually_pass: bool,
    }
    #[activities]
    impl EventuallyPassingActivity {
        #[activity]
        async fn echo(self: Arc<Self>, _: ActivityContext, _: String) -> Result<(), ActivityError> {
            // Succeed on 3rd attempt (which is ==2 since fetch_add returns prev val)
            if 2 == self.attempts.fetch_add(1, Ordering::Relaxed) && self.eventually_pass {
                Ok(())
            } else {
                Err(anyhow!("Oh no I failed!").into())
            }
        }
    }

    worker.register_activities(EventuallyPassingActivity {
        attempts: attempts.clone(),
        eventually_pass,
    });
    worker.run_until_done().await.unwrap();
    let expected_attempts = if eventually_pass { 3 } else { 5 };
    assert_eq!(expected_attempts, attempts.load(Ordering::Relaxed));
}

#[tokio::test]
async fn local_act_retry_long_backoff_uses_timer() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_local_activity_marker(
        1,
        "1",
        None,
        Some(Failure::application_failure("la failed".to_string(), false)),
        |m| m.activity_type = StdActivities::always_fail.name().to_owned(),
    );
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.add_local_activity_marker(
        2,
        "2",
        None,
        Some(Failure::application_failure("la failed".to_string(), false)),
        |m| m.activity_type = StdActivities::always_fail.name().to_owned(),
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

    #[workflow]
    #[derive(Default)]
    struct LocalActRetryLongBackoffUsesTimerWf;

    #[workflow_methods]
    impl LocalActRetryLongBackoffUsesTimerWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            let la_res = ctx
                .start_local_activity(
                    StdActivities::always_fail,
                    (),
                    LocalActivityOptions {
                        retry_policy: RetryPolicy {
                            initial_interval: Some(prost_dur!(from_millis(65))),
                            backoff_coefficient: 1_000.,
                            maximum_interval: Some(prost_dur!(from_secs(600))),
                            maximum_attempts: 3,
                            non_retryable_error_types: vec![],
                        },
                        ..Default::default()
                    },
                )?
                .await;
            assert!(la_res.failed());
            ctx.timer(Duration::from_secs(1)).await;
            Ok(().into())
        }
    }

    worker.register_workflow::<LocalActRetryLongBackoffUsesTimerWf>();
    worker.register_activities(StdActivities);
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn local_act_null_result() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_local_activity_marker(1, "1", None, None, |m| {
        m.activity_type = StdActivities::no_op.name().to_owned()
    });
    t.add_workflow_execution_completed();

    let wf_id = "fakeid";
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::AllHistory], mock);
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 1);

    #[workflow]
    #[derive(Default)]
    struct LocalActNullResultWf;

    #[workflow_methods]
    impl LocalActNullResultWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            ctx.start_local_activity(StdActivities::no_op, (), LocalActivityOptions::default())?
                .await;
            Ok(().into())
        }
    }

    worker.register_workflow::<LocalActNullResultWf>();
    worker.register_activities(StdActivities);
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
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [3], mock);
    let mut worker = mock_sdk_cfg(mh, |w| w.max_cached_workflows = 0);

    #[workflow]
    #[derive(Default)]
    struct LocalActCommandImmediatelyFollowsLaMarkerWf;

    #[workflow_methods]
    impl LocalActCommandImmediatelyFollowsLaMarkerWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            ctx.start_local_activity(StdActivities::no_op, (), LocalActivityOptions::default())?
                .await;
            ctx.timer(Duration::from_secs(1)).await;
            Ok(().into())
        }
    }

    worker.register_workflow::<LocalActCommandImmediatelyFollowsLaMarkerWf>();
    worker.register_activities(StdActivities);
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

    #[workflow]
    #[derive(Default)]
    struct TestScheduleToStartTimeoutWf;

    #[workflow_methods]
    impl TestScheduleToStartTimeoutWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            let la_res = ctx
                .start_local_activity(
                    StdActivities::echo,
                    "hi".to_string(),
                    LocalActivityOptions {
                        schedule_to_start_timeout: prost_dur!(from_nanos(1)),
                        ..Default::default()
                    },
                )?
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
        }
    }

    worker.register_workflow::<TestScheduleToStartTimeoutWf>();
    worker.register_activities(StdActivities);
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

    let schedule_to_close_timeout = Some(if is_sched_to_start {
        Duration::from_secs(60 * 60)
    } else {
        Duration::from_secs(10 * 60)
    });

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.set_wf_input(
        (is_sched_to_start, schedule_to_close_timeout)
            .as_json_payload()
            .unwrap(),
    );
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

    #[workflow]
    #[derive(Default)]
    struct TestScheduleToStartTimeoutNotBasedOnOriginalTimeWf;

    #[workflow_methods]
    impl TestScheduleToStartTimeoutNotBasedOnOriginalTimeWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(
            &mut self,
            ctx: &mut WorkflowContext,
            input: (bool, Option<Duration>),
        ) -> WorkflowResult<()> {
            let (is_sched_to_start, schedule_to_close_timeout) = input;
            let la_res = ctx
                .start_local_activity(
                    StdActivities::echo,
                    "hi".to_string(),
                    LocalActivityOptions {
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
                    },
                )?
                .await;
            if is_sched_to_start {
                assert!(la_res.completed_ok());
            } else {
                assert_eq!(la_res.timed_out(), Some(TimeoutType::ScheduleToClose));
            }
            Ok(().into())
        }
    }

    worker.register_workflow::<TestScheduleToStartTimeoutNotBasedOnOriginalTimeWf>();
    worker.register_activities(StdActivities);
    worker.run_until_done().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn start_to_close_timeout_allows_retries(#[values(true, false)] la_completes: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.set_wf_input(la_completes.as_json_payload().unwrap());
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

    #[workflow]
    #[derive(Default)]
    struct StartToCloseTimeoutAllowsRetriesWf;

    #[workflow_methods]
    impl StartToCloseTimeoutAllowsRetriesWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(
            &mut self,
            ctx: &mut WorkflowContext,
            la_completes: bool,
        ) -> WorkflowResult<()> {
            let la_res = ctx
                .start_local_activity(
                    ActivityWithRetriesAndCancellation::go,
                    (),
                    LocalActivityOptions {
                        retry_policy: RetryPolicy {
                            initial_interval: Some(prost_dur!(from_millis(20))),
                            backoff_coefficient: 1.0,
                            maximum_interval: None,
                            maximum_attempts: 5,
                            non_retryable_error_types: vec![],
                        },
                        start_to_close_timeout: Some(prost_dur!(from_millis(25))),
                        ..Default::default()
                    },
                )?
                .await;
            if la_completes {
                assert!(la_res.completed_ok());
            } else {
                assert_eq!(la_res.timed_out(), Some(TimeoutType::StartToClose));
            }
            Ok(().into())
        }
    }

    worker.register_workflow::<StartToCloseTimeoutAllowsRetriesWf>();
    let attempts = Arc::new(AtomicUsize::new(0));
    let cancels = Arc::new(AtomicUsize::new(0));

    struct ActivityWithRetriesAndCancellation {
        attempts: Arc<AtomicUsize>,
        cancels: Arc<AtomicUsize>,
        la_completes: bool,
    }
    #[activities]
    impl ActivityWithRetriesAndCancellation {
        #[activity(name = DEFAULT_ACTIVITY_TYPE)]
        async fn go(self: Arc<Self>, ctx: ActivityContext) -> Result<(), ActivityError> {
            // Timeout the first 4 attempts, or all of them if we intend to fail
            if self.attempts.fetch_add(1, Ordering::AcqRel) < 4 || !self.la_completes {
                select! {
                    _ = tokio::time::sleep(Duration::from_millis(100)) => (),
                    _ = ctx.cancelled() => {
                        self.cancels.fetch_add(1, Ordering::AcqRel);
                        return Err(ActivityError::cancelled());
                    }
                }
            }
            Ok(())
        }
    }

    worker.register_activities(ActivityWithRetriesAndCancellation {
        attempts: attempts.clone(),
        cancels: cancels.clone(),
        la_completes,
    });
    worker.run_until_done().await.unwrap();
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

    #[workflow]
    #[derive(Default)]
    struct WftFailureCancelsRunningLasWf;

    #[workflow_methods]
    impl WftFailureCancelsRunningLasWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            let la_handle = ctx.start_local_activity(
                ActivityThatExpectsCancellation::go,
                (),
                Default::default(),
            )?;
            tokio::join!(
                async {
                    ctx.timer(Duration::from_secs(1)).await;
                    panic!("ahhh I'm failing wft")
                },
                la_handle
            );
            Ok(().into())
        }
    }

    worker.register_workflow::<WftFailureCancelsRunningLasWf>();

    struct ActivityThatExpectsCancellation;
    #[activities]
    impl ActivityThatExpectsCancellation {
        #[activity]
        async fn go(ctx: ActivityContext) -> Result<(), ActivityError> {
            let res = tokio::time::timeout(Duration::from_millis(500), ctx.cancelled()).await;
            if res.is_err() {
                panic!("Activity must be cancelled!!!!");
            }
            Err(ActivityError::cancelled())
        }
    }

    worker.register_activities(ActivityThatExpectsCancellation);
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

    #[workflow]
    #[derive(Default)]
    struct ResolvedLasNotRecordedIfWftFailsManyTimesWf;

    #[workflow_methods]
    impl ResolvedLasNotRecordedIfWftFailsManyTimesWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        #[allow(unreachable_code)]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            ctx.start_local_activity(
                StdActivities::echo,
                "hi".to_string(),
                LocalActivityOptions {
                    ..Default::default()
                },
            )?
            .await;
            panic!()
        }
    }

    worker.register_workflow::<ResolvedLasNotRecordedIfWftFailsManyTimesWf>();
    worker.register_activities(StdActivities);
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

    #[workflow]
    #[derive(Default)]
    struct LocalActRecordsNonfirstAttemptsOkWf;

    #[workflow_methods]
    impl LocalActRecordsNonfirstAttemptsOkWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            ctx.start_local_activity(
                StdActivities::always_fail,
                (),
                LocalActivityOptions {
                    retry_policy: RetryPolicy {
                        initial_interval: Some(prost_dur!(from_millis(10))),
                        backoff_coefficient: 1.0,
                        maximum_interval: None,
                        maximum_attempts: 0,
                        non_retryable_error_types: vec![],
                    },
                    ..Default::default()
                },
            )?
            .await;
            Ok(().into())
        }
    }

    worker.register_workflow::<LocalActRecordsNonfirstAttemptsOkWf>();
    worker.register_activities(StdActivities);
    worker.run_until_done().await.unwrap();
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

    #[workflow]
    #[derive(Default)]
    struct LocalActRetryExplicitDelayWf;

    #[workflow_methods]
    impl LocalActRetryExplicitDelayWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            let la_res = ctx
                .start_local_activity(
                    ActivityWithExplicitBackoff::go,
                    (),
                    LocalActivityOptions {
                        retry_policy: RetryPolicy {
                            initial_interval: Some(prost_dur!(from_millis(50))),
                            backoff_coefficient: 1.0,
                            maximum_attempts: 5,
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )?
                .await;
            assert!(la_res.completed_ok());
            Ok(().into())
        }
    }

    worker.register_workflow::<LocalActRetryExplicitDelayWf>();
    let attempts = Arc::new(AtomicUsize::new(0));

    struct ActivityWithExplicitBackoff {
        attempts: Arc<AtomicUsize>,
    }
    #[activities]
    impl ActivityWithExplicitBackoff {
        #[activity]
        async fn go(self: Arc<Self>, _: ActivityContext) -> Result<(), ActivityError> {
            // Succeed on 3rd attempt (which is ==2 since fetch_add returns prev val)
            let last_attempt = self.attempts.fetch_add(1, Ordering::Relaxed);
            if 0 == last_attempt {
                Err(ActivityError::Retryable {
                    source: anyhow!("Explicit backoff error").into_boxed_dyn_error(),
                    explicit_delay: Some(Duration::from_millis(300)),
                })
            } else if 2 == last_attempt {
                Ok(())
            } else {
                Err(anyhow!("Oh no I failed!").into())
            }
        }
    }

    worker.register_activities(ActivityWithExplicitBackoff {
        attempts: attempts.clone(),
    });
    let start = Instant::now();
    worker.run_until_done().await.unwrap();
    let expected_attempts = 3;
    assert_eq!(expected_attempts, attempts.load(Ordering::Relaxed));
    assert!(start.elapsed() > Duration::from_millis(350));
}

#[workflow]
#[derive(Default)]
struct LaWf;

#[workflow_methods]
impl LaWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        ctx.start_local_activity(
            StdActivities::default,
            (),
            LocalActivityOptions {
                retry_policy: RetryPolicy {
                    maximum_attempts: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
        )?
        .await;
        Ok(().into())
    }
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
    worker.register_workflow::<LaWf>();

    struct ActivityWithReplayCheck {
        replay: bool,
        completes_ok: bool,
    }
    #[activities]
    impl ActivityWithReplayCheck {
        #[activity(name = DEFAULT_ACTIVITY_TYPE)]
        #[allow(unused)]
        async fn echo(
            self: Arc<Self>,
            _: ActivityContext,
            _: (),
        ) -> Result<&'static str, ActivityError> {
            if self.replay {
                panic!("Should not be invoked on replay");
            }
            if self.completes_ok {
                Ok("hi")
            } else {
                Err(anyhow!("Oh no I failed!").into())
            }
        }
    }

    worker.register_activities(ActivityWithReplayCheck {
        replay,
        completes_ok,
    });
    worker.run().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct TwoLaWf;

#[workflow_methods]
impl TwoLaWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        ctx.start_local_activity(StdActivities::default, (), LocalActivityOptions::default())?
            .await;
        ctx.start_local_activity(StdActivities::default, (), LocalActivityOptions::default())?
            .await;
        Ok(().into())
    }
}

#[workflow]
#[derive(Default)]
struct TwoLaWfParallel;

#[workflow_methods]
impl TwoLaWfParallel {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        tokio::join!(
            ctx.start_local_activity(StdActivities::default, (), LocalActivityOptions::default())?,
            ctx.start_local_activity(StdActivities::default, (), LocalActivityOptions::default())?
        );
        Ok(().into())
    }
}

struct ResolvedActivity;
#[activities]
impl ResolvedActivity {
    #[allow(unused)]
    #[activity(name = DEFAULT_ACTIVITY_TYPE)]
    async fn echo(_: ActivityContext, _: ()) -> Result<&'static str, ActivityError> {
        Ok("Resolved")
    }
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
        worker.register_workflow::<TwoLaWfParallel>();
    } else {
        worker.register_workflow::<TwoLaWf>();
    }
    worker.register_activities(ResolvedActivity);
    worker.run().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct LaTimerLaWf;

#[workflow_methods]
impl LaTimerLaWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        ctx.start_local_activity(StdActivities::default, (), LocalActivityOptions::default())?
            .await;
        ctx.timer(Duration::from_secs(5)).await;
        ctx.start_local_activity(StdActivities::default, (), LocalActivityOptions::default())?
            .await;
        Ok(().into())
    }
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
    worker.register_workflow::<LaTimerLaWf>();
    worker.register_activities(ResolvedActivity);
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
    worker.register_workflow::<LaWf>();
    worker.register_activities(ResolvedActivity);
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
    t.set_wf_input(cancel_type.as_json_payload().unwrap());
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

    #[workflow]
    #[derive(Default)]
    struct CancelBeforeActStartsWf;

    #[workflow_methods]
    impl CancelBeforeActStartsWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(
            &mut self,
            ctx: &mut WorkflowContext,
            cancel_type: ActivityCancellationType,
        ) -> WorkflowResult<()> {
            let la = ctx.start_local_activity(
                StdActivities::default,
                (),
                LocalActivityOptions {
                    cancel_type,
                    ..Default::default()
                },
            )?;
            la.cancel(ctx);
            la.await;
            Ok(().into())
        }
    }

    worker.register_workflow::<CancelBeforeActStartsWf>();
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
    t.set_wf_input(cancel_type.as_json_payload().unwrap());
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

    #[workflow]
    #[derive(Default)]
    struct CancelAfterActStartsCannedWf;

    #[workflow_methods]
    impl CancelAfterActStartsCannedWf {
        #[run(name = DEFAULT_WORKFLOW_TYPE)]
        async fn run(
            &mut self,
            ctx: &mut WorkflowContext,
            cancel_type: ActivityCancellationType,
        ) -> WorkflowResult<()> {
            let la = ctx.start_local_activity(
                ActivityWithConditionalCancelWait::echo,
                (),
                LocalActivityOptions {
                    cancel_type,
                    ..Default::default()
                },
            )?;
            ctx.timer(Duration::from_secs(1)).await;
            la.cancel(ctx);
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
        }
    }

    worker.register_workflow::<CancelAfterActStartsCannedWf>();

    struct ActivityWithConditionalCancelWait {
        cancel_type: ActivityCancellationType,
        allow_cancel_barr: CancellationToken,
    }
    #[activities]
    impl ActivityWithConditionalCancelWait {
        #[activity(name = DEFAULT_ACTIVITY_TYPE)]
        async fn echo(self: Arc<Self>, ctx: ActivityContext, _: ()) -> Result<(), ActivityError> {
            if self.cancel_type == ActivityCancellationType::WaitCancellationCompleted {
                ctx.cancelled().await;
            }
            self.allow_cancel_barr.cancelled().await;
            Err(ActivityError::cancelled())
        }
    }

    worker.register_activities(ActivityWithConditionalCancelWait {
        cancel_type,
        allow_cancel_barr: allow_cancel_barr_clone,
    });
    worker.run().await.unwrap();
}
