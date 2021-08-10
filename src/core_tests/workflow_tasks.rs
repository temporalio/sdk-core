use crate::test_help::poll_and_reply_clears_outstanding_evicts;
use crate::{
    errors::PollWfError,
    job_assert,
    pollers::MockServerGatewayApis,
    protos::{
        coresdk::{
            activity_result::{self as ar, activity_result, ActivityResult},
            workflow_activation::{
                wf_activation_job, FireTimer, ResolveActivity, StartWorkflow, UpdateRandomSeed,
                WfActivationJob,
            },
            workflow_commands::{
                ActivityCancellationType, CancelTimer, CompleteWorkflowExecution,
                FailWorkflowExecution, RequestCancelActivity, ScheduleActivity, StartTimer,
            },
        },
        temporal::api::{
            enums::v1::EventType,
            failure::v1::Failure,
            history::v1::{history_event, TimerFiredEventAttributes},
            workflowservice::v1::RespondWorkflowTaskCompletedResponse,
        },
    },
    test_help::{
        build_fake_core, build_multihist_mock_sg, canned_histories, gen_assert_and_fail,
        gen_assert_and_reply, hist_to_poll_resp, mock_core, mock_core_with_opts_no_workers,
        poll_and_reply, single_hist_mock_sg, FakeWfResponses, MocksHolder, ResponseType,
        TestHistoryBuilder, TEST_Q,
    },
    workflow::WorkflowCachingPolicy::{self, AfterEveryReply, NonSticky},
    Core, CoreInitOptionsBuilder, CoreSDK, ServerGatewayApis, WfActivationCompletion,
    WorkerConfigBuilder,
};
use rstest::{fixture, rstest};
use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Duration,
};
use test_utils::fanout_tasks;

#[fixture(hist_batches = &[])]
fn single_timer_setup(
    hist_batches: &'static [usize],
) -> CoreSDK<impl ServerGatewayApis + Send + Sync + 'static> {
    let wfid = "fake_wf_id";

    let t = canned_histories::single_timer("fake_timer");
    build_fake_core(wfid, t, hist_batches)
}

#[fixture(hist_batches = &[])]
fn single_activity_setup(
    hist_batches: &'static [usize],
) -> CoreSDK<impl ServerGatewayApis + Send + Sync + 'static> {
    let wfid = "fake_wf_id";

    let t = canned_histories::single_activity("fake_activity");
    build_fake_core(wfid, t, hist_batches)
}

#[fixture(hist_batches = &[])]
fn single_activity_failure_setup(
    hist_batches: &'static [usize],
) -> CoreSDK<impl ServerGatewayApis + Send + Sync + 'static> {
    let wfid = "fake_wf_id";

    let t = canned_histories::single_failed_activity("fake_activity");
    build_fake_core(wfid, t, hist_batches)
}

#[rstest]
#[case::incremental(single_timer_setup(&[1, 2]), NonSticky)]
#[case::replay(single_timer_setup(&[2]), NonSticky)]
#[case::incremental_evict(single_timer_setup(&[1, 2]), AfterEveryReply)]
#[case::replay_evict(single_timer_setup(&[2, 2]), AfterEveryReply)]
#[tokio::test]
async fn single_timer(
    #[case] core: CoreSDK<impl ServerGatewayApis + Send + Sync + 'static>,
    #[case] evict: WorkflowCachingPolicy,
) {
    poll_and_reply(
        &core,
        evict,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![StartTimer {
                    timer_id: "fake_timer".to_string(),
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::FireTimer(_)),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[rstest(core,
case::incremental(single_activity_setup(&[1, 2])),
case::incremental_activity_failure(single_activity_failure_setup(&[1, 2])),
case::replay(single_activity_setup(&[2])),
case::replay_activity_failure(single_activity_failure_setup(&[2]))
)]
#[tokio::test]
async fn single_activity_completion(core: CoreSDK<impl ServerGatewayApis + Send + Sync + 'static>) {
    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    activity_id: "fake_activity".to_string(),
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::ResolveActivity(_)),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn parallel_timer_test_across_wf_bridge(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let timer_1_id = "timer1";
    let timer_2_id = "timer2";

    let t = canned_histories::parallel_timer(timer_1_id, timer_2_id);
    let core = build_fake_core(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![
                    StartTimer {
                        timer_id: timer_1_id.to_string(),
                        ..Default::default()
                    }
                    .into(),
                    StartTimer {
                        timer_id: timer_2_id.to_string(),
                        ..Default::default()
                    }
                    .into(),
                ],
            ),
            gen_assert_and_reply(
                &|res| {
                    assert_matches!(
                        res.jobs.as_slice(),
                        [
                            WfActivationJob {
                                variant: Some(wf_activation_job::Variant::FireTimer(
                                    FireTimer { timer_id: t1_id }
                                )),
                            },
                            WfActivationJob {
                                variant: Some(wf_activation_job::Variant::FireTimer(
                                    FireTimer { timer_id: t2_id }
                                )),
                            }
                        ] => {
                            assert_eq!(t1_id, &timer_1_id);
                            assert_eq!(t2_id, &timer_2_id);
                        }
                    );
                },
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn timer_cancel(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let timer_id = "wait_timer";
    let cancel_timer_id = "cancel_timer";

    let t = canned_histories::cancel_timer(timer_id, cancel_timer_id);
    let core = build_fake_core(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![
                    StartTimer {
                        timer_id: cancel_timer_id.to_string(),
                        ..Default::default()
                    }
                    .into(),
                    StartTimer {
                        timer_id: timer_id.to_string(),
                        ..Default::default()
                    }
                    .into(),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::FireTimer(_)),
                vec![
                    CancelTimer {
                        timer_id: cancel_timer_id.to_string(),
                    }
                    .into(),
                    CompleteWorkflowExecution { result: None }.into(),
                ],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn scheduled_activity_cancellation_try_cancel(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::cancel_scheduled_activity(activity_id, signal_id);
    let core = build_fake_core(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    activity_id: activity_id.to_string(),
                    cancellation_type: ActivityCancellationType::TryCancel as i32,
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::SignalWorkflow(_)),
                vec![RequestCancelActivity {
                    activity_id: activity_id.to_string(),
                }
                .into()],
            ),
            // Activity is getting resolved right away as we are in the TryCancel mode.
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::ResolveActivity(_)),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn scheduled_activity_timeout(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";

    let t = canned_histories::scheduled_activity_timeout(activity_id);
    let core = build_fake_core(wfid, t, hist_batches);
    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    activity_id: activity_id.to_string(),
                    ..Default::default()
                }
                    .into()],
            ),
            // Activity is getting resolved right away as it has been timed out.
            gen_assert_and_reply(
                &|res| {
                    assert_matches!(
                    res.jobs.as_slice(),
                    [
                        WfActivationJob {
                            variant: Some(wf_activation_job::Variant::ResolveActivity(
                                ResolveActivity {
                                    activity_id: aid,
                                    result: Some(ActivityResult {
                                        status: Some(activity_result::Status::Failed(ar::Failure {
                                            failure: Some(failure)
                                        })),
                                    })
                                }
                            )),
                        }
                    ] => {
                        assert_eq!(failure.message, "Activity task timed out".to_string());
                        assert_eq!(aid, &activity_id.to_string());
                    }
                );
                },
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
        .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn started_activity_timeout(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";

    let t = canned_histories::started_activity_timeout(activity_id);
    let core = build_fake_core(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    activity_id: activity_id.to_string(),
                    ..Default::default()
                }
                    .into()],
            ),
            // Activity is getting resolved right away as it has been timed out.
            gen_assert_and_reply(
                &|res| {
                    assert_matches!(
                                res.jobs.as_slice(),
                                [
                                    WfActivationJob {
                                        variant: Some(wf_activation_job::Variant::ResolveActivity(
                                            ResolveActivity {
                                                activity_id: aid,
                                                result: Some(ActivityResult {
                                                    status: Some(activity_result::Status::Failed(ar::Failure {
                                                        failure: Some(failure)
                                                    })),
                                                })
                                            }
                                        )),
                                    }
                                ] => {
                                    assert_eq!(failure.message, "Activity task timed out".to_string());
                                    assert_eq!(aid, &activity_id.to_string());
                                }
                            );
                },
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
        .await;
}

#[rstest(hist_batches, case::incremental(&[1, 3]), case::replay(&[3]))]
#[tokio::test]
async fn cancelled_activity_timeout(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::scheduled_cancelled_activity_timeout(activity_id, signal_id);
    let core = build_fake_core(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    activity_id: activity_id.to_string(),
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::SignalWorkflow(_)),
                vec![RequestCancelActivity {
                    activity_id: activity_id.to_string(),
                }
                .into()],
            ),
            // Activity is resolved right away as it has timed out.
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        activity_id: _,
                        result: Some(ActivityResult {
                            status: Some(activity_result::Status::Canceled(..)),
                        })
                    }
                )),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn scheduled_activity_cancellation_abandon(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::cancel_scheduled_activity_abandon(activity_id, signal_id);
    let core = build_fake_core(wfid, t, hist_batches);

    verify_activity_cancellation(&core, activity_id, ActivityCancellationType::Abandon).await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn started_activity_cancellation_abandon(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::cancel_started_activity_abandon(activity_id, signal_id);
    let core = build_fake_core(wfid, t, hist_batches);

    verify_activity_cancellation(&core, activity_id, ActivityCancellationType::Abandon).await;
}

#[rstest(hist_batches, case::incremental(&[1, 3]), case::replay(&[3]))]
#[tokio::test]
async fn scheduled_activity_cancellation_try_cancel_task_canceled(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::cancel_scheduled_activity_with_activity_task_cancel(
        activity_id,
        signal_id,
    );
    let core = build_fake_core(wfid, t, hist_batches);

    verify_activity_cancellation(&core, activity_id, ActivityCancellationType::TryCancel).await;
}

#[rstest(hist_batches, case::incremental(&[1, 3]), case::replay(&[3]))]
#[tokio::test]
async fn started_activity_cancellation_try_cancel_task_canceled(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t =
        canned_histories::cancel_started_activity_with_activity_task_cancel(activity_id, signal_id);
    let core = build_fake_core(wfid, t, hist_batches);

    verify_activity_cancellation(&core, activity_id, ActivityCancellationType::TryCancel).await;
}

/// Verification for try cancel & abandon histories
async fn verify_activity_cancellation(
    core: &CoreSDK<impl ServerGatewayApis + Send + Sync + 'static>,
    activity_id: &str,
    cancel_type: ActivityCancellationType,
) {
    poll_and_reply(
        core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    activity_id: activity_id.to_string(),
                    cancellation_type: cancel_type as i32,
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::SignalWorkflow(_)),
                vec![RequestCancelActivity {
                    activity_id: activity_id.to_string(),
                }
                .into()],
            ),
            // Activity should be resolved right away
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        activity_id: _,
                        result: Some(ActivityResult {
                            status: Some(activity_result::Status::Canceled(..)),
                        })
                    }
                )),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2, 3, 4]), case::replay(&[4]))]
#[tokio::test]
async fn scheduled_activity_cancellation_wait_for_cancellation(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::cancel_scheduled_activity_with_signal_and_activity_task_cancel(
        activity_id,
        signal_id,
    );
    let core = build_fake_core(wfid, t, hist_batches);

    verify_activity_cancellation_wait_for_cancellation(activity_id, &core).await;
}

#[rstest(hist_batches, case::incremental(&[1, 2, 3, 4]), case::replay(&[4]))]
#[tokio::test]
async fn started_activity_cancellation_wait_for_cancellation(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::cancel_started_activity_with_signal_and_activity_task_cancel(
        activity_id,
        signal_id,
    );
    let core = build_fake_core(wfid, t, hist_batches);

    verify_activity_cancellation_wait_for_cancellation(activity_id, &core).await;
}

async fn verify_activity_cancellation_wait_for_cancellation(
    activity_id: &str,
    core: &CoreSDK<impl ServerGatewayApis + Send + Sync + 'static>,
) {
    poll_and_reply(
        core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    activity_id: activity_id.to_string(),
                    cancellation_type: ActivityCancellationType::WaitCancellationCompleted as i32,
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::SignalWorkflow(_)),
                vec![RequestCancelActivity {
                    activity_id: activity_id.to_string(),
                }
                .into()],
            ),
            // Making sure that activity is not resolved until it's cancelled.
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::SignalWorkflow(_)),
                vec![],
            ),
            // Now ActivityTaskCanceled has been processed and activity can be resolved.
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        activity_id: _,
                        result: Some(ActivityResult {
                            status: Some(activity_result::Status::Canceled(..)),
                        })
                    }
                )),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn workflow_update_random_seed_on_workflow_reset() {
    let wfid = "fake_wf_id";
    let new_run_id = "86E39A5F-AE31-4626-BDFE-398EE072D156";
    let timer_1_id = "timer1";
    let randomness_seed_from_start = AtomicU64::new(0);

    let t = canned_histories::workflow_fails_with_reset_after_timer(timer_1_id, new_run_id);
    let core = build_fake_core(wfid, t, &[2]);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &|res| {
                    assert_matches!(
                        res.jobs.as_slice(),
                        [WfActivationJob {
                            variant: Some(wf_activation_job::Variant::StartWorkflow(
                            StartWorkflow{randomness_seed, ..}
                            )),
                        }] => {
                        randomness_seed_from_start.store(*randomness_seed, Ordering::SeqCst);
                        }
                    );
                },
                vec![StartTimer {
                    timer_id: timer_1_id.to_string(),
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &|res| {
                    assert_matches!(
                        res.jobs.as_slice(),
                        [WfActivationJob {
                            variant: Some(wf_activation_job::Variant::FireTimer(_),),
                        },
                        WfActivationJob {
                            variant: Some(wf_activation_job::Variant::UpdateRandomSeed(
                                UpdateRandomSeed{randomness_seed})),
                        }] => {
                            assert_ne!(randomness_seed_from_start.load(Ordering::SeqCst),
                                      *randomness_seed)
                        }
                    )
                },
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn cancel_timer_before_sent_wf_bridge() {
    let wfid = "fake_wf_id";
    let cancel_timer_id = "cancel_timer";

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let core = build_fake_core(wfid, t, &[1]);

    poll_and_reply(
        &core,
        NonSticky,
        &[gen_assert_and_reply(
            &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
            vec![
                StartTimer {
                    timer_id: cancel_timer_id.to_string(),
                    ..Default::default()
                }
                .into(),
                CancelTimer {
                    timer_id: cancel_timer_id.to_string(),
                }
                .into(),
                CompleteWorkflowExecution { result: None }.into(),
            ],
        )],
    )
    .await;
}

#[rstest]
#[case::no_evict_inc(&[1, 2, 2], NonSticky)]
#[case::no_evict(&[2, 2], NonSticky)]
#[tokio::test]
async fn complete_activation_with_failure(
    #[case] batches: &'static [usize],
    #[case] evict: WorkflowCachingPolicy,
) {
    let wfid = "fake_wf_id";
    let timer_id = "timer";

    let hist = canned_histories::workflow_fails_with_failure_after_timer(timer_id);
    let mock_sg = build_multihist_mock_sg(
        vec![FakeWfResponses {
            wf_id: wfid.to_string(),
            hist,
            response_batches: batches.iter().map(Into::into).collect(),
            task_q: TEST_Q.to_owned(),
        }],
        true,
        Some(1),
    );
    let core = mock_core(mock_sg);

    poll_and_reply(
        &core,
        evict,
        &[
            gen_assert_and_reply(
                &|_| {},
                vec![StartTimer {
                    timer_id: timer_id.to_owned(),
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::FireTimer(_)),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
    core.shutdown().await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn simple_timer_fail_wf_execution(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let timer_id = "timer1";

    let t = canned_histories::single_timer(timer_id);
    let core = build_fake_core(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![StartTimer {
                    timer_id: timer_id.to_string(),
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::FireTimer(_)),
                vec![FailWorkflowExecution {
                    failure: Some(Failure {
                        message: "I'm ded".to_string(),
                        ..Default::default()
                    }),
                }
                .into()],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn two_signals(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";

    let t = canned_histories::two_signals("sig1", "sig2");
    let core = build_fake_core(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                // Task is completed with no commands
                vec![],
            ),
            gen_assert_and_reply(
                &job_assert!(
                    wf_activation_job::Variant::SignalWorkflow(_),
                    wf_activation_job::Variant::SignalWorkflow(_)
                ),
                vec![],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn workflow_failures_only_reported_once() {
    let wfid = "fake_wf_id";
    let timer_1 = "timer1";
    let timer_2 = "timer2";

    let hist = canned_histories::workflow_fails_with_failure_two_different_points(timer_1, timer_2);
    let response_batches = vec![
        1, 2, // Start then first good reply
        2, 2, 2, // Poll for every failure
        // Poll again after evicting after second good reply, then two more fails
        3, 3, 3,
    ];
    let mocks = build_multihist_mock_sg(
        vec![FakeWfResponses {
            wf_id: wfid.to_string(),
            hist,
            response_batches: response_batches.into_iter().map(Into::into).collect(),
            task_q: TEST_Q.to_owned(),
        }],
        true,
        // We should only call the server to say we failed twice (once after each success)
        Some(2),
    );
    let omap = mocks.outstanding_task_map.clone();
    let core = mock_core(mocks);

    poll_and_reply_clears_outstanding_evicts(
        &core,
        omap,
        NonSticky,
        &[
            gen_assert_and_reply(
                &|_| {},
                vec![StartTimer {
                    timer_id: timer_1.to_owned(),
                    ..Default::default()
                }
                .into()],
            ),
            // Fail a few times in a row (only one of which should be reported)
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::FireTimer(_)),
                vec![StartTimer {
                    timer_id: timer_2.to_string(),
                    ..Default::default()
                }
                .into()],
            ),
            // Again (a new fail should be reported here)
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::FireTimer(_)),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn max_concurrent_wft_respected() {
    // Create long histories for three workflows
    let t1 = canned_histories::long_sequential_timers(20);
    let t2 = canned_histories::long_sequential_timers(20);
    let mut tasks = VecDeque::from(vec![
        hist_to_poll_resp(
            &t1,
            "wf1".to_owned(),
            ResponseType::AllHistory,
            TEST_Q.to_string(),
        ),
        hist_to_poll_resp(
            &t2,
            "wf2".to_owned(),
            ResponseType::AllHistory,
            TEST_Q.to_string(),
        ),
    ]);
    // Limit the core to two outstanding workflow tasks, hence we should only see polling
    // happen twice, since we will not actually finish the two workflows
    let mut mock_gateway = MockServerGatewayApis::new();
    mock_gateway
        .expect_poll_workflow_task()
        .times(2)
        .returning(move |_| Ok(tasks.pop_front().unwrap()));
    // Response not really important here
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));

    let core = mock_core_with_opts_no_workers(mock_gateway, CoreInitOptionsBuilder::default());
    core.register_worker(
        WorkerConfigBuilder::default()
            .task_queue(TEST_Q)
            .max_cached_workflows(2_usize)
            .max_outstanding_workflow_tasks(2_usize)
            .build()
            .unwrap(),
    )
    .await
    .unwrap();

    // Poll twice in a row before completing -- we should be at limit
    let r1 = core.poll_workflow_task(TEST_Q).await.unwrap();
    let r1_run_id = r1.run_id.clone();
    let r2 = core.poll_workflow_task(TEST_Q).await.unwrap();
    // Now we immediately poll for new work, and complete one of the existing activations. The
    // poll must not unblock until the completion goes through.
    let last_finisher = AtomicUsize::new(0);
    let (_, mut r1) = tokio::join! {
        async {
            core.complete_workflow_task(WfActivationCompletion::from_cmd(
                TEST_Q,
                r1.run_id,
                StartTimer {
                    timer_id: "timer-1".to_string(),
                    ..Default::default()
                }
                .into())
            ).await.unwrap();
            last_finisher.store(1, Ordering::SeqCst);
        },
        async {
            let r = core.poll_workflow_task(TEST_Q).await.unwrap();
            last_finisher.store(2, Ordering::SeqCst);
            r
        }
    };
    // So that we know we blocked
    assert_eq!(last_finisher.load(Ordering::Acquire), 2);

    // Since we never did anything with r2, all subsequent activations should be for wf1
    for i in 2..20 {
        core.complete_workflow_task(WfActivationCompletion::from_cmd(
            TEST_Q,
            r1.run_id,
            StartTimer {
                timer_id: format!("timer-{}", i),
                ..Default::default()
            }
            .into(),
        ))
        .await
        .unwrap();
        r1 = core.poll_workflow_task(TEST_Q).await.unwrap();
        assert_eq!(r1.run_id, r1_run_id);
    }
    // Finish the tasks so we can shut down
    core.complete_workflow_task(WfActivationCompletion::from_cmd(
        TEST_Q,
        r1.run_id,
        CompleteWorkflowExecution { result: None }.into(),
    ))
    .await
    .unwrap();
    // Just evict r2, we don't care about completing it properly
    core.request_workflow_eviction(TEST_Q, &r2.run_id);
    let _ = core
        .complete_workflow_task(WfActivationCompletion::empty(TEST_Q, r2.run_id))
        .await;
    let r2 = core.poll_workflow_task(TEST_Q).await.unwrap();
    dbg!(&r2);
    let _ = core
        .complete_workflow_task(WfActivationCompletion::empty(TEST_Q, r2.run_id))
        .await;
    core.shutdown().await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[3]))]
#[tokio::test]
async fn activity_not_canceled_on_replay_repro(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let t = canned_histories::unsent_at_cancel_repro();
    let core = build_fake_core(wfid, t, hist_batches);
    let activity_id = "act-1";

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                // Start timer and activity
                vec![
                    ScheduleActivity {
                        activity_id: activity_id.to_string(),
                        cancellation_type: ActivityCancellationType::TryCancel as i32,
                        ..Default::default()
                    }
                    .into(),
                    StartTimer {
                        timer_id: "timer-1".to_owned(),
                        ..Default::default()
                    }
                    .into(),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::FireTimer(_)),
                vec![RequestCancelActivity {
                    activity_id: activity_id.to_string(),
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        result: Some(ActivityResult {
                            status: Some(activity_result::Status::Canceled(..)),
                        }),
                        ..
                    }
                )),
                vec![StartTimer {
                    timer_id: "timer-2".to_owned(),
                    ..Default::default()
                }
                .into()],
            ),
        ],
    )
    .await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[3]))]
#[tokio::test]
async fn activity_not_canceled_when_also_completed_repro(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let t = canned_histories::cancel_not_sent_when_also_complete_repro();
    let core = build_fake_core(wfid, t, hist_batches);
    let activity_id = "act-1";

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    activity_id: activity_id.to_string(),
                    cancellation_type: ActivityCancellationType::TryCancel as i32,
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::SignalWorkflow(_)),
                vec![
                    RequestCancelActivity {
                        activity_id: activity_id.to_string(),
                    }
                    .into(),
                    StartTimer {
                        timer_id: "timer-1".to_owned(),
                        ..Default::default()
                    }
                    .into(),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        result: Some(ActivityResult {
                            status: Some(activity_result::Status::Canceled(..)),
                        }),
                        ..
                    }
                )),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn lots_of_workflows() {
    let hists = (0..500).into_iter().map(|i| {
        let wf_id = format!("fake-wf-{}", i);
        let hist = canned_histories::single_timer("fake_timer");
        FakeWfResponses {
            wf_id,
            hist,
            response_batches: vec![1.into(), 2.into()],
            task_q: TEST_Q.to_owned(),
        }
    });

    let mock = build_multihist_mock_sg(hists, false, None);
    let core = &mock_core(mock);

    fanout_tasks(5, |_| async move {
        while let Ok(wft) = core.poll_workflow_task(TEST_Q).await {
            let job = &wft.jobs[0];
            let reply = match job.variant {
                Some(wf_activation_job::Variant::StartWorkflow(_)) => StartTimer {
                    timer_id: "fake_timer".to_string(),
                    ..Default::default()
                }
                .into(),
                Some(wf_activation_job::Variant::RemoveFromCache(_)) => {
                    core.complete_workflow_task(WfActivationCompletion::empty(TEST_Q, wft.run_id))
                        .await
                        .unwrap();
                    continue;
                }
                _ => CompleteWorkflowExecution { result: None }.into(),
            };
            core.complete_workflow_task(WfActivationCompletion::from_cmd(
                TEST_Q, wft.run_id, reply,
            ))
            .await
            .unwrap();
        }
    })
    .await;
    assert_eq!(
        core.workers
            .get(TEST_Q)
            .unwrap()
            .outstanding_workflow_tasks(),
        0
    );
    core.shutdown().await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn wft_timeout_repro(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let t = canned_histories::wft_timeout_repro();
    let core = build_fake_core(wfid, t, hist_batches);
    let activity_id = "act-1";

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    activity_id: activity_id.to_string(),
                    cancellation_type: ActivityCancellationType::TryCancel as i32,
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(
                    wf_activation_job::Variant::SignalWorkflow(_),
                    wf_activation_job::Variant::SignalWorkflow(_),
                    wf_activation_job::Variant::ResolveActivity(ResolveActivity {
                        result: Some(ActivityResult {
                            status: Some(activity_result::Status::Completed(..)),
                        }),
                        ..
                    })
                ),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn complete_after_eviction() {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("fake_timer");
    let mut mock = MockServerGatewayApis::new();
    mock.expect_complete_workflow_task().times(0);
    let mock = single_hist_mock_sg(wfid, t, &[2], mock, true);
    let core = mock_core(mock);

    let activation = core.poll_workflow_task(TEST_Q).await.unwrap();
    // We just got start workflow, immediately evict
    core.request_workflow_eviction(TEST_Q, &activation.run_id);
    // Original task must be completed before we get the eviction
    core.complete_workflow_task(WfActivationCompletion::from_cmd(
        TEST_Q,
        activation.run_id,
        StartTimer {
            timer_id: "fake_timer".to_string(),
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();
    let eviction_activation = core.poll_workflow_task(TEST_Q).await.unwrap();
    assert_matches!(
        eviction_activation.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(_)),
            },
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::RemoveFromCache(_)),
            }
        ]
    );
    // Complete the activation containing the eviction, the way we normally would have
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        TEST_Q,
        eviction_activation.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn sends_appropriate_sticky_task_queue_responses() {
    // This test verifies that when completions are sent with sticky queues enabled, that they
    // include the information that tells the server to enqueue the next task on a sticky queue.
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("fake_timer");
    let mut mock = MockServerGatewayApis::new();
    mock.expect_complete_workflow_task()
        .withf(|comp| comp.sticky_attributes.is_some())
        .times(1)
        .returning(|_| Ok(Default::default()));
    mock.expect_complete_workflow_task().times(0);
    let mut mock = single_hist_mock_sg(wfid, t, &[1], mock, false);
    mock.worker_cfg(TEST_Q, |wc| wc.max_cached_workflows = 10);
    let core = mock_core(mock);

    let activation = core.poll_workflow_task(TEST_Q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmd(
        TEST_Q,
        activation.run_id,
        StartTimer {
            timer_id: "fake_timer".to_string(),
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
#[should_panic(expected = "called more than 2 times")]
async fn new_server_work_while_eviction_outstanding_doesnt_overwrite_activation() {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("fake_timer");
    let mock = MockServerGatewayApis::new();
    let mock = single_hist_mock_sg(wfid, t, &[1, 2], mock, true);
    let core = mock_core(mock);

    // Poll for and complete first workflow task
    let activation = core.poll_workflow_task(TEST_Q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmd(
        TEST_Q,
        activation.run_id,
        StartTimer {
            timer_id: "fake_timer".to_string(),
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();
    let eviction_activation = core.poll_workflow_task(TEST_Q).await.unwrap();
    assert_matches!(
        eviction_activation.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    // Poll again. We should not overwrite the eviction with the new work from the server to fire
    // the timer. IE: We will panic here because the mock has no more responses.
    core.poll_workflow_task(TEST_Q).await.unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn buffered_work_drained_on_shutdown() {
    let wfid = "fake_wf_id";
    // Build a one-timer history where first task times out
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    // Need to build the first response before adding the timeout events b/c otherwise the history
    // builder will include them in the first task
    let resp_1 = hist_to_poll_resp(&t, wfid.to_owned(), 1.into(), TEST_Q.to_string());
    t.add_workflow_task_timed_out();
    t.add_full_wf_task();
    let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
    t.add(
        EventType::TimerFired,
        history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
            started_event_id: timer_started_event_id,
            timer_id: "timer".to_string(),
        }),
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut tasks = VecDeque::from(vec![resp_1]);
    // Extend the task list with the now timeout-included version of the task. We add a bunch of
    // them because the poll loop will spin while new tasks are available and it is buffering them
    tasks.extend(
        std::iter::repeat(hist_to_poll_resp(
            &t,
            wfid.to_owned(),
            2.into(),
            TEST_Q.to_string(),
        ))
        .take(50),
    );
    let mut mock = MockServerGatewayApis::new();
    mock.expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
    let mut mock = MocksHolder::from_gateway_with_responses(mock, tasks, vec![].into());
    // Cache on to avoid being super repetitive
    mock.worker_cfg(TEST_Q, |wc| wc.max_cached_workflows = 10);
    let core = &mock_core(mock);

    // Poll for first WFT
    let act1 = core.poll_workflow_task(TEST_Q).await.unwrap();
    let poll_fut = async move {
        // Now poll again, which will start spinning, and buffer the next WFT with timer fired in it
        // - it won't stop spinning until the first task is complete
        let t = core.poll_workflow_task(TEST_Q).await.unwrap();
        core.complete_workflow_task(WfActivationCompletion::from_cmds(
            TEST_Q,
            t.run_id,
            vec![CompleteWorkflowExecution { result: None }.into()],
        ))
        .await
        .unwrap();
    };
    let complete_first = async move {
        core.complete_workflow_task(WfActivationCompletion::from_cmd(
            TEST_Q,
            act1.run_id,
            StartTimer {
                timer_id: "timer".to_string(),
                ..Default::default()
            }
            .into(),
        ))
        .await
        .unwrap();
    };
    tokio::join!(poll_fut, complete_first, async {
        // If the shutdown is sent too too fast, we might not have got a chance to even buffer work
        tokio::time::sleep(Duration::from_millis(5)).await;
        core.shutdown().await;
    });
}

#[tokio::test]
async fn buffering_tasks_doesnt_count_toward_outstanding_max() {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("fake_timer");
    let mock = MockServerGatewayApis::new();
    let mut tasks = VecDeque::new();
    // A way bigger task list than allowed outstanding tasks
    tasks.extend(
        std::iter::repeat(hist_to_poll_resp(
            &t,
            wfid.to_owned(),
            2.into(),
            TEST_Q.to_string(),
        ))
        .take(20),
    );
    let mut mock = MocksHolder::from_gateway_with_responses(mock, tasks, vec![].into());
    mock.worker_cfg(TEST_Q, |wc| {
        wc.max_cached_workflows = 10;
        wc.max_outstanding_workflow_tasks = 5;
    });
    let core = mock_core(mock);
    // Poll for first WFT
    core.poll_workflow_task(TEST_Q).await.unwrap();
    // This will error out when the mock runs out of responses. Otherwise it would hang when we
    // hit the max
    assert_matches!(
        core.poll_workflow_task(TEST_Q).await.unwrap_err(),
        PollWfError::TonicError(_)
    );
}
