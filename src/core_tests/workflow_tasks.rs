use crate::{
    job_assert,
    machines::test_help::{
        build_fake_core, build_multihist_mock_sg, fake_core_from_mock_sg, fake_sg_opts,
        gen_assert_and_fail, gen_assert_and_reply, hist_to_poll_resp, poll_and_reply, EvictionMode,
        FakeCore, FakeWfResponses, TestHistoryBuilder,
    },
    pollers::MockServerGatewayApis,
    protos::{
        coresdk::{
            activity_result::{self as ar, activity_result, ActivityResult},
            common::UserCodeFailure,
            workflow_activation::{
                wf_activation_job, FireTimer, ResolveActivity, StartWorkflow, UpdateRandomSeed,
                WfActivationJob,
            },
            workflow_commands::{
                ActivityCancellationType, CancelTimer, CompleteWorkflowExecution,
                FailWorkflowExecution, RequestCancelActivity, ScheduleActivity, StartTimer,
            },
            workflow_completion,
        },
        temporal::api::{
            enums::v1::EventType, workflowservice::v1::RespondWorkflowTaskCompletedResponse,
        },
    },
    test_help::canned_histories,
    Core, CoreInitOptions, CoreSDK, WfActivationCompletion,
};
use rstest::{fixture, rstest};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
};

#[fixture(hist_batches = &[])]
fn single_timer_setup(hist_batches: &[usize]) -> FakeCore {
    let wfid = "fake_wf_id";

    let t = canned_histories::single_timer("fake_timer");
    build_fake_core(wfid, t, hist_batches)
}

#[fixture(hist_batches = &[])]
fn single_activity_setup(hist_batches: &[usize]) -> FakeCore {
    let wfid = "fake_wf_id";

    let t = canned_histories::single_activity("fake_activity");
    build_fake_core(wfid, t, hist_batches)
}

#[fixture(hist_batches = &[])]
fn single_activity_failure_setup(hist_batches: &[usize]) -> FakeCore {
    let wfid = "fake_wf_id";

    let t = canned_histories::single_failed_activity("fake_activity");
    build_fake_core(wfid, t, hist_batches)
}

#[rstest]
#[case::incremental(single_timer_setup(&[1, 2]), EvictionMode::NotSticky)]
#[case::replay(single_timer_setup(&[2]), EvictionMode::NotSticky)]
#[case::incremental_evict(single_timer_setup(&[1, 2]), EvictionMode::AfterEveryReply)]
#[case::replay_evict(single_timer_setup(&[2, 2]), EvictionMode::AfterEveryReply)]
#[tokio::test]
async fn single_timer_test_across_wf_bridge(#[case] core: FakeCore, #[case] evict: EvictionMode) {
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
async fn single_activity_completion(core: FakeCore) {
    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
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
async fn parallel_timer_test_across_wf_bridge(hist_batches: &[usize]) {
    let wfid = "fake_wf_id";
    let timer_1_id = "timer1";
    let timer_2_id = "timer2";

    let t = canned_histories::parallel_timer(timer_1_id, timer_2_id);
    let core = build_fake_core(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
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
async fn timer_cancel_test_across_wf_bridge(hist_batches: &[usize]) {
    let wfid = "fake_wf_id";
    let timer_id = "wait_timer";
    let cancel_timer_id = "cancel_timer";

    let t = canned_histories::cancel_timer(timer_id, cancel_timer_id);
    let core = build_fake_core(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
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
async fn scheduled_activity_cancellation_try_cancel(hist_batches: &[usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::cancel_scheduled_activity(activity_id, signal_id);
    let core = build_fake_core(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
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
                    ..Default::default()
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
async fn scheduled_activity_timeout(hist_batches: &[usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";

    let t = canned_histories::scheduled_activity_timeout(activity_id);
    let core = build_fake_core(wfid, t, hist_batches);
    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
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
async fn started_activity_timeout(hist_batches: &[usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";

    let t = canned_histories::started_activity_timeout(activity_id);
    let core = build_fake_core(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
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
async fn cancelled_activity_timeout(hist_batches: &[usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::scheduled_cancelled_activity_timeout(activity_id, signal_id);
    let core = build_fake_core(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
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
                    ..Default::default()
                }
                .into()],
            ),
            // Activity is getting resolved right away as it has been timed out.
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
async fn scheduled_activity_cancellation_abandon(hist_batches: &[usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::cancel_scheduled_activity_abandon(activity_id, signal_id);
    let core = build_fake_core(wfid, t, hist_batches);

    verify_activity_cancellation_abandon(&activity_id, &core).await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn started_activity_cancellation_abandon(hist_batches: &[usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::cancel_started_activity_abandon(activity_id, signal_id);
    let core = build_fake_core(wfid, t, hist_batches);

    verify_activity_cancellation_abandon(&activity_id, &core).await;
}

async fn verify_activity_cancellation_abandon(activity_id: &&str, core: &FakeCore) {
    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    activity_id: activity_id.to_string(),
                    cancellation_type: ActivityCancellationType::Abandon as i32,
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::SignalWorkflow(_)),
                vec![RequestCancelActivity {
                    activity_id: activity_id.to_string(),
                    ..Default::default()
                }
                .into()],
            ),
            // Activity is getting resolved right away as we are in the Abandon mode.
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
async fn scheduled_activity_cancellation_wait_for_cancellation(hist_batches: &[usize]) {
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
async fn started_activity_cancellation_wait_for_cancellation(hist_batches: &[usize]) {
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

async fn verify_activity_cancellation_wait_for_cancellation(activity_id: &str, core: &FakeCore) {
    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
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
                    ..Default::default()
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

#[rstest(hist_batches, case::incremental(&[1, 3]), case::replay(&[3]))]
#[tokio::test]
async fn scheduled_activity_cancellation_try_cancel_task_canceled(hist_batches: &[usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::cancel_scheduled_activity_with_activity_task_cancel(
        activity_id,
        signal_id,
    );
    let core = build_fake_core(wfid, t, hist_batches);

    verify_activity_cancellation_try_cancel_task_canceled(&activity_id, &core).await;
}

#[rstest(hist_batches, case::incremental(&[1, 3]), case::replay(&[3]))]
#[tokio::test]
async fn started_activity_cancellation_try_cancel_task_canceled(hist_batches: &[usize]) {
    let wfid = "fake_wf_id";
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t =
        canned_histories::cancel_started_activity_with_activity_task_cancel(activity_id, signal_id);
    let core = build_fake_core(wfid, t, hist_batches);

    verify_activity_cancellation_try_cancel_task_canceled(&activity_id, &core).await;
}

async fn verify_activity_cancellation_try_cancel_task_canceled(
    activity_id: &&str,
    core: &FakeCore,
) {
    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
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
                    ..Default::default()
                }
                .into()],
            ),
            // Making sure that activity is not resolved until it's cancelled.
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
        EvictionMode::NotSticky,
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
        EvictionMode::NotSticky,
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
#[case::no_evict_inc(&[1, 2, 2], EvictionMode::NotSticky)]
#[case::no_evict(&[2, 2], EvictionMode::NotSticky)]
#[case::evict(&[1, 2, 2, 2], EvictionMode::AfterEveryReply)]
#[tokio::test]
async fn complete_activation_with_failure(#[case] batches: &[usize], #[case] evict: EvictionMode) {
    let wfid = "fake_wf_id";
    let timer_id = "timer";

    let hist = canned_histories::workflow_fails_with_failure_after_timer(timer_id);
    let mock_sg = build_multihist_mock_sg(
        vec![FakeWfResponses {
            wf_id: wfid.to_string(),
            hist,
            response_batches: batches.to_vec(),
        }],
        true,
        Some(1),
    );
    let core = fake_core_from_mock_sg(mock_sg);

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
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn simple_timer_fail_wf_execution(hist_batches: &[usize]) {
    let wfid = "fake_wf_id";
    let timer_id = "timer1";

    let t = canned_histories::single_timer(timer_id);
    let core = build_fake_core(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
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
                    failure: Some(UserCodeFailure {
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
async fn two_signals(hist_batches: &[usize]) {
    let wfid = "fake_wf_id";

    let t = canned_histories::two_signals("sig1", "sig2");
    let core = build_fake_core(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
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
    let mock_sg = build_multihist_mock_sg(
        vec![FakeWfResponses {
            wf_id: wfid.to_string(),
            hist,
            response_batches,
        }],
        true,
        // We should only call the server to say we failed twice (once after each success)
        Some(2),
    );
    let core = fake_core_from_mock_sg(mock_sg);

    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
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
        hist_to_poll_resp(&t1, "wf1".to_owned(), 100),
        hist_to_poll_resp(&t2, "wf2".to_owned(), 100),
    ]);
    // Limit the core to two outstanding workflow tasks, hence we should only see polling
    // happen twice, since we will not actually finish the two workflows
    let mut mock_gateway = MockServerGatewayApis::new();
    mock_gateway
        .expect_poll_workflow_task()
        .times(2)
        .returning(move || Ok(tasks.pop_front().unwrap()));
    // Response not really important here
    mock_gateway
        .expect_complete_workflow_task()
        .returning(|_, _| Ok(RespondWorkflowTaskCompletedResponse::default()));

    let core = CoreSDK::new(
        mock_gateway,
        CoreInitOptions {
            gateway_opts: fake_sg_opts(),
            evict_after_pending_cleared: true,
            max_outstanding_workflow_tasks: 2,
            max_outstanding_activities: 1,
        },
    );

    // Poll twice in a row before completing -- we should be at limit
    let r1 = core.poll_workflow_task().await.unwrap();
    let _r2 = core.poll_workflow_task().await.unwrap();
    // Now we immediately poll for new work, and complete one of the existing activations. The
    // poll must not unblock until the completion goes through.
    let last_finisher = AtomicUsize::new(0);
    let (_, mut r1) = tokio::join! {
        async {
            core.complete_workflow_task(WfActivationCompletion::from_status(
                r1.task_token,
                workflow_completion::Success::from_cmds(vec![StartTimer {
                    timer_id: "timer-1".to_string(),
                    ..Default::default()
                }
                .into()]).into()
            )).await.unwrap();
            last_finisher.store(1, Ordering::SeqCst);
        },
        async {
            let r = core.poll_workflow_task().await.unwrap();
            last_finisher.store(2, Ordering::SeqCst);
            r
        }
    };
    // So that we know we blocked
    assert_eq!(last_finisher.load(Ordering::Acquire), 2);

    // Since we never did anything with r2, all subsequent activations should be for wf1
    for i in 2..19 {
        core.complete_workflow_task(WfActivationCompletion::from_status(
            r1.task_token,
            workflow_completion::Success::from_cmds(vec![StartTimer {
                timer_id: format!("timer-{}", i),
                ..Default::default()
            }
            .into()])
            .into(),
        ))
        .await
        .unwrap();
        r1 = core.poll_workflow_task().await.unwrap();
    }
}

// TODO: Test doesn't fail when mock not called enough times (even though it panics??), but it
//   actually only should be called two times (with 1,2,3)
#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[3]))]
#[tokio::test]
async fn activity_not_canceled_on_replay_repro(hist_batches: &[usize]) {
    let wfid = "fake_wf_id";
    let t = canned_histories::unsent_at_cancel_repro();
    let core = build_fake_core(wfid, t, hist_batches);
    let activity_id = "act-1";

    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
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
                    ..Default::default()
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
async fn activity_not_canceled_when_also_completed_repro(hist_batches: &[usize]) {
    let wfid = "fake_wf_id";
    let t = canned_histories::cancel_not_sent_when_also_complete_repro();
    let core = build_fake_core(wfid, t, hist_batches);
    let activity_id = "act-1";

    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                // Start timer and activity
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
            response_batches: vec![1, 2],
        }
    });

    let mock = build_multihist_mock_sg(hists, false, None);
    let core = CoreSDK::new(
        mock.sg,
        CoreInitOptions {
            gateway_opts: fake_sg_opts(),
            evict_after_pending_cleared: true,
            max_outstanding_workflow_tasks: 20,
            max_outstanding_activities: 5,
        },
    );

    let core = Arc::new(core);
    let mut handles = vec![];
    for _ in 0..5 {
        let core = core.clone();
        let h = tokio::spawn(async move {
            while let Ok(wft) = core.poll_workflow_task().await {
                let job = &wft.jobs[0];
                let reply = match job.variant {
                    Some(wf_activation_job::Variant::StartWorkflow(_)) => StartTimer {
                        timer_id: "fake_timer".to_string(),
                        ..Default::default()
                    }
                    .into(),
                    Some(wf_activation_job::Variant::RemoveFromCache(_)) => continue,
                    _ => CompleteWorkflowExecution { result: None }.into(),
                };
                core.complete_workflow_task(WfActivationCompletion::from_status(
                    wft.task_token,
                    workflow_completion::Success::from_cmds(vec![reply]).into(),
                ))
                .await
                .unwrap();
            }
        });
        handles.push(h);
    }
    for h in handles {
        h.await.unwrap()
    }
    assert_eq!(core.wft_manager.outstanding_wft(), 0);
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn wft_timeout_repro(hist_batches: &[usize]) {
    let wfid = "fake_wf_id";
    let t = canned_histories::wft_timeout_repro();
    let core = build_fake_core(wfid, t, hist_batches);
    let activity_id = "act-1";

    poll_and_reply(
        &core,
        EvictionMode::NotSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                // Start timer and activity
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
