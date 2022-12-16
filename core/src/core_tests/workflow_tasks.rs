use crate::{
    advance_fut, job_assert,
    replay::TestHistoryBuilder,
    test_help::{
        build_fake_worker, build_mock_pollers, build_multihist_mock_sg, canned_histories,
        gen_assert_and_fail, gen_assert_and_reply, hist_to_poll_resp, mock_sdk, mock_sdk_cfg,
        mock_worker, poll_and_reply, poll_and_reply_clears_outstanding_evicts, single_hist_mock_sg,
        test_worker_cfg, FakeWfResponses, MockPollCfg, MocksHolder, ResponseType,
        WorkflowCachingPolicy::{self, AfterEveryReply, NonSticky},
    },
    worker::client::mocks::{mock_manual_workflow_client, mock_workflow_client},
    Worker,
};
use futures::{stream, FutureExt};
use rstest::{fixture, rstest};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use temporal_sdk::{ActivityOptions, CancellableFuture, WfContext};
use temporal_sdk_core_api::{errors::PollWfError, Worker as WorkerTrait};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{self as ar, activity_resolution, ActivityResolution},
        workflow_activation::{
            remove_from_cache::EvictionReason, workflow_activation_job, FireTimer, ResolveActivity,
            StartWorkflow, UpdateRandomSeed, WorkflowActivationJob,
        },
        workflow_commands::{
            ActivityCancellationType, CancelTimer, CompleteWorkflowExecution,
            ContinueAsNewWorkflowExecution, FailWorkflowExecution, RequestCancelActivity,
            ScheduleActivity,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    default_wes_attribs,
    temporal::api::{
        command::v1::command::Attributes,
        common::v1::{Payload, RetryPolicy},
        enums::v1::{EventType, WorkflowTaskFailedCause},
        failure::v1::Failure,
        history::v1::{history_event, TimerFiredEventAttributes},
        workflowservice::v1::{
            GetWorkflowExecutionHistoryResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
    DEFAULT_WORKFLOW_TYPE,
};
use temporal_sdk_core_test_utils::{fanout_tasks, start_timer_cmd};
use tokio::{
    join,
    sync::{Barrier, Semaphore},
};

#[fixture(hist_batches = &[])]
fn single_timer_setup(hist_batches: &'static [usize]) -> Worker {
    let wfid = "fake_wf_id";

    let t = canned_histories::single_timer("1");
    build_fake_worker(wfid, t, hist_batches)
}

#[fixture(hist_batches = &[])]
fn single_activity_setup(hist_batches: &'static [usize]) -> Worker {
    let wfid = "fake_wf_id";

    let t = canned_histories::single_activity("fake_activity");
    build_fake_worker(wfid, t, hist_batches)
}

#[fixture(hist_batches = &[])]
fn single_activity_failure_setup(hist_batches: &'static [usize]) -> Worker {
    let wfid = "fake_wf_id";

    let t = canned_histories::single_failed_activity("fake_activity");
    build_fake_worker(wfid, t, hist_batches)
}

#[rstest]
#[case::incremental(single_timer_setup(&[1, 2]), NonSticky)]
#[case::replay(single_timer_setup(&[2]), NonSticky)]
#[case::incremental_evict(single_timer_setup(&[1, 2]), AfterEveryReply)]
#[case::replay_evict(single_timer_setup(&[2]), AfterEveryReply)]
#[tokio::test]
async fn single_timer(#[case] worker: Worker, #[case] evict: WorkflowCachingPolicy) {
    poll_and_reply(
        &worker,
        evict,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
                vec![start_timer_cmd(1, Duration::from_secs(1))],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::FireTimer(_)),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[rstest(worker,
case::incremental(single_activity_setup(&[1, 2])),
case::incremental_activity_failure(single_activity_failure_setup(&[1, 2])),
case::replay(single_activity_setup(&[2])),
case::replay_activity_failure(single_activity_failure_setup(&[2]))
)]
#[tokio::test]
async fn single_activity_completion(worker: Worker) {
    poll_and_reply(
        &worker,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    activity_id: "fake_activity".to_string(),
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(_)),
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
    let timer_1_id = 1;
    let timer_2_id = 2;

    let t = canned_histories::parallel_timer(
        timer_1_id.to_string().as_str(),
        timer_2_id.to_string().as_str(),
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
                vec![
                    start_timer_cmd(timer_1_id, Duration::from_secs(1)),
                    start_timer_cmd(timer_2_id, Duration::from_secs(1)),
                ],
            ),
            gen_assert_and_reply(
                &|res| {
                    assert_matches!(
                        res.jobs.as_slice(),
                        [
                            WorkflowActivationJob {
                                variant: Some(workflow_activation_job::Variant::FireTimer(
                                    FireTimer { seq: t1_id }
                                )),
                            },
                            WorkflowActivationJob {
                                variant: Some(workflow_activation_job::Variant::FireTimer(
                                    FireTimer { seq: t2_id }
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
    let timer_id = 1;
    let cancel_timer_id = 2;

    let t = canned_histories::cancel_timer(
        timer_id.to_string().as_str(),
        cancel_timer_id.to_string().as_str(),
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
                vec![
                    start_timer_cmd(cancel_timer_id, Duration::from_secs(1)),
                    start_timer_cmd(timer_id, Duration::from_secs(1)),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::FireTimer(_)),
                vec![
                    CancelTimer {
                        seq: cancel_timer_id,
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
    let activity_seq = 1;
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::cancel_scheduled_activity(activity_id, signal_id);
    let core = build_fake_worker(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    seq: activity_seq,
                    activity_id: activity_id.to_string(),
                    cancellation_type: ActivityCancellationType::TryCancel as i32,
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::SignalWorkflow(_)),
                vec![RequestCancelActivity { seq: activity_seq }.into()],
            ),
            // Activity is getting resolved right away as we are in the TryCancel mode.
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(_)),
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
    let activity_seq = 1;
    let activity_id = "fake_activity";

    let t = canned_histories::scheduled_activity_timeout(activity_id);
    let core = build_fake_worker(wfid, t, hist_batches);
    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    seq: activity_seq,
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
                        WorkflowActivationJob {
                            variant: Some(workflow_activation_job::Variant::ResolveActivity(
                                ResolveActivity {
                                    seq,
                                    result: Some(ActivityResolution {
                                        status: Some(activity_resolution::Status::Failed(ar::Failure {
                                            failure: Some(failure)
                                        })),
                                    })
                                }
                            )),
                        }
                    ] => {
                        assert_eq!(failure.message, "Activity task timed out".to_string());
                        assert_eq!(*seq, activity_seq);
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
    let activity_seq = 1;

    let t = canned_histories::started_activity_timeout(activity_seq.to_string().as_str());
    let core = build_fake_worker(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    seq: activity_seq,
                    activity_id: activity_seq.to_string(),
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
                        WorkflowActivationJob {
                            variant: Some(workflow_activation_job::Variant::ResolveActivity(
                                ResolveActivity {
                                    seq,
                                    result: Some(ActivityResolution {
                                        status: Some(activity_resolution::Status::Failed(ar::Failure {
                                            failure: Some(failure)
                                        })),
                                    })
                                }
                            )),
                        }
                    ] => {
                        assert_eq!(failure.message, "Activity task timed out".to_string());
                        assert_eq!(*seq, activity_seq);
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
    let activity_seq = 0;
    let activity_id = "fake_activity";
    let signal_id = "signal";

    let t = canned_histories::scheduled_cancelled_activity_timeout(activity_id, signal_id);
    let core = build_fake_worker(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    seq: activity_seq,
                    activity_id: activity_id.to_string(),
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::SignalWorkflow(_)),
                vec![RequestCancelActivity { seq: activity_seq }.into()],
            ),
            // Activity is resolved right away as it has timed out.
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        seq: _,
                        result: Some(ActivityResolution {
                            status: Some(activity_resolution::Status::Cancelled(..)),
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
    let activity_id = 1;
    let signal_id = "signal";

    let t = canned_histories::cancel_scheduled_activity_abandon(
        activity_id.to_string().as_str(),
        signal_id,
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    verify_activity_cancellation(&core, activity_id, ActivityCancellationType::Abandon).await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn started_activity_cancellation_abandon(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = 1;
    let signal_id = "signal";

    let t = canned_histories::cancel_started_activity_abandon(
        activity_id.to_string().as_str(),
        signal_id,
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    verify_activity_cancellation(&core, activity_id, ActivityCancellationType::Abandon).await;
}

#[rstest(hist_batches, case::incremental(&[1, 2, 3, 4]), case::replay(&[4]))]
#[tokio::test]
async fn abandoned_activities_ignore_start_and_complete(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let activity_id = "1";

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let act_scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
    let act_started_event_id = t.add_activity_task_started(act_scheduled_event_id);
    t.add_activity_task_completed(
        act_scheduled_event_id,
        act_started_event_id,
        Default::default(),
    );
    t.add_full_wf_task();
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    let mock = mock_workflow_client();
    let mut worker = mock_sdk(MockPollCfg::from_resp_batches(wfid, t, hist_batches, mock));

    worker.register_wf(wf_type.to_owned(), |ctx: WfContext| async move {
        let act_fut = ctx.activity(ActivityOptions {
            activity_type: "echo_activity".to_string(),
            start_to_close_timeout: Some(Duration::from_secs(5)),
            cancellation_type: ActivityCancellationType::Abandon,
            ..Default::default()
        });
        ctx.timer(Duration::from_secs(1)).await;
        act_fut.cancel(&ctx);
        ctx.timer(Duration::from_secs(3)).await;
        act_fut.await;
        Ok(().into())
    });
    worker
        .submit_wf(wfid, wf_type, vec![], Default::default())
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[rstest(hist_batches, case::incremental(&[1, 3]), case::replay(&[3]))]
#[tokio::test]
async fn scheduled_activity_cancellation_try_cancel_task_canceled(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = 1;
    let signal_id = "signal";

    let t = canned_histories::cancel_scheduled_activity_with_activity_task_cancel(
        activity_id.to_string().as_str(),
        signal_id,
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    verify_activity_cancellation(&core, activity_id, ActivityCancellationType::TryCancel).await;
}

#[rstest(hist_batches, case::incremental(&[1, 3]), case::replay(&[3]))]
#[tokio::test]
async fn started_activity_cancellation_try_cancel_task_canceled(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = 1;
    let signal_id = "signal";

    let t = canned_histories::cancel_started_activity_with_activity_task_cancel(
        activity_id.to_string().as_str(),
        signal_id,
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    verify_activity_cancellation(&core, activity_id, ActivityCancellationType::TryCancel).await;
}

/// Verification for try cancel & abandon histories
async fn verify_activity_cancellation(
    worker: &Worker,
    activity_seq: u32,
    cancel_type: ActivityCancellationType,
) {
    poll_and_reply(
        worker,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    seq: activity_seq,
                    activity_id: activity_seq.to_string(),
                    cancellation_type: cancel_type as i32,
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::SignalWorkflow(_)),
                vec![RequestCancelActivity { seq: activity_seq }.into()],
            ),
            // Activity should be resolved right away
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        seq: _,
                        result: Some(ActivityResolution {
                            status: Some(activity_resolution::Status::Cancelled(..)),
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
    let activity_id = 1;
    let signal_id = "signal";

    let t = canned_histories::cancel_scheduled_activity_with_signal_and_activity_task_cancel(
        activity_id.to_string().as_str(),
        signal_id,
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    verify_activity_cancellation_wait_for_cancellation(activity_id, &core).await;
}

#[rstest(hist_batches, case::incremental(&[1, 2, 3, 4]), case::replay(&[4]))]
#[tokio::test]
async fn started_activity_cancellation_wait_for_cancellation(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let activity_id = 1;
    let signal_id = "signal";

    let t = canned_histories::cancel_started_activity_with_signal_and_activity_task_cancel(
        activity_id.to_string().as_str(),
        signal_id,
    );
    let core = build_fake_worker(wfid, t, hist_batches);

    verify_activity_cancellation_wait_for_cancellation(activity_id, &core).await;
}

async fn verify_activity_cancellation_wait_for_cancellation(activity_id: u32, worker: &Worker) {
    poll_and_reply(
        worker,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    seq: activity_id,
                    activity_id: activity_id.to_string(),
                    cancellation_type: ActivityCancellationType::WaitCancellationCompleted as i32,
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::SignalWorkflow(_)),
                vec![RequestCancelActivity { seq: activity_id }.into()],
            ),
            // Making sure that activity is not resolved until it's cancelled.
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::SignalWorkflow(_)),
                vec![],
            ),
            // Now ActivityTaskCanceled has been processed and activity can be resolved.
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        seq: _,
                        result: Some(ActivityResolution {
                            status: Some(activity_resolution::Status::Cancelled(..)),
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
    let timer_1_id = 1;
    let randomness_seed_from_start = AtomicU64::new(0);

    let t = canned_histories::workflow_fails_with_reset_after_timer(
        timer_1_id.to_string().as_str(),
        new_run_id,
    );
    let core = build_fake_worker(wfid, t, [2]);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &|res| {
                    assert_matches!(
                        res.jobs.as_slice(),
                        [WorkflowActivationJob {
                            variant: Some(workflow_activation_job::Variant::StartWorkflow(
                            StartWorkflow{randomness_seed, ..}
                            )),
                        }] => {
                        randomness_seed_from_start.store(*randomness_seed, Ordering::SeqCst);
                        }
                    );
                },
                vec![start_timer_cmd(timer_1_id, Duration::from_secs(1))],
            ),
            gen_assert_and_reply(
                &|res| {
                    assert_matches!(
                        res.jobs.as_slice(),
                        [WorkflowActivationJob {
                            variant: Some(workflow_activation_job::Variant::FireTimer(_),),
                        },
                        WorkflowActivationJob {
                            variant: Some(workflow_activation_job::Variant::UpdateRandomSeed(
                                UpdateRandomSeed{randomness_seed})),
                        }] => {
                            assert_ne!(randomness_seed_from_start.load(Ordering::SeqCst),
                                      *randomness_seed);
                        }
                    );
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
    let cancel_timer_id = 1;

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let core = build_fake_worker(wfid, t, [1]);

    poll_and_reply(
        &core,
        NonSticky,
        &[gen_assert_and_reply(
            &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
            vec![
                start_timer_cmd(cancel_timer_id, Duration::from_secs(1)),
                CancelTimer {
                    seq: cancel_timer_id,
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
    let timer_id = 1;

    let hist =
        canned_histories::workflow_fails_with_failure_after_timer(timer_id.to_string().as_str());
    let mock_sg = build_multihist_mock_sg(
        vec![FakeWfResponses {
            wf_id: wfid.to_string(),
            hist,
            response_batches: batches.iter().map(Into::into).collect(),
        }],
        true,
        1,
    );
    let core = mock_worker(mock_sg);

    poll_and_reply(
        &core,
        evict,
        &[
            gen_assert_and_reply(
                &|_| {},
                vec![start_timer_cmd(timer_id, Duration::from_secs(1))],
            ),
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::FireTimer(_)),
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
    let timer_id = 1;

    let t = canned_histories::single_timer(timer_id.to_string().as_str());
    let core = build_fake_worker(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
                vec![start_timer_cmd(timer_id, Duration::from_secs(1))],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::FireTimer(_)),
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
    let core = build_fake_worker(wfid, t, hist_batches);

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
                // Task is completed with no commands
                vec![],
            ),
            gen_assert_and_reply(
                &job_assert!(
                    workflow_activation_job::Variant::SignalWorkflow(_),
                    workflow_activation_job::Variant::SignalWorkflow(_)
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
    let timer_1 = 1;
    let timer_2 = 2;

    let hist = canned_histories::workflow_fails_with_failure_two_different_points(
        timer_1.to_string().as_str(),
        timer_2.to_string().as_str(),
    );
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
        }],
        true,
        // We should only call the server to say we failed twice (once after each success)
        2,
    );
    let omap = mocks.outstanding_task_map.clone();
    let core = mock_worker(mocks);

    poll_and_reply_clears_outstanding_evicts(
        &core,
        omap,
        NonSticky,
        &[
            gen_assert_and_reply(
                &|_| {},
                vec![start_timer_cmd(timer_1, Duration::from_secs(1))],
            ),
            // Fail a few times in a row (only one of which should be reported)
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::FireTimer(_)),
                vec![start_timer_cmd(timer_2, Duration::from_secs(1))],
            ),
            // Again (a new fail should be reported here)
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_fail(&|_| {}),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::FireTimer(_)),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;
}

#[tokio::test]
async fn max_wft_respected() {
    let total_wfs = 100;
    let wf_ids: Vec<_> = (0..total_wfs)
        .into_iter()
        .map(|i| format!("fake-wf-{}", i))
        .collect();
    let hists = wf_ids.iter().map(|wf_id| {
        let hist = canned_histories::single_timer("1");
        FakeWfResponses {
            wf_id: wf_id.to_string(),
            hist,
            response_batches: vec![1.into(), 2.into()],
        }
    });
    let mh = MockPollCfg::new(hists.into_iter().collect(), true, 0);
    let mut worker = mock_sdk_cfg(mh, |cfg| {
        cfg.max_cached_workflows = total_wfs as usize;
        cfg.max_outstanding_workflow_tasks = 1;
    });
    let active_count: &'static _ = Box::leak(Box::new(Semaphore::new(1)));
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, move |ctx: WfContext| async move {
        drop(
            active_count
                .try_acquire()
                .expect("No multiple concurrent workflow tasks!"),
        );
        ctx.timer(Duration::from_secs(1)).await;
        Ok(().into())
    });

    for wf_id in wf_ids {
        worker
            .submit_wf(wf_id, DEFAULT_WORKFLOW_TYPE, vec![], Default::default())
            .await
            .unwrap();
    }
    worker.run_until_done().await.unwrap();
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[3]))]
#[tokio::test]
async fn activity_not_canceled_on_replay_repro(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let t = canned_histories::unsent_at_cancel_repro();
    let core = build_fake_worker(wfid, t, hist_batches);
    let activity_id = 1;

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
                // Start timer and activity
                vec![
                    ScheduleActivity {
                        seq: activity_id,
                        activity_id: activity_id.to_string(),
                        cancellation_type: ActivityCancellationType::TryCancel as i32,
                        ..Default::default()
                    }
                    .into(),
                    start_timer_cmd(1, Duration::from_secs(1)),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::FireTimer(_)),
                vec![RequestCancelActivity { seq: activity_id }.into()],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        result: Some(ActivityResolution {
                            status: Some(activity_resolution::Status::Cancelled(..)),
                        }),
                        ..
                    }
                )),
                vec![start_timer_cmd(2, Duration::from_secs(1))],
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
    let core = build_fake_worker(wfid, t, hist_batches);
    let activity_id = 1;

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    seq: activity_id,
                    activity_id: activity_id.to_string(),
                    cancellation_type: ActivityCancellationType::TryCancel as i32,
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::SignalWorkflow(_)),
                vec![
                    RequestCancelActivity { seq: activity_id }.into(),
                    start_timer_cmd(2, Duration::from_secs(1)),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::ResolveActivity(
                    ResolveActivity {
                        result: Some(ActivityResolution {
                            status: Some(activity_resolution::Status::Cancelled(..)),
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
    let total_wfs = 500;
    let hists = (0..total_wfs).into_iter().map(|i| {
        let wf_id = format!("fake-wf-{}", i);
        let hist = canned_histories::single_timer("1");
        FakeWfResponses {
            wf_id,
            hist,
            response_batches: vec![1.into(), 2.into()],
        }
    });
    let mut mock = build_multihist_mock_sg(hists, false, 0);
    mock.make_wft_stream_interminable();
    let worker = &mock_worker(mock);
    let completed_count = Arc::new(Semaphore::new(0));
    let killer = async {
        let _ = completed_count.acquire_many(total_wfs).await.unwrap();
        worker.initiate_shutdown();
    };
    let poller = fanout_tasks(5, |_| {
        let completed_count = completed_count.clone();
        async move {
            while let Ok(wft) = worker.poll_workflow_activation().await {
                let job = &wft.jobs[0];
                let reply = match job.variant {
                    Some(workflow_activation_job::Variant::StartWorkflow(_)) => {
                        start_timer_cmd(1, Duration::from_secs(1))
                    }
                    Some(workflow_activation_job::Variant::RemoveFromCache(_)) => {
                        worker
                            .complete_workflow_activation(WorkflowActivationCompletion::empty(
                                wft.run_id,
                            ))
                            .await
                            .unwrap();
                        continue;
                    }
                    _ => {
                        completed_count.add_permits(1);
                        CompleteWorkflowExecution { result: None }.into()
                    }
                };
                worker
                    .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                        wft.run_id, reply,
                    ))
                    .await
                    .unwrap();
            }
        }
    });
    join!(killer, poller);
    worker.shutdown().await;
}

#[rstest(hist_batches, case::incremental(&[1, 2]), case::replay(&[2]))]
#[tokio::test]
async fn wft_timeout_repro(hist_batches: &'static [usize]) {
    let wfid = "fake_wf_id";
    let t = canned_histories::wft_timeout_repro();
    let core = build_fake_worker(wfid, t, hist_batches);
    let activity_id = 1;

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(workflow_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    seq: activity_id,
                    activity_id: activity_id.to_string(),
                    cancellation_type: ActivityCancellationType::TryCancel as i32,
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(
                    workflow_activation_job::Variant::SignalWorkflow(_),
                    workflow_activation_job::Variant::SignalWorkflow(_),
                    workflow_activation_job::Variant::ResolveActivity(ResolveActivity {
                        result: Some(ActivityResolution {
                            status: Some(activity_resolution::Status::Completed(..)),
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
    let t = canned_histories::single_timer("1");
    let mut mock = mock_workflow_client();
    mock.expect_complete_workflow_task().times(0);
    let mock = single_hist_mock_sg(wfid, t, [2], mock, true);
    let core = mock_worker(mock);

    let activation = core.poll_workflow_activation().await.unwrap();
    // We just got start workflow, immediately evict
    core.request_workflow_eviction(&activation.run_id);
    // Since we got whole history, we must finish replay before eviction will appear
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        activation.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    let next_activation = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        next_activation.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        next_activation.run_id,
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
    let t = canned_histories::single_timer("1");
    let mut mock = mock_workflow_client();
    mock.expect_complete_workflow_task()
        .withf(|comp| comp.sticky_attributes.is_some())
        .times(1)
        .returning(|_| Ok(Default::default()));
    mock.expect_complete_workflow_task().times(0);
    let mut mock = single_hist_mock_sg(wfid, t, [1], mock, false);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = mock_worker(mock);

    let activation = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        activation.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn new_server_work_while_eviction_outstanding_doesnt_overwrite_activation() {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("1");
    let mock = single_hist_mock_sg(wfid, t, [1, 2], mock_workflow_client(), false);
    let taskmap = mock.outstanding_task_map.clone().unwrap();
    let core = mock_worker(mock);

    // Poll for and complete first workflow task
    let activation = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        activation.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    let evict_act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        evict_act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    // Ensure mock has delivered both tasks
    assert!(taskmap.all_work_delivered());
    // Now we can complete the evict
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(evict_act.run_id))
        .await
        .unwrap();
    // The task buffered during eviction is applied and we start over
    let start_again = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        start_again.jobs[0].variant,
        Some(workflow_activation_job::Variant::StartWorkflow(_))
    );
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
    let resp_1 = hist_to_poll_resp(&t, wfid.to_owned(), 1.into()).resp;
    t.add_workflow_task_timed_out();
    t.add_full_wf_task();
    let timer_started_event_id = t.add_get_event_id(EventType::TimerStarted, None);
    t.add(
        EventType::TimerFired,
        history_event::Attributes::TimerFiredEventAttributes(TimerFiredEventAttributes {
            started_event_id: timer_started_event_id,
            timer_id: "1".to_string(),
        }),
    );
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut tasks = VecDeque::from(vec![resp_1]);
    // Extend the task list with the now timeout-included version of the task. We add a bunch of
    // them because the poll loop will spin while new tasks are available and it is buffering them
    tasks.extend(
        std::iter::repeat_with(|| hist_to_poll_resp(&t, wfid.to_owned(), 2.into()).resp).take(50),
    );
    let mut mock = mock_workflow_client();
    mock.expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
    let mut mock = MocksHolder::from_wft_stream(mock, stream::iter(tasks));
    // Cache on to avoid being super repetitive
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = &mock_worker(mock);

    // Poll for first WFT
    let act1 = core.poll_workflow_activation().await.unwrap();
    let poll_fut = async move {
        // Now poll again, which will start spinning, and buffer the next WFT with timer fired in it
        // - it won't stop spinning until the first task is complete
        let t = core.poll_workflow_activation().await.unwrap();
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            t.run_id,
            vec![CompleteWorkflowExecution { result: None }.into()],
        ))
        .await
        .unwrap();
    };
    let complete_first = async move {
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            act1.run_id,
            start_timer_cmd(1, Duration::from_secs(1)),
        ))
        .await
        .unwrap();
    };
    join!(poll_fut, complete_first, async {
        // If the shutdown is sent too too fast, we might not have got a chance to even buffer work
        tokio::time::sleep(Duration::from_millis(5)).await;
        core.shutdown().await;
    });
}

#[tokio::test]
async fn fail_wft_then_recover() {
    let t = canned_histories::long_sequential_timers(1);
    let mut mh = MockPollCfg::from_resp_batches(
        "fake_wf_id",
        t,
        // We need to deliver all of history twice because of eviction
        [ResponseType::AllHistory, ResponseType::AllHistory],
        mock_workflow_client(),
    );
    mh.num_expected_fails = 1;
    mh.expect_fail_wft_matcher =
        Box::new(|_, cause, _| matches!(cause, WorkflowTaskFailedCause::NonDeterministicError));
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 2;
    });
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    // Start an activity instead of a timer, triggering nondeterminism error
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id.clone(),
        vec![ScheduleActivity {
            activity_id: "fake_activity".to_string(),
            ..Default::default()
        }
        .into()],
    ))
    .await
    .unwrap();
    // We must handle an eviction now
    let evict_act = core.poll_workflow_activation().await.unwrap();
    assert_eq!(evict_act.run_id, act.run_id);
    assert_matches!(
        evict_act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(evict_act.run_id))
        .await
        .unwrap();

    // Workflow starting over, this time issue the right command
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    ))
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn poll_response_triggers_wf_error() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    // Add this nonsense event here to make applying the poll response fail
    t.add_external_signal_completed(100);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mh = MockPollCfg::from_resp_batches(
        "fake_wf_id",
        t,
        [ResponseType::AllHistory],
        mock_workflow_client(),
    );
    let mock = build_mock_pollers(mh);
    let core = mock_worker(mock);
    // Poll for first WFT, which is immediately an eviction
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
}

// Verifies we can handle multiple wft timeouts in a row if lang is being very slow in responding
#[tokio::test]
async fn lang_slower_than_wft_timeouts() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_timed_out();
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock = mock_workflow_client();
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(|_| Err(tonic::Status::not_found("Workflow task not found.")));
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(|_| Ok(Default::default()));
    let mut mock = single_hist_mock_sg(wfid, t, [1, 1], mock, true);
    let tasksmap = mock.outstanding_task_map.clone().unwrap();
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 2;
    });
    let core = mock_worker(mock);

    // This completion runs into the workflow task not found error
    let wf_task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(wf_task.run_id))
        .await
        .unwrap();
    // It will get an eviction
    let wf_task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    // Before we complete, unlock the next task from the mock so that we'll see it get buffered.
    tasksmap.release_run(&wf_task.run_id);
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(wf_task.run_id))
        .await
        .unwrap();
    // The buffered WFT should be applied now
    let start_again = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        start_again.jobs[0].variant,
        Some(workflow_activation_job::Variant::StartWorkflow(_))
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        start_again.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn tries_cancel_of_completed_activity() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled("1");
    t.add_we_signaled("sig", vec![]);
    let started_event_id = t.add_activity_task_started(scheduled_event_id);
    t.add_activity_task_completed(scheduled_event_id, started_event_id, Default::default());
    t.add_workflow_task_scheduled_and_started();

    let mock = mock_workflow_client();
    let mut mock = single_hist_mock_sg("fake_wf_id", t, [1, 2], mock, true);
    mock.worker_cfg(|cfg| cfg.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let activation = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        activation.run_id,
        ScheduleActivity {
            seq: 1,
            activity_id: "1".to_string(),
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();
    let activation = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        activation.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
            },
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }
        ]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        activation.run_id,
        vec![
            RequestCancelActivity { seq: 1 }.into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();

    core.shutdown().await;
}

#[tokio::test]
async fn failing_wft_doesnt_eat_permit_forever() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let mock = mock_workflow_client();
    let mut mock = MockPollCfg::from_resp_batches("fake_wf_id", t, [1, 1, 1], mock);
    mock.num_expected_fails = 1;
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|cfg| {
        cfg.max_cached_workflows = 2;
        cfg.max_outstanding_workflow_tasks = 2;
    });
    let outstanding_mock_tasks = mock.outstanding_task_map.clone();
    let worker = mock_worker(mock);

    let mut run_id = "".to_string();
    // Fail twice, verifying a permit is not eaten. We cannot fail the same run more than twice in a
    // row because we purposefully time out rather than spamming.
    for _ in 1..=2 {
        let activation = worker.poll_workflow_activation().await.unwrap();
        // Issue a nonsense completion that will trigger a WFT failure
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                activation.run_id,
                RequestCancelActivity { seq: 1 }.into(),
            ))
            .await
            .unwrap();
        let activation = worker.poll_workflow_activation().await.unwrap();
        assert_matches!(
            activation.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
            },]
        );
        run_id = activation.run_id.clone();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(activation.run_id))
            .await
            .unwrap();
    }
    assert_eq!(worker.outstanding_workflow_tasks().await, 0);
    // 1 permit is in use because the next task is buffered and has re-used the permit
    assert_eq!(worker.available_wft_permits().await, 1);
    // We should be "out of work" because the mock service thinks we didn't complete the last task,
    // which we didn't, because we don't spam failures. The real server would eventually time out
    // the task. Mock doesn't understand that, so the WFT permit is released because eventually a
    // new one will be generated. We manually clear the mock's outstanding task list so the next
    // poll will work.
    outstanding_mock_tasks.unwrap().release_run(&run_id);
    let activation = worker.poll_workflow_activation().await.unwrap();
    // There should be no change in permits, since this just unbuffered the buffered task
    assert_eq!(worker.available_wft_permits().await, 1);
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            activation.run_id,
            CompleteWorkflowExecution { result: None }.into(),
        ))
        .await
        .unwrap();
    assert_eq!(worker.available_wft_permits().await, 2);

    worker.shutdown().await;
}

#[tokio::test]
async fn cache_miss_will_fetch_history() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled("sig", vec![]);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    let get_exec_resp: GetWorkflowExecutionHistoryResponse = t.get_history_info(2).unwrap().into();

    let mut mh = MockPollCfg::from_resp_batches(
        "fake_wf_id",
        t,
        [ResponseType::ToTaskNum(1), ResponseType::OneTask(2)],
        mock_workflow_client(),
    );
    mh.mock_client
        .expect_get_workflow_execution_history()
        .times(1)
        .returning(move |_, _, _| Ok(get_exec_resp.clone()));
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|cfg| {
        cfg.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);

    let activation = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(activation.history_length, 3);
    assert_matches!(
        activation.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::StartWorkflow(_)),
        }]
    );
    // Force an eviction (before complete matters, so that we will be sure the eviction is queued
    // up before the next fake WFT is unlocked)
    worker.request_wf_eviction(
        &activation.run_id,
        "whatever",
        EvictionReason::LangRequested,
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(&activation.run_id))
        .await
        .unwrap();
    // Handle the eviction, and the restart
    for i in 1..=2 {
        let activation = worker.poll_workflow_activation().await.unwrap();
        assert_eq!(activation.history_length, 3);
        if i == 1 {
            assert_matches!(
                activation.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
                }]
            );
        } else {
            assert_matches!(
                activation.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::StartWorkflow(_)),
                }]
            );
        }
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::empty(activation.run_id))
            .await
            .unwrap();
    }
    let activation = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(activation.history_length, 7);
    assert_matches!(
        activation.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            activation.run_id,
            CompleteWorkflowExecution { result: None }.into(),
        ))
        .await
        .unwrap();
    assert_eq!(worker.outstanding_workflow_tasks().await, 0);
    worker.shutdown().await;
}

/// This test verifies that WFTs which come as replies to completing a WFT are properly delivered
/// via activation polling.
#[tokio::test]
async fn tasks_from_completion_are_delivered() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_we_signaled("sig", vec![]);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock = mock_workflow_client();
    let complete_resp = RespondWorkflowTaskCompletedResponse {
        workflow_task: Some(hist_to_poll_resp(&t, wfid.to_owned(), 2.into()).resp),
        activity_tasks: vec![],
        reset_history_event_id: 0,
    };
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(move |_| Ok(complete_resp.clone()));
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(|_| Ok(Default::default()));
    let mut mock = single_hist_mock_sg(wfid, t, [1], mock, true);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 2);
    let core = mock_worker(mock);

    let wf_task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(wf_task.run_id))
        .await
        .unwrap();
    let wf_task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        },]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        wf_task.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn poll_faster_than_complete_wont_overflow_cache() {
    // Make workflow tasks for 5 different runs
    let tasks: Vec<_> = (1..=5)
        .map(|i| FakeWfResponses {
            wf_id: format!("wf-{}", i),
            hist: canned_histories::single_timer("1"),
            response_batches: vec![ResponseType::ToTaskNum(1)],
        })
        .collect();
    let mut mock_client = mock_workflow_client();
    mock_client
        .expect_complete_workflow_task()
        .times(3)
        .returning(|_| Ok(Default::default()));
    let mut mock_cfg = MockPollCfg::new(tasks, true, 0);
    mock_cfg.mock_client = mock_client;
    let mut mock = build_mock_pollers(mock_cfg);
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 3;
        wc.max_outstanding_workflow_tasks = 3;
    });
    let core = mock_worker(mock);
    // Poll 4 times, completing once, such that max tasks are never exceeded
    let p1 = core.poll_workflow_activation().await.unwrap();
    let p2 = core.poll_workflow_activation().await.unwrap();
    let p3 = core.poll_workflow_activation().await.unwrap();
    for (i, p_res) in [&p1, &p2, &p3].into_iter().enumerate() {
        assert_matches!(
            &p_res.jobs[0].variant,
            Some(workflow_activation_job::Variant::StartWorkflow(sw))
            if sw.workflow_id == format!("wf-{}", i + 1)
        );
    }
    // Complete first task to free a wft slot. Cache size is at 3
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        p1.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    // Now we're at cache limit. We will poll for a task, discover it is for a new run, issue
    // an eviction, and buffer the new run task. However, the run we're trying to evict has pending
    // activations! Thus, we must complete them first before this poll will unblock, and then it
    // will unblock with the eviciton.
    let p4 = core.poll_workflow_activation();
    // Make sure the task gets buffered before we start the complete, so the LRU list is in the
    // expected order and what we expect to evict will be evicted.
    advance_fut!(p4);
    let p4 = async {
        let p4 = p4.await.unwrap();
        assert_matches!(
            &p4.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
            }]
        );
        p4
    };
    let p2_pending_completer = async {
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            p2.run_id,
            start_timer_cmd(1, Duration::from_secs(1)),
        ))
        .await
        .unwrap();
    };
    let (p4, _) = join!(p4, p2_pending_completer);
    assert_eq!(core.cached_workflows().await, 3);

    // This poll should also block until the eviction is actually completed
    let blocking_poll = async {
        let res = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            &res.jobs[0].variant,
            Some(workflow_activation_job::Variant::StartWorkflow(sw))
            if sw.workflow_id == format!("wf-{}", 4)
        );
        res
    };
    let complete_evict = async {
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(p4.run_id))
            .await
            .unwrap();
    };

    let (_p5, _) = join!(blocking_poll, complete_evict);
    assert_eq!(core.cached_workflows().await, 3);
    // The next poll will get an buffer a task for a new run, and generate an eviction for p3 but
    // that eviction cannot be obtained until we complete the existing outstanding task.
    let p6 = async {
        let p6 = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            p6.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
            }]
        );
        p6
    };
    let completer = async {
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            p3.run_id,
            start_timer_cmd(1, Duration::from_secs(1)),
        ))
        .await
        .unwrap();
    };
    let (p6, _) = join!(p6, completer);
    let complete_evict = async {
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(p6.run_id))
            .await
            .unwrap();
    };
    let blocking_poll = async {
        // This poll will also block until the last eviction goes through, and when it does it'll
        // produce the final start workflow task
        let res = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            &res.jobs[0].variant,
            Some(workflow_activation_job::Variant::StartWorkflow(sw))
            if sw.workflow_id == "wf-5"
        );
    };

    join!(blocking_poll, complete_evict);
    // p5 outstanding and final poll outstanding -- hence one permit available
    assert_eq!(core.available_wft_permits().await, 1);
    assert_eq!(core.cached_workflows().await, 3);
}

#[tokio::test]
async fn eviction_waits_until_replay_finished() {
    let wfid = "fake_wf_id";
    let t = canned_histories::long_sequential_timers(3);
    let mock = mock_workflow_client();
    let mock = single_hist_mock_sg(wfid, t, [3], mock, true);
    let core = mock_worker(mock);

    let activation = core.poll_workflow_activation().await.unwrap();
    assert_eq!(activation.history_length, 3);
    // Immediately request eviction after getting start workflow
    core.request_workflow_eviction(&activation.run_id);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        activation.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    let t1_fired = core.poll_workflow_activation().await.unwrap();
    assert_eq!(t1_fired.history_length, 8);
    assert_matches!(
        t1_fired.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        t1_fired.run_id,
        start_timer_cmd(2, Duration::from_secs(1)),
    ))
    .await
    .unwrap();
    let t2_fired = core.poll_workflow_activation().await.unwrap();
    assert_eq!(t2_fired.history_length, 13);
    assert_matches!(
        t2_fired.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::FireTimer(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        t2_fired.run_id,
        vec![CompleteWorkflowExecution { result: None }.into()],
    ))
    .await
    .unwrap();

    core.shutdown().await;
}

#[tokio::test]
async fn autocompletes_wft_no_work() {
    let wfid = "fake_wf_id";
    let activity_id = "1";

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled(activity_id);
    t.add_full_wf_task();
    t.add_we_signaled("sig1", vec![]);
    t.add_full_wf_task();
    let started_event_id = t.add_activity_task_started(scheduled_event_id);
    t.add_activity_task_completed(scheduled_event_id, started_event_id, Default::default());
    t.add_full_wf_task();
    let mock = mock_workflow_client();
    let mut mock = single_hist_mock_sg(wfid, t, [1, 2, 3, 4], mock, true);
    mock.worker_cfg(|w| w.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::StartWorkflow(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        act.run_id,
        ScheduleActivity {
            seq: 1,
            activity_id: activity_id.to_string(),
            cancellation_type: ActivityCancellationType::Abandon as i32,
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        act.run_id,
        RequestCancelActivity { seq: 1 }.into(),
    ))
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
        .await
        .unwrap();
    // The last task will autocomplete, and thus this will return shutdown since there is no more
    // work
    assert_matches!(
        core.poll_workflow_activation().await.unwrap_err(),
        PollWfError::ShutDown
    );

    core.shutdown().await;
}

#[tokio::test]
async fn no_race_acquiring_permits() {
    let wfid = "fake_wf_id";
    let mut mock_client = mock_manual_workflow_client();
    // We need to allow two polls to happen by triggering two processing events in the workflow
    // stream, but then delivering the actual tasks after that
    let task_barr: &'static Barrier = Box::leak(Box::new(Barrier::new(2)));
    mock_client
        .expect_poll_workflow_task()
        .returning(move |_, _| {
            let t = canned_histories::single_timer("1");
            let poll_resp = hist_to_poll_resp(&t, wfid.to_owned(), 2.into()).resp;
            async move {
                task_barr.wait().await;
                Ok(poll_resp.clone())
            }
            .boxed()
        });
    mock_client
        .expect_complete_workflow_task()
        .returning(|_| async move { Ok(Default::default()) }.boxed());

    let worker = Worker::new_test(
        test_worker_cfg()
            .max_outstanding_workflow_tasks(1_usize)
            .max_cached_workflows(10_usize)
            .build()
            .unwrap(),
        mock_client,
    );

    // Two polls in a row, both of which will get stuck on the barrier and are only allowed to
    // proceed after a call which will cause the workflow stream to process an event. Without the
    // fix, this would've meant the stream though it was OK to poll twice, but once the tasks
    // are received, it would find there was only one permit.
    let poll_1_f = async {
        let r = worker.poll_workflow_activation().await.unwrap();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                r.run_id,
                start_timer_cmd(1, Duration::from_secs(1)),
            ))
            .await
            .unwrap();
    };
    let poll_2_f = async {
        let r = worker.poll_workflow_activation().await.unwrap();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                r.run_id,
                start_timer_cmd(1, Duration::from_secs(1)),
            ))
            .await
            .unwrap();
    };
    let other_f = async {
        worker.cached_workflows().await;
        task_barr.wait().await;
        worker.cached_workflows().await;
        task_barr.wait().await;
    };
    join!(poll_1_f, poll_2_f, other_f);
}

#[tokio::test]
async fn continue_as_new_preserves_some_values() {
    let wfid = "fake_wf_id";
    let memo = HashMap::<String, Payload>::from([("enchi".to_string(), b"cat".into())]).into();
    let search = HashMap::<String, Payload>::from([("noisy".to_string(), b"kitty".into())]).into();
    let retry_policy = RetryPolicy {
        backoff_coefficient: 13.37,
        ..Default::default()
    };
    let mut wes_attrs = default_wes_attribs();
    wes_attrs.memo = Some(memo);
    wes_attrs.search_attributes = Some(search);
    wes_attrs.retry_policy = Some(retry_policy);
    let mut mock_client = mock_workflow_client();
    let hist = {
        let mut t = TestHistoryBuilder::default();
        t.add(
            EventType::WorkflowExecutionStarted,
            wes_attrs.clone().into(),
        );
        t.add_full_wf_task();
        t
    };
    mock_client
        .expect_poll_workflow_task()
        .returning(move |_, _| {
            Ok(hist_to_poll_resp(&hist, wfid.to_owned(), ResponseType::AllHistory).resp)
        });
    mock_client
        .expect_complete_workflow_task()
        .returning(move |mut c| {
            let can_cmd = c.commands.pop().unwrap().attributes.unwrap();
            if let Attributes::ContinueAsNewWorkflowExecutionCommandAttributes(a) = can_cmd {
                assert_eq!(a.workflow_type.unwrap().name, "meow");
                assert_eq!(a.memo, wes_attrs.memo);
                assert_eq!(a.search_attributes, wes_attrs.search_attributes);
                assert_eq!(a.retry_policy, wes_attrs.retry_policy);
            } else {
                panic!("Wrong attributes type");
            }
            Ok(Default::default())
        });

    let worker = Worker::new_test(test_worker_cfg().build().unwrap(), mock_client);
    let r = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            r.run_id,
            ContinueAsNewWorkflowExecution {
                workflow_type: "meow".to_string(),
                ..Default::default()
            }
            .into(),
        ))
        .await
        .unwrap();
}

#[rstest]
#[tokio::test]
async fn ignorable_events_are_ok(#[values(true, false)] attribs_unset: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    let id = t.add_get_event_id(
        EventType::Unspecified,
        Some(
            history_event::Attributes::WorkflowPropertiesModifiedExternallyEventAttributes(
                Default::default(),
            ),
        ),
    );
    t.modify_event(id, |e| e.worker_may_ignore = true);
    if attribs_unset {
        t.modify_event(id, |e| {
            e.event_type = EventType::WorkflowPropertiesModifiedExternally as i32;
            e.attributes = None;
        });
    }
    t.add_workflow_task_scheduled_and_started();

    let mock = mock_workflow_client();
    let mock = single_hist_mock_sg("wheee", t, [ResponseType::AllHistory], mock, true);
    let core = mock_worker(mock);

    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs[0].variant,
        Some(workflow_activation_job::Variant::StartWorkflow(_))
    );
}
