use crate::{
    advance_fut, job_assert, prost_dur,
    test_help::{
        build_fake_worker, build_mock_pollers, canned_histories, gen_assert_and_reply,
        mock_manual_poller, mock_poller, mock_poller_from_resps, mock_worker, poll_and_reply,
        single_hist_mock_sg, test_worker_cfg, MockPollCfg, MockWorkerInputs, MocksHolder,
        ResponseType, WorkflowCachingPolicy, TEST_Q,
    },
    worker::client::mocks::{mock_manual_workflow_client, mock_workflow_client},
    ActivityHeartbeat, Worker, WorkerConfigBuilder,
};
use futures::FutureExt;
use itertools::Itertools;
use std::{
    cell::RefCell,
    collections::{hash_map::Entry, HashMap, VecDeque},
    rc::Rc,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use temporal_client::WorkflowOptions;
use temporal_sdk::{ActivityOptions, WfContext};
use temporal_sdk_core_api::{errors::CompleteActivityError, Worker as WorkerTrait};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{
            activity_execution_result, activity_resolution, ActivityExecutionResult,
            ActivityResolution, Success,
        },
        activity_task::{activity_task, ActivityTask},
        workflow_activation::{workflow_activation_job, ResolveActivity, WorkflowActivationJob},
        workflow_commands::{
            ActivityCancellationType, CompleteWorkflowExecution, RequestCancelActivity,
            ScheduleActivity,
        },
        workflow_completion::WorkflowActivationCompletion,
        ActivityTaskCompletion,
    },
    temporal::api::{
        command::v1::{command::Attributes, ScheduleActivityTaskCommandAttributes},
        enums::v1::EventType,
        workflowservice::v1::{
            PollActivityTaskQueueResponse, RecordActivityTaskHeartbeatResponse,
            RespondActivityTaskCanceledResponse, RespondActivityTaskCompletedResponse,
            RespondActivityTaskFailedResponse, RespondWorkflowTaskCompletedResponse,
        },
    },
    TestHistoryBuilder, DEFAULT_WORKFLOW_TYPE,
};
use temporal_sdk_core_test_utils::{fanout_tasks, start_timer_cmd, TestWorker};
use tokio::{sync::Barrier, time::sleep};

#[tokio::test]
async fn max_activities_respected() {
    let _task_q = "q";
    let mut tasks = VecDeque::from(vec![
        PollActivityTaskQueueResponse {
            task_token: vec![1],
            activity_id: "act1".to_string(),
            ..Default::default()
        },
        PollActivityTaskQueueResponse {
            task_token: vec![2],
            activity_id: "act2".to_string(),
            ..Default::default()
        },
        PollActivityTaskQueueResponse {
            task_token: vec![3],
            activity_id: "act3".to_string(),
            ..Default::default()
        },
    ]);
    let mut mock_client = mock_workflow_client();
    mock_client
        .expect_poll_activity_task()
        .times(3)
        .returning(move |_, _| Ok(tasks.pop_front().unwrap()));
    mock_client
        .expect_complete_activity_task()
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));

    let worker = Worker::new_test(
        test_worker_cfg()
            .max_outstanding_activities(2_usize)
            .build()
            .unwrap(),
        mock_client,
    );

    // We allow two outstanding activities, therefore first two polls should return right away
    let r1 = worker.poll_activity_task().await.unwrap();
    let _r2 = worker.poll_activity_task().await.unwrap();
    // Third poll should block until we complete one of the first two. To ensure this, manually
    // poll it a bunch to see it's not resolving.
    let poll_fut = worker.poll_activity_task();
    advance_fut!(poll_fut);
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: r1.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    poll_fut.await.unwrap();
}

#[tokio::test]
async fn activity_not_found_returns_ok() {
    let mut mock_client = mock_workflow_client();
    // Mock won't even be called, since we weren't tracking activity
    mock_client.expect_complete_activity_task().times(0);

    let core = mock_worker(MocksHolder::from_client_with_activities(mock_client, []));

    core.complete_activity_task(ActivityTaskCompletion {
        task_token: vec![1],
        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn heartbeats_report_cancels_only_once() {
    let mut mock_client = mock_workflow_client();
    mock_client
        .expect_record_activity_heartbeat()
        .times(2)
        .returning(|_, _| {
            Ok(RecordActivityTaskHeartbeatResponse {
                cancel_requested: true,
            })
        });
    mock_client
        .expect_complete_activity_task()
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));
    mock_client
        .expect_cancel_activity_task()
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskCanceledResponse::default()));

    let core = mock_worker(MocksHolder::from_client_with_activities(
        mock_client,
        [
            PollActivityTaskQueueResponse {
                task_token: vec![1],
                activity_id: "act1".to_string(),
                heartbeat_timeout: Some(prost_dur!(from_millis(1))),
                ..Default::default()
            }
            .into(),
            PollActivityTaskQueueResponse {
                task_token: vec![2],
                activity_id: "act2".to_string(),
                heartbeat_timeout: Some(prost_dur!(from_millis(1))),
                ..Default::default()
            }
            .into(),
        ],
    ));

    let act = core.poll_activity_task().await.unwrap();
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: act.task_token.clone(),
        details: vec![vec![1_u8, 2, 3].into()],
    });
    // We have to wait a beat for the heartbeat to be processed
    sleep(Duration::from_millis(10)).await;
    let act = core.poll_activity_task().await.unwrap();
    assert_matches!(
        &act,
        ActivityTask {
            task_token,
            variant: Some(activity_task::Variant::Cancel(_)),
            ..
        } => { task_token == &vec![1] }
    );

    // Verify if we try to record another heartbeat for this task we do not issue a double cancel
    // Allow heartbeat delay to elapse
    sleep(Duration::from_millis(10)).await;
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: act.task_token.clone(),
        details: vec![vec![1_u8, 2, 3].into()],
    });
    // Wait delay again to flush heartbeat
    sleep(Duration::from_millis(10)).await;
    // Now complete it as cancelled
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act.task_token,

        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();
    // Since cancels always come before new tasks, if we get a new non-cancel task, we did not
    // double-issue cancels.
    let act = core.poll_activity_task().await.unwrap();
    assert_matches!(
        &act,
        ActivityTask {
            task_token,
            variant: Some(activity_task::Variant::Start(_)),
            ..
        } => { task_token == &[2] }
    );
    // Complete it so shutdown goes through
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act.task_token,

        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn activity_cancel_interrupts_poll() {
    let mut mock_poller = mock_manual_poller();
    let mut poll_resps = VecDeque::from(vec![
        async {
            Some(Ok(PollActivityTaskQueueResponse {
                task_token: vec![1],
                heartbeat_timeout: Some(prost_dur!(from_secs(1))),
                ..Default::default()
            }))
        }
        .boxed(),
        async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Some(Ok(Default::default()))
        }
        .boxed(),
    ]);
    mock_poller
        .expect_poll()
        .times(2)
        .returning(move || poll_resps.pop_front().unwrap());

    let mut mock_client = mock_manual_workflow_client();
    mock_client
        .expect_record_activity_heartbeat()
        .times(1)
        .returning(|_, _| {
            async {
                Ok(RecordActivityTaskHeartbeatResponse {
                    cancel_requested: true,
                })
            }
            .boxed()
        });
    mock_client
        .expect_complete_activity_task()
        .times(1)
        .returning(|_, _| async { Ok(RespondActivityTaskCompletedResponse::default()) }.boxed());

    let mw = MockWorkerInputs {
        act_poller: Some(Box::from(mock_poller)),
        ..Default::default()
    };
    let core = mock_worker(MocksHolder::from_mock_worker(mock_client, mw));
    let last_finisher = AtomicUsize::new(0);
    // Perform first poll to get the activity registered
    let act = core.poll_activity_task().await.unwrap();
    // Poll should block until heartbeat is sent, issuing the cancel, and interrupting the poll
    tokio::join! {
        async {
            core.record_activity_heartbeat(ActivityHeartbeat {
                task_token: act.task_token,

                details: vec![vec![1_u8, 2, 3].into()],
            });
            last_finisher.store(1, Ordering::SeqCst);
        },
        async {
            let act = core.poll_activity_task().await.unwrap();
            // Must complete this activity for shutdown to finish
            core.complete_activity_task(
                ActivityTaskCompletion {
                    task_token: act.task_token,

                    result: Some(ActivityExecutionResult::ok(vec![1].into())),
                }
            ).await.unwrap();
            last_finisher.store(2, Ordering::SeqCst);
        }
    };
    // So that we know we blocked
    assert_eq!(last_finisher.load(Ordering::Acquire), 2);
    core.shutdown().await;
}

#[tokio::test]
async fn activity_poll_timeout_retries() {
    let mock_client = mock_workflow_client();
    let mut calls = 0;
    let mut mock_act_poller = mock_poller();
    mock_act_poller.expect_poll().times(3).returning(move || {
        calls += 1;
        if calls <= 2 {
            Some(Ok(PollActivityTaskQueueResponse::default()))
        } else {
            Some(Ok(PollActivityTaskQueueResponse {
                task_token: b"hello!".to_vec(),
                ..Default::default()
            }))
        }
    });
    let mw = MockWorkerInputs {
        act_poller: Some(Box::from(mock_act_poller)),
        ..Default::default()
    };
    let core = mock_worker(MocksHolder::from_mock_worker(mock_client, mw));
    let r = core.poll_activity_task().await.unwrap();
    assert_matches!(r.task_token.as_slice(), b"hello!");
}

#[tokio::test]
async fn many_concurrent_heartbeat_cancels() {
    // Run a whole bunch of activities in parallel, having the server return cancellations for
    // them after a few successful heartbeats
    const CONCURRENCY_NUM: usize = 5;

    let mut mock_client = mock_manual_workflow_client();
    let mut poll_resps = VecDeque::from(
        (0..CONCURRENCY_NUM)
            .map(|i| {
                async move {
                    Ok(PollActivityTaskQueueResponse {
                        task_token: i.to_be_bytes().to_vec(),
                        heartbeat_timeout: Some(prost_dur!(from_millis(200))),
                        ..Default::default()
                    })
                }
                .boxed()
            })
            .collect::<Vec<_>>(),
    );
    // Because the mock is so fast, it's possible it can return before the cancel channel in
    // the activity task poll selector. So, the final poll when there are no more tasks must
    // take a while.
    poll_resps.push_back(
        async {
            sleep(Duration::from_secs(10)).await;
            unreachable!("Long poll")
        }
        .boxed(),
    );
    let mut calls_map = HashMap::<_, i32>::new();
    mock_client
        .expect_poll_activity_task()
        .returning(move |_, _| poll_resps.pop_front().unwrap());
    mock_client
        .expect_cancel_activity_task()
        .returning(move |_, _| async move { Ok(Default::default()) }.boxed());
    mock_client
        .expect_record_activity_heartbeat()
        .returning(move |tt, _| {
            let calls = match calls_map.entry(tt) {
                Entry::Occupied(mut e) => {
                    *e.get_mut() += 1;
                    *e.get()
                }
                Entry::Vacant(v) => *v.insert(1),
            };
            async move {
                if calls < 5 {
                    Ok(RecordActivityTaskHeartbeatResponse {
                        cancel_requested: false,
                    })
                } else {
                    Ok(RecordActivityTaskHeartbeatResponse {
                        cancel_requested: true,
                    })
                }
            }
            .boxed()
        });

    let worker = &Worker::new_test(
        test_worker_cfg()
            .max_outstanding_activities(CONCURRENCY_NUM)
            // Only 1 poll at a time to avoid over-polling and running out of responses
            .max_concurrent_at_polls(1_usize)
            .build()
            .unwrap(),
        mock_client,
    );

    // Poll all activities first so they are registered
    for _ in 0..CONCURRENCY_NUM {
        worker.poll_activity_task().await.unwrap();
    }

    // Spawn "activities"
    fanout_tasks(CONCURRENCY_NUM, |i| async move {
        let task_token = i.to_be_bytes().to_vec();
        for _ in 0..12 {
            worker.record_activity_heartbeat(ActivityHeartbeat {
                task_token: task_token.clone(),
                details: vec![],
            });
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    // Read all the cancellations and reply to them concurrently
    fanout_tasks(CONCURRENCY_NUM, |_| async move {
        let r = worker.poll_activity_task().await.unwrap();
        assert_matches!(
            r,
            ActivityTask {
                variant: Some(activity_task::Variant::Cancel(_)),
                ..
            }
        );
        worker
            .complete_activity_task(ActivityTaskCompletion {
                task_token: r.task_token.clone(),
                result: Some(ActivityExecutionResult::cancel_from_details(None)),
            })
            .await
            .unwrap();
    })
    .await;

    worker.shutdown().await;
}

#[tokio::test]
async fn activity_timeout_no_double_resolve() {
    let t = canned_histories::activity_double_resolve_repro();
    let core = build_fake_worker("fake_wf_id", t, [3]);
    let activity_id = 1;

    poll_and_reply(
        &core,
        WorkflowCachingPolicy::NonSticky,
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
                vec![],
            ),
            gen_assert_and_reply(
                &job_assert!(
                    workflow_activation_job::Variant::SignalWorkflow(_),
                    workflow_activation_job::Variant::FireTimer(_)
                ),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;

    core.shutdown().await;
}

#[tokio::test]
async fn can_heartbeat_acts_during_shutdown() {
    let mut mock_client = mock_workflow_client();
    mock_client
        .expect_record_activity_heartbeat()
        .times(1)
        .returning(|_, _| {
            Ok(RecordActivityTaskHeartbeatResponse {
                cancel_requested: false,
            })
        });
    mock_client
        .expect_complete_activity_task()
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));

    let core = mock_worker(MocksHolder::from_client_with_activities(
        mock_client,
        [PollActivityTaskQueueResponse {
            task_token: vec![1],
            activity_id: "act1".to_string(),
            heartbeat_timeout: Some(prost_dur!(from_millis(1))),
            ..Default::default()
        }
        .into()],
    ));

    let act = core.poll_activity_task().await.unwrap();
    // Make sure shutdown has progressed before trying to record heartbeat / complete
    let shutdown_fut = core.shutdown();
    advance_fut!(shutdown_fut);
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: act.task_token.clone(),

        details: vec![vec![1_u8, 2, 3].into()],
    });
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act.task_token,

        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();
    shutdown_fut.await;
}

/// Verifies that if a user has tried to record a heartbeat and then immediately after failed the
/// activity, that we flush those details before reporting the failure completion.
#[tokio::test]
async fn complete_act_with_fail_flushes_heartbeat() {
    let last_hb = 50;
    let mut mock_client = mock_workflow_client();
    let last_seen_payload = Rc::new(RefCell::new(None));
    let lsp = last_seen_payload.clone();
    mock_client
        .expect_record_activity_heartbeat()
        // Two times b/c we always record the first heartbeat, and we'll flush the last
        .times(2)
        .returning_st(move |_, payload| {
            *lsp.borrow_mut() = payload;
            Ok(RecordActivityTaskHeartbeatResponse {
                cancel_requested: false,
            })
        });
    mock_client
        .expect_fail_activity_task()
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskFailedResponse::default()));

    let core = mock_worker(MocksHolder::from_client_with_activities(
        mock_client,
        [PollActivityTaskQueueResponse {
            task_token: vec![1],
            activity_id: "act1".to_string(),
            heartbeat_timeout: Some(prost_dur!(from_secs(10))),
            ..Default::default()
        }
        .into()],
    ));

    let act = core.poll_activity_task().await.unwrap();
    // Record a bunch of heartbeats
    for i in 1..=last_hb {
        core.record_activity_heartbeat(ActivityHeartbeat {
            task_token: act.task_token.clone(),
            details: vec![vec![i].into()],
        });
    }
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act.task_token.clone(),
        result: Some(ActivityExecutionResult::fail("Ahh".into())),
    })
    .await
    .unwrap();
    core.shutdown().await;

    // Verify the last seen call to record a heartbeat had the last detail payload
    let last_seen_payload = &last_seen_payload.take().unwrap().payloads[0];
    assert_eq!(last_seen_payload.data, &[last_hb]);
}

#[tokio::test]
async fn max_tq_acts_set_passed_to_poll_properly() {
    let rate = 9.28;
    let mut mock_client = mock_workflow_client();
    mock_client
        .expect_poll_activity_task()
        .returning(move |_, tps| {
            assert_eq!(tps, Some(rate));
            Ok(PollActivityTaskQueueResponse {
                task_token: vec![1],
                ..Default::default()
            })
        });

    let cfg = WorkerConfigBuilder::default()
        .namespace("enchi")
        .task_queue("cat")
        .max_concurrent_at_polls(1_usize)
        .worker_build_id("test_bin_id")
        .max_task_queue_activities_per_second(rate)
        .build()
        .unwrap();
    let worker = Worker::new_test(cfg, mock_client);
    worker.poll_activity_task().await.unwrap();
}

/// This test doesn't test the real worker config since [mock_worker] bypasses the worker
/// constructor, [mock_worker] will not pass an activity poller to the worker when
/// `no_remote_activities` is set to `true`.
#[tokio::test]
async fn no_eager_activities_requested_when_worker_options_disable_remote_activities() {
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let scheduled_event_id = t.add_activity_task_scheduled("act_id");
    let started_event_id = t.add_activity_task_started(scheduled_event_id);
    t.add_activity_task_completed(scheduled_event_id, started_event_id, b"hi".into());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    let num_eager_requested = Arc::new(AtomicUsize::new(0));
    // Clone it to move into the callback below
    let num_eager_requested_clone = num_eager_requested.clone();

    let mut mock = mock_workflow_client();
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(move |req| {
            // Store the number of eager activities requested to be checked below
            let count = req
                .commands
                .into_iter()
                .filter(|c| match c.attributes {
                    Some(Attributes::ScheduleActivityTaskCommandAttributes(
                        ScheduleActivityTaskCommandAttributes {
                            request_eager_execution,
                            ..
                        },
                    )) => request_eager_execution,
                    _ => false,
                })
                .count();
            num_eager_requested_clone.store(count, Ordering::Relaxed);
            Ok(RespondWorkflowTaskCompletedResponse {
                workflow_task: None,
                activity_tasks: vec![],
                reset_history_event_id: 0,
            })
        });
    let mut mock = single_hist_mock_sg(wfid, t, [1], mock, true);
    let mut mock_poller = mock_manual_poller();
    mock_poller
        .expect_poll()
        .returning(|| futures::future::pending().boxed());
    mock.set_act_poller(Box::new(mock_poller));
    mock.worker_cfg(|wc| {
        wc.max_cached_workflows = 2;
        wc.no_remote_activities = true;
    });
    let core = mock_worker(mock);

    // Test start
    let wf_task = core.poll_workflow_activation().await.unwrap();
    let cmds = vec![ScheduleActivity {
        seq: 1,
        activity_id: "act_id".to_string(),
        task_queue: TEST_Q.to_string(),
        cancellation_type: ActivityCancellationType::TryCancel as i32,
        ..Default::default()
    }
    .into()];

    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        wf_task.run_id,
        cmds,
    ))
    .await
    .unwrap();

    core.shutdown().await;

    assert_eq!(num_eager_requested.load(Ordering::Relaxed), 0);
}

/// This test verifies that activity tasks which come as replies to completing a WFT are properly
/// delivered via polling.
#[tokio::test]
async fn activity_tasks_from_completion_are_delivered() {
    // Construct the history - one task with 5 activities, 4 on the same task queue, and 1 on a
    // different queue, 3 activities will be executed eagerly as specified by the
    // MAX_EAGER_ACTIVITY_RESERVATIONS_PER_WORKFLOW_TASK constant.
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let act_same_queue_scheduled_ids = (1..4)
        .map(|i| t.add_activity_task_scheduled(format!("act_id_{}_same_queue", i)))
        .collect_vec();
    t.add_activity_task_scheduled("act_id_same_queue_not_eager");
    t.add_activity_task_scheduled("act_id_different_queue");
    for scheduled_event_id in act_same_queue_scheduled_ids {
        let started_event_id = t.add_activity_task_started(scheduled_event_id);
        t.add_activity_task_completed(scheduled_event_id, started_event_id, b"hi".into());
    }
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let num_eager_requested = Arc::new(AtomicUsize::new(0));
    // Clone it to move into the callback below
    let num_eager_requested_clone = num_eager_requested.clone();

    let mut mock = mock_workflow_client();
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(move |req| {
            // Store the number of eager activities requested to be checked below
            let count = req
                .commands
                .into_iter()
                .filter(|c| match c.attributes {
                    Some(Attributes::ScheduleActivityTaskCommandAttributes(
                        ScheduleActivityTaskCommandAttributes {
                            request_eager_execution,
                            ..
                        },
                    )) => request_eager_execution,
                    _ => false,
                })
                .count();
            num_eager_requested_clone.store(count, Ordering::Relaxed);
            Ok(RespondWorkflowTaskCompletedResponse {
                workflow_task: None,
                activity_tasks: (1..4)
                    .map(|i| PollActivityTaskQueueResponse {
                        task_token: vec![i],
                        activity_id: format!("act_id_{}_same_queue", i),
                        ..Default::default()
                    })
                    .collect_vec(),
                reset_history_event_id: 0,
            })
        });
    mock.expect_complete_activity_task()
        .times(3)
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));
    let mut mock = single_hist_mock_sg(wfid, t, [1], mock, true);
    let mut mock_poller = mock_manual_poller();
    mock_poller
        .expect_poll()
        .returning(|| futures::future::pending().boxed());
    mock.set_act_poller(Box::new(mock_poller));
    mock.worker_cfg(|wc| wc.max_cached_workflows = 2);
    let core = mock_worker(mock);

    // Test start
    let wf_task = core.poll_workflow_activation().await.unwrap();
    let mut cmds = (1..4)
        .map(|seq| {
            ScheduleActivity {
                seq,
                activity_id: format!("act_id_{}_same_queue", seq),
                task_queue: TEST_Q.to_string(),
                cancellation_type: ActivityCancellationType::TryCancel as i32,
                ..Default::default()
            }
            .into()
        })
        .collect_vec();
    cmds.push(
        ScheduleActivity {
            seq: 4,
            activity_id: "act_id_same_queue_not_eager".to_string(),
            task_queue: TEST_Q.to_string(),
            cancellation_type: ActivityCancellationType::TryCancel as i32,
            ..Default::default()
        }
        .into(),
    );
    cmds.push(
        ScheduleActivity {
            seq: 5,
            activity_id: "act_id_different_queue".to_string(),
            task_queue: "different_queue".to_string(),
            cancellation_type: ActivityCancellationType::Abandon as i32,
            ..Default::default()
        }
        .into(),
    );

    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        wf_task.run_id,
        cmds,
    ))
    .await
    .unwrap();

    // We should see the 3 eager activities when we poll now
    for i in 1..4 {
        let act_task = core.poll_activity_task().await.unwrap();
        assert_eq!(act_task.task_token, vec![i]);

        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act_task.task_token.clone(),
            result: Some(ActivityExecutionResult::ok("hi".into())),
        })
        .await
        .unwrap();
    }

    core.shutdown().await;

    // Verify only a single eager activity was scheduled (the one on our worker's task queue)
    assert_eq!(num_eager_requested.load(Ordering::Relaxed), 3);
}

#[tokio::test]
async fn activity_tasks_from_completion_reserve_slots() {
    let wf_id = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let schedid = t.add_activity_task_scheduled("1");
    let startid = t.add_activity_task_started(schedid);
    t.add_activity_task_completed(schedid, startid, b"hi".into());
    t.add_full_wf_task();
    let schedid = t.add_activity_task_scheduled("2");
    let startid = t.add_activity_task_started(schedid);
    t.add_activity_task_completed(schedid, startid, b"hi".into());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock = mock_workflow_client();
    // Set up two tasks to be returned via normal activity polling
    let act_tasks = VecDeque::from(vec![
        PollActivityTaskQueueResponse {
            task_token: vec![1],
            activity_id: "act1".to_string(),
            ..Default::default()
        }
        .into(),
        PollActivityTaskQueueResponse {
            task_token: vec![2],
            activity_id: "act2".to_string(),
            ..Default::default()
        }
        .into(),
    ]);
    mock.expect_complete_activity_task()
        .times(2)
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));
    let barr: &'static Barrier = Box::leak(Box::new(Barrier::new(2)));
    let mut mh = MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [
            ResponseType::ToTaskNum(1),
            // We don't want the second task to be delivered until *after* the activity tasks
            // have been completed, so that the second activity schedule will have slots available
            ResponseType::UntilResolved(
                async {
                    barr.wait().await;
                    barr.wait().await;
                }
                .boxed(),
                2,
            ),
            ResponseType::AllHistory,
        ],
        mock,
    );
    mh.completion_asserts = Some(Box::new(|wftc| {
        // Make sure when we see the completion with the schedule act command that it does
        // not have the eager execution flag set the first time, and does the second.
        if let Some(Attributes::ScheduleActivityTaskCommandAttributes(attrs)) =
            wftc.commands.get(0).and_then(|cmd| cmd.attributes.as_ref())
        {
            if attrs.activity_id == "1" {
                assert!(!attrs.request_eager_execution);
            } else {
                assert!(attrs.request_eager_execution);
            }
        }
    }));
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|cfg| {
        cfg.max_cached_workflows = 2;
        cfg.max_outstanding_activities = 2;
    });
    mock.set_act_poller(mock_poller_from_resps(act_tasks));
    let core = Arc::new(mock_worker(mock));
    let mut worker = TestWorker::new(core.clone(), TEST_Q.to_string());

    // First poll for activities twice, occupying both slots
    let at1 = core.poll_activity_task().await.unwrap();
    let at2 = core.poll_activity_task().await.unwrap();

    worker.register_wf(DEFAULT_WORKFLOW_TYPE, move |ctx: WfContext| async move {
        ctx.activity(ActivityOptions {
            activity_type: "act1".to_string(),
            ..Default::default()
        })
        .await;
        ctx.activity(ActivityOptions {
            activity_type: "act2".to_string(),
            ..Default::default()
        })
        .await;
        Ok(().into())
    });

    worker
        .submit_wf(
            wf_id.to_owned(),
            DEFAULT_WORKFLOW_TYPE,
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    let act_completer = async {
        barr.wait().await;
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: at1.task_token,
            result: Some(ActivityExecutionResult::ok("hi".into())),
        })
        .await
        .unwrap();
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: at2.task_token,
            result: Some(ActivityExecutionResult::ok("hi".into())),
        })
        .await
        .unwrap();
        barr.wait().await;
    };
    // This wf poll should *not* set the flag that it wants tasks back since both slots are
    // occupied
    let run_fut = async { worker.run_until_done().await.unwrap() };
    tokio::join!(run_fut, act_completer);
}

#[tokio::test]
async fn retryable_net_error_exhaustion_is_nonfatal() {
    let mut mock_client = mock_workflow_client();
    mock_client
        .expect_complete_activity_task()
        .times(1)
        .returning(|_, _| Err(tonic::Status::internal("retryable error")));

    let core = mock_worker(MocksHolder::from_client_with_activities(
        mock_client,
        [PollActivityTaskQueueResponse {
            task_token: vec![1],
            activity_id: "act1".to_string(),
            heartbeat_timeout: Some(prost_dur!(from_secs(10))),
            ..Default::default()
        }
        .into()],
    ));

    let act = core.poll_activity_task().await.unwrap();
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act.task_token,
        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn cant_complete_activity_with_unset_result_payload() {
    let mut mock_client = mock_workflow_client();
    mock_client
        .expect_poll_activity_task()
        .returning(move |_, _| {
            Ok(PollActivityTaskQueueResponse {
                task_token: vec![1],
                ..Default::default()
            })
        });

    let cfg = WorkerConfigBuilder::default()
        .namespace("enchi")
        .task_queue("cat")
        .worker_build_id("enchi_loves_salmon")
        .build()
        .unwrap();
    let worker = Worker::new_test(cfg, mock_client);
    let t = worker.poll_activity_task().await.unwrap();
    let res = worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: t.task_token,
            result: Some(ActivityExecutionResult {
                status: Some(activity_execution_result::Status::Completed(Success {
                    result: None,
                })),
            }),
        })
        .await;
    assert_matches!(
        res,
        Err(CompleteActivityError::MalformedActivityCompletion { .. })
    )
}
