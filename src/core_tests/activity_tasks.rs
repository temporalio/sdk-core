use crate::{
    job_assert,
    test_help::{
        build_fake_core, canned_histories, fake_sg_opts, gen_assert_and_reply, mock_core,
        mock_core_with_opts_no_workers, mock_gateway, mock_manual_poller, mock_poller,
        mock_poller_from_resps, poll_and_reply, MockWorker, MocksHolder, TEST_Q,
    },
    workflow::WorkflowCachingPolicy::NonSticky,
    ActivityHeartbeat, ActivityTask, Core, CoreInitOptionsBuilder, CoreSDK, WorkerConfigBuilder,
};
use futures::FutureExt;
use std::{
    cell::RefCell,
    collections::{hash_map::Entry, HashMap, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use temporal_client::MockManualGateway;
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::{activity_resolution, ActivityExecutionResult, ActivityResolution},
        activity_task::activity_task,
        workflow_activation::{wf_activation_job, ResolveActivity, WfActivationJob},
        workflow_commands::{
            ActivityCancellationType, CompleteWorkflowExecution, RequestCancelActivity,
            ScheduleActivity,
        },
        ActivityTaskCompletion,
    },
    temporal::api::workflowservice::v1::{
        PollActivityTaskQueueResponse, RecordActivityTaskHeartbeatResponse,
        RespondActivityTaskCanceledResponse, RespondActivityTaskCompletedResponse,
    },
};
use test_utils::{fanout_tasks, start_timer_cmd};
use tokio::{join, sync::Notify, time::sleep};

#[tokio::test]
async fn max_activities_respected() {
    let task_q = "q";
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
    let mut mock_gateway = mock_gateway();
    mock_gateway
        .expect_poll_activity_task()
        .times(3)
        .returning(move |_| Ok(tasks.pop_front().unwrap()));
    mock_gateway
        .expect_complete_activity_task()
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));

    let core = CoreSDK::new(
        mock_gateway,
        CoreInitOptionsBuilder::default()
            .gateway_opts(fake_sg_opts())
            .build()
            .unwrap(),
    );
    core.register_worker(
        WorkerConfigBuilder::default()
            .task_queue(TEST_Q)
            .max_outstanding_activities(2_usize)
            .build()
            .unwrap(),
    )
    .await
    .unwrap();

    // We allow two outstanding activities, therefore first two polls should return right away
    let r1 = core.poll_activity_task(task_q).await.unwrap();
    let _r2 = core.poll_activity_task(task_q).await.unwrap();
    // Third should block until we complete one of the first two
    let last_finisher = AtomicUsize::new(0);
    tokio::join! {
        async {
            core.complete_activity_task(ActivityTaskCompletion {
                task_token: r1.task_token,
                task_queue: TEST_Q.to_string(),
                result: Some(ActivityExecutionResult::ok(vec![1].into()))
            }).await.unwrap();
            last_finisher.store(1, Ordering::SeqCst);
        },
        async {
            core.poll_activity_task(task_q).await.unwrap();
            last_finisher.store(2, Ordering::SeqCst);
        }
    };
    // So that we know we blocked
    assert_eq!(last_finisher.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn activity_not_found_returns_ok() {
    let mut mock_gateway = mock_gateway();
    // Mock won't even be called, since we weren't tracking activity
    mock_gateway.expect_complete_activity_task().times(0);

    let core = mock_core(MocksHolder::from_gateway_with_responses(
        mock_gateway,
        [],
        [],
    ));

    core.complete_activity_task(ActivityTaskCompletion {
        task_token: vec![1],
        task_queue: TEST_Q.to_string(),
        result: Some(ActivityExecutionResult::ok(vec![1].into())),
    })
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn heartbeats_report_cancels_only_once() {
    let mut mock_gateway = mock_gateway();
    mock_gateway
        .expect_record_activity_heartbeat()
        .times(2)
        .returning(|_, _| {
            Ok(RecordActivityTaskHeartbeatResponse {
                cancel_requested: true,
            })
        });
    mock_gateway
        .expect_complete_activity_task()
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));
    mock_gateway
        .expect_cancel_activity_task()
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskCanceledResponse::default()));

    let core = mock_core(MocksHolder::from_gateway_with_responses(
        mock_gateway,
        [],
        [
            PollActivityTaskQueueResponse {
                task_token: vec![1],
                activity_id: "act1".to_string(),
                heartbeat_timeout: Some(Duration::from_millis(1).into()),
                ..Default::default()
            },
            PollActivityTaskQueueResponse {
                task_token: vec![2],
                activity_id: "act2".to_string(),
                heartbeat_timeout: Some(Duration::from_millis(1).into()),
                ..Default::default()
            },
        ],
    ));

    let act = core.poll_activity_task(TEST_Q).await.unwrap();
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: act.task_token.clone(),
        task_queue: TEST_Q.to_string(),
        details: vec![vec![1_u8, 2, 3].into()],
    });
    // We have to wait a beat for the heartbeat to be processed
    sleep(Duration::from_millis(10)).await;
    let act = core.poll_activity_task(TEST_Q).await.unwrap();
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
        task_queue: TEST_Q.to_string(),
        details: vec![vec![1_u8, 2, 3].into()],
    });
    // Wait delay again to flush heartbeat
    sleep(Duration::from_millis(10)).await;
    // Now complete it as cancelled
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: act.task_token,
        task_queue: TEST_Q.to_string(),
        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();
    // Since cancels always come before new tasks, if we get a new non-cancel task, we did not
    // double-issue cancels.
    let act = core.poll_activity_task(TEST_Q).await.unwrap();
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
        task_queue: TEST_Q.to_string(),
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
                heartbeat_timeout: Some(Duration::from_secs(1).into()),
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

    let mut mock_gateway = MockManualGateway::new();
    mock_gateway
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
    mock_gateway
        .expect_complete_activity_task()
        .times(1)
        .returning(|_, _| async { Ok(RespondActivityTaskCompletedResponse::default()) }.boxed());

    let mock_worker = MockWorker {
        act_poller: Some(Box::from(mock_poller)),
        ..Default::default()
    };

    let core = mock_core(MocksHolder::from_mock_workers(mock_gateway, [mock_worker]));
    let last_finisher = AtomicUsize::new(0);
    // Perform first poll to get the activity registered
    let act = core.poll_activity_task(TEST_Q).await.unwrap();
    // Poll should block until heartbeat is sent, issuing the cancel, and interrupting the poll
    tokio::join! {
        async {
            core.record_activity_heartbeat(ActivityHeartbeat {
                task_token: act.task_token,
                task_queue: TEST_Q.to_string(),
                details: vec![vec![1_u8, 2, 3].into()],
            });
            last_finisher.store(1, Ordering::SeqCst);
        },
        async {
            let act = core.poll_activity_task(TEST_Q).await.unwrap();
            // Must complete this activity for shutdown to finish
            core.complete_activity_task(
                ActivityTaskCompletion {
                    task_token: act.task_token,
                    task_queue: TEST_Q.to_string(),
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
    let mock_gateway = mock_gateway();
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
    let mock_worker = MockWorker {
        act_poller: Some(Box::from(mock_act_poller)),
        ..Default::default()
    };
    let core = mock_core(MocksHolder::from_mock_workers(mock_gateway, [mock_worker]));
    let r = core.poll_activity_task(TEST_Q).await.unwrap();
    assert_matches!(r.task_token.as_slice(), b"hello!");
}

#[tokio::test]
async fn many_concurrent_heartbeat_cancels() {
    // Run a whole bunch of activities in parallel, having the server return cancellations for
    // them after a few successful heartbeats
    const CONCURRENCY_NUM: usize = 5;

    let mut mock_gateway = MockManualGateway::new();
    let mut poll_resps = VecDeque::from(
        (0..CONCURRENCY_NUM)
            .map(|i| {
                async move {
                    Ok(PollActivityTaskQueueResponse {
                        task_token: i.to_be_bytes().to_vec(),
                        heartbeat_timeout: Some(Duration::from_millis(200).into()),
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
    mock_gateway
        .expect_poll_activity_task()
        .returning(move |_| poll_resps.pop_front().unwrap());
    mock_gateway
        .expect_cancel_activity_task()
        .returning(move |_, _| async move { Ok(Default::default()) }.boxed());
    mock_gateway
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

    let core = &mock_core_with_opts_no_workers(mock_gateway, CoreInitOptionsBuilder::default());
    core.register_worker(
        WorkerConfigBuilder::default()
            .task_queue(TEST_Q)
            .max_outstanding_activities(CONCURRENCY_NUM)
            // Only 1 poll at a time to avoid over-polling and running out of responses
            .max_concurrent_at_polls(1_usize)
            .build()
            .unwrap(),
    )
    .await
    .unwrap();

    // Poll all activities first so they are registered
    for _ in 0..CONCURRENCY_NUM {
        core.poll_activity_task(TEST_Q).await.unwrap();
    }

    // Spawn "activities"
    fanout_tasks(CONCURRENCY_NUM, |i| async move {
        let task_token = i.to_be_bytes().to_vec();
        for _ in 0..12 {
            core.record_activity_heartbeat(ActivityHeartbeat {
                task_token: task_token.clone(),
                task_queue: TEST_Q.to_string(),
                details: vec![],
            });
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await;

    // Read all the cancellations and reply to them concurrently
    fanout_tasks(CONCURRENCY_NUM, |_| async move {
        let r = core.poll_activity_task(TEST_Q).await.unwrap();
        assert_matches!(
            r,
            ActivityTask {
                variant: Some(activity_task::Variant::Cancel(_)),
                ..
            }
        );
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: r.task_token.clone(),
            task_queue: TEST_Q.to_string(),
            result: Some(ActivityExecutionResult::cancel_from_details(None)),
        })
        .await
        .unwrap();
    })
    .await;

    core.shutdown().await;
}

#[tokio::test]
async fn activity_timeout_no_double_resolve() {
    let t = canned_histories::activity_double_resolve_repro();
    let core = build_fake_core("fake_wf_id", t, &[3]);
    let activity_id = 1;

    poll_and_reply(
        &core,
        NonSticky,
        &[
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::StartWorkflow(_)),
                vec![ScheduleActivity {
                    seq: activity_id,
                    activity_id: activity_id.to_string(),
                    cancellation_type: ActivityCancellationType::TryCancel as i32,
                    ..Default::default()
                }
                .into()],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::SignalWorkflow(_)),
                vec![
                    RequestCancelActivity { seq: activity_id }.into(),
                    start_timer_cmd(2, Duration::from_secs(1)),
                ],
            ),
            gen_assert_and_reply(
                &job_assert!(wf_activation_job::Variant::ResolveActivity(
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
                    wf_activation_job::Variant::SignalWorkflow(_),
                    wf_activation_job::Variant::FireTimer(_)
                ),
                vec![CompleteWorkflowExecution { result: None }.into()],
            ),
        ],
    )
    .await;

    core.shutdown().await;
}

#[tokio::test]
async fn only_returns_cancels_for_desired_queue() {
    let mut mock_gateway = mock_gateway();
    let seen_cancel = Arc::new(Notify::new());
    let sc = seen_cancel.clone();
    mock_gateway
        .expect_record_activity_heartbeat()
        .times(1)
        .returning(move |_, _| {
            // Mark the activity as needing cancel when heartbeated
            sc.notify_one();
            Ok(RecordActivityTaskHeartbeatResponse {
                cancel_requested: true,
            })
        });
    mock_gateway
        .expect_cancel_activity_task()
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskCanceledResponse::default()));

    let mut w1 = MockWorker::for_queue("q1");
    w1.act_poller = Some(mock_poller_from_resps([PollActivityTaskQueueResponse {
        task_token: vec![1],
        activity_id: "act1".to_string(),
        heartbeat_timeout: Some(Duration::from_millis(1).into()),
        ..Default::default()
    }]));
    let mut mock_act_poller = mock_poller();
    mock_act_poller
        .expect_poll()
        .returning(|| Some(Ok(Default::default())));
    let mut w2 = MockWorker::for_queue("q2");
    w2.act_poller = Some(Box::from(mock_act_poller));

    let core = mock_core(MocksHolder::from_mock_workers(mock_gateway, [w1, w2]));
    // First poll should get the activity
    core.poll_activity_task("q1").await.unwrap();
    // Now record a heartbeat which will get the cancel response and mark the act as cancelled
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: vec![1],
        task_queue: "q1".to_string(),
        details: vec![],
    });
    // Worker two's poll should never resolve, since it's just getting pretend long poll timeouts
    let q1poll = async {
        // Wait for cancel to propagate
        seen_cancel.notified().await;
        core.poll_activity_task("q1").await
    };
    let q1_res = tokio::select! {
        _ = core.poll_activity_task("q2") => panic!("q2 poll resolved!"),
        r = q1poll => {
            r.unwrap()
        }
    };
    assert_matches!(
        &q1_res,
        ActivityTask {
            variant: Some(activity_task::Variant::Cancel(_)),
            ..
        }
    );
    // act needs to complete to finalize shutdown
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: q1_res.task_token,
        task_queue: "q1".to_string(),
        result: Some(ActivityExecutionResult::cancel_from_details(None)),
    })
    .await
    .unwrap();

    core.shutdown().await;
}

#[tokio::test]
async fn can_heartbeat_acts_during_shutdown() {
    let mut mock_gateway = mock_gateway();
    mock_gateway
        .expect_record_activity_heartbeat()
        .times(1)
        .returning(|_, _| {
            Ok(RecordActivityTaskHeartbeatResponse {
                cancel_requested: false,
            })
        });
    mock_gateway
        .expect_complete_activity_task()
        .times(1)
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));

    let core = mock_core(MocksHolder::from_gateway_with_responses(
        mock_gateway,
        [],
        [PollActivityTaskQueueResponse {
            task_token: vec![1],
            activity_id: "act1".to_string(),
            heartbeat_timeout: Some(Duration::from_millis(1).into()),
            ..Default::default()
        }],
    ));

    let act = core.poll_activity_task(TEST_Q).await.unwrap();
    let complete_order = RefCell::new(vec![]);
    // Start shutdown before completing the activity
    let shutdown_fut = async {
        core.shutdown().await;
        complete_order.borrow_mut().push(1);
    };
    let complete_fut = async {
        core.record_activity_heartbeat(ActivityHeartbeat {
            task_token: act.task_token.clone(),
            task_queue: TEST_Q.to_string(),
            details: vec![vec![1_u8, 2, 3].into()],
        });
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: act.task_token,
            task_queue: TEST_Q.to_string(),
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
        complete_order.borrow_mut().push(2);
    };
    join!(shutdown_fut, complete_fut);
    assert_eq!(&complete_order.into_inner(), &[2, 1])
}
