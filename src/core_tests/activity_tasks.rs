use crate::{
    errors::PollActivityError,
    machines::test_help::{fake_sg_opts, mock_core},
    pollers::{MockManualGateway, MockServerGatewayApis},
    protos::{
        coresdk::{
            activity_result::ActivityResult, activity_task::activity_task, ActivityTaskCompletion,
        },
        temporal::api::workflowservice::v1::{
            PollActivityTaskQueueResponse, RecordActivityTaskHeartbeatResponse,
            RespondActivityTaskCompletedResponse,
        },
    },
    ActivityHeartbeat, ActivityTask, Core, CoreInitOptionsBuilder, CoreSDK,
};
use futures::FutureExt;
use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::time::sleep;

#[tokio::test]
async fn max_activites_respected() {
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
    let mut mock_gateway = MockServerGatewayApis::new();
    mock_gateway
        .expect_poll_activity_task()
        .times(3)
        .returning(move || Ok(tasks.pop_front().unwrap()));
    mock_gateway
        .expect_complete_activity_task()
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));

    let core = CoreSDK::new(
        mock_gateway,
        CoreInitOptionsBuilder::default()
            .gateway_opts(fake_sg_opts())
            .max_outstanding_activities(2usize)
            .build()
            .unwrap(),
    );

    // We allow two outstanding activities, therefore first two polls should return right away
    let r1 = core.poll_activity_task().await.unwrap();
    let _r2 = core.poll_activity_task().await.unwrap();
    // Third should block until we complete one of the first two
    let last_finisher = AtomicUsize::new(0);
    tokio::join! {
        async {
            core.complete_activity_task(ActivityTaskCompletion {
                task_token: r1.task_token,
                result: Some(ActivityResult::ok(vec![1].into()))
            }).await.unwrap();
            last_finisher.store(1, Ordering::SeqCst);
        },
        async {
            core.poll_activity_task().await.unwrap();
            last_finisher.store(2, Ordering::SeqCst);
        }
    };
    // So that we know we blocked
    assert_eq!(last_finisher.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn activity_not_found_returns_ok() {
    let mut mock_gateway = MockServerGatewayApis::new();
    mock_gateway
        .expect_complete_activity_task()
        .times(1)
        .returning(|_, _| Err(tonic::Status::not_found("unimportant")));

    let core = mock_core(mock_gateway);

    core.complete_activity_task(ActivityTaskCompletion {
        task_token: vec![1],
        result: Some(ActivityResult::ok(vec![1].into())),
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn heartbeats_report_cancels() {
    let mut mock_gateway = MockServerGatewayApis::new();
    mock_gateway
        .expect_poll_activity_task()
        .times(1)
        .returning(|| {
            Ok(PollActivityTaskQueueResponse {
                task_token: vec![1],
                activity_id: "act1".to_string(),
                heartbeat_timeout: Some(Duration::from_secs(1).into()),
                ..Default::default()
            })
        });
    mock_gateway
        .expect_record_activity_heartbeat()
        .times(1)
        .returning(|_, _| {
            Ok(RecordActivityTaskHeartbeatResponse {
                cancel_requested: true,
            })
        });

    let core = mock_core(mock_gateway);

    let act = core.poll_activity_task().await.unwrap();
    core.record_activity_heartbeat(ActivityHeartbeat {
        task_token: act.task_token,
        details: vec![vec![1u8, 2, 3].into()],
    });
    // We have to wait a beat for the heartbeat to be processed
    sleep(Duration::from_millis(50)).await;
    let act = core.poll_activity_task().await.unwrap();
    assert_matches!(
        act,
        ActivityTask {
            task_token,
            variant: Some(activity_task::Variant::Cancel(_)),
            ..
        } => { task_token == vec![1] }
    );
}

#[tokio::test]
async fn activity_cancel_interrupts_poll() {
    let mut mock_gateway = MockManualGateway::new();
    let mut poll_resps = VecDeque::from(vec![
        async {
            Ok(PollActivityTaskQueueResponse {
                task_token: vec![1],
                heartbeat_timeout: Some(Duration::from_secs(1).into()),
                ..Default::default()
            })
        }
        .boxed(),
        async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            Ok(Default::default())
        }
        .boxed(),
    ]);
    mock_gateway
        .expect_poll_activity_task()
        .times(2)
        .returning(move || poll_resps.pop_front().unwrap());
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

    let core = mock_core(mock_gateway);
    let last_finisher = AtomicUsize::new(0);
    // Perform first poll to get the activity registered
    let act = core.poll_activity_task().await.unwrap();
    // Poll should block until heartbeat is sent, issuing the cancel, and interrupting the poll
    tokio::join! {
        async {
            core.record_activity_heartbeat(ActivityHeartbeat {
                task_token: act.task_token,
                details: vec![vec![1u8, 2, 3].into()],
            });
            last_finisher.store(1, Ordering::SeqCst);
        },
        async {
            core.poll_activity_task().await.unwrap();
            last_finisher.store(2, Ordering::SeqCst);
        }
    };
    // So that we know we blocked
    assert_eq!(last_finisher.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn activity_poll_timeout_retries() {
    let mut mock_gateway = MockServerGatewayApis::new();
    let mut calls = 0;
    mock_gateway
        .expect_poll_activity_task()
        .times(3)
        .returning(move || {
            calls += 1;
            if calls <= 2 {
                Ok(PollActivityTaskQueueResponse::default())
            } else {
                Err(tonic::Status::unknown("Test done"))
            }
        });
    let core = mock_core(mock_gateway);
    let r = core.poll_activity_task().await;
    assert_matches!(r.unwrap_err(), PollActivityError::TonicError(_));
}

#[tokio::test]
async fn many_concurrent_heartbeat_cancels() {
    // Run a whole bunch of activities in parallel, having the server return cancellations for
    // them after a few successful heartbeats
    const CONCURRENCY_NUM: u32 = 1000;

    let mut mock_gateway = MockManualGateway::new();
    let mut poll_resps = VecDeque::from(
        (0..CONCURRENCY_NUM)
            .map(|i| {
                async move {
                    Ok(PollActivityTaskQueueResponse {
                        task_token: i.to_be_bytes().to_vec(),
                        heartbeat_timeout: Some(Duration::from_millis(500).into()),
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
        .returning(move || poll_resps.pop_front().unwrap());
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

    let core = CoreSDK::new(
        mock_gateway,
        CoreInitOptionsBuilder::default()
            .gateway_opts(fake_sg_opts())
            .max_outstanding_activities(CONCURRENCY_NUM as usize)
            .build()
            .unwrap(),
    );
    let core = Arc::new(core);

    // Poll all activities first so they are registered
    for _ in 0..CONCURRENCY_NUM {
        core.poll_activity_task().await.unwrap();
    }

    // Spawn "activities"
    let mut handles = vec![];
    for i in 0..CONCURRENCY_NUM {
        let core = core.clone();
        let jh = tokio::spawn(async move {
            for _ in 0..10 {
                core.record_activity_heartbeat(ActivityHeartbeat {
                    task_token: i.to_be_bytes().to_vec(),
                    details: vec![],
                });
                sleep(Duration::from_millis(100)).await;
            }
        });
        handles.push(jh);
    }

    // Wait for all the heartbeating to finish, which should result in a bunch of pending
    // cancellations
    for h in handles.drain(..) {
        h.await.unwrap()
    }
    let mut handles = vec![];

    // Read all the cancellations and reply to them concurrently
    for _ in 0..CONCURRENCY_NUM {
        let core = core.clone();
        let jh = tokio::spawn(async move {
            let r = core.poll_activity_task().await.unwrap();
            assert_matches!(
                r,
                ActivityTask {
                    variant: Some(activity_task::Variant::Cancel(_)),
                    ..
                }
            );
            core.complete_activity_task(ActivityTaskCompletion {
                task_token: r.task_token.clone(),
                result: Some(ActivityResult::cancel_from_details(None)),
            })
            .await
            .unwrap();
        });
        handles.push(jh);
    }

    for h in handles.drain(..) {
        h.await.unwrap();
    }
    assert!(core.outstanding_activity_tasks.is_empty());
}
