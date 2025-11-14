use temporalio_common::protos::temporal::api::workflowservice::v1::RespondNexusTaskCompletedResponse;
use crate::{
    PollError, prost_dur,
    test_help::{
        MockPollCfg, MockWorkerInputs, MocksHolder, ResponseType, WorkerExt, WorkerTestHelpers,
        build_fake_worker, build_mock_pollers, mock_worker, test_worker_cfg,
    },
    worker::{
        self,
        client::{
            MockWorkerClient,
            mocks::{DEFAULT_TEST_CAPABILITIES, DEFAULT_WORKERS_REGISTRY, mock_worker_client},
        },
    },
};
use futures_util::{stream, stream::StreamExt};
use std::{cell::RefCell, time::Duration};
use temporalio_common::{
    Worker,
    protos::{
        canned_histories,
        coresdk::{
            workflow_activation::workflow_activation_job,
            workflow_commands::{CompleteWorkflowExecution, StartTimer, workflow_command},
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::workflowservice::v1::{
            PollWorkflowTaskQueueResponse, PollNexusTaskQueueResponse, RespondWorkflowTaskCompletedResponse,
            ShutdownWorkerResponse,
        },
        temporal::api::common::v1::{ActivityType, WorkflowExecution},
        test_utils::start_timer_cmd,
    },
    worker::{PollerBehavior, WorkerTaskTypes},
};
use tokio::sync::{Barrier, watch};
use tokio::time::timeout;
use temporalio_common::protos::coresdk::activity_result::ActivityExecutionResult;
use temporalio_common::protos::coresdk::ActivityTaskCompletion;
use temporalio_common::protos::coresdk::nexus::NexusTaskCompletion;
use temporalio_common::protos::temporal::api::nexus::v1::{Request as NexusRequest, Response as NexusResponse, StartOperationRequest, StartOperationResponse, start_operation_response};
use temporalio_common::protos::temporal::api::workflowservice::v1::{PollActivityTaskQueueResponse, RespondActivityTaskCompletedResponse};
use crate::test_help::QueueResponse;

#[tokio::test]
async fn after_shutdown_of_worker_get_shutdown_err() {
    let t = canned_histories::single_timer("1");
    let worker = build_fake_worker("fake_wf_id", t, [1]);
    let res = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    let run_id = res.run_id;

    tokio::join!(worker.shutdown(), async {
        // Need to complete task for shutdown to finish
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                run_id.clone(),
                workflow_command::Variant::StartTimer(StartTimer {
                    seq: 1,
                    start_to_fire_timeout: Some(prost_dur!(from_secs(1))),
                }),
            ))
            .await
            .unwrap();

        // Shutdown proceeds if the only outstanding activations are evictions
        assert_matches!(
            worker.poll_workflow_activation().await.unwrap_err(),
            PollError::ShutDown
        );
    });
}

#[tokio::test]
async fn shutdown_worker_can_complete_pending_activation() {
    let t = canned_histories::single_timer("1");
    let worker = build_fake_worker("fake_wf_id", t, [2]);
    let res = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    // Complete the timer, will queue PA
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![start_timer_cmd(1, Duration::from_secs(1))],
        ))
        .await
        .unwrap();

    tokio::join!(worker.shutdown(), async {
        let res = worker.poll_workflow_activation().await.unwrap();
        // The timer fires
        assert_eq!(res.jobs.len(), 1);
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                res.run_id,
                vec![CompleteWorkflowExecution::default().into()],
            ))
            .await
            .unwrap();
        // Shutdown proceeds if the only outstanding activations are evictions
        assert_matches!(
            worker.poll_workflow_activation().await.unwrap_err(),
            PollError::ShutDown
        );
    });
}

#[tokio::test]
async fn worker_shutdown_during_poll_doesnt_deadlock() {
    let (tx, rx) = watch::channel(false);
    let rx = rx.clone();
    let stream = stream::unfold(rx, |mut rx| async move {
        // Don't resolve polls until worker shuts down
        rx.changed().await.unwrap();
        // We don't want to return a real response here because it would get buffered and
        // then we'd have real work to do to be able to finish shutdown.
        Some((
            Ok(PollWorkflowTaskQueueResponse::default().try_into().unwrap()),
            rx,
        ))
    });
    let mw = MockWorkerInputs::new(stream.boxed());
    let mut mock_client = mock_worker_client();
    mock_client
        .expect_complete_workflow_task()
        .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
    let worker = mock_worker(MocksHolder::from_mock_worker(mock_client, mw));
    let pollfut = worker.poll_workflow_activation();
    let shutdownfut = async {
        worker.shutdown().await;
        // Either the send works and unblocks the poll or the poll future is dropped before actually
        // polling -- either way things worked OK
        let _ = tx.send(true);
    };
    let (pollres, _) = tokio::join!(pollfut, shutdownfut);
    assert_matches!(pollres.unwrap_err(), PollError::ShutDown);
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn can_shutdown_local_act_only_worker_when_act_polling() {
    let t = canned_histories::single_timer("1");
    let mock = mock_worker_client();
    let mh = MockPollCfg::from_resp_batches("fakeid", t, [1], mock);
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|w| {
        w.task_types = WorkerTaskTypes::workflow_only();
        w.max_cached_workflows = 1;
    });
    let worker = mock_worker(mock);
    let barrier = Barrier::new(2);

    tokio::join!(
        async {
            barrier.wait().await;
            worker.shutdown().await;
        },
        async {
            let res = worker.poll_workflow_activation().await.unwrap();
            // Complete so there's no outstanding WFT and shutdown can finish
            worker
                .complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id))
                .await
                .unwrap();
            barrier.wait().await;
            // We need to see workflow poll return shutdown before activity poll will
            assert_matches!(
                worker.poll_workflow_activation().await.unwrap_err(),
                PollError::ShutDown
            );
            assert_matches!(
                worker.poll_activity_task().await.unwrap_err(),
                PollError::ShutDown
            );
        }
    );
    worker.drain_pollers_and_shutdown().await;
}

#[tokio::test]
async fn complete_with_task_not_found_during_shutdown() {
    let t = canned_histories::single_timer("1");
    let mut mock = mock_worker_client();
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(|_| Err(tonic::Status::not_found("Workflow task not found.")));
    let mh = MockPollCfg::from_resp_batches("fakeid", t, [1], mock);
    let core = mock_worker(build_mock_pollers(mh));

    let res = core.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);

    let complete_order = RefCell::new(vec![]);
    // Initiate shutdown before completing the activation
    let shutdown_fut = async {
        core.shutdown().await;
        complete_order.borrow_mut().push(2);
    };
    let poll_fut = async {
        // This will return shutdown once the completion goes through
        assert_matches!(
            core.poll_workflow_activation().await.unwrap_err(),
            PollError::ShutDown
        );
    };
    let complete_fut = async {
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![start_timer_cmd(1, Duration::from_secs(1))],
        ))
        .await
        .unwrap();
        complete_order.borrow_mut().push(1);
    };
    tokio::join!(shutdown_fut, poll_fut, complete_fut);
    assert_eq!(&complete_order.into_inner(), &[1, 2])
}

#[tokio::test]
async fn complete_eviction_after_shutdown_doesnt_panic() {
    let t = canned_histories::single_timer("1");
    let mut mh = build_mock_pollers(MockPollCfg::from_resp_batches(
        "fakeid",
        t,
        [1],
        mock_worker_client(),
    ));
    mh.make_wft_stream_interminable();
    let core = mock_worker(mh);

    let res = core.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    ))
    .await
    .unwrap();
    let res = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        res.jobs[0].variant,
        Some(workflow_activation_job::Variant::RemoveFromCache(_))
    );
    core.shutdown().await;
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id))
        .await
        .unwrap();
}

#[tokio::test]
async fn worker_does_not_panic_on_retry_exhaustion_of_nonfatal_net_err() {
    let t = canned_histories::single_timer("1");
    let mut mock = mock_worker_client();
    // Return a failure that counts as retryable, and hence we want to be swallowed
    mock.expect_complete_workflow_task()
        .times(1)
        .returning(|_| Err(tonic::Status::internal("Some retryable error")));
    let mut mh =
        MockPollCfg::from_resp_batches("fakeid", t, [1.into(), ResponseType::AllHistory], mock);
    mh.enforce_correct_number_of_polls = false;
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|w| w.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let res = core.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    // This should not return a fatal error
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id,
        vec![start_timer_cmd(1, Duration::from_secs(1))],
    ))
    .await
    .unwrap();
    // We should see an eviction
    core.handle_eviction().await;
}

#[rstest::rstest]
#[tokio::test]
async fn worker_can_shutdown_after_never_polling_ok(#[values(true, false)] poll_workflow: bool) {
    let mut mock = mock_worker_client();
    mock.expect_poll_activity_task()
        .returning(|_, _| Err(tonic::Status::permission_denied("you shall not pass")));
    if poll_workflow {
        mock.expect_poll_workflow_task()
            .returning(|_, _| Err(tonic::Status::permission_denied("you shall not pass")));
    }
    let core = worker::Worker::new_test(
        test_worker_cfg()
            .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
            .build()
            .unwrap(),
        mock,
    );

    loop {
        // Must continue polling until polls return shutdown.
        if poll_workflow {
            let res = core.poll_workflow_activation().await.unwrap_err();
            if !matches!(res, PollError::ShutDown) {
                continue;
            }
        }
        let res = core.poll_activity_task().await.unwrap_err();
        if !matches!(res, PollError::ShutDown) {
            continue;
        }
        core.finalize_shutdown().await;
        break;
    }
}

#[rstest::rstest]
#[case::ok(true, true)]
#[case::best_effort(true, false)]
#[case::not_sticky(false, true)]
#[tokio::test]
async fn worker_shutdown_api(#[case] use_cache: bool, #[case] api_success: bool) {
    // Manually need to create MockWorkerClient because we want to specify
    // the expected number of calls for shutdown_worker.
    // This will no longer be needed if
    // https://github.com/asomers/mockall/issues/283 is implemented.
    let mut mock = MockWorkerClient::new();
    mock.expect_capabilities()
        .returning(|| Some(*DEFAULT_TEST_CAPABILITIES));
    mock.expect_workers()
        .returning(|| DEFAULT_WORKERS_REGISTRY.clone());
    mock.expect_is_mock().returning(|| true);
    mock.expect_sdk_name_and_version()
        .returning(|| ("test-core".to_string(), "0.0.0".to_string()));
    mock.expect_identity()
        .returning(|| "test-identity".to_string());
    if use_cache {
        if api_success {
            mock.expect_shutdown_worker()
                .times(1)
                .returning(|_, _| Ok(ShutdownWorkerResponse {}));
        } else {
            // worker.shutdown() should succeed even if shutdown_worker fails
            mock.expect_shutdown_worker()
                .times(1)
                .returning(|_, _| Err(tonic::Status::unavailable("fake shutdown error")));
        }
    } else {
        mock.expect_shutdown_worker().times(0);
    }

    let t = canned_histories::single_timer("1");
    let mut mh =
        MockPollCfg::from_resp_batches("fakeid", t, [1.into(), ResponseType::AllHistory], mock);
    mh.enforce_correct_number_of_polls = false;
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|w| w.max_cached_workflows = if use_cache { 1 } else { 0 });
    let worker = mock_worker(mock);

    let res = worker.poll_workflow_activation().await.unwrap();
    assert_eq!(res.jobs.len(), 1);
    let run_id = res.run_id;

    tokio::join!(worker.shutdown(), async {
        // Need to complete task for shutdown to finish
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                run_id.clone(),
                workflow_command::Variant::StartTimer(StartTimer {
                    seq: 1,
                    start_to_fire_timeout: Some(prost_dur!(from_secs(1))),
                }),
            ))
            .await
            .unwrap();

        // Shutdown proceeds if the only outstanding activations are evictions
        assert_matches!(
            worker.poll_workflow_activation().await.unwrap_err(),
            PollError::ShutDown
        );
    });
}

#[tokio::test]
async fn test_worker_type_shutdown_all_combinations() {
    let combinations = [
        (WorkerTaskTypes::workflow_only(), "workflows only"),
        (WorkerTaskTypes::activity_only(), "activities only"),
        (WorkerTaskTypes::nexus_only(), "nexus only"),
        (
            WorkerTaskTypes {
                enable_workflows: true,
                enable_activities: true,
                enable_nexus: false,
            },
            "workflows + activities",
        ),
        (
            WorkerTaskTypes {
                enable_workflows: true,
                enable_activities: false,
                enable_nexus: true,
            },
            "workflows + nexus",
        ),
        (
            WorkerTaskTypes {
                enable_workflows: false,
                enable_activities: true,
                enable_nexus: true,
            },
            "activities + nexus",
        ),
        (WorkerTaskTypes::all(), "all types"),
    ];

    for (task_types, description) in combinations {
        let mut cfg = test_worker_cfg();
        cfg.task_types(task_types);

        let mock_inputs = MockWorkerInputs {
            config: cfg.build().unwrap(),
            ..Default::default()
        };
        let worker = mock_worker(MocksHolder::from_mock_worker(
            mock_worker_client(),
            mock_inputs,
        ));

        let shutdown_result = timeout(Duration::from_secs(1), worker.shutdown()).await;
        assert!(
            shutdown_result.is_ok(),
            "worker shutdown should not hang for {description}"
        );
    }
}

// #[tokio::test]
// async fn test_type_shutdown_with_tasks2() {
//     telemetry_init_fallback();
//     let combinations = [
//         // (WorkerTaskTypes::workflow_only(), "workflows only"),
//         (WorkerTaskTypes::activity_only(), "activities only"),
//         // (WorkerTaskTypes::nexus_only(), "nexus only"),
//         // (
//         //     WorkerTaskTypes {
//         //         enable_workflows: true,
//         //         enable_activities: true,
//         //         enable_nexus: false,
//         //     },
//         //     "workflows + activities",
//         // ),
//         // (
//         //     WorkerTaskTypes {
//         //         enable_workflows: true,
//         //         enable_activities: false,
//         //         enable_nexus: true,
//         //     },
//         //     "workflows + nexus",
//         // ),
//         // (
//         //     WorkerTaskTypes {
//         //         enable_workflows: false,
//         //         enable_activities: true,
//         //         enable_nexus: true,
//         //     },
//         //     "activities + nexus",
//         // ),
//         // (WorkerTaskTypes::all(), "all types"),
//     ];
//
//     for (task_types, description) in combinations {
//         eprintln!("\nTesting: {description}");
//
//         let mut mock_client = mock_worker_client();
//         if task_types.enable_workflows {
//             mock_client
//                 .expect_complete_workflow_task()
//                 .times(1)
//                 .returning(|_| Ok(RespondWorkflowTaskCompletedResponse::default()));
//             // mock_client.expect_poll_workflow_task().times(1).returning(|_, _| Ok(PollWorkflowTaskQueueResponse::default()));
//         }
//         if task_types.enable_activities {
//             mock_client.expect_poll_activity_task().times(1).returning(|_, _| Ok(PollActivityTaskQueueResponse {
//                 task_token: vec![1],
//                 ..Default::default()
//             }));
//             mock_client.expect_complete_activity_task().times(1).returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));
//         }
//         if task_types.enable_nexus {
//             mock_client.expect_poll_nexus_task().times(1).returning(|_, _| Ok(Default::default()));
//             mock_client.expect_complete_nexus_task().times(1).returning(|_, _| Ok(RespondNexusTaskCompletedResponse::default()));
//         }
//
//         let t = canned_histories::single_timer("1");
//         let mut mh = MockPollCfg::from_resp_batches("fakeid", t, [1], mock_client);
//         mh.enforce_correct_number_of_polls = false;
//         let act_tasks: Vec<QueueResponse<PollActivityTaskQueueResponse>> = vec![QueueResponse::from(PollActivityTaskQueueResponse::default())];
//         mh.activity_responses = Some(act_tasks);
//         let mut mock = build_mock_pollers(mh);
//         mock.worker_cfg(|w| {
//             w.task_types = task_types;
//             w.max_cached_workflows = 1;
//         });
//         let worker = mock_worker(mock);
//
//         println!("a");
//         if task_types.enable_workflows {
//             let activation = worker.poll_workflow_activation().await.unwrap();
//             let _ = worker
//                 .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
//                     activation.run_id,
//                     vec![],
//                 ))
//                 .await;
//         }
//
//         println!("a");
//         if task_types.enable_activities {
//             let task = worker.poll_activity_task().await.unwrap();
//             worker.complete_activity_task(ActivityTaskCompletion {
//                 task_token: task.task_token,
//                 result: Some(ActivityExecutionResult::ok(Default::default())),
//             })
//                 .await.unwrap();
//         }
//
//         println!("a");
//         if task_types.enable_nexus {
//             let task = worker.poll_nexus_task().await.unwrap();
//             worker
//                 .complete_nexus_task(NexusTaskCompletion {
//                     task_token: task.task_token().to_vec(),
//                     status: None,
//                 })
//                 .await.unwrap();
//
//         }
//     }
// }

#[tokio::test]
async fn test_task_type_activity_only() {
    let queue = "activity-only-queue";

    let mut client = mock_worker_client();
    client
        .expect_complete_activity_task()
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));

    let mut act_task = PollActivityTaskQueueResponse::default();
    act_task.task_token = b"act-task".to_vec();
    act_task.workflow_execution = Some(WorkflowExecution {
        workflow_id: "activity-only".to_string(),
        run_id: "run-id".to_string(),
    });
    act_task.activity_id = "activity".to_string();
    act_task.activity_type = Some(ActivityType {
        name: "activity".to_string(),
    });

    let mut mocks = MocksHolder::from_client_with_activities(
        client,
        vec![QueueResponse::from(act_task)]
    );
    mocks.worker_cfg(|w| {
        w.task_queue = queue.to_string();
        w.task_types = WorkerTaskTypes::activity_only();
        w.skip_client_worker_set_check = true;
    });
    let worker = mock_worker(mocks);

    let activity_task = worker.poll_activity_task().await.unwrap();
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: activity_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();

    worker.initiate_shutdown();
    assert_matches!(
        worker.poll_activity_task().await.unwrap_err(),
        PollError::ShutDown
    );
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn test_task_type_nexus_only() {
    let shared_queue = "shared-nexus-queue";

    let t = canned_histories::single_timer("wf-only");
    let wf_cfg = MockPollCfg::from_resp_batches("wf-only", t, [1], mock_worker_client());
    let mut wf_mocks = build_mock_pollers(wf_cfg);
    wf_mocks.worker_cfg(|w| {
        w.task_queue = shared_queue.to_string();
        w.task_types = WorkerTaskTypes::workflow_only();
        w.skip_client_worker_set_check = true;
    });
    let workflow_worker = mock_worker(wf_mocks);

    let mut nex_client = mock_worker_client();
    nex_client
        .expect_complete_nexus_task()
        .returning(|_, _| Ok(RespondNexusTaskCompletedResponse::default()));
    let nex_task = PollNexusTaskQueueResponse {
        task_token: b"nex-task".to_vec(),
        request: Some(NexusRequest {
            header: Default::default(),
            scheduled_time: None,
            variant: Some(temporalio_common::protos::temporal::api::nexus::v1::request::Variant::StartOperation(
                StartOperationRequest {
                    service: "test-service".to_string(),
                    operation: "test-operation".to_string(),
                    request_id: "test-request-id".to_string(),
                    callback: "".to_string(),
                    payload: None,
                    callback_header: Default::default(),
                    links: vec![],
                }
            )),
        }),
        poller_scaling_decision: None,
    };
    let mut nex_mocks =
        MocksHolder::from_client_with_nexus(nex_client, vec![QueueResponse::from(nex_task)]);
    nex_mocks.worker_cfg(|w| {
        w.task_queue = shared_queue.to_string();
        w.task_types = WorkerTaskTypes::nexus_only();
        w.skip_client_worker_set_check = true;
    });
    let nexus_worker = mock_worker(nex_mocks);

    let activation = workflow_worker.poll_workflow_activation().await.unwrap();
    workflow_worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            activation.run_id.clone(),
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();

    let nexus_task = nexus_worker.poll_nexus_task().await.unwrap();
    nexus_worker
        .complete_nexus_task(NexusTaskCompletion {
            task_token: nexus_task.task_token().to_vec(),
            status: Some(temporalio_common::protos::coresdk::nexus::nexus_task_completion::Status::Completed(
                NexusResponse {
                    variant: Some(temporalio_common::protos::temporal::api::nexus::v1::response::Variant::StartOperation(
                        StartOperationResponse {
                            variant: Some(start_operation_response::Variant::SyncSuccess(
                                start_operation_response::Sync {
                                    payload: None,
                                    links: vec![],
                                }
                            )),
                        }
                    )),
                }
            )),
        })
        .await
        .unwrap();

    let workflow_shutdown = workflow_worker.drain_pollers_and_shutdown();
    let nexus_shutdown = async move {
        nexus_worker.initiate_shutdown();
        assert_matches!(
            nexus_worker.poll_nexus_task().await.unwrap_err(),
            PollError::ShutDown
        );
        nexus_worker.shutdown().await;
        nexus_worker.finalize_shutdown().await;
    };

    timeout(Duration::from_secs(5), async {
        tokio::join!(workflow_shutdown, nexus_shutdown);
    })
        .await
        .expect("workers failed to shutdown");
}

#[tokio::test]
async fn test_task_type_workflow_and_activity() {
    let shared_queue = "shared-wf-act-queue";

    let mut client = mock_worker_client();
    client
        .expect_complete_activity_task()
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));

    let t = canned_histories::single_timer("wf-act");
    let wf_cfg = MockPollCfg::from_resp_batches("wf-act", t, [1], client);
    let mut act_task = PollActivityTaskQueueResponse::default();
    act_task.task_token = b"act-task".to_vec();
    act_task.workflow_execution = Some(WorkflowExecution {
        workflow_id: "wf-act".to_string(),
        run_id: "run-id".to_string(),
    });
    act_task.activity_id = "activity".to_string();
    act_task.activity_type = Some(ActivityType {
        name: "activity".to_string(),
    });
    let mut mocks = build_mock_pollers(wf_cfg);
    mocks.set_act_poller_from_resps(vec![QueueResponse::from(act_task)]);
    mocks.worker_cfg(|w| {
        w.task_queue = shared_queue.to_string();
        w.task_types = WorkerTaskTypes {
            enable_workflows: true,
            enable_activities: true,
            enable_nexus: false,
        };
        w.skip_client_worker_set_check = true;
    });
    let worker = mock_worker(mocks);

    let activation = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            activation.run_id.clone(),
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();

    let activity_task = worker.poll_activity_task().await.unwrap();
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: activity_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();

    let shutdown = worker.drain_pollers_and_shutdown();
    timeout(Duration::from_secs(5), shutdown)
        .await
        .expect("worker failed to shutdown");
}

#[tokio::test]
async fn test_task_type_workflow_and_nexus() {
    let shared_queue = "shared-wf-nex-queue";

    let mut client = mock_worker_client();
    client
        .expect_complete_nexus_task()
        .returning(|_, _| Ok(RespondNexusTaskCompletedResponse::default()));

    let t = canned_histories::single_timer("wf-nex");
    let wf_cfg = MockPollCfg::from_resp_batches("wf-nex", t, [1], client);
    let nex_task = PollNexusTaskQueueResponse {
        task_token: b"nex-task".to_vec(),
        request: Some(NexusRequest {
            header: Default::default(),
            scheduled_time: None,
            variant: Some(temporalio_common::protos::temporal::api::nexus::v1::request::Variant::StartOperation(
                StartOperationRequest {
                    service: "test-service".to_string(),
                    operation: "test-operation".to_string(),
                    request_id: "test-request-id".to_string(),
                    callback: "".to_string(),
                    payload: None,
                    callback_header: Default::default(),
                    links: vec![],
                }
            )),
        }),
        poller_scaling_decision: None,
    };
    let mut mocks = build_mock_pollers(wf_cfg);
    mocks.set_nexus_poller_from_resps(vec![QueueResponse::from(nex_task)]);
    mocks.worker_cfg(|w| {
        w.task_queue = shared_queue.to_string();
        w.task_types = WorkerTaskTypes {
            enable_workflows: true,
            enable_activities: false,
            enable_nexus: true,
        };
        w.skip_client_worker_set_check = true;
    });
    let worker = mock_worker(mocks);

    let activation = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            activation.run_id.clone(),
            vec![CompleteWorkflowExecution::default().into()],
        ))
        .await
        .unwrap();

    let nexus_task = worker.poll_nexus_task().await.unwrap();
    worker
        .complete_nexus_task(NexusTaskCompletion {
            task_token: nexus_task.task_token().to_vec(),
            status: Some(temporalio_common::protos::coresdk::nexus::nexus_task_completion::Status::Completed(
                NexusResponse {
                    variant: Some(temporalio_common::protos::temporal::api::nexus::v1::response::Variant::StartOperation(
                        StartOperationResponse {
                            variant: Some(start_operation_response::Variant::SyncSuccess(
                                start_operation_response::Sync {
                                    payload: None,
                                    links: vec![],
                                }
                            )),
                        }
                    )),
                }
            )),
        })
        .await
        .unwrap();

    worker.initiate_shutdown();
    // Drain nexus poller
    assert_matches!(
        worker.poll_nexus_task().await.unwrap_err(),
        PollError::ShutDown
    );
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

#[tokio::test]
async fn test_task_type_activity_and_nexus() {
    let shared_queue = "shared-act-nex-queue";

    let mut client = mock_worker_client();
    client
        .expect_complete_activity_task()
        .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));
    client
        .expect_complete_nexus_task()
        .returning(|_, _| Ok(RespondNexusTaskCompletedResponse::default()));

    let mut act_task = PollActivityTaskQueueResponse::default();
    act_task.task_token = b"act-task".to_vec();
    act_task.workflow_execution = Some(WorkflowExecution {
        workflow_id: "act-nex".to_string(),
        run_id: "run-id".to_string(),
    });
    act_task.activity_id = "activity".to_string();
    act_task.activity_type = Some(ActivityType {
        name: "activity".to_string(),
    });

    let nex_task = PollNexusTaskQueueResponse {
        task_token: b"nex-task".to_vec(),
        request: Some(NexusRequest {
            header: Default::default(),
            scheduled_time: None,
            variant: Some(temporalio_common::protos::temporal::api::nexus::v1::request::Variant::StartOperation(
                StartOperationRequest {
                    service: "test-service".to_string(),
                    operation: "test-operation".to_string(),
                    request_id: "test-request-id".to_string(),
                    callback: "".to_string(),
                    payload: None,
                    callback_header: Default::default(),
                    links: vec![],
                }
            )),
        }),
        poller_scaling_decision: None,
    };

    let mut mocks = MocksHolder::from_client_with_activities(client, vec![QueueResponse::from(act_task)]);
    mocks.set_nexus_poller_from_resps(vec![QueueResponse::from(nex_task)]);
    mocks.worker_cfg(|w| {
        w.task_queue = shared_queue.to_string();
        w.task_types = WorkerTaskTypes {
            enable_workflows: false,
            enable_activities: true,
            enable_nexus: true,
        };
        w.skip_client_worker_set_check = true;
    });
    let worker = mock_worker(mocks);

    let activity_task = worker.poll_activity_task().await.unwrap();
    worker
        .complete_activity_task(ActivityTaskCompletion {
            task_token: activity_task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();

    let nexus_task = worker.poll_nexus_task().await.unwrap();
    worker
        .complete_nexus_task(NexusTaskCompletion {
            task_token: nexus_task.task_token().to_vec(),
            status: Some(temporalio_common::protos::coresdk::nexus::nexus_task_completion::Status::Completed(
                NexusResponse {
                    variant: Some(temporalio_common::protos::temporal::api::nexus::v1::response::Variant::StartOperation(
                        StartOperationResponse {
                            variant: Some(start_operation_response::Variant::SyncSuccess(
                                start_operation_response::Sync {
                                    payload: None,
                                    links: vec![],
                                }
                            )),
                        }
                    )),
                }
            )),
        })
        .await
        .unwrap();

    worker.initiate_shutdown();
    // Drain activity poller
    assert_matches!(
        worker.poll_activity_task().await.unwrap_err(),
        PollError::ShutDown
    );
    // Drain nexus poller
    assert_matches!(
        worker.poll_nexus_task().await.unwrap_err(),
        PollError::ShutDown
    );
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}

// Helper functions for creating consistent test data
fn create_test_activity_task() -> PollActivityTaskQueueResponse {
    PollActivityTaskQueueResponse {
        task_token: b"act-task".to_vec(),
        workflow_execution: Some(WorkflowExecution {
            workflow_id: "test".to_string(),
            run_id: "run-id".to_string(),
        }),
        activity_id: "activity".to_string(),
        activity_type: Some(ActivityType {
            name: "activity".to_string(),
        }),
        ..Default::default()
    }
}

fn create_test_nexus_task() -> PollNexusTaskQueueResponse {
    PollNexusTaskQueueResponse {
        task_token: b"nex-task".to_vec(),
        request: Some(NexusRequest {
            header: Default::default(),
            scheduled_time: None,
            variant: Some(temporalio_common::protos::temporal::api::nexus::v1::request::Variant::StartOperation(
                StartOperationRequest {
                    service: "test-service".to_string(),
                    operation: "test-operation".to_string(),
                    request_id: "test-request-id".to_string(),
                    callback: "".to_string(),
                    payload: None,
                    callback_header: Default::default(),
                    links: vec![],
                }
            )),
        }),
        poller_scaling_decision: None,
    }
}

fn create_test_nexus_completion(task_token: &[u8]) -> NexusTaskCompletion {
    NexusTaskCompletion {
        task_token: task_token.to_vec(),
        status: Some(temporalio_common::protos::coresdk::nexus::nexus_task_completion::Status::Completed(
            NexusResponse {
                variant: Some(temporalio_common::protos::temporal::api::nexus::v1::response::Variant::StartOperation(
                    StartOperationResponse {
                        variant: Some(start_operation_response::Variant::SyncSuccess(
                            start_operation_response::Sync {
                                payload: None,
                                links: vec![],
                            }
                        )),
                    }
                )),
            }
        )),
    }
}

#[rstest::rstest]
// With tasks
#[case::activity_only_with_task(false, true, false, true, "activity-only")]
#[case::nexus_only_with_task(false, false, true, true, "nexus-only")]
#[case::workflow_only_with_task(true, false, false, true, "workflow-only")]
#[case::workflow_and_activity_with_task(true, true, false, true, "workflow-activity")]
#[case::workflow_and_nexus_with_task(true, false, true, true, "workflow-nexus")]
#[case::activity_and_nexus_with_task(false, true, true, true, "activity-nexus")]
// Without tasks (idle worker shutdown)
#[case::activity_only_idle(false, true, false, false, "activity-only-idle")]
#[case::nexus_only_idle(false, false, true, false, "nexus-only-idle")]
#[case::workflow_only_idle(true, false, false, false, "workflow-only-idle")]
#[case::workflow_and_activity_idle(true, true, false, false, "workflow-activity-idle")]
#[case::workflow_and_nexus_idle(true, false, true, false, "workflow-nexus-idle")]
#[case::activity_and_nexus_idle(false, true, true, false, "activity-nexus-idle")]
#[tokio::test]
async fn test_task_type_combinations_unified(
    #[case] enable_workflows: bool,
    #[case] enable_activities: bool,
    #[case] enable_nexus: bool,
    #[case] with_task: bool,
    #[case] queue_name: &str,
) {
    let mut client = mock_worker_client();

    // Setup expectations based on enabled types (only if with_task is true)
    if enable_activities && with_task {
        client
            .expect_complete_activity_task()
            .returning(|_, _| Ok(RespondActivityTaskCompletedResponse::default()));
    }
    if enable_nexus && with_task {
        client
            .expect_complete_nexus_task()
            .returning(|_, _| Ok(RespondNexusTaskCompletedResponse::default()));
    }

    // Build worker based on enabled task types
    let mut mocks = if enable_workflows && with_task {
        // When workflows are enabled AND we have tasks, use build_mock_pollers as the base
        let t = canned_histories::single_timer(queue_name);
        let wf_cfg = MockPollCfg::from_resp_batches(queue_name, t, [1], client);
        let mut mocks = build_mock_pollers(wf_cfg);
        if enable_activities {
            mocks.set_act_poller_from_resps(vec![QueueResponse::from(create_test_activity_task())]);
        }
        if enable_nexus {
            mocks.set_nexus_poller_from_resps(vec![QueueResponse::from(create_test_nexus_task())]);
        }
        mocks
    } else {
        // When workflows are disabled OR idle (no tasks), use from_client_with_custom
        // Provide workflow stream if workflows enabled (but no tasks in it)
        let wft_stream = if enable_workflows {
            Some(stream::empty())
        } else {
            None
        };
        let activity_tasks = if enable_activities && with_task {
            Some(vec![QueueResponse::from(create_test_activity_task())])
        } else {
            None
        };
        let nexus_tasks = if enable_nexus && with_task {
            Some(vec![QueueResponse::from(create_test_nexus_task())])
        } else {
            None
        };
        MocksHolder::from_client_with_custom(client, wft_stream, activity_tasks, nexus_tasks)
    };

    mocks.worker_cfg(|w| {
        w.task_queue = queue_name.to_string();
        w.task_types = WorkerTaskTypes {
            enable_workflows,
            enable_activities,
            enable_nexus,
        };
        w.skip_client_worker_set_check = true;
    });
    let worker = mock_worker(mocks);

    // Poll and complete tasks ONLY if with_task is true
    if with_task {
        if enable_workflows {
            let activation = worker.poll_workflow_activation().await.unwrap();
            worker
                .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                    activation.run_id.clone(),
                    vec![CompleteWorkflowExecution::default().into()],
                ))
                .await
                .unwrap();
        }

        if enable_activities {
            let activity_task = worker.poll_activity_task().await.unwrap();
            worker
                .complete_activity_task(ActivityTaskCompletion {
                    task_token: activity_task.task_token,
                    result: Some(ActivityExecutionResult::ok(vec![1].into())),
                })
                .await
                .unwrap();
        }

        if enable_nexus {
            let nexus_task = worker.poll_nexus_task().await.unwrap();
            worker
                .complete_nexus_task(create_test_nexus_completion(nexus_task.task_token()))
                .await
                .unwrap();
        }
    }

    // Shutdown (works whether tasks were processed or not)
    worker.initiate_shutdown();
    if enable_workflows {
        assert_matches!(
            worker.poll_workflow_activation().await.unwrap_err(),
            PollError::ShutDown
        );
    }
    if enable_activities {
        assert_matches!(
            worker.poll_activity_task().await.unwrap_err(),
            PollError::ShutDown
        );
    }
    if enable_nexus {
        assert_matches!(
            worker.poll_nexus_task().await.unwrap_err(),
            PollError::ShutDown
        );
    }
    worker.shutdown().await;
    worker.finalize_shutdown().await;
}


