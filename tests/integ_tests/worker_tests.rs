use crate::{
    common::{CoreWfStarter, get_integ_server_options, get_integ_telem_options, mock_sdk_cfg},
    shared_tests,
};
use assert_matches::assert_matches;
use futures_util::FutureExt;
use std::{
    cell::Cell,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering::Relaxed},
    },
    time::Duration,
};
use temporal_client::WorkflowOptions;
use temporal_sdk::{ActivityOptions, WfContext, interceptors::WorkerInterceptor};
use temporal_sdk_core::{
    CoreRuntime, ResourceBasedTuner, ResourceSlotOptions, init_worker,
    test_help::{
        FakeWfResponses, MockPollCfg, ResponseType, TEST_Q, build_mock_pollers,
        drain_pollers_and_shutdown, hist_to_poll_resp, mock_worker, mock_worker_client,
    },
};
use temporal_sdk_core_api::{
    Worker,
    errors::WorkerValidationError,
    worker::{PollerBehavior, WorkerConfigBuilder, WorkerVersioningStrategy},
};
use temporal_sdk_core_protos::{
    DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder, canned_histories,
    coresdk::{
        ActivityTaskCompletion,
        activity_result::ActivityExecutionResult,
        workflow_completion::{
            Failure, WorkflowActivationCompletion, workflow_activation_completion::Status,
        },
    },
    temporal::api::{
        command::v1::command::Attributes,
        common::v1::WorkerVersionStamp,
        enums::v1::{
            EventType, WorkflowTaskFailedCause, WorkflowTaskFailedCause::GrpcMessageTooLarge,
        },
        failure::v1::Failure as InnerFailure,
        history::v1::{
            ActivityTaskScheduledEventAttributes, history_event,
            history_event::Attributes::{
                self as EventAttributes, WorkflowTaskFailedEventAttributes,
            },
        },
        workflowservice::v1::{
            GetWorkflowExecutionHistoryResponse, PollActivityTaskQueueResponse,
            RespondActivityTaskCompletedResponse,
        },
    },
};
use tokio::sync::{Barrier, Notify, Semaphore};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
async fn worker_validation_fails_on_nonexistent_namespace() {
    let opts = get_integ_server_options();
    let runtime = CoreRuntime::new_assume_tokio(get_integ_telem_options()).unwrap();
    let retrying_client = opts
        .connect_no_namespace(runtime.telemetry().get_temporal_metric_meter())
        .await
        .unwrap();

    let worker = init_worker(
        &runtime,
        WorkerConfigBuilder::default()
            .namespace("i_dont_exist")
            .task_queue("Wheee!")
            .versioning_strategy(WorkerVersioningStrategy::None {
                build_id: "blah".to_owned(),
            })
            .build()
            .unwrap(),
        retrying_client,
    )
    .unwrap();

    let res = worker.validate().await;
    assert_matches!(
        res,
        Err(WorkerValidationError::NamespaceDescribeError { .. })
    );
}

#[tokio::test]
async fn worker_handles_unknown_workflow_types_gracefully() {
    let wf_type = "worker_handles_unknown_workflow_types_gracefully";
    let mut starter = CoreWfStarter::new(wf_type);
    let mut worker = starter.worker().await;

    let run_id = worker
        .submit_wf(
            format!("wce-{}", Uuid::new_v4()),
            "unregistered".to_string(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    struct GracefulAsserter {
        notify: Arc<Notify>,
        run_id: String,
        unregistered_failure_seen: Cell<bool>,
    }
    #[async_trait::async_trait(?Send)]
    impl WorkerInterceptor for GracefulAsserter {
        async fn on_workflow_activation_completion(
            &self,
            completion: &WorkflowActivationCompletion,
        ) {
            if matches!(
                completion,
                WorkflowActivationCompletion {
                    status: Some(Status::Failed(Failure {
                        failure: Some(InnerFailure { message, .. }),
                        ..
                    })),
                    run_id,
                } if message == "Workflow type unregistered not found" && *run_id == self.run_id
            ) {
                self.unregistered_failure_seen.set(true);
            }
            // If we've seen the failure, and the completion is a success for the same run, we're done
            if matches!(
                completion,
                WorkflowActivationCompletion {
                    status: Some(Status::Successful(..)),
                    run_id,
                } if self.unregistered_failure_seen.get() && *run_id == self.run_id
            ) {
                // Shutdown the worker
                self.notify.notify_one();
            }
        }
        fn on_shutdown(&self, _: &temporal_sdk::Worker) {}
    }

    let inner = worker.inner_mut();
    let notify = Arc::new(Notify::new());
    inner.set_worker_interceptor(GracefulAsserter {
        notify: notify.clone(),
        run_id,
        unregistered_failure_seen: Cell::new(false),
    });
    tokio::join!(async { inner.run().await.unwrap() }, async move {
        notify.notified().await;
        let worker = starter.get_worker().await.clone();
        drain_pollers_and_shutdown(&worker).await;
    });
}

#[tokio::test]
async fn resource_based_few_pollers_guarantees_non_sticky_poll() {
    let wf_name = "resource_based_few_pollers_guarantees_non_sticky_poll";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .worker_config
        .clear_max_outstanding_opts()
        .no_remote_activities(true)
        // 3 pollers so the minimum slots of 2 can both be handed out to a sticky poller
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(3_usize));
    // Set the limits to zero so it's essentially unwilling to hand out slots
    let mut tuner = ResourceBasedTuner::new(0.0, 0.0);
    tuner.with_workflow_slots_options(ResourceSlotOptions::new(2, 10, Duration::from_millis(0)));
    starter.worker_config.tuner(Arc::new(tuner));
    let mut worker = starter.worker().await;

    // Workflow doesn't actually need to do anything. We just need to see that we don't get stuck
    // by assigning all slots to sticky pollers.
    worker.register_wf(
        wf_name.to_owned(),
        |_: WfContext| async move { Ok(().into()) },
    );
    for i in 0..20 {
        worker
            .submit_wf(
                format!("{wf_name}_{i}"),
                wf_name.to_owned(),
                vec![],
                WorkflowOptions::default(),
            )
            .await
            .unwrap();
    }
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn oversize_grpc_message() {
    let wf_name = "oversize_grpc_message";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut core = starter.worker().await;

    static OVERSIZE_GRPC_MESSAGE_RUN: AtomicBool = AtomicBool::new(false);
    core.register_wf(wf_name.to_owned(), |_ctx: WfContext| async move {
        if OVERSIZE_GRPC_MESSAGE_RUN.load(Relaxed) {
            Ok(vec![].into())
        } else {
            OVERSIZE_GRPC_MESSAGE_RUN.store(true, Relaxed);
            let result: Vec<u8> = vec![0; 5000000];
            Ok(result.into())
        }
    });
    starter.start_with_worker(wf_name, &mut core).await;
    core.run_until_done().await.unwrap();

    assert!(starter.get_history().await.events.iter().any(|e| {
        e.event_type == EventType::WorkflowTaskFailed as i32
            && if let WorkflowTaskFailedEventAttributes(attr) = e.attributes.as_ref().unwrap() {
                attr.cause == GrpcMessageTooLarge as i32
                    && attr.failure.as_ref().unwrap().message == "GRPC Message too large"
            } else {
                false
            }
    }))
}

#[tokio::test]
async fn grpc_message_too_large_test() {
    shared_tests::grpc_message_too_large().await
}

#[tokio::test]
async fn activity_tasks_from_completion_reserve_slots() {
    let wf_id = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let schedid = t.add(EventAttributes::ActivityTaskScheduledEventAttributes(
        ActivityTaskScheduledEventAttributes {
            activity_id: "1".to_string(),
            activity_type: Some("act1".into()),
            ..Default::default()
        },
    ));
    let startid = t.add_activity_task_started(schedid);
    t.add_activity_task_completed(schedid, startid, b"hi".into());
    t.add_full_wf_task();
    let schedid = t.add(EventAttributes::ActivityTaskScheduledEventAttributes(
        ActivityTaskScheduledEventAttributes {
            activity_id: "2".to_string(),
            activity_type: Some("act2".into()),
            ..Default::default()
        },
    ));
    let startid = t.add_activity_task_started(schedid);
    t.add_activity_task_completed(schedid, startid, b"hi".into());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock = mock_worker_client();
    // Set up two tasks to be returned via normal activity polling
    let act_tasks = vec![
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
    ];
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
    mh.completion_mock_fn = Some(Box::new(|wftc| {
        // Make sure when we see the completion with the schedule act command that it does
        // not have the eager execution flag set the first time, and does the second.
        if let Some(Attributes::ScheduleActivityTaskCommandAttributes(attrs)) = wftc
            .commands
            .first()
            .and_then(|cmd| cmd.attributes.as_ref())
        {
            if attrs.activity_id == "1" {
                assert!(!attrs.request_eager_execution);
            } else {
                assert!(attrs.request_eager_execution);
            }
        }
        Ok(Default::default())
    }));
    mh.activity_responses = Some(act_tasks);
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|cfg| {
        cfg.max_cached_workflows = 2;
        cfg.max_outstanding_activities = Some(2);
    });
    let core = Arc::new(mock_worker(mock));
    let mut worker = crate::common::TestWorker::new(core.clone(), TEST_Q.to_string());

    // First poll for activities twice, occupying both slots
    let at1 = core.poll_activity_task().await.unwrap();
    let at2 = core.poll_activity_task().await.unwrap();
    let workflow_complete_token = CancellationToken::new();
    let workflow_complete_token_clone = workflow_complete_token.clone();

    worker.register_wf(DEFAULT_WORKFLOW_TYPE, move |ctx: WfContext| {
        let complete_token = workflow_complete_token.clone();
        async move {
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
            complete_token.cancel();
            Ok(().into())
        }
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
        // Wait for workflow to complete in order for all eager activities to be requested before shutting down.
        // After shutdown, no eager activities slots can be allocated.
        workflow_complete_token_clone.cancelled().await;
        core.initiate_shutdown();
        // Even though this test requests eager activity tasks, none are returned in poll responses.
        let err = core.poll_activity_task().await.unwrap_err();
        assert_matches!(err, temporal_sdk_core_api::errors::PollError::ShutDown);
    };
    // This wf poll should *not* set the flag that it wants tasks back since both slots are
    // occupied
    let run_fut = async { worker.run_until_done().await.unwrap() };
    tokio::join!(run_fut, act_completer);
}

#[tokio::test]
async fn max_wft_respected() {
    let total_wfs = 100;
    let wf_ids: Vec<_> = (0..total_wfs).map(|i| format!("fake-wf-{i}")).collect();
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
        cfg.max_outstanding_workflow_tasks = Some(1);
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

#[rstest]
#[tokio::test]
async fn history_length_with_fail_and_timeout(
    #[values(true, false)] use_cache: bool,
    #[values(1, 2, 3)] history_responses_case: u8,
) {
    if !use_cache && history_responses_case == 3 {
        eprintln!(
            "Skipping history_length_with_fail_and_timeout::use_cache_2_false::history_responses_case_3_3 due to flaky hang"
        );
        return;
    }
    let wfid = "fake_wf_id";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_failed_with_failure(WorkflowTaskFailedCause::Unspecified, "ahh".into());
    t.add_workflow_task_scheduled_and_started();
    t.add_workflow_task_timed_out();
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_client = mock_worker_client();
    let history_responses = match history_responses_case {
        1 => vec![ResponseType::AllHistory],
        2 => vec![
            ResponseType::ToTaskNum(1),
            ResponseType::ToTaskNum(2),
            ResponseType::AllHistory,
        ],
        3 => {
            let mut needs_fetch = hist_to_poll_resp(&t, wfid, ResponseType::ToTaskNum(2)).resp;
            needs_fetch.next_page_token = vec![1];
            // Truncate the history a bit in order to force incomplete WFT
            needs_fetch.history.as_mut().unwrap().events.truncate(6);
            let needs_fetch_resp = ResponseType::Raw(needs_fetch);
            let mut empty_fetch_resp: GetWorkflowExecutionHistoryResponse =
                t.get_history_info(1).unwrap().into();
            empty_fetch_resp.history.as_mut().unwrap().events = vec![];
            mock_client
                .expect_get_workflow_execution_history()
                .returning(move |_, _, _| Ok(empty_fetch_resp.clone()))
                .times(1);
            vec![
                ResponseType::ToTaskNum(1),
                needs_fetch_resp,
                ResponseType::ToTaskNum(2),
                ResponseType::AllHistory,
            ]
        }
        _ => unreachable!(),
    };

    let mut mh = MockPollCfg::from_resp_batches(wfid, t, history_responses, mock_client);
    if history_responses_case == 3 {
        // Expect the failed pagination fetch
        mh.num_expected_fails = 1;
    }
    let mut worker = mock_sdk_cfg(mh, |wc| {
        if use_cache {
            wc.max_cached_workflows = 1;
        }
    });
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, |ctx: WfContext| async move {
        assert_eq!(ctx.history_length(), 3);
        ctx.timer(Duration::from_secs(1)).await;
        assert_eq!(ctx.history_length(), 14);
        ctx.timer(Duration::from_secs(1)).await;
        assert_eq!(ctx.history_length(), 19);
        Ok(().into())
    });
    worker
        .submit_wf(
            wfid.to_owned(),
            DEFAULT_WORKFLOW_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[allow(deprecated)]
#[tokio::test]
async fn sets_build_id_from_wft_complete() {
    let wfid = "fake_wf_id";

    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "1".to_string());
    t.add_full_wf_task();
    t.modify_event(t.current_event_id(), |he| {
        if let history_event::Attributes::WorkflowTaskCompletedEventAttributes(a) =
            he.attributes.as_mut().unwrap()
        {
            a.worker_version = Some(WorkerVersionStamp {
                build_id: "enchi-cat".to_string(),
                ..Default::default()
            });
        }
    });
    let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
    t.add_timer_fired(timer_started_event_id, "2".to_string());
    t.add_workflow_task_scheduled_and_started();

    let mock = mock_worker_client();
    let mut worker = mock_sdk_cfg(
        MockPollCfg::from_resp_batches(wfid, t, [ResponseType::AllHistory], mock),
        |cfg| {
            cfg.versioning_strategy = WorkerVersioningStrategy::None {
                build_id: "fierce-predator".to_string(),
            };
            cfg.max_cached_workflows = 1;
        },
    );

    worker.register_wf(DEFAULT_WORKFLOW_TYPE, |ctx: WfContext| async move {
        // First task, it should be empty, since replaying and nothing in first WFT completed
        assert_eq!(ctx.current_deployment_version(), None);
        ctx.timer(Duration::from_secs(1)).await;
        assert_eq!(
            ctx.current_deployment_version().unwrap().build_id,
            "enchi-cat"
        );
        ctx.timer(Duration::from_secs(1)).await;
        // Not replaying at this point, so we should see the worker's build id
        assert_eq!(
            ctx.current_deployment_version().unwrap().build_id,
            "fierce-predator"
        );
        ctx.timer(Duration::from_secs(1)).await;
        assert_eq!(
            ctx.current_deployment_version().unwrap().build_id,
            "fierce-predator"
        );
        Ok(().into())
    });
    worker
        .submit_wf(wfid, DEFAULT_WORKFLOW_TYPE, vec![], Default::default())
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}
