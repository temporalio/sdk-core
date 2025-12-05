use crate::{
    common::{
        CoreWfStarter, INTEG_CLIENT_NAME, INTEG_CLIENT_VERSION, get_integ_server_options,
        init_core_and_create_wf, init_integ_telem, integ_dev_server_config, integ_worker_config,
    },
    integ_tests::activity_functions::echo,
};
use assert_matches::assert_matches;
use futures_util::{FutureExt, StreamExt, future::join_all};
use std::{
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use temporalio_client::{WfClientExt, WorkflowClientTrait, WorkflowOptions};
use temporalio_common::{
    Worker, prost_dur,
    protos::{
        coresdk::{
            AsJsonPayloadExt, IntoCompletion,
            activity_task::activity_task as act_task,
            workflow_activation::{FireTimer, WorkflowActivationJob, workflow_activation_job},
            workflow_commands::{ActivityCancellationType, RequestCancelActivity, StartTimer},
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::enums::v1::EventType,
        test_utils::schedule_activity_cmd,
    },
    telemetry::{Logger, TelemetryOptions},
    worker::PollerBehavior,
};
use temporalio_sdk::{ActivityOptions, WfContext};
use temporalio_sdk_core::{
    ClientOptions, CoreRuntime, RuntimeOptions,
    ephemeral_server::{TemporalDevServerConfig, default_cached_download},
    init_worker,
    telemetry::CoreLogStreamConsumer,
    test_help::{NAMESPACE, WorkerTestHelpers, drain_pollers_and_shutdown},
};
use tokio::{sync::Notify, time::timeout};
use tracing::info;
use url::Url;

#[tokio::test]
async fn out_of_order_completion_doesnt_hang() {
    let mut starter = init_core_and_create_wf("out_of_order_completion_doesnt_hang").await;
    let core = starter.get_worker().await;
    let task_q = starter.get_task_queue();
    let activity_id = "act-1";
    let task = core.poll_workflow_activation().await.unwrap();
    // Complete workflow task and schedule activity and a timer that fires immediately
    core.complete_workflow_activation(
        vec![
            schedule_activity_cmd(
                0,
                task_q,
                activity_id,
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
                Duration::from_secs(60),
            ),
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(prost_dur!(from_millis(50))),
            }
            .into(),
        ]
        .into_completion(task.run_id),
    )
    .await
    .unwrap();
    // Poll activity and verify that it's been scheduled, we don't expect to complete it in this
    // test as activity is try-cancelled.
    let activity_task = core.poll_activity_task().await.unwrap();
    assert_matches!(activity_task.variant, Some(act_task::Variant::Start(_)));
    // Poll workflow task and verify that activity has failed.
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::FireTimer(
                    FireTimer { seq: t_seq }
                )),
            },
        ] => {
            assert_eq!(*t_seq, 1);
        }
    );

    // Start polling again *before* we complete the WFT
    let cc = core.clone();
    let jh = tokio::spawn(async move {
        // We want to fail the test if this takes too long -- we should not hit long poll timeout
        let task = timeout(Duration::from_secs(1), cc.poll_workflow_activation())
            .await
            .expect("Poll should come back right away")
            .unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        cc.complete_execution(&task.run_id).await;
    });

    tokio::time::sleep(Duration::from_millis(100)).await;
    // Then complete the (last) WFT with a request to cancel the AT, which should produce a
    // pending activation, unblocking the (already started) poll
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![RequestCancelActivity { seq: 0 }.into()],
    ))
    .await
    .unwrap();

    jh.await.unwrap();
}

#[tokio::test]
async fn switching_worker_client_changes_poll() {
    // Start two servers
    info!("Starting servers");
    let server_config = TemporalDevServerConfig::builder()
        .exe(default_cached_download())
        // We need to lower the poll timeout so the poll call rolls over
        .extra_args(vec![
            "--dynamic-config-value".to_string(),
            "matching.longPollExpirationInterval=\"1s\"".to_string(),
        ])
        .build();
    let mut server1 = server_config
        .start_server_with_output(Stdio::null(), Stdio::null())
        .await
        .unwrap();
    let mut server2 = server_config
        .start_server_with_output(Stdio::null(), Stdio::null())
        .await
        .unwrap();

    let result = std::panic::AssertUnwindSafe(async {
        // Connect clients to both servers
        info!("Connecting clients");
        let client1 = ClientOptions::builder()
            .identity("integ_tester".to_owned())
            .client_name("temporal-core".to_owned())
            .client_version("0.1.0".to_owned())
            .target_url(Url::parse(&format!("http://{}", server1.target)).unwrap())
            .build()
            .connect("default", None)
            .await
            .unwrap();
        let client2 = ClientOptions::builder()
            .identity("integ_tester".to_owned())
            .client_name("temporal-core".to_owned())
            .client_version("0.1.0".to_owned())
            .target_url(Url::parse(&format!("http://{}", server2.target)).unwrap())
            .build()
            .connect("default", None)
            .await
            .unwrap();

        // Start a workflow on both servers
        info!("Starting workflows");
        let wf1 = client1
            .start_workflow(
                vec![],
                "my-task-queue".to_owned(),
                "my-workflow-1".to_owned(),
                "my-workflow-type".to_owned(),
                None,
                WorkflowOptions::default(),
            )
            .await
            .unwrap();
        let wf2 = client2
            .start_workflow(
                vec![],
                "my-task-queue".to_owned(),
                "my-workflow-2".to_owned(),
                "my-workflow-type".to_owned(),
                None,
                WorkflowOptions::default(),
            )
            .await
            .unwrap();

        // Create a worker only on the first server
        let mut config = integ_worker_config("my-task-queue");
        // We want a cache so we don't get extra remove-job activations
        config.max_cached_workflows = 100_usize;
        let worker = init_worker(init_integ_telem().unwrap(), config, client1.clone()).unwrap();

        // Poll for first task, confirm it's first wf, complete, and wait for complete
        info!("Doing initial poll");
        let act1 = worker.poll_workflow_activation().await.unwrap();
        assert_eq!(wf1.run_id, act1.run_id);
        worker.complete_execution(&act1.run_id).await;
        worker.handle_eviction().await;
        info!("Waiting on first workflow complete");
        client1
            .get_untyped_workflow_handle("my-workflow-1", wf1.run_id)
            .get_workflow_result(Default::default())
            .await
            .unwrap();

        // Swap client, poll for next task, confirm it's second wf, and respond w/ empty
        info!("Replacing client and polling again");
        worker
            .replace_client(client2.get_client().inner().clone())
            .unwrap();
        let act2 = worker.poll_workflow_activation().await.unwrap();
        assert_eq!(wf2.run_id, act2.run_id);
        worker.complete_execution(&act2.run_id).await;
        worker.handle_eviction().await;
        info!("Waiting on second workflow complete");
        client2
            .get_untyped_workflow_handle("my-workflow-2", wf2.run_id)
            .get_workflow_result(Default::default())
            .await
            .unwrap();

        // Shutdown workers and servers
        drain_pollers_and_shutdown(&(Arc::new(worker) as Arc<dyn Worker>)).await;
    })
    .catch_unwind()
    .await;

    let shutdown_results = join_all([server1.shutdown(), server2.shutdown()]).await;
    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
    for r in shutdown_results {
        r.unwrap();
    }
}

#[rstest::rstest]
#[tokio::test]
async fn small_workflow_slots_and_pollers(#[values(false, true)] use_autoscaling: bool) {
    let wf_name = "only_one_workflow_slot_and_two_pollers";
    let mut starter = CoreWfStarter::new(wf_name);
    if use_autoscaling {
        starter.worker_config.workflow_task_poller_behavior = PollerBehavior::Autoscaling {
            minimum: 1,
            maximum: 5,
            initial: 1,
        };
    } else {
        starter.worker_config.workflow_task_poller_behavior = PollerBehavior::SimpleMaximum(2);
    }
    starter.worker_config.max_outstanding_workflow_tasks = Some(2_usize);
    starter.worker_config.max_outstanding_local_activities = Some(1_usize);
    starter.worker_config.activity_task_poller_behavior = PollerBehavior::SimpleMaximum(1);
    starter.worker_config.max_outstanding_activities = Some(1_usize);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        for _ in 0..3 {
            ctx.activity(ActivityOptions {
                activity_type: "echo_activity".to_string(),
                start_to_close_timeout: Some(Duration::from_secs(5)),
                input: "hi!".as_json_payload().expect("serializes fine"),
                ..Default::default()
            })
            .await;
        }
        Ok(().into())
    });
    worker.register_activity("echo_activity", echo);
    worker
        .submit_wf(
            starter.get_task_queue(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    let wf2id = format!("{}-2", starter.get_task_queue());
    worker
        .submit_wf(
            wf2id.clone(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    // If we don't fail the workflow on nondeterminism, we'll get stuck here retrying the WFT
    worker.run_until_done().await.unwrap();
    // Verify no task timeouts happened
    let history = starter.get_history().await;
    let any_task_timeouts = history
        .events
        .iter()
        .any(|e| e.event_type() == EventType::WorkflowTaskTimedOut);
    assert!(!any_task_timeouts);
    let history = starter
        .get_client()
        .await
        .get_workflow_execution_history(wf2id, None, vec![])
        .await
        .unwrap()
        .history
        .unwrap();
    let any_task_timeouts = history
        .events
        .iter()
        .any(|e| e.event_type() == EventType::WorkflowTaskTimedOut);
    assert!(!any_task_timeouts);
}

#[tokio::test]
async fn replace_client_works_after_polling_failure() {
    let (log_consumer, mut log_rx) = CoreLogStreamConsumer::new(100);
    let telem_opts = TelemetryOptions::builder()
        .logging(Logger::Push {
            filter: "OFF,temporalio_client=DEBUG".into(),
            consumer: Arc::new(log_consumer),
        })
        .build();
    let runtime_opts = RuntimeOptions::builder()
        .telemetry_options(telem_opts)
        .build()
        .unwrap();
    let rt = Arc::new(CoreRuntime::new_assume_tokio(runtime_opts).unwrap());

    // Spawning background task to read logs and notify the test when polling failure occurs.
    let look_for_poll_failure_log = Arc::new(AtomicBool::new(false));
    let poll_retry_log_found = Arc::new(Notify::new());
    let log_reader_join_handle = tokio::spawn({
        let look_for_poll_retry_log = look_for_poll_failure_log.clone();
        let poll_retry_log_found = poll_retry_log_found.clone();
        async move {
            let mut enabled = false;
            loop {
                let Some(log) = log_rx.next().await else {
                    break;
                };
                if !enabled {
                    enabled = look_for_poll_retry_log.load(Ordering::Acquire);
                }
                if enabled
                    && (log
                        .message
                        .starts_with("gRPC call poll_workflow_task_queue failed")
                        || log
                            .message
                            .starts_with("gRPC call poll_workflow_task_queue retried"))
                {
                    poll_retry_log_found.notify_one();
                    break;
                }
            }
        }
    });
    let abort_handles = Arc::new(Mutex::new(vec![log_reader_join_handle.abort_handle()]));

    // Starting a second dev server for the worker to connect to initially. Later this server will be shut down
    // and the worker client replaced with a client connected to the main integration test server.
    let initial_server_config = integ_dev_server_config(vec![], false);
    let initial_server = Arc::new(Mutex::new(Some(
        initial_server_config
            .start_server_with_output(Stdio::null(), Stdio::null())
            .await
            .unwrap(),
    )));

    let result = {
        let initial_server = initial_server.clone();
        let abort_handles = abort_handles.clone();
        std::panic::AssertUnwindSafe(async move {
            let initial_server_target = format!(
                "http://{}",
                initial_server.lock().unwrap().as_ref().unwrap().target
            );
            let client_for_initial_server = ClientOptions::builder()
                .identity("client_for_initial_server".to_string())
                .target_url(Url::parse(&initial_server_target).unwrap())
                .client_name(INTEG_CLIENT_NAME.to_string())
                .client_version(INTEG_CLIENT_VERSION.to_string())
                .build()
                .connect(NAMESPACE, rt.telemetry().get_temporal_metric_meter())
                .await
                .unwrap();

            let wf_name = "replace_client_works_after_polling_failure";
            let task_queue = format!("{wf_name}_tq");

            let mut config = integ_worker_config(&task_queue);
            config.max_cached_workflows = 100_usize;
            let worker =
                Arc::new(init_worker(&rt, config, client_for_initial_server.clone()).unwrap());

            // Polling the initial server the first time is successful.
            let wf_1 = client_for_initial_server
                .start_workflow(
                    vec![],
                    task_queue.clone(),
                    wf_name.into(),
                    wf_name.into(),
                    None,
                    WorkflowOptions::default(),
                )
                .await
                .unwrap();
            let act_1 =
                tokio::time::timeout(Duration::from_secs(60), worker.poll_workflow_activation())
                    .await
                    .unwrap()
                    .unwrap();
            assert_eq!(act_1.run_id, wf_1.run_id);

            // Initial server is shut down.
            let mut server = initial_server.lock().unwrap().take().unwrap();
            server.shutdown().await.unwrap();

            // Start polling in a background task.
            look_for_poll_failure_log.store(true, Ordering::Release);
            let poll_join_handle = tokio::spawn({
                let worker = worker.clone();
                async move { worker.poll_workflow_activation().await }
            });
            abort_handles
                .try_lock()
                .unwrap()
                .push(poll_join_handle.abort_handle());

            // Wait until polling failure is detected.
            tokio::time::timeout(Duration::from_secs(60), poll_retry_log_found.notified())
                .await
                .unwrap();

            // Start a new WF on main integration server.
            let client_for_integ_server = get_integ_server_options()
                .connect(NAMESPACE, rt.telemetry().get_temporal_metric_meter())
                .await
                .unwrap();
            let wf_2 = client_for_integ_server
                .start_workflow(
                    vec![],
                    task_queue,
                    wf_name.into(),
                    wf_name.into(),
                    None,
                    WorkflowOptions {
                        execution_timeout: Some(Duration::from_secs(60)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();

            // Switch worker over to the main integration server.
            // The polling started on the initial server should complete with a task from the new server.
            worker.replace_client(client_for_integ_server).unwrap();
            let act_2 = tokio::time::timeout(Duration::from_secs(60), poll_join_handle)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(act_2.run_id, wf_2.run_id);
        })
    }
    .catch_unwind()
    .await;

    // Cleaning up spawned background tasks if they're still running.
    for handle in &*abort_handles.lock().unwrap() {
        handle.abort();
    }

    // If the test panicked, we may or may not need to shut down the server here.
    // If the test succeeded, the server should always be shut down by this point.
    let server = initial_server.lock().unwrap().take();
    if let Some(mut server) = server {
        let _ = server.shutdown().await;
        assert_matches!(
            result,
            Err(_),
            "Server should have been shut down during the test"
        );
    }

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}
