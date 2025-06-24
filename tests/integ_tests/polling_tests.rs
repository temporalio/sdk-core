use crate::integ_tests::activity_functions::echo;
use assert_matches::assert_matches;
use std::{sync::Arc, time::Duration};
use temporal_client::{WfClientExt, WorkflowClientTrait, WorkflowOptions};
use temporal_sdk::{ActivityOptions, WfContext};
use temporal_sdk_core::{
    ClientOptionsBuilder, ephemeral_server::TemporalDevServerConfigBuilder, init_worker,
};
use temporal_sdk_core_api::{Worker, worker::PollerBehavior};
use temporal_sdk_core_protos::{
    coresdk::{
        AsJsonPayloadExt, IntoCompletion,
        activity_task::activity_task as act_task,
        workflow_activation::{FireTimer, WorkflowActivationJob, workflow_activation_job},
        workflow_commands::{ActivityCancellationType, RequestCancelActivity, StartTimer},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::enums::v1::EventType,
};
use temporal_sdk_core_test_utils::{
    CoreWfStarter, WorkerTestHelpers, default_cached_download, drain_pollers_and_shutdown,
    init_core_and_create_wf, init_integ_telem, integ_worker_config, schedule_activity_cmd,
};
use tokio::time::timeout;
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
    let server_config = TemporalDevServerConfigBuilder::default()
        .exe(default_cached_download())
        // We need to lower the poll timeout so the poll call rolls over
        .extra_args(vec![
            "--dynamic-config-value".to_string(),
            "matching.longPollExpirationInterval=\"1s\"".to_string(),
        ])
        .build()
        .unwrap();
    let mut server1 = server_config.start_server().await.unwrap();
    let mut server2 = server_config.start_server().await.unwrap();

    // Connect clients to both servers
    info!("Connecting clients");
    let mut client_common_config = ClientOptionsBuilder::default();
    client_common_config
        .identity("integ_tester".to_owned())
        .client_name("temporal-core".to_owned())
        .client_version("0.1.0".to_owned());
    let client1 = client_common_config
        .clone()
        .target_url(Url::parse(&format!("http://{}", server1.target)).unwrap())
        .build()
        .unwrap()
        .connect("default", None)
        .await
        .unwrap();
    let client2 = client_common_config
        .clone()
        .target_url(Url::parse(&format!("http://{}", server2.target)).unwrap())
        .build()
        .unwrap()
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
    let worker = init_worker(
        init_integ_telem().unwrap(),
        integ_worker_config("my-task-queue")
            // We want a cache so we don't get extra remove-job activations
            .max_cached_workflows(100_usize)
            .build()
            .unwrap(),
        client1.clone(),
    )
    .unwrap();

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
    worker.replace_client(client2.get_client().inner().clone());
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
    server1.shutdown().await.unwrap();
    server2.shutdown().await.unwrap();
}

#[rstest::rstest]
#[tokio::test]
async fn small_workflow_slots_and_pollers(#[values(false, true)] use_autoscaling: bool) {
    let wf_name = "only_one_workflow_slot_and_two_pollers";
    let mut starter = CoreWfStarter::new(wf_name);
    if use_autoscaling {
        starter
            .worker_config
            .workflow_task_poller_behavior(PollerBehavior::Autoscaling {
                minimum: 1,
                maximum: 5,
                initial: 1,
            });
    } else {
        starter
            .worker_config
            .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(2));
    }
    starter
        .worker_config
        .max_outstanding_workflow_tasks(2_usize)
        .max_outstanding_local_activities(1_usize)
        .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(1))
        .max_outstanding_activities(1_usize);
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
