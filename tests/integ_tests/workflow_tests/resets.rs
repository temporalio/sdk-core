use crate::integ_tests::activity_functions::echo;
use futures_util::StreamExt;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};
use temporal_client::{WfClientExt, WorkflowClientTrait, WorkflowOptions, WorkflowService};
use temporal_sdk::{LocalActivityOptions, WfContext};
use temporal_sdk_core_protos::{
    coresdk::AsJsonPayloadExt,
    temporal::api::{
        common::v1::WorkflowExecution, workflowservice::v1::ResetWorkflowExecutionRequest,
    },
};
use temporal_sdk_core_test_utils::{CoreWfStarter, NAMESPACE};
use tokio::sync::Notify;

const POST_RESET_SIG: &str = "post-reset";

#[tokio::test]
async fn reset_workflow() {
    let wf_name = "reset_me_wf";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.fetch_results = false;
    let notify = Arc::new(Notify::new());

    let wf_notify = notify.clone();
    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| {
        let notify = wf_notify.clone();
        async move {
            // Make a couple workflow tasks
            ctx.timer(Duration::from_secs(1)).await;
            ctx.timer(Duration::from_secs(1)).await;
            // Tell outer scope to send the reset
            notify.notify_one();
            let _ = ctx
                .make_signal_channel(POST_RESET_SIG)
                .next()
                .await
                .unwrap();
            Ok(().into())
        }
    });

    let run_id = worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    let mut client = starter.get_client().await;
    let client = Arc::make_mut(&mut client);
    let resetter_fut = async {
        notify.notified().await;
        // Do the reset
        client
            .reset_workflow_execution(ResetWorkflowExecutionRequest {
                namespace: NAMESPACE.to_owned(),
                workflow_execution: Some(WorkflowExecution {
                    workflow_id: wf_name.to_owned(),
                    run_id: run_id.clone(),
                }),
                // End of first WFT
                workflow_task_finish_event_id: 4,
                request_id: "test-req-id".to_owned(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Unblock the workflow by sending the signal. Run ID will have changed after reset so
        // we use empty run id
        WorkflowClientTrait::signal_workflow_execution(
            client,
            wf_name.to_owned(),
            "".to_owned(),
            POST_RESET_SIG.to_owned(),
            None,
            None,
        )
        .await
        .unwrap();

        // Wait for the now-reset workflow to finish
        client
            .get_untyped_workflow_handle(wf_name.to_owned(), "")
            .get_workflow_result(Default::default())
            .await
            .unwrap();
        starter.shutdown().await;
    };
    let run_fut = worker.run_until_done();
    let (_, rr) = tokio::join!(resetter_fut, run_fut);
    rr.unwrap();
}

#[tokio::test]
async fn reset_randomseed() {
    let wf_name = "reset_randomseed";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.fetch_results = false;
    let notify = Arc::new(Notify::new());

    const POST_FAIL_SIG: &str = "post-fail";
    static DID_FAIL: AtomicBool = AtomicBool::new(false);
    static RAND_SEED: AtomicU64 = AtomicU64::new(0);

    let wf_notify = notify.clone();
    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| {
        let notify = wf_notify.clone();
        async move {
            let _ = RAND_SEED.compare_exchange(
                0,
                ctx.random_seed(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
            // Make a couple workflow tasks
            ctx.timer(Duration::from_millis(100)).await;
            ctx.timer(Duration::from_millis(100)).await;
            if DID_FAIL
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                // Tell outer scope to send the post-task-failure-signal
                notify.notify_one();
                panic!("Ahh");
            }
            // Make a command that is one thing with the initial seed, but another after reset
            if RAND_SEED.load(Ordering::Relaxed) == ctx.random_seed() {
                ctx.timer(Duration::from_millis(100)).await;
            } else {
                ctx.local_activity(LocalActivityOptions {
                    activity_type: "echo".to_string(),
                    input: "hi!".as_json_payload().expect("serializes fine"),
                    ..Default::default()
                })
                .await;
            }
            // Wait for the post-task-fail signal
            let _ = ctx.make_signal_channel(POST_FAIL_SIG).next().await.unwrap();
            // Tell outer scope to send the reset
            notify.notify_one();
            let _ = ctx
                .make_signal_channel(POST_RESET_SIG)
                .next()
                .await
                .unwrap();
            Ok(().into())
        }
    });
    worker.register_activity("echo", echo);

    let run_id = worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    let mut client = starter.get_client().await;
    let client = Arc::make_mut(&mut client);
    let client_fur = async {
        notify.notified().await;
        WorkflowClientTrait::signal_workflow_execution(
            client,
            wf_name.to_owned(),
            run_id.clone(),
            POST_FAIL_SIG.to_string(),
            None,
            None,
        )
        .await
        .unwrap();
        notify.notified().await;
        // Reset the workflow to be after first timer has fired
        client
            .reset_workflow_execution(ResetWorkflowExecutionRequest {
                namespace: NAMESPACE.to_owned(),
                workflow_execution: Some(WorkflowExecution {
                    workflow_id: wf_name.to_owned(),
                    run_id: run_id.clone(),
                }),
                workflow_task_finish_event_id: 14,
                request_id: "test-req-id".to_owned(),
                ..Default::default()
            })
            .await
            .unwrap();

        // Unblock the workflow by sending the signal. Run ID will have changed after reset so
        // we use empty run id
        WorkflowClientTrait::signal_workflow_execution(
            client,
            wf_name.to_owned(),
            "".to_owned(),
            POST_RESET_SIG.to_owned(),
            None,
            None,
        )
        .await
        .unwrap();

        // Wait for the now-reset workflow to finish
        client
            .get_untyped_workflow_handle(wf_name.to_owned(), "")
            .get_workflow_result(Default::default())
            .await
            .unwrap();
        starter.shutdown().await;
    };
    let run_fut = worker.run_until_done();
    let (_, rr) = tokio::join!(client_fur, run_fut);
    rr.unwrap();
}
