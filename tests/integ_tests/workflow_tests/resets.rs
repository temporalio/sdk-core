use futures::StreamExt;
use std::{sync::Arc, time::Duration};
use temporal_client::{WfClientExt, WorkflowClientTrait, WorkflowOptions, WorkflowService};
use temporal_sdk::WfContext;
use temporal_sdk_core_protos::temporal::api::{
    common::v1::WorkflowExecution, workflowservice::v1::ResetWorkflowExecutionRequest,
};
use temporal_sdk_core_test_utils::{CoreWfStarter, NAMESPACE};
use tokio::sync::Notify;

const POST_RESET_SIG: &str = "post-reset";

#[tokio::test]
async fn reset_workflow() {
    let wf_name = "reset_me_wf";
    let mut starter = CoreWfStarter::new(wf_name);
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
