use temporal_client::{GetWorkflowResultOpts, WfClientExt, WorkflowOptions};
use temporal_sdk::{WfContext, WorkflowResult};
use temporal_sdk_core_protos::coresdk::{FromJsonPayloadExt, common::NamespacedWorkflowExecution};
use temporal_sdk_core_test_utils::CoreWfStarter;

const RECEIVER_WFID: &str = "sends-cancel-receiver";

async fn cancel_sender(ctx: WfContext) -> WorkflowResult<()> {
    let run_id = std::str::from_utf8(&ctx.get_args()[0].data)?.to_owned();
    let sigres = ctx
        .cancel_external(
            NamespacedWorkflowExecution {
                workflow_id: RECEIVER_WFID.to_string(),
                run_id,
                namespace: ctx.namespace().to_string(),
            },
            "cancel-reason".to_string(),
        )
        .await;
    if ctx.get_args().get(1).is_some() {
        // We expect failure
        assert!(sigres.is_err());
    } else {
        sigres.unwrap();
    }
    Ok(().into())
}

async fn cancel_receiver(ctx: WfContext) -> WorkflowResult<String> {
    let r = ctx.cancelled().await;
    Ok(r.into())
}

#[tokio::test]
async fn sends_cancel_to_other_wf() {
    let mut starter = CoreWfStarter::new("sends_cancel_to_other_wf");
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf("sender", cancel_sender);
    worker.register_wf("receiver", cancel_receiver);

    let receiver_run_id = worker
        .submit_wf(
            RECEIVER_WFID,
            "receiver",
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker
        .submit_wf(
            "sends-cancel-sender",
            "sender",
            vec![receiver_run_id.clone().into()],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let h = starter
        .get_client()
        .await
        .get_untyped_workflow_handle(RECEIVER_WFID, receiver_run_id);
    let res = String::from_json_payload(
        &h.get_workflow_result(GetWorkflowResultOpts::default())
            .await
            .unwrap()
            .unwrap_success()[0],
    )
    .unwrap();
    assert!(res.contains("Cancel requested by workflow"));
    assert!(res.contains("cancel-reason"));
}
