use temporal_client::WorkflowOptions;
use temporal_sdk::{WfContext, WorkflowResult};
use temporal_sdk_core_protos::coresdk::common::NamespacedWorkflowExecution;
use temporal_sdk_core_test_utils::CoreWfStarter;

const RECEIVER_WFID: &str = "sends-cancel-receiver";

async fn cancel_sender(ctx: WfContext) -> WorkflowResult<()> {
    let run_id = std::str::from_utf8(&ctx.get_args()[0].data)
        .unwrap()
        .to_owned();
    let sigres = ctx
        .cancel_external(NamespacedWorkflowExecution {
            workflow_id: RECEIVER_WFID.to_string(),
            run_id,
            namespace: ctx.namespace().to_string(),
        })
        .await;
    if ctx.get_args().get(1).is_some() {
        // We expect failure
        assert!(sigres.is_err());
    } else {
        sigres.unwrap();
    }
    Ok(().into())
}

async fn cancel_receiver(mut ctx: WfContext) -> WorkflowResult<()> {
    ctx.cancelled().await;
    Ok(().into())
}

#[tokio::test]
async fn sends_cancel_to_other_wf() {
    let mut starter = CoreWfStarter::new("sends_cancel_to_other_wf");
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
            vec![receiver_run_id.into()],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}
