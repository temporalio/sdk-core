use temporal_sdk_core::prototype_rust_sdk::{WfContext, WorkflowResult};
use temporal_sdk_core::tracing_init;
use test_utils::CoreWfStarter;
use uuid::Uuid;

async fn signal_sender(mut ctx: WfContext) -> WorkflowResult<()> {
    let res = ctx
        .signal_workflow(
            "fake_wid",
            Uuid::new_v4().to_string(),
            "some signal name",
            b"hi!",
        )
        .await;
    Ok(().into())
}

#[tokio::test]
async fn sends_signal_to_missing_wf() {
    tracing_init();

    let wf_name = "sends_signal";
    let mut starter = CoreWfStarter::new(wf_name);
    let worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), signal_sender);

    worker
        .submit_wf(wf_name.to_owned(), wf_name.to_owned(), vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    starter.shutdown().await;
}
