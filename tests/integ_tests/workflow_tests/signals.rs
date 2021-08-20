use futures::StreamExt;
use temporal_sdk_core::{
    prototype_rust_sdk::{WfContext, WorkflowResult},
    tracing_init,
};
use test_utils::CoreWfStarter;
use uuid::Uuid;

const SIGNAME: &str = "signame";
const RECEIVER_WFID: &str = "sends-signal-signal-receiver";

async fn signal_sender(mut ctx: WfContext) -> WorkflowResult<()> {
    let run_id = std::str::from_utf8(&ctx.get_args()[0].data)
        .unwrap()
        .to_owned();
    ctx.signal_workflow(RECEIVER_WFID, run_id, SIGNAME, b"hi!")
        .await
        .unwrap();
    Ok(().into())
}

#[tokio::test]
async fn sends_signal_to_missing_wf() {
    let wf_name = "sends_signal";
    let mut starter = CoreWfStarter::new(wf_name);
    let worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), signal_sender);

    worker
        .submit_wf(wf_name, wf_name, vec![Uuid::new_v4().as_bytes().into()])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    starter.shutdown().await;
}

async fn signal_receiver(mut ctx: WfContext) -> WorkflowResult<()> {
    ctx.make_signal_channel(SIGNAME).next().await.unwrap();
    Ok(().into())
}

#[tokio::test]
async fn sends_signal_to_other_wf() {
    tracing_init();

    let mut starter = CoreWfStarter::new("sends_signal_to_other_wf");
    let worker = starter.worker().await;
    worker.register_wf("sender", signal_sender);
    worker.register_wf("receiver", signal_receiver);

    let receiver_run_id = worker
        .submit_wf(RECEIVER_WFID, "receiver", vec![])
        .await
        .unwrap();
    worker
        .submit_wf(
            "sends-signal-sender",
            "sender",
            vec![receiver_run_id.into()],
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    starter.shutdown().await;
}
