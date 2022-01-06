use futures::StreamExt;
use temporal_sdk::{ChildWorkflowOptions, WfContext, WorkflowResult};
use test_utils::CoreWfStarter;
use uuid::Uuid;

const SIGNAME: &str = "signame";
const RECEIVER_WFID: &str = "sends-signal-signal-receiver";

async fn signal_sender(ctx: WfContext) -> WorkflowResult<()> {
    let run_id = std::str::from_utf8(&ctx.get_args()[0].data)
        .unwrap()
        .to_owned();
    let sigres = ctx
        .signal_workflow(RECEIVER_WFID, run_id, SIGNAME, b"hi!")
        .await;
    if ctx.get_args().get(1).is_some() {
        // We expect failure
        assert!(sigres.is_err());
    } else {
        sigres.unwrap();
    }
    Ok(().into())
}

#[tokio::test]
async fn sends_signal_to_missing_wf() {
    let wf_name = "sends_signal_to_missing_wf";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), signal_sender);

    worker
        .submit_wf(
            wf_name,
            wf_name,
            vec![Uuid::new_v4().to_string().into(), [1].into()],
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

async fn signal_receiver(ctx: WfContext) -> WorkflowResult<()> {
    ctx.make_signal_channel(SIGNAME).next().await.unwrap();
    Ok(().into())
}

#[tokio::test]
async fn sends_signal_to_other_wf() {
    let mut starter = CoreWfStarter::new("sends_signal_to_other_wf");
    let mut worker = starter.worker().await;
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
}

async fn signals_child(ctx: WfContext) -> WorkflowResult<()> {
    let started_child = ctx
        .child_workflow(ChildWorkflowOptions {
            workflow_id: "my_precious_child".to_string(),
            workflow_type: "child_receiver".to_string(),
            ..Default::default()
        })
        .start(&ctx)
        .await
        .into_started()
        .expect("Must start ok");
    started_child.signal(&ctx, SIGNAME, b"hiya!").await.unwrap();
    started_child.result().await.status.unwrap();
    Ok(().into())
}

#[tokio::test]
async fn sends_signal_to_child() {
    let mut starter = CoreWfStarter::new("sends_signal_to_child");
    let mut worker = starter.worker().await;
    worker.register_wf("child_signaler", signals_child);
    worker.register_wf("child_receiver", signal_receiver);

    worker.incr_expected_run_count(1); // Expect another WF to be run as child
    worker
        .submit_wf("sends-signal-to-child", "child_signaler", vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}
