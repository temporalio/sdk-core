use std::collections::HashMap;

use futures_util::StreamExt;
use temporal_client::{SignalWithStartOptions, WorkflowClientTrait, WorkflowOptions};
use temporal_sdk::{
    ChildWorkflowOptions, Signal, SignalWorkflowOptions, WfContext, WorkflowResult,
};
use temporal_sdk_core_protos::{coresdk::IntoPayloadsExt, temporal::api::common::v1::Payload};
use temporal_sdk_core_test_utils::CoreWfStarter;
use uuid::Uuid;

const SIGNAME: &str = "signame";
const RECEIVER_WFID: &str = "sends-signal-signal-receiver";

async fn signal_sender(ctx: WfContext) -> WorkflowResult<()> {
    let run_id = std::str::from_utf8(&ctx.get_args()[0].data)
        .unwrap()
        .to_owned();
    let mut dat = SignalWorkflowOptions::new(RECEIVER_WFID, run_id, SIGNAME, [b"hi!"]);
    dat.with_header("tupac", b"shakur");
    let sigres = ctx.signal_workflow(dat).await;
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
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), signal_sender);

    worker
        .submit_wf(
            wf_name,
            wf_name,
            vec![Uuid::new_v4().to_string().into(), [1].into()],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

async fn signal_receiver(ctx: WfContext) -> WorkflowResult<()> {
    let res = ctx.make_signal_channel(SIGNAME).next().await.unwrap();
    assert_eq!(&res.input, &[b"hi!".into()]);
    assert_eq!(
        *res.headers.get("tupac").expect("tupac header exists"),
        b"shakur".into()
    );
    Ok(().into())
}

async fn signal_with_create_wf_receiver(ctx: WfContext) -> WorkflowResult<()> {
    let res = ctx.make_signal_channel(SIGNAME).next().await.unwrap();
    assert_eq!(&res.input, &[b"tada".into()]);
    assert_eq!(
        *res.headers.get("tupac").expect("tupac header exists"),
        b"shakur".into()
    );
    Ok(().into())
}

#[tokio::test]
async fn sends_signal_to_other_wf() {
    let mut starter = CoreWfStarter::new("sends_signal_to_other_wf");
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf("sender", signal_sender);
    worker.register_wf("receiver", signal_receiver);

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
            "sends-signal-sender",
            "sender",
            vec![receiver_run_id.into()],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn sends_signal_with_create_wf() {
    let mut starter = CoreWfStarter::new("sends_signal_with_create_wf");
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf("receiver_signal", signal_with_create_wf_receiver);

    let client = starter.get_client().await;
    let mut header: HashMap<String, Payload> = HashMap::new();
    header.insert("tupac".into(), "shakur".into());
    let options = SignalWithStartOptions::builder()
        .task_queue(worker.inner_mut().task_queue())
        .workflow_id("sends_signal_with_create_wf")
        .workflow_type("receiver_signal")
        .signal_name(SIGNAME)
        .signal_input(vec![b"tada".into()].into_payloads())
        .signal_header(header.into())
        .build()
        .unwrap();
    let res = client
        .signal_with_start_workflow_execution(options, WorkflowOptions::default())
        .await
        .expect("request succeeds.qed");

    worker.expect_workflow_completion("sends_signal_with_create_wf", Some(res.run_id));
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
    let mut sig = Signal::new(SIGNAME, [b"hi!"]);
    sig.data.with_header("tupac", b"shakur");
    started_child.signal(&ctx, sig).await.unwrap();
    started_child.result().await.status.unwrap();
    Ok(().into())
}

#[tokio::test]
async fn sends_signal_to_child() {
    let mut starter = CoreWfStarter::new("sends_signal_to_child");
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf("child_signaler", signals_child);
    worker.register_wf("child_receiver", signal_receiver);

    worker
        .submit_wf(
            "sends-signal-to-child",
            "child_signaler",
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}
