use std::time::Duration;
use temporal_sdk_core::prototype_rust_sdk::{LocalActivityOptions, WfContext, WorkflowResult};
use temporal_sdk_core_protos::coresdk::AsJsonPayloadExt;
use test_utils::CoreWfStarter;

pub async fn one_local_activity_wf(mut ctx: WfContext) -> WorkflowResult<()> {
    ctx.local_activity(LocalActivityOptions {
        activity_type: "echo_activity".to_string(),
        input: "hi!".as_json_payload().expect("serializes fine"),
        ..Default::default()
    })
    .await;
    Ok(().into())
}

#[tokio::test]
async fn one_local_activity() {
    let wf_name = "one_local_activity";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), one_local_activity_wf);
    worker.register_activity("echo_activity", |echo_me: String| echo_me);

    worker
        .submit_wf(wf_name.to_owned(), wf_name.to_owned(), vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    starter.shutdown().await;
}

pub async fn local_act_concurrent_with_timer_wf(mut ctx: WfContext) -> WorkflowResult<()> {
    let la = ctx.local_activity(LocalActivityOptions {
        activity_type: "echo_activity".to_string(),
        input: "hi!".as_json_payload().expect("serializes fine"),
        ..Default::default()
    });
    let timer = ctx.timer(Duration::from_secs(1));
    tokio::join!(la, timer);
    Ok(().into())
}

#[tokio::test]
async fn local_act_concurrent_with_timer() {
    let wf_name = "local_act_concurrent_with_timer";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), local_act_concurrent_with_timer_wf);
    worker.register_activity("echo_activity", |echo_me: String| echo_me);

    worker
        .submit_wf(wf_name.to_owned(), wf_name.to_owned(), vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    starter.shutdown().await;
}
