use temporal_sdk_core::prototype_rust_sdk::{LocalActivityOptions, WfContext, WorkflowResult};
use test_utils::CoreWfStarter;

pub async fn one_local_activity_wf(mut ctx: WfContext) -> WorkflowResult<()> {
    let res = ctx
        .local_activity(LocalActivityOptions {
            activity_type: "echo_activity".to_string(),
            ..Default::default()
        })
        .await;
    dbg!(res);
    Ok(().into())
}

#[tokio::test]
async fn one_local_activity() {
    let wf_name = "one_local_activity";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), one_local_activity_wf);
    worker.register_activity("echo_activity", |echo_me: String| {
        dbg!(&echo_me);
        echo_me
    });

    worker
        .submit_wf(wf_name.to_owned(), wf_name.to_owned(), vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    starter.shutdown().await;
}
