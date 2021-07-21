use std::time::Duration;
use temporal_sdk_core::{
    protos::coresdk::workflow_commands::{ContinueAsNewWorkflowExecution, StartTimer},
    prototype_rust_sdk::{WfContext, WfExitValue, WorkflowResult},
    tracing_init,
};
use test_utils::CoreWfStarter;

async fn continue_as_new_wf(mut ctx: WfContext) -> WorkflowResult<()> {
    let run_ct = ctx.get_args()[0].data[0];
    let timer = StartTimer {
        timer_id: "sometimer".to_string(),
        start_to_fire_timeout: Some(Duration::from_millis(500).into()),
    };
    ctx.timer(timer).await;
    Ok(if run_ct < 5 {
        WfExitValue::ContinueAsNew(ContinueAsNewWorkflowExecution {
            arguments: vec![[run_ct + 1].into()],
            ..Default::default()
        })
    } else {
        ().into()
    })
}

#[tokio::test]
async fn continue_as_new_happy_path() {
    tracing_init();
    let wf_name = "continue_as_new_happy_path";
    let mut starter = CoreWfStarter::new(wf_name);
    let worker = starter.worker().await;

    worker
        .submit_wf(vec![[1].into()], wf_name.to_owned(), continue_as_new_wf)
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    // Terminate the continued workflow
    starter
        .get_core()
        .await
        .server_gateway()
        .terminate_workflow_execution(wf_name.to_owned(), None)
        .await
        .unwrap();

    starter.shutdown().await;
}
