use std::time::Duration;
use temporal_client::WorkflowOptions;
use temporal_sdk::{WfContext, WfExitValue, WorkflowResult};
use temporal_sdk_core_protos::coresdk::workflow_commands::ContinueAsNewWorkflowExecution;
use temporal_sdk_core_test_utils::CoreWfStarter;

async fn continue_as_new_wf(ctx: WfContext) -> WorkflowResult<()> {
    let run_ct = ctx.get_args()[0].data[0];
    ctx.timer(Duration::from_millis(500)).await;
    Ok(if run_ct < 5 {
        WfExitValue::continue_as_new(ContinueAsNewWorkflowExecution {
            arguments: vec![[run_ct + 1].into()],
            ..Default::default()
        })
    } else {
        ().into()
    })
}

#[tokio::test]
async fn continue_as_new_happy_path() {
    let wf_name = "continue_as_new_happy_path";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_string(), continue_as_new_wf);

    worker
        .submit_wf(
            wf_name.to_string(),
            wf_name.to_string(),
            vec![[1].into()],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn continue_as_new_multiple_concurrent() {
    let wf_name = "continue_as_new_multiple_concurrent";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.max_cached_workflows(3).max_wft(3);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_string(), continue_as_new_wf);

    let wf_names = (1..=20).map(|i| format!("{}-{}", wf_name, i));
    for name in wf_names.clone() {
        worker
            .submit_wf(
                name.to_string(),
                wf_name.to_string(),
                vec![[1].into()],
                WorkflowOptions::default(),
            )
            .await
            .unwrap();
    }
    worker.run_until_done().await.unwrap();
}
