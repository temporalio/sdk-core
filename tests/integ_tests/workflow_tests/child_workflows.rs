use anyhow::anyhow;
use temporal_client::WorkflowOptions;
use temporal_sdk::{ChildWorkflowOptions, WfContext, WorkflowResult};
use temporal_sdk_core_protos::coresdk::child_workflow::{child_workflow_result, Success};
use temporal_sdk_core_test_utils::CoreWfStarter;

static PARENT_WF_TYPE: &str = "parent_wf";
static CHILD_WF_TYPE: &str = "child_wf";

async fn child_wf(_ctx: WfContext) -> WorkflowResult<()> {
    Ok(().into())
}

async fn parent_wf(ctx: WfContext) -> WorkflowResult<()> {
    let child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_id: "child-1".to_owned(),
        workflow_type: CHILD_WF_TYPE.to_owned(),
        ..Default::default()
    });

    let started = child
        .start(&ctx)
        .await
        .into_started()
        .expect("Child chould start OK");
    match started.result().await.status {
        Some(child_workflow_result::Status::Completed(Success { .. })) => Ok(().into()),
        _ => Err(anyhow!("Unexpected child WF status")),
    }
}

#[tokio::test]
async fn child_workflow_happy_path() {
    let mut starter = CoreWfStarter::new("child-workflows");
    let mut worker = starter.worker().await;

    worker.register_wf(PARENT_WF_TYPE.to_string(), parent_wf);
    worker.register_wf(CHILD_WF_TYPE.to_string(), child_wf);

    worker
        .submit_wf(
            "parent".to_string(),
            PARENT_WF_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}
