use anyhow::anyhow;
use temporal_sdk_core::{
    protos::coresdk::child_workflow::{child_workflow_result, Success},
    prototype_rust_sdk::{ChildWorkflowOptions, WfContext, WorkflowResult},
};
use test_utils::CoreWfStarter;

static PARENT_WF_TYPE: &str = "parent_wf";
static CHILD_WF_TYPE: &str = "child_wf";

async fn child_wf(_ctx: WfContext) -> WorkflowResult<()> {
    Ok(().into())
}

async fn parent_wf(mut ctx: WfContext) -> WorkflowResult<()> {
    let mut child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_id: "child-1".to_owned(),
        workflow_type: CHILD_WF_TYPE.to_owned(),
        ..Default::default()
    });

    let started = child
        .start(&mut ctx)
        .await
        .as_started()
        .expect("Child chould start OK");
    match started.result(&mut ctx).await.status {
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
    worker.incr_expected_run_count(1); // Expect another WF to be run as child

    worker
        .submit_wf("parent".to_string(), PARENT_WF_TYPE.to_owned(), vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    starter.shutdown().await;
}
