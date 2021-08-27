use crate::{
    prototype_rust_sdk::{ChildWorkflowOptions, WfContext, WorkflowFunction, WorkflowResult},
    test_help::canned_histories,
    workflow::managed_wf::ManagedWFFunc,
};
use temporal_sdk_core_protos::coresdk::child_workflow::{
    child_workflow_result, ChildWorkflowCancellationType,
};
use tokio::join;

const SIGNAME: &str = "SIGNAME";

async fn parent_wf(mut ctx: WfContext) -> WorkflowResult<()> {
    let child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_id: "child-id-1".to_string(),
        workflow_type: "child".to_string(),
        ..Default::default()
    });

    let start_res = child
        .start(&mut ctx)
        .await
        .as_started()
        .expect("Child should get started");
    let sigfut = start_res.signal(&mut ctx, SIGNAME, b"Hi!");
    let resfut = start_res.result(&mut ctx);
    let (sigres, res) = join!(sigfut, resfut);
    sigres.expect("signal result is ok");
    res.status.expect("child wf result is ok");
    Ok(().into())
}

#[tokio::test]
async fn signal_child_workflow() {
    let func = WorkflowFunction::new(parent_wf);
    let t = canned_histories::single_child_workflow_signaled("child-id-1", SIGNAME);
    let mut wfm = ManagedWFFunc::new(t, func, vec![]);
    wfm.process_all_activations().await.unwrap();
    wfm.shutdown().await.unwrap();
}

async fn parent_cancels_child_wf(mut ctx: WfContext) -> WorkflowResult<()> {
    let child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_id: "child-id-1".to_string(),
        workflow_type: "child".to_string(),
        cancel_type: ChildWorkflowCancellationType::WaitCancellationCompleted,
        ..Default::default()
    });

    let start_res = child
        .start(&mut ctx)
        .await
        .as_started()
        .expect("Child should get started");
    let cancel_fut = start_res.cancel(&mut ctx);
    let resfut = start_res.result(&mut ctx);
    let (cancel_res, res) = join!(cancel_fut, resfut);
    cancel_res.expect("cancel result is ok");
    let stat = res.status.expect("child wf result is ok");
    assert_matches!(stat, child_workflow_result::Status::Cancelled(_));
    Ok(().into())
}

#[tokio::test]
async fn cancel_child_workflow() {
    let func = WorkflowFunction::new(parent_cancels_child_wf);
    let t = canned_histories::single_child_workflow_cancelled("child-id-1");
    let mut wfm = ManagedWFFunc::new(t, func, vec![]);
    wfm.process_all_activations().await.unwrap();
    wfm.shutdown().await.unwrap();
}
