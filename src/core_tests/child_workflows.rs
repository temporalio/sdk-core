use crate::{
    prototype_rust_sdk::{ChildWorkflowOptions, WfContext, WorkflowFunction, WorkflowResult},
    test_help::canned_histories,
    workflow::managed_wf::ManagedWFFunc,
};
use tokio::join;

const SIGNAME: &str = "SIGNAME";

async fn parent_wf(mut ctx: WfContext) -> WorkflowResult<()> {
    let mut child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_id: "child-id-1".to_string(),
        workflow_type: "child".to_string(),
        input: vec![],
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
