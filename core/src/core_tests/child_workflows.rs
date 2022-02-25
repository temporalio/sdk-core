use crate::{
    replay::DEFAULT_WORKFLOW_TYPE,
    test_help::{
        build_mock_pollers, canned_histories, mock_worker, MockPollCfg, ResponseType, TEST_Q,
    },
    workflow::managed_wf::ManagedWFFunc,
};
use std::sync::Arc;
use temporal_client::mocks::mock_gateway;
use temporal_sdk::{
    ChildWorkflowOptions, Signal, TestRustWorker, WfContext, WorkflowFunction, WorkflowResult,
};
use temporal_sdk_core_protos::coresdk::child_workflow::{
    child_workflow_result, ChildWorkflowCancellationType,
};
use tokio::join;

const SIGNAME: &str = "SIGNAME";

#[rstest::rstest]
#[case::signal_then_result(true)]
#[case::signal_and_result_concurrent(false)]
#[tokio::test]
async fn signal_child_workflow(#[case] serial: bool) {
    let wf_id = "fakeid";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let t = canned_histories::single_child_workflow_signaled("child-id-1", SIGNAME);
    let mock = mock_gateway();
    let mh = MockPollCfg::from_resp_batches(wf_id, t, [ResponseType::AllHistory], mock);
    let mock = build_mock_pollers(mh);
    let core = mock_worker(mock);
    let mut worker = TestRustWorker::new(Arc::new(core), TEST_Q.to_string(), None);

    let wf = move |ctx: WfContext| async move {
        let child = ctx.child_workflow(ChildWorkflowOptions {
            workflow_id: "child-id-1".to_string(),
            workflow_type: "child".to_string(),
            ..Default::default()
        });

        let start_res = child
            .start(&ctx)
            .await
            .into_started()
            .expect("Child should get started");
        let (sigres, res) = if serial {
            let sigres = start_res.signal(&ctx, Signal::new(SIGNAME, [b"Hi!"])).await;
            let res = start_res.result().await;
            (sigres, res)
        } else {
            let sigfut = start_res.signal(&ctx, Signal::new(SIGNAME, [b"Hi!"]));
            let resfut = start_res.result();
            join!(sigfut, resfut)
        };
        sigres.expect("signal result is ok");
        res.status.expect("child wf result is ok");
        Ok(().into())
    };

    worker.register_wf(wf_type.to_owned(), wf);
    worker
        .submit_wf(wf_id.to_owned(), wf_type.to_owned(), vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

async fn parent_cancels_child_wf(ctx: WfContext) -> WorkflowResult<()> {
    let child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_id: "child-id-1".to_string(),
        workflow_type: "child".to_string(),
        cancel_type: ChildWorkflowCancellationType::WaitCancellationCompleted,
        ..Default::default()
    });

    let start_res = child
        .start(&ctx)
        .await
        .into_started()
        .expect("Child should get started");
    let cancel_fut = start_res.cancel(&ctx);
    let resfut = start_res.result();
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
