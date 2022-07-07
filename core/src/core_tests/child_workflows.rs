use crate::{
    replay::DEFAULT_WORKFLOW_TYPE,
    test_help::{
        canned_histories, mock_sdk, mock_worker, single_hist_mock_sg, MockPollCfg, ResponseType,
    },
    worker::{client::mocks::mock_workflow_client, ManagedWFFunc},
};
use temporal_client::WorkflowOptions;
use temporal_sdk::{ChildWorkflowOptions, Signal, WfContext, WorkflowFunction, WorkflowResult};
use temporal_sdk_core_api::Worker;
use temporal_sdk_core_protos::coresdk::{
    child_workflow::{child_workflow_result, ChildWorkflowCancellationType},
    workflow_activation::{workflow_activation_job, WorkflowActivationJob},
    workflow_commands::{CancelUnstartedChildWorkflowExecution, StartChildWorkflowExecution},
    workflow_completion::WorkflowActivationCompletion,
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
    let mock = mock_workflow_client();
    let mut worker = mock_sdk(MockPollCfg::from_resp_batches(
        wf_id,
        t,
        [ResponseType::AllHistory],
        mock,
    ));

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
        .submit_wf(
            wf_id.to_owned(),
            wf_type.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
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

#[tokio::test]
async fn cancel_child_workflow_lang_thinks_not_started_but_is() {
    crate::telemetry::test_telem_console();
    // Since signal handlers always run first, it's possible lang might try to cancel
    // a child workflow it thinks isn't started, but we've told it is in the same activation.
    // It would be annoying for lang to have to peek ahead at jobs to be consistent in that case.
    let t = canned_histories::single_child_workflow_cancelled("child-id-1");
    let mock = mock_workflow_client();
    let mock = single_hist_mock_sg("fakeid", t, [ResponseType::AllHistory], mock, true);
    let core = mock_worker(mock);
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        act.run_id,
        StartChildWorkflowExecution {
            seq: 1,
            ..Default::default()
        }
        .into(),
    ))
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveChildWorkflowExecutionStart(_)),
        }]
    );
    // Respond with "incorrect" cancel type command
    core.complete_workflow_activation(
        WorkflowActivationCompletion::from_cmd(
            act.run_id,
            CancelUnstartedChildWorkflowExecution {
                child_workflow_seq: 1,
            }
            .into(),
        )
        .into(),
    )
    .await
    .unwrap();
}
