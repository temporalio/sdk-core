use crate::{
    replay::DEFAULT_WORKFLOW_TYPE,
    test_help::{
        build_fake_sdk, canned_histories, mock_sdk, mock_worker, single_hist_mock_sg, MockPollCfg,
        ResponseType,
    },
    worker::client::mocks::mock_workflow_client,
};
use temporal_client::WorkflowOptions;
use temporal_sdk::{ChildWorkflowOptions, Signal, WfContext, WorkflowResult};
use temporal_sdk_core_api::Worker;
use temporal_sdk_core_protos::coresdk::{
    child_workflow::{child_workflow_result, ChildWorkflowCancellationType},
    workflow_activation::{workflow_activation_job, WorkflowActivationJob},
    workflow_commands::{
        CancelChildWorkflowExecution, CompleteWorkflowExecution, StartChildWorkflowExecution,
    },
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
    start_res.cancel(&ctx);
    let stat = start_res
        .result()
        .await
        .status
        .expect("child wf result is ok");
    assert_matches!(stat, child_workflow_result::Status::Cancelled(_));
    Ok(().into())
}

#[tokio::test]
async fn cancel_child_workflow() {
    let t = canned_histories::single_child_workflow_cancelled("child-id-1");
    let mut worker = build_fake_sdk(MockPollCfg::from_resps(t, [ResponseType::AllHistory]));
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, parent_cancels_child_wf);
    worker.run().await.unwrap();
}

#[rstest::rstest]
#[case::abandon(ChildWorkflowCancellationType::Abandon)]
#[case::try_cancel(ChildWorkflowCancellationType::TryCancel)]
#[case::wait_cancel_completed(ChildWorkflowCancellationType::WaitCancellationCompleted)]
#[tokio::test]
async fn cancel_child_workflow_lang_thinks_not_started_but_is(
    #[case] cancellation_type: ChildWorkflowCancellationType,
) {
    // Since signal handlers always run first, it's possible lang might try to cancel
    // a child workflow it thinks isn't started, but we've told it is in the same activation.
    // It would be annoying for lang to have to peek ahead at jobs to be consistent in that case.
    let t = match cancellation_type {
        ChildWorkflowCancellationType::Abandon => {
            canned_histories::single_child_workflow_abandon_cancelled("child-id-1")
        }
        ChildWorkflowCancellationType::TryCancel => {
            canned_histories::single_child_workflow_try_cancelled("child-id-1")
        }
        _ => canned_histories::single_child_workflow_cancelled("child-id-1"),
    };
    let mock = mock_workflow_client();
    let mock = single_hist_mock_sg("fakeid", t, [ResponseType::AllHistory], mock, true);
    let core = mock_worker(mock);
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        act.run_id,
        StartChildWorkflowExecution {
            seq: 1,
            cancellation_type: cancellation_type as i32,
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
    // Issue the cancel command
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        act.run_id,
        CancelChildWorkflowExecution {
            child_workflow_seq: 1,
        }
        .into(),
    ))
    .await
    .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    // Make sure that a resolve for the "request cancel external workflow" command does *not* appear
    // since lang didn't actually issue one. The only job should be resolving the child workflow.
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveChildWorkflowExecution(_)),
        }]
    );
    // Request cancel external is technically fallible, but the only reasons relate to targeting
    // a not-found workflow, which couldn't happen in this case.
}

#[tokio::test]
async fn cancel_already_complete_child_ignored() {
    let t = canned_histories::single_child_workflow("child-id-1");
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
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
        .await
        .unwrap();
    let act = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        act.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::ResolveChildWorkflowExecution(_)),
        }]
    );
    // Try to cancel post-completion, it should be ignored. Also complete the wf.
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id,
        vec![
            CancelChildWorkflowExecution {
                child_workflow_seq: 1,
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();
}
