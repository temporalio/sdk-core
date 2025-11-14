use crate::common::{CoreWfStarter, build_fake_sdk, mock_sdk, mock_sdk_cfg};
use anyhow::anyhow;
use assert_matches::assert_matches;
use std::time::Duration;
use temporalio_client::{WorkflowClientTrait, WorkflowOptions};
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_common::{
    Worker,
    protos::{
        TestHistoryBuilder, canned_histories,
        coresdk::{
            AsJsonPayloadExt,
            child_workflow::{
                ChildWorkflowCancellationType, StartChildWorkflowExecutionFailedCause, Success,
                child_workflow_result,
            },
            workflow_activation::{
                WorkflowActivationJob,
                resolve_child_workflow_execution_start::Status as StartStatus,
                workflow_activation_job,
            },
            workflow_commands::{
                CancelChildWorkflowExecution, CompleteWorkflowExecution,
                StartChildWorkflowExecution,
            },
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            common::v1::Payload,
            enums::v1::{CommandType, EventType, ParentClosePolicy},
            history::v1::{
                StartChildWorkflowExecutionFailedEventAttributes,
                StartChildWorkflowExecutionInitiatedEventAttributes,
            },
            sdk::v1::UserMetadata,
        },
    },
};
use temporalio_sdk::{
    CancellableFuture, ChildWorkflowOptions, Signal, WfContext, WfExitValue, WorkflowResult,
};
use temporalio_sdk_core::{
    replay::DEFAULT_WORKFLOW_TYPE,
    test_help::{MockPollCfg, ResponseType, mock_worker, mock_worker_client, single_hist_mock_sg},
};
use tokio::{join, sync::Barrier};

static PARENT_WF_TYPE: &str = "parent_wf";
static CHILD_WF_TYPE: &str = "child_wf";
const SIGNAME: &str = "SIGNAME";

async fn child_wf(ctx: WfContext) -> WorkflowResult<()> {
    assert_eq!(
        ctx.workflow_initial_info()
            .parent_workflow_info
            .as_ref()
            .unwrap()
            .workflow_id,
        ctx.workflow_initial_info()
            .root_workflow
            .as_ref()
            .unwrap()
            .workflow_id
    );
    Ok(().into())
}

async fn happy_parent(ctx: WfContext) -> WorkflowResult<()> {
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
    starter
        .worker_config
        .task_types(WorkerTaskTypes::workflow_only());
    let mut worker = starter.worker().await;

    worker.register_wf(PARENT_WF_TYPE.to_string(), happy_parent);
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

#[tokio::test]
async fn abandoned_child_bug_repro() {
    let mut starter = CoreWfStarter::new("child-workflow-abandon-bug");
    starter
        .worker_config
        .task_types(WorkerTaskTypes::workflow_only());
    let mut worker = starter.worker().await;
    let barr: &'static Barrier = Box::leak(Box::new(Barrier::new(2)));

    worker.register_wf(
        PARENT_WF_TYPE.to_string(),
        move |ctx: WfContext| async move {
            let child = ctx.child_workflow(ChildWorkflowOptions {
                workflow_id: "abandoned-child".to_owned(),
                workflow_type: CHILD_WF_TYPE.to_owned(),
                parent_close_policy: ParentClosePolicy::Abandon,
                cancel_type: ChildWorkflowCancellationType::Abandon,
                ..Default::default()
            });

            let started = child
                .start(&ctx)
                .await
                .into_started()
                .expect("Child chould start OK");
            barr.wait().await;
            // Wait for cancel signal
            ctx.cancelled().await;
            // Cancel the child immediately
            started.cancel(&ctx, "Die reason!".to_string());
            // Need to do something else, so we'll see the ChildWorkflowExecutionCanceled event
            ctx.timer(Duration::from_secs(1)).await;
            started.result().await;
            Ok(().into())
        },
    );
    worker.register_wf(CHILD_WF_TYPE.to_string(), |ctx: WfContext| async move {
        ctx.cancelled().await;
        Ok(WfExitValue::<()>::Cancelled)
    });

    worker
        .submit_wf(
            "parent-abandoner".to_string(),
            PARENT_WF_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    let client = starter.get_client().await;
    let canceller = async {
        barr.wait().await;
        client
            .cancel_workflow_execution(
                "parent-abandoner".to_string(),
                None,
                "die".to_string(),
                None,
            )
            .await
            .unwrap();
        client
            .cancel_workflow_execution("abandoned-child".to_string(), None, "die".to_string(), None)
            .await
            .unwrap();
    };
    let runner = async move {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(canceller, runner);
}

#[tokio::test]
async fn abandoned_child_resolves_post_cancel() {
    let mut starter = CoreWfStarter::new("child-workflow-resolves-post-cancel");
    starter
        .worker_config
        .task_types(WorkerTaskTypes::workflow_only());
    let mut worker = starter.worker().await;
    let barr: &'static Barrier = Box::leak(Box::new(Barrier::new(2)));

    worker.register_wf(
        PARENT_WF_TYPE.to_string(),
        move |ctx: WfContext| async move {
            let child = ctx.child_workflow(ChildWorkflowOptions {
                workflow_id: "abandoned-child-resolve-post-cancel".to_owned(),
                workflow_type: CHILD_WF_TYPE.to_owned(),
                parent_close_policy: ParentClosePolicy::Abandon,
                cancel_type: ChildWorkflowCancellationType::Abandon,
                ..Default::default()
            });

            let started = child
                .start(&ctx)
                .await
                .into_started()
                .expect("Child chould start OK");
            barr.wait().await;
            // Wait for cancel signal
            ctx.cancelled().await;
            // Cancel the child immediately
            started.cancel(&ctx, "Die reason".to_string());
            // Need to do something else, so we will see the child completing
            ctx.timer(Duration::from_secs(1)).await;
            started.result().await;
            Ok(().into())
        },
    );
    worker.register_wf(CHILD_WF_TYPE.to_string(), |_: WfContext| async move {
        Ok("I'm done".into())
    });

    worker
        .submit_wf(
            "parent-abandoner-resolving".to_string(),
            PARENT_WF_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    let client = starter.get_client().await;
    let canceller = async {
        barr.wait().await;
        client
            .cancel_workflow_execution(
                "parent-abandoner-resolving".to_string(),
                None,
                "die".to_string(),
                None,
            )
            .await
            .unwrap();
    };
    let runner = async move {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(canceller, runner);
}

#[tokio::test]
async fn cancelled_child_gets_reason() {
    let wf_name = "cancelled-child-gets-reason";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .worker_config
        .task_types(WorkerTaskTypes::workflow_only());
    let mut worker = starter.worker().await;

    worker.register_wf(wf_name.to_string(), move |ctx: WfContext| async move {
        let child = ctx.child_workflow(ChildWorkflowOptions {
            workflow_id: format!("{}-child", ctx.task_queue()),
            workflow_type: CHILD_WF_TYPE.to_owned(),
            cancel_type: ChildWorkflowCancellationType::WaitCancellationRequested,
            ..Default::default()
        });

        let started = child
            .start(&ctx)
            .await
            .into_started()
            .expect("Child chould start OK");
        // Cancel the child  after start
        started.cancel(&ctx, "Die reason".to_string());
        let r = started.result().await;
        let out = assert_matches!(r.status,
            Some(child_workflow_result::Status::Completed(reason)) => reason);
        assert_eq!(out.result.unwrap(), "Die reason".as_json_payload().unwrap());
        Ok(().into())
    });
    worker.register_wf(CHILD_WF_TYPE.to_string(), |c: WfContext| async move {
        let r = c.cancelled().await;
        Ok(r.into())
    });

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[rstest::rstest]
#[case::signal_then_result(true)]
#[case::signal_and_result_concurrent(false)]
#[tokio::test]
async fn signal_child_workflow(#[case] serial: bool) {
    let wf_id = "fakeid";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let t = canned_histories::single_child_workflow_signaled("child-id-1", SIGNAME);
    let mock = mock_worker_client();
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
    start_res.cancel(&ctx, "cancel reason".to_string());
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
    let mock = mock_worker_client();
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
            reason: "dieee".to_string(),
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
    let mock = mock_worker_client();
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
                reason: "go away!".to_string(),
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn pass_child_workflow_summary_to_metadata() {
    let wf_id = "1";
    let wf_type = DEFAULT_WORKFLOW_TYPE;
    let t = canned_histories::single_child_workflow(wf_id);
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    let expected_user_metadata = Some(UserMetadata {
        summary: Some(b"child summary".into()),
        details: Some(b"child details".into()),
    });
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::StartChildWorkflowExecution
                );
                assert_eq!(wft.commands[0].user_metadata, expected_user_metadata)
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            });
    });

    let mut worker = mock_sdk_cfg(mock_cfg, |_| {});
    worker.register_wf(wf_type, move |ctx: WfContext| async move {
        ctx.child_workflow(ChildWorkflowOptions {
            workflow_id: wf_id.to_string(),
            workflow_type: "child".to_string(),
            static_summary: Some("child summary".to_string()),
            static_details: Some("child details".to_string()),
            ..Default::default()
        })
        .start(&ctx)
        .await;
        Ok(().into())
    });
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

#[derive(Clone, Copy)]
enum Expectation {
    Success,
    Failure,
    StartFailure,
}

impl Expectation {
    const fn try_from_u8(x: u8) -> Option<Self> {
        Some(match x {
            0 => Self::Success,
            1 => Self::Failure,
            2 => Self::StartFailure,
            _ => return None,
        })
    }
}

#[fixture]
fn child_workflow_happy_hist() -> MockPollCfg {
    let mut t = canned_histories::single_child_workflow("child-id-1");
    t.set_wf_input(Payload::from([Expectation::Success as u8]));
    MockPollCfg::from_hist_builder(t)
}

#[fixture]
fn child_workflow_fail_hist() -> MockPollCfg {
    let mut t = canned_histories::single_child_workflow_fail("child-id-1");
    t.set_wf_input(Payload::from([Expectation::Failure as u8]));
    MockPollCfg::from_hist_builder(t)
}

async fn parent_wf(ctx: WfContext) -> WorkflowResult<()> {
    let expectation = Expectation::try_from_u8(ctx.get_args()[0].data[0]).unwrap();
    let child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_id: "child-id-1".to_string(),
        workflow_type: "child".to_string(),
        ..Default::default()
    });

    let start_res = child.start(&ctx).await;
    match (expectation, &start_res.status) {
        (Expectation::Success | Expectation::Failure, StartStatus::Succeeded(_)) => {}
        (Expectation::StartFailure, StartStatus::Failed(_)) => return Ok(().into()),
        _ => return Err(anyhow!("Unexpected start status")),
    };
    match (
        expectation,
        start_res.into_started().unwrap().result().await.status,
    ) {
        (Expectation::Success, Some(child_workflow_result::Status::Completed(_))) => Ok(().into()),
        (Expectation::Failure, _) => Ok(().into()),
        _ => Err(anyhow!("Unexpected child WF status")),
    }
}

#[rstest(
    mock_cfg,
    case::success(child_workflow_happy_hist()),
    case::failure(child_workflow_fail_hist())
)]
#[tokio::test]
async fn single_child_workflow_until_completion(mut mock_cfg: MockPollCfg) {
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_matches!(
                    wft.commands[0].command_type(),
                    CommandType::StartChildWorkflowExecution
                );
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 0);
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_matches!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, parent_wf);
    worker.run().await.unwrap();
}

#[tokio::test]
async fn single_child_workflow_start_fail() {
    let child_wf_id = "child-id-1";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.set_wf_input(Payload::from([Expectation::StartFailure as u8]));
    t.add_full_wf_task();
    let initiated_event_id = t.add(StartChildWorkflowExecutionInitiatedEventAttributes {
        workflow_id: child_wf_id.to_owned(),
        workflow_type: Some("child".into()),
        ..Default::default()
    });
    t.add(StartChildWorkflowExecutionFailedEventAttributes {
        workflow_id: child_wf_id.to_owned(),
        initiated_event_id,
        cause: StartChildWorkflowExecutionFailedCause::WorkflowAlreadyExists as i32,
        ..Default::default()
    });
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_matches!(
                    wft.commands[0].command_type(),
                    CommandType::StartChildWorkflowExecution
                );
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_matches!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, parent_wf);
    worker.run().await.unwrap();
}

async fn cancel_before_send_wf(ctx: WfContext) -> WorkflowResult<()> {
    let workflow_id = "child-id-1";
    let child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_id: workflow_id.to_string(),
        workflow_type: "child".to_string(),
        ..Default::default()
    });
    let start = child.start(&ctx);
    start.cancel(&ctx);
    match start.await.status {
        StartStatus::Cancelled(_) => Ok(().into()),
        _ => Err(anyhow!("Unexpected start status")),
    }
}

#[tokio::test]
async fn single_child_workflow_cancel_before_sent() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            // Workflow starts and cancels the child workflow, no commands should be sent besides
            // workflow completion
            assert_eq!(wft.commands.len(), 1);
            assert_matches!(
                wft.commands[0].command_type(),
                CommandType::CompleteWorkflowExecution
            );
        });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, cancel_before_send_wf);
    worker.run().await.unwrap();
}
