use crate::common::{CoreWfStarter, build_fake_sdk};
use temporalio_client::{GetWorkflowResultOpts, WfClientExt, WorkflowOptions};
use temporalio_common::protos::{
    DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder,
    coresdk::{FromJsonPayloadExt, common::NamespacedWorkflowExecution},
    temporal::api::enums::v1::{CommandType, EventType},
};
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_sdk::{WfContext, WorkflowResult};
use temporalio_sdk_core::test_help::MockPollCfg;

const RECEIVER_WFID: &str = "sends-cancel-receiver";

async fn cancel_sender(ctx: WfContext) -> WorkflowResult<()> {
    let run_id = std::str::from_utf8(&ctx.get_args()[0].data)?.to_owned();
    let sigres = ctx
        .cancel_external(
            NamespacedWorkflowExecution {
                workflow_id: RECEIVER_WFID.to_string(),
                run_id,
                namespace: ctx.namespace().to_string(),
            },
            "cancel-reason".to_string(),
        )
        .await;
    if ctx.get_args().get(1).is_some() {
        // We expect failure
        assert!(sigres.is_err());
    } else {
        sigres.unwrap();
    }
    Ok(().into())
}

async fn cancel_receiver(ctx: WfContext) -> WorkflowResult<String> {
    let r = ctx.cancelled().await;
    Ok(r.into())
}

#[tokio::test]
async fn sends_cancel_to_other_wf() {
    let mut starter = CoreWfStarter::new("sends_cancel_to_other_wf");
    starter
        .worker_config
        .task_types(WorkerTaskTypes::workflow_only());
    let mut worker = starter.worker().await;
    worker.register_wf("sender", cancel_sender);
    worker.register_wf("receiver", cancel_receiver);

    let receiver_run_id = worker
        .submit_wf(
            RECEIVER_WFID,
            "receiver",
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker
        .submit_wf(
            "sends-cancel-sender",
            "sender",
            vec![receiver_run_id.clone().into()],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let h = starter
        .get_client()
        .await
        .get_untyped_workflow_handle(RECEIVER_WFID, receiver_run_id);
    let res = String::from_json_payload(
        &h.get_workflow_result(GetWorkflowResultOpts::default())
            .await
            .unwrap()
            .unwrap_success()[0],
    )
    .unwrap();
    assert!(res.contains("Cancel requested by workflow"));
    assert!(res.contains("cancel-reason"));
}

async fn cancel_sender_canned(ctx: WfContext) -> WorkflowResult<()> {
    let res = ctx
        .cancel_external(
            NamespacedWorkflowExecution {
                namespace: "some_namespace".to_string(),
                workflow_id: "fake_wid".to_string(),
                run_id: "fake_rid".to_string(),
            },
            "cancel reason".to_string(),
        )
        .await;
    if res.is_err() {
        Err(anyhow::anyhow!("Cancel fail!"))
    } else {
        Ok(().into())
    }
}

#[rstest::rstest]
#[case::succeeds(false)]
#[case::fails(true)]
#[tokio::test]
async fn sends_cancel_canned(#[case] fails: bool) {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    let id = t.add_cancel_external_wf(NamespacedWorkflowExecution {
        namespace: "some_namespace".to_string(),
        workflow_id: "fake_wid".to_string(),
        run_id: "fake_rid".to_string(),
    });
    if fails {
        t.add_cancel_external_wf_failed(id);
    } else {
        t.add_cancel_external_wf_completed(id);
    }
    t.add_full_wf_task();
    if fails {
        t.add_workflow_execution_failed();
    } else {
        t.add_workflow_execution_completed();
    }

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(|wft| {
                assert_matches!(
                    wft.commands[0].command_type(),
                    CommandType::RequestCancelExternalWorkflowExecution
                );
            })
            .then(move |wft| {
                if fails {
                    assert_eq!(
                        wft.commands[0].command_type(),
                        CommandType::FailWorkflowExecution
                    );
                } else {
                    assert_eq!(
                        wft.commands[0].command_type(),
                        CommandType::CompleteWorkflowExecution
                    );
                }
            });
    });
    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, cancel_sender_canned);
    worker.run().await.unwrap();
}
