use crate::common::{CoreWfStarter, build_fake_sdk};
use temporalio_client::{
    NamespacedClient, WorkflowExecutionInfo, WorkflowGetResultOptions, WorkflowStartOptions,
};
use temporalio_common::{
    protos::{
        DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder,
        coresdk::{AsJsonPayloadExt, FromJsonPayloadExt, common::NamespacedWorkflowExecution},
        temporal::api::enums::v1::{CommandType, EventType},
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowResult};
use temporalio_sdk_core::test_help::MockPollCfg;

const RECEIVER_WFID: &str = "sends-cancel-receiver";

#[workflow]
#[derive(Default)]
struct CancelSender;

#[workflow_methods]
impl CancelSender {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, run_id: String) -> WorkflowResult<()> {
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
        sigres.unwrap();
        Ok(())
    }
}

#[workflow]
#[derive(Default)]
struct CancelReceiver;

#[workflow_methods]
impl CancelReceiver {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        let r = ctx.cancelled().await;
        Ok(r)
    }
}

#[tokio::test]
async fn sends_cancel_to_other_wf() {
    let mut starter = CoreWfStarter::new("sends_cancel_to_other_wf");
    starter.sdk_config.task_types = Some(WorkerTaskTypes::workflow_only());
    let mut worker = starter.worker().await;
    worker.register_workflow::<CancelSender>();
    worker.register_workflow::<CancelReceiver>();

    let task_queue = starter.get_task_queue().to_owned();
    let receiver_run_id = worker
        .submit_wf(
            "CancelReceiver",
            vec![().as_json_payload().unwrap()],
            WorkflowStartOptions::new(task_queue.clone(), RECEIVER_WFID).build(),
        )
        .await
        .unwrap();
    worker
        .submit_wf(
            "CancelSender",
            vec![receiver_run_id.clone().as_json_payload().unwrap()],
            WorkflowStartOptions::new(task_queue, "sends-cancel-sender").build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let client = starter.get_client().await;
    let h = WorkflowExecutionInfo {
        namespace: client.namespace(),
        workflow_id: RECEIVER_WFID.into(),
        run_id: Some(receiver_run_id),
        first_execution_run_id: None,
    }
    .bind_untyped(client.clone());
    let res = String::from_json_payload(
        h.get_result(WorkflowGetResultOptions::default())
            .await
            .unwrap()
            .payloads
            .first()
            .unwrap(),
    )
    .unwrap();
    assert!(res.contains("Cancel requested by workflow"));
    assert!(res.contains("cancel-reason"));
}

#[workflow]
#[derive(Default)]
struct CancelSenderCanned;

#[workflow_methods]
impl CancelSenderCanned {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
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
            Err(anyhow::anyhow!("Cancel fail!").into())
        } else {
            Ok(())
        }
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
    worker.register_workflow::<CancelSenderCanned>();
    worker.run().await.unwrap();
}
