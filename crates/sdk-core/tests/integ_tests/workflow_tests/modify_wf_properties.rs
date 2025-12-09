use crate::common::{CoreWfStarter, build_fake_sdk};
use temporalio_client::WorkflowClientTrait;
use temporalio_common::{
    protos::{
        DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder,
        coresdk::{AsJsonPayloadExt, FromJsonPayloadExt},
        temporal::api::{
            command::v1::{Command, command},
            common::v1::Payload,
            enums::v1::EventType,
        },
    },
    worker::WorkerTaskTypes,
};
use temporalio_sdk::{WfContext, WorkflowResult};
use temporalio_sdk_core::test_help::MockPollCfg;
use uuid::Uuid;

static FIELD_A: &str = "cat_name";
static FIELD_B: &str = "cute_level";

async fn memo_upserter(ctx: WfContext) -> WorkflowResult<()> {
    ctx.upsert_memo([
        (FIELD_A.to_string(), "enchi".as_json_payload().unwrap()),
        (FIELD_B.to_string(), 9001.as_json_payload().unwrap()),
    ]);
    Ok(().into())
}

#[tokio::test]
async fn sends_modify_wf_props() {
    let wf_name = "can_upsert_memo";
    let wf_id = Uuid::new_v4();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;

    worker.register_wf(wf_name, memo_upserter);
    let run_id = worker
        .submit_wf(wf_id.to_string(), wf_name, vec![], Default::default())
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let memo = starter
        .get_client()
        .await
        .describe_workflow_execution(wf_id.to_string(), Some(run_id))
        .await
        .unwrap()
        .workflow_execution_info
        .unwrap()
        .memo
        .unwrap()
        .fields;
    let catname = memo.get(FIELD_A).unwrap();
    let cuteness = memo.get(FIELD_B).unwrap();
    for payload in [catname, cuteness] {
        assert!(payload.is_json_payload());
    }
    assert_eq!("enchi", String::from_json_payload(catname).unwrap());
    assert_eq!(9001, usize::from_json_payload(cuteness).unwrap());
}

#[tokio::test]
async fn workflow_modify_props() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();

    let (k1, k2) = ("foo", "bar");

    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(|wft| {
            assert_matches!(
                wft.commands.as_slice(),
                [Command {
                    attributes: Some(
                        command::Attributes::ModifyWorkflowPropertiesCommandAttributes(msg)
                    ),
                    ..
                }, ..] => {
                    let fields = &msg.upserted_memo.as_ref().unwrap().fields;
                    let payload1 = fields.get(k1).unwrap();
                    let payload2 = fields.get(k2).unwrap();
                    assert_eq!(payload1.data[0], 0x01);
                    assert_eq!(payload2.data[0], 0x02);
                    assert_eq!(fields.len(), 2);
                }
            );
        });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, move |ctx: WfContext| async move {
        ctx.upsert_memo([
            (
                String::from(k1),
                Payload {
                    data: vec![0x01],
                    ..Default::default()
                },
            ),
            (
                String::from(k2),
                Payload {
                    data: vec![0x02],
                    ..Default::default()
                },
            ),
        ]);
        Ok(().into())
    });
    worker.run().await.unwrap();
}
