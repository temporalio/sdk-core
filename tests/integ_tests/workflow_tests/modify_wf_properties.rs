use temporal_client::WorkflowClientTrait;
use temporal_sdk::{WfContext, WorkflowResult};
use temporal_sdk_core_protos::coresdk::{AsJsonPayloadExt, FromJsonPayloadExt};
use temporal_sdk_core_test_utils::CoreWfStarter;
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
    starter.worker_config.no_remote_activities(true);
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
