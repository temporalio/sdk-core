use assert_matches::assert_matches;
use std::{collections::HashMap, time::Duration};
use temporal_client::{
    GetWorkflowResultOpts, WfClientExt, WorkflowClientTrait, WorkflowExecutionResult,
    WorkflowOptions,
};
use temporal_sdk::{WfContext, WfExitValue, WorkflowResult};
use temporal_sdk_core_protos::coresdk::{AsJsonPayloadExt, FromJsonPayloadExt};
use temporal_sdk_core_test_utils::{CoreWfStarter, SEARCH_ATTR_INT, SEARCH_ATTR_TXT};
use uuid::Uuid;

async fn search_attr_updater(ctx: WfContext) -> WorkflowResult<()> {
    let mut int_val = ctx
        .search_attributes()
        .indexed_fields
        .get(SEARCH_ATTR_INT)
        .cloned()
        .unwrap_or_default();
    let orig_val = int_val.data[0];
    int_val.data[0] += 1;
    ctx.upsert_search_attributes([
        (SEARCH_ATTR_TXT.to_string(), "goodbye".as_json_payload()?),
        (SEARCH_ATTR_INT.to_string(), int_val),
    ]);
    // 49 is ascii 1
    if orig_val == 49 {
        Ok(WfExitValue::ContinueAsNew(Box::default()))
    } else {
        Ok(().into())
    }
}

#[tokio::test]
async fn sends_upsert() {
    let wf_name = "sends_upsert_search_attrs";
    let wf_id = Uuid::new_v4();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;

    worker.register_wf(wf_name, search_attr_updater);
    worker
        .submit_wf(
            wf_id.to_string(),
            wf_name,
            vec![],
            WorkflowOptions {
                search_attributes: Some(HashMap::from([
                    (
                        SEARCH_ATTR_TXT.to_string(),
                        "hello".as_json_payload().unwrap(),
                    ),
                    (SEARCH_ATTR_INT.to_string(), 1.as_json_payload().unwrap()),
                ])),
                execution_timeout: Some(Duration::from_secs(4)),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let client = starter.get_client().await;
    let search_attrs = client
        .describe_workflow_execution(wf_id.to_string(), None)
        .await
        .unwrap()
        .workflow_execution_info
        .unwrap()
        .search_attributes
        .unwrap()
        .indexed_fields;
    let txt_attr_payload = search_attrs.get(SEARCH_ATTR_TXT).unwrap();
    let int_attr_payload = search_attrs.get(SEARCH_ATTR_INT).unwrap();
    for payload in [txt_attr_payload, int_attr_payload] {
        assert!(payload.is_json_payload());
    }
    assert_eq!(
        "goodbye",
        String::from_json_payload(txt_attr_payload).unwrap()
    );
    assert_eq!(3, usize::from_json_payload(int_attr_payload).unwrap());
    let handle = client.get_untyped_workflow_handle(wf_id.to_string(), "");
    let res = handle
        .get_workflow_result(GetWorkflowResultOpts::default())
        .await
        .unwrap();
    assert_matches!(res, WorkflowExecutionResult::Succeeded(_));
}
