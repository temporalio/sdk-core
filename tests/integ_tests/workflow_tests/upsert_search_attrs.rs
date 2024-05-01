use std::{collections::HashMap, env};
use temporal_client::{WorkflowClientTrait, WorkflowOptions};
use temporal_sdk::{WfContext, WorkflowResult};
use temporal_sdk_core_protos::coresdk::{AsJsonPayloadExt, FromJsonPayloadExt};
use temporal_sdk_core_test_utils::{CoreWfStarter, INTEG_TEMPORAL_DEV_SERVER_USED_ENV_VAR};
use tracing::warn;
use uuid::Uuid;

// These are initialized on the server as part of the autosetup container which we
// use for integration tests.
static TXT_ATTR: &str = "CustomTextField";
static INT_ATTR: &str = "CustomIntField";

async fn search_attr_updater(ctx: WfContext) -> WorkflowResult<()> {
    ctx.upsert_search_attributes([
        (TXT_ATTR.to_string(), "goodbye".as_json_payload().unwrap()),
        (INT_ATTR.to_string(), 98.as_json_payload().unwrap()),
    ]);
    Ok(().into())
}

#[tokio::test]
async fn sends_upsert() {
    let wf_name = "sends_upsert_search_attrs";
    let wf_id = Uuid::new_v4();
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    // TODO: this should be supported in server 1.20, remove this condition when CLI is upgraded.
    if env::var(INTEG_TEMPORAL_DEV_SERVER_USED_ENV_VAR).is_ok() {
        warn!("skipping sends_upsert -- does not work on temporal dev server");
        return;
    }

    worker.register_wf(wf_name, search_attr_updater);
    let run_id = worker
        .submit_wf(
            wf_id.to_string(),
            wf_name,
            vec![],
            WorkflowOptions {
                search_attributes: Some(HashMap::from([
                    (TXT_ATTR.to_string(), "hello".as_json_payload().unwrap()),
                    (INT_ATTR.to_string(), 1.as_json_payload().unwrap()),
                ])),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let search_attrs = starter
        .get_client()
        .await
        .describe_workflow_execution(wf_id.to_string(), Some(run_id))
        .await
        .unwrap()
        .workflow_execution_info
        .unwrap()
        .search_attributes
        .unwrap()
        .indexed_fields;
    let txt_attr_payload = search_attrs.get(TXT_ATTR).unwrap();
    let int_attr_payload = search_attrs.get(INT_ATTR).unwrap();
    for payload in [txt_attr_payload, int_attr_payload] {
        assert!(payload.is_json_payload());
    }
    assert_eq!(
        "goodbye",
        String::from_json_payload(txt_attr_payload).unwrap()
    );
    assert_eq!(98, usize::from_json_payload(int_attr_payload).unwrap());
}
