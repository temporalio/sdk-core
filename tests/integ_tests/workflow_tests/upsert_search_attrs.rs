use std::collections::HashMap;
use temporal_client::WorkflowOptions;
use temporal_sdk::{WfContext, WorkflowResult};
use temporal_sdk_core_protos::coresdk::AsJsonPayloadExt;
use temporal_sdk_core_test_utils::CoreWfStarter;
use uuid::Uuid;

// These are initialized for the server as part of the autosetup server container which we
// use for integration tests.
static ATTR1: &str = "CustomTextField";
static ATTR2: &str = "CustomIntField";

async fn search_attr_updater(ctx: WfContext) -> WorkflowResult<()> {
    ctx.upsert_search_attributes([
        (ATTR1.to_string(), "goodbye".as_json_payload().unwrap()),
        (ATTR2.to_string(), 98.as_json_payload().unwrap()),
    ]);
    Ok(().into())
}

#[tokio::test]
async fn sends_upsert() {
    let wf_name = "sends_upsert_search_attrs";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name, search_attr_updater);
    worker
        .submit_wf(
            Uuid::new_v4().to_string(),
            wf_name,
            vec![],
            WorkflowOptions {
                search_attributes: Some(HashMap::from([
                    (ATTR1.to_string(), "hello".as_json_payload().unwrap()),
                    (ATTR2.to_string(), 1.as_json_payload().unwrap()),
                ])),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}
