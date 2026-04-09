#![allow(unreachable_pub)]
use temporalio_common::protos::coresdk::AsJsonPayloadExt;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowResult};

#[workflow]
#[derive(Default)]
pub struct SearchAttributesWorkflow;

#[workflow_methods]
impl SearchAttributesWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, _input: ()) -> WorkflowResult<String> {
        let initial_attrs = ctx.search_attributes();
        let initial_keyword = initial_attrs
            .indexed_fields
            .get("CustomKeywordField")
            .and_then(|p| serde_json::from_slice::<String>(&p.data).ok())
            .unwrap_or_default();

        ctx.upsert_search_attributes([
            (
                "CustomKeywordField".to_string(),
                "updated-value".as_json_payload().unwrap(),
            ),
            (
                "CustomIntField".to_string(),
                42i64.as_json_payload().unwrap(),
            ),
        ]);

        Ok(format!(
            "initial_keyword={initial_keyword}, upserted CustomIntField=42"
        ))
    }
}
