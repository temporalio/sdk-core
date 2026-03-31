#![allow(unreachable_pub)]
use std::time::Duration;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowResult};

pub struct EncryptionActivities;

#[activities]
impl EncryptionActivities {
    #[activity]
    pub async fn greet(_ctx: ActivityContext, name: String) -> Result<String, ActivityError> {
        Ok(format!("Hello, {name}!"))
    }
}

#[workflow]
#[derive(Default)]
pub struct EncryptionWorkflow;

#[workflow_methods]
impl EncryptionWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        let greeting = ctx
            .start_activity(
                EncryptionActivities::greet,
                name,
                ActivityOptions {
                    start_to_close_timeout: Some(Duration::from_secs(10)),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(greeting)
    }
}
