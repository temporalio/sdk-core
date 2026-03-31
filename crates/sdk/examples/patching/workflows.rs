#![allow(unreachable_pub)]
use std::time::Duration;
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use temporalio_sdk::{ActivityOptions, WorkflowContext, WorkflowResult};

pub struct PatchingActivities;

#[activities]
impl PatchingActivities {
    #[activity]
    pub async fn old_greeting(
        _ctx: ActivityContext,
        name: String,
    ) -> Result<String, ActivityError> {
        Ok(format!("Hello, {name}"))
    }

    #[activity]
    pub async fn new_greeting(
        _ctx: ActivityContext,
        name: String,
    ) -> Result<String, ActivityError> {
        Ok(format!("Hello, {name}! Welcome to Temporal."))
    }
}

fn activity_opts() -> ActivityOptions {
    ActivityOptions {
        start_to_close_timeout: Some(Duration::from_secs(10)),
        ..Default::default()
    }
}

#[workflow]
#[derive(Default)]
pub struct PatchingWorkflow;

#[workflow_methods]
impl PatchingWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, name: String) -> WorkflowResult<String> {
        let greeting = if ctx.patched("v2-greeting") {
            ctx.start_activity(PatchingActivities::new_greeting, name, activity_opts())
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
        } else {
            ctx.start_activity(PatchingActivities::old_greeting, name, activity_opts())
                .await
                .map_err(|e| anyhow::anyhow!("{e}"))?
        };
        Ok(greeting)
    }
}
