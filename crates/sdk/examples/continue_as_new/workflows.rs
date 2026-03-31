#![allow(unreachable_pub)]
use std::time::Duration;
use temporalio_common::protos::coresdk::{
    AsJsonPayloadExt, workflow_commands::ContinueAsNewWorkflowExecution,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowResult, WorkflowTermination};

#[workflow]
#[derive(Default)]
pub struct ContinueAsNewWorkflow;

#[workflow_methods]
impl ContinueAsNewWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, input: (u32, u32)) -> WorkflowResult<String> {
        let (current_iteration, max_iterations) = input;
        ctx.timer(Duration::from_millis(100)).await;

        if current_iteration < max_iterations {
            Err(WorkflowTermination::continue_as_new(
                ContinueAsNewWorkflowExecution {
                    arguments: vec![
                        (current_iteration + 1, max_iterations)
                            .as_json_payload()
                            .unwrap(),
                    ],
                    ..Default::default()
                },
            ))
        } else {
            Ok(format!("Completed after {max_iterations} iterations"))
        }
    }
}
