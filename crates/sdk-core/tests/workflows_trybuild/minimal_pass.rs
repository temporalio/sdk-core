use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowResult};

#[workflow]
pub struct MinimalWorkflow;

#[workflow_methods]
impl MinimalWorkflow {
    #[run]
    pub async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }
}

impl Default for MinimalWorkflow {
    fn default() -> Self {
        Self
    }
}

fn main() {}
