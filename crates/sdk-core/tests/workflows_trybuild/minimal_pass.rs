use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WfExitValue, WorkflowContext, WorkflowResult};

#[workflow]
pub struct MinimalWorkflow;

#[workflow_methods]
impl MinimalWorkflow {
    #[run]
    pub async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(WfExitValue::Normal(()))
    }
}

impl Default for MinimalWorkflow {
    fn default() -> Self {
        Self
    }
}

fn main() {}
