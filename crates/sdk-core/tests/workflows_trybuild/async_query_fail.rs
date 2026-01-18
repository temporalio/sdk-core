use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowContextView};

#[workflow]
pub struct BadWorkflow;

#[workflow_methods]
impl BadWorkflow {
    #[run]
    pub async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(WfExitValue::Normal(()))
    }

    // This should fail - queries must not be async
    #[query]
    pub async fn get_value(&self, _ctx: &WorkflowContextView) -> u32 {
        42
    }
}

impl Default for BadWorkflow {
    fn default() -> Self {
        Self
    }
}

fn main() {}
