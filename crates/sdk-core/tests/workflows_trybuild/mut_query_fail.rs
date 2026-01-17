use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowContextView};

#[workflow]
pub struct BadWorkflow;

#[workflow_methods]
impl BadWorkflow {
    #[run]
    pub async fn run(&self, _ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(WfExitValue::Normal(()))
    }

    // This should fail - queries must use &self, not &mut self
    #[query]
    pub fn get_value(&mut self, _ctx: &WorkflowContextView) -> u32 {
        42
    }
}

impl Default for BadWorkflow {
    fn default() -> Self {
        Self
    }
}

fn main() {}
