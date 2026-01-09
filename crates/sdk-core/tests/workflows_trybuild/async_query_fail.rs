use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WfContext, WfExitValue, WorkflowResult};

#[workflow]
pub struct BadWorkflow;

#[workflow_methods]
impl BadWorkflow {
    #[run]
    pub async fn run(&mut self, _ctx: &mut WfContext) -> WorkflowResult<()> {
        Ok(WfExitValue::Normal(()))
    }

    #[query]
    pub async fn get_value(&self, _ctx: &WfContext) -> u32 {
        42
    }
}

impl Default for BadWorkflow {
    fn default() -> Self {
        Self
    }
}

fn main() {}
