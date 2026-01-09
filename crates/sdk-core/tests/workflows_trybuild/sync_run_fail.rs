use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WfContext, WfExitValue, WorkflowResult};

#[workflow]
pub struct BadWorkflow;

#[workflow_methods]
impl BadWorkflow {
    #[run]
    pub fn run(&mut self, _ctx: &mut WfContext) -> WorkflowResult<()> {
        Ok(WfExitValue::Normal(()))
    }
}

impl Default for BadWorkflow {
    fn default() -> Self {
        Self
    }
}

fn main() {}
