use temporalio_macros::{workflow, workflow_methods};

#[workflow]
pub struct BadWorkflow;

#[workflow_methods]
impl BadWorkflow {
    #[run]
    pub async fn run(&mut self, _ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        Ok(WfExitValue::Normal(()))
    }

    #[query]
    pub fn get_value(&mut self, _ctx: &WorkflowContext) -> u32 {
        42
    }
}

impl Default for BadWorkflow {
    fn default() -> Self {
        Self
    }
}

fn main() {}
