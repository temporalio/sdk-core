use temporalio_macros::{workflow, workflow_methods};

#[workflow]
pub struct BadWorkflow;

#[workflow_methods]
impl BadWorkflow {
    #[run]
    pub fn run(&mut self, _ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        Ok(WfExitValue::Normal(()))
    }
}

impl Default for BadWorkflow {
    fn default() -> Self {
        Self
    }
}

fn main() {}
