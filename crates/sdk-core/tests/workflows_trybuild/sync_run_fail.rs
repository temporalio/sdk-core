use temporalio_macros::{workflow, workflow_methods};

#[workflow]
pub struct BadWorkflow;

#[workflow_methods]
impl BadWorkflow {
    // This should fail - run must be async
    #[run]
    pub fn run(&self, _ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(WfExitValue::Normal(()))
    }
}

impl Default for BadWorkflow {
    fn default() -> Self {
        Self
    }
}

fn main() {}
