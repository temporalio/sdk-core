use std::time::Duration;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WfExitValue, WorkflowContext, WorkflowResult};

#[workflow]
#[derive(Default)]
pub struct GuardAcrossAwaitWorkflow {
    counter: i32,
}

#[workflow_methods]
impl GuardAcrossAwaitWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<i32> {
        // Get a state guard - this borrows ctx mutably
        let guard = ctx.state();
        let initial = guard.counter;

        // Try to await while holding the guard - this should fail to compile
        // because ctx is already borrowed by the guard
        ctx.timer(Duration::from_secs(1)).await;

        // Try to use the guard after the await - this is the unsound pattern
        let final_value = guard.counter;

        Ok(WfExitValue::Normal(final_value - initial))
    }
}

fn main() {}
