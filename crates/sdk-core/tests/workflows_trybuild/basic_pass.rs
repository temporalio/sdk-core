use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowContextView, WorkflowResult};

#[workflow]
pub struct MyWorkflow {
    counter: u32,
}

#[workflow_methods]
impl MyWorkflow {
    #[init]
    pub fn new(_ctx: &WorkflowContextView, _input: String) -> Self {
        Self { counter: 0 }
    }

    // Async run uses &self
    #[run]
    pub async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<String> {
        Ok("hi".to_owned())
    }

    // Sync signal uses &mut self
    #[signal(name = "increment")]
    pub fn increment_counter(&mut self, _ctx: &mut WorkflowContext<Self>, amount: u32) {
        self.counter += amount;
    }

    #[signal]
    pub async fn async_signal(_ctx: &mut WorkflowContext<Self>) {}

    // Query uses &self with read-only context
    #[query]
    pub fn get_counter(&self, _ctx: &WorkflowContextView) -> u32 {
        self.counter
    }

    #[update(name = "double")]
    pub fn double_counter(&mut self, _ctx: &mut WorkflowContext<Self>) -> u32 {
        self.counter *= 2;
        self.counter
    }

    #[update]
    pub async fn async_update(_ctx: &mut WorkflowContext<Self>, val: i32) -> i32 {
        val * 2
    }
}

fn main() {}
