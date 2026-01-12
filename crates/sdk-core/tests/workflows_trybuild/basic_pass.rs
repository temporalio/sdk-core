use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WfExitValue, WorkflowContext, WorkflowResult};

#[workflow]
pub struct MyWorkflow {
    counter: u32,
}

#[workflow_methods]
impl MyWorkflow {
    #[init]
    pub fn new(_ctx: &WorkflowContext, _input: String) -> Self {
        Self { counter: 0 }
    }

    #[run]
    pub async fn run(&mut self, _ctx: &mut WorkflowContext) -> WorkflowResult<String> {
        Ok(WfExitValue::Normal(format!("Counter: {}", self.counter)))
    }

    #[signal(name = "increment")]
    pub fn increment_counter(&mut self, _ctx: &mut WorkflowContext, amount: u32) {
        self.counter += amount;
    }

    #[signal]
    pub async fn async_signal(&mut self, _ctx: &mut WorkflowContext) {
        // Async signals are allowed
    }

    #[query]
    pub fn get_counter(&self, _ctx: &WorkflowContext) -> u32 {
        self.counter
    }

    #[update(name = "double")]
    pub fn double_counter(&mut self, _ctx: &mut WorkflowContext) -> u32 {
        self.counter *= 2;
        self.counter
    }

    #[update]
    pub async fn async_update(&mut self, _ctx: &mut WorkflowContext, val: i32) -> i32 {
        val * 2
    }
}

fn main() {}
