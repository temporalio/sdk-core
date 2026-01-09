use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WfContext, WfExitValue, WorkflowResult};

#[workflow]
pub struct MyWorkflow {
    counter: u32,
}

#[workflow_methods]
impl MyWorkflow {
    #[init]
    pub fn new(_ctx: &WfContext, _input: String) -> Self {
        Self { counter: 0 }
    }

    #[run]
    pub async fn run(&mut self, _ctx: &mut WfContext) -> WorkflowResult<String> {
        Ok(WfExitValue::Normal(format!("Counter: {}", self.counter)))
    }

    #[signal(name = "increment")]
    pub fn increment_counter(&mut self, _ctx: &mut WfContext, amount: u32) {
        self.counter += amount;
    }

    #[signal]
    pub async fn async_signal(&mut self, _ctx: &mut WfContext) {
        // Async signals are allowed
    }

    #[query]
    pub fn get_counter(&self, _ctx: &WfContext) -> u32 {
        self.counter
    }

    #[update(name = "double")]
    pub fn double_counter(&mut self, _ctx: &mut WfContext) -> u32 {
        self.counter *= 2;
        self.counter
    }

    #[update]
    pub async fn async_update(&mut self, _ctx: &mut WfContext, val: i32) -> i32 {
        val * 2
    }
}

fn main() {}
