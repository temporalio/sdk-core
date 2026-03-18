// Regression test for https://github.com/temporalio/sdk-core/issues/1161
//
// Verifies that typed signal/query/update APIs on WorkflowHandle work with
// handles parameterized by either the workflow struct or the Run marker struct.

use temporalio_client::{
    WorkflowExecuteUpdateOptions, WorkflowHandle, WorkflowQueryOptions, WorkflowSignalOptions,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult};

#[workflow]
#[derive(Default)]
pub struct MyWorkflow {
    counter: u32,
}

#[workflow_methods]
impl MyWorkflow {
    #[run]
    pub async fn run(_ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        Ok(())
    }

    #[signal]
    pub fn increment(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _amount: u32) {
        self.counter += _amount;
    }

    #[query]
    pub fn get_counter(&self, _ctx: &WorkflowContextView) -> u32 {
        self.counter
    }

    #[update]
    pub fn handle_update(
        &mut self,
        _ctx: &mut SyncWorkflowContext<Self>,
        input: String,
    ) -> String {
        input
    }
}

// The existing convention: handles parameterized with module::Run.
async fn use_typed_handle_with_run_marker(handle: &WorkflowHandle<temporalio_client::Client, my_workflow::Run>) {
    handle
        .signal(MyWorkflow::increment, 42, WorkflowSignalOptions::default())
        .await
        .unwrap();
    let _count: u32 = handle
        .query(MyWorkflow::get_counter, (), WorkflowQueryOptions::default())
        .await
        .unwrap();
    let _result: String = handle
        .execute_update(
            MyWorkflow::handle_update,
            "hello".to_string(),
            WorkflowExecuteUpdateOptions::default(),
        )
        .await
        .unwrap();
}

// The fix for #1161: handles parameterized with the workflow struct itself.
async fn use_typed_handle_with_workflow_struct(handle: &WorkflowHandle<temporalio_client::Client, MyWorkflow>) {
    handle
        .signal(MyWorkflow::increment, 42, WorkflowSignalOptions::default())
        .await
        .unwrap();
    let _count: u32 = handle
        .query(MyWorkflow::get_counter, (), WorkflowQueryOptions::default())
        .await
        .unwrap();
    let _result: String = handle
        .execute_update(
            MyWorkflow::handle_update,
            "hello".to_string(),
            WorkflowExecuteUpdateOptions::default(),
        )
        .await
        .unwrap();
}

fn main() {}
