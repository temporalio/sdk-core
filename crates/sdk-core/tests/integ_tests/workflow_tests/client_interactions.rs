#![allow(dead_code, unused)] // remove when tests are re-enabled

use crate::common::CoreWfStarter;
use assert_matches::assert_matches;
use temporalio_client::{WorkflowExecutionResult, WorkflowOptions};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WfExitValue, WorkflowContext, WorkflowResult};

#[workflow]
#[derive(Default)]
struct InteractionWorkflow {
    counter: i32,
}

#[workflow_methods]
impl InteractionWorkflow {
    #[run]
    async fn run(
        &mut self,
        ctx: &mut WorkflowContext,
        wait_for_signal: bool,
    ) -> WorkflowResult<i32> {
        if wait_for_signal {
            ctx.wait_condition(|| self.counter != 0).await;
        }
        Ok(WfExitValue::Normal(self.counter))
    }

    #[signal]
    fn increment(&mut self, _ctx: &mut WorkflowContext, amount: i32) {
        self.counter += amount;
    }

    #[query]
    fn get_counter(&self, _ctx: &WorkflowContext) -> i32 {
        self.counter
    }

    #[update]
    fn set_counter(&mut self, _ctx: &mut WorkflowContext, value: i32) -> i32 {
        let old = self.counter;
        self.counter = value;
        old
    }
}

/* TODO [rust-sdk-branch]: Enable when handlers are hooked up to invocation
#[tokio::test]
async fn test_typed_signal() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<InteractionWorkflow>();
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            InteractionWorkflow::run,
            wf_name.to_owned(),
            true,
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    handle
        .signal(InteractionWorkflow::increment, 42)
        .await
        .unwrap();

    worker.run_until_done().await.unwrap();

    let res = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    let result = assert_matches!(res, WorkflowExecutionResult::Succeeded(r) => r);
    assert_eq!(result, 42);
}

#[tokio::test]
async fn test_typed_query() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<InteractionWorkflow>();
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            InteractionWorkflow::run,
            wf_name.to_owned(),
            true,
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    let counter = handle
        .query(InteractionWorkflow::get_counter, ())
        .await
        .unwrap();
    assert_eq!(counter, 0);

    handle
        .signal(InteractionWorkflow::increment, 100)
        .await
        .unwrap();

    let counter = handle
        .query(InteractionWorkflow::get_counter, ())
        .await
        .unwrap();
    assert_eq!(counter, 100);

    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn test_typed_update() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_workflow::<InteractionWorkflow>();
    let mut worker = starter.worker().await;

    let handle = worker
        .submit_workflow(
            InteractionWorkflow::run,
            wf_name.to_owned(),
            true,
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    let old_value = handle
        .update(InteractionWorkflow::set_counter, 999)
        .await
        .unwrap();
    assert_eq!(old_value, 0);

    let current = handle
        .query(InteractionWorkflow::get_counter, ())
        .await
        .unwrap();
    assert_eq!(current, 999);

    worker.run_until_done().await.unwrap();

    let res = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    let result = assert_matches!(res, WorkflowExecutionResult::Succeeded(r) => r);
    assert_eq!(result, 999);
}
*/
