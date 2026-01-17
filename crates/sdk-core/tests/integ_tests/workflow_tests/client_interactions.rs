#![allow(dead_code, unused)] // remove when tests are re-enabled

use crate::common::CoreWfStarter;
use assert_matches::assert_matches;
use temporalio_client::{WorkflowExecutionResult, WorkflowOptions};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WfExitValue, WorkflowContext, WorkflowContextView, WorkflowResult};

#[workflow]
#[derive(Default)]
struct InteractionWorkflow {
    counter: i32,
    log: Vec<&'static str>,
}

#[workflow_methods]
impl InteractionWorkflow {
    // Async - uses &self, reads directly, mutates via ctx.state_mut()
    #[run]
    async fn run(
        &self,
        ctx: &mut WorkflowContext<Self>,
        wait_for_value: i32,
    ) -> WorkflowResult<i32> {
        ctx.state_mut(|s| s.log.push("run"));
        ctx.wait_condition(|| self.counter == wait_for_value).await;
        Ok(WfExitValue::Normal(self.counter))
    }

    // Sync signal - uses &mut self, direct field access
    #[signal]
    fn increment(&mut self, _ctx: &mut WorkflowContext<Self>, amount: i32) {
        self.counter += amount;
    }

    // Query - uses &self with read-only context
    #[query]
    fn get_counter(&self, _ctx: &WorkflowContextView) -> i32 {
        self.counter
    }

    // Sync update - uses &mut self, direct field access
    #[update]
    fn set_counter(&mut self, _ctx: &mut WorkflowContext<Self>, value: i32) -> i32 {
        let old = self.counter;
        self.counter = value;
        old
    }

    // Async update - uses &self, reads directly, mutates via ctx.state_mut()
    #[update]
    async fn change_and_wait(&self, ctx: &mut WorkflowContext<Self>, amount_and_wait: (i32, i32)) {
        ctx.state_mut(|s| {
            s.log.push("starting change_and_wait");
            s.counter += amount_and_wait.0;
        });
        ctx.wait_condition(|| self.counter == amount_and_wait.1)
            .await;
        ctx.state_mut(|s| s.log.push("done change_and_wait"));
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
