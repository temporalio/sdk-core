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
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, wait_for_value: i32) -> WorkflowResult<i32> {
        ctx.state_mut(|s| s.log.push("run"));
        ctx.wait_condition(|s| s.counter == wait_for_value).await;
        Ok(WfExitValue::Normal(ctx.state(|s| s.counter)))
    }

    #[signal]
    fn increment(&mut self, _ctx: &mut WorkflowContext<Self>, amount: i32) {
        self.counter += amount;
    }

    // Query - uses &self with read-only context
    // Note: Queries not yet implemented in Rust SDK, so this is unused for now
    #[allow(dead_code)]
    #[query]
    fn get_counter(&self, _ctx: &WorkflowContextView) -> i32 {
        self.counter
    }

    #[update]
    fn set_counter(&mut self, _ctx: &mut WorkflowContext<Self>, value: i32) -> i32 {
        self.log.push("set counter");
        let old = self.counter;
        self.counter = value;
        old
    }

    #[update]
    fn invalidate_vec(&mut self, _ctx: &mut WorkflowContext<Self>) {
        self.log = vec![]
    }

    #[update]
    async fn change_and_wait(ctx: &mut WorkflowContext<Self>, amount_and_wait: (i32, i32)) {
        ctx.state_mut(|s| {
            s.log.push("starting change_and_wait");
            s.counter += amount_and_wait.0;
        });
        ctx.wait_condition(|s| s.counter == amount_and_wait.1).await;
        ctx.state_mut(|s| s.log.push("done change_and_wait"));
    }
}

// TODO: add another test that uses wait condition concurrently with some future that uses
// state_mut

#[tokio::test]
async fn test_typed_signal() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_workflow::<InteractionWorkflow>();

    let handle = worker
        .submit_workflow(
            InteractionWorkflow::run,
            starter.get_task_queue().to_owned(),
            42, // Wait for counter == 42
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    // Send signal concurrently with worker
    let signaler = async {
        handle
            .signal(InteractionWorkflow::increment, 42)
            .await
            .unwrap();
    };

    let (_, worker_res) = tokio::join!(signaler, worker.run_until_done());
    worker_res.unwrap();

    let res = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    let result = assert_matches!(res, WorkflowExecutionResult::Succeeded(r) => r);
    assert_eq!(result, 42);
}

#[tokio::test]
async fn test_typed_update() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_workflow::<InteractionWorkflow>();

    let handle = worker
        .submit_workflow(
            InteractionWorkflow::run,
            starter.get_task_queue().to_owned(),
            999, // Wait for counter == 999
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    let updates = async {
        let old_value = handle
            .update(InteractionWorkflow::set_counter, 2)
            .await
            .unwrap();
        assert_eq!(old_value, 0);
        handle
            .update(InteractionWorkflow::invalidate_vec, ())
            .await
            .unwrap();
        handle
            .update(InteractionWorkflow::change_and_wait, (997, 999))
            .await
            .unwrap();
    };

    let (_, workerres) = tokio::join!(updates, worker.run_until_done());
    workerres.unwrap();

    let res = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    let result = assert_matches!(res, WorkflowExecutionResult::Succeeded(r) => r);
    assert_eq!(result, 999);
}

// Query tests are commented out - queries are not yet implemented in the Rust SDK
// See workflow_future.rs where QueryWorkflow variant logs an error
/*
#[tokio::test]
async fn test_typed_query() {
    ...
}
*/
