use crate::common::CoreWfStarter;
use assert_matches::assert_matches;
use temporalio_client::{WorkflowExecutionResult, WorkflowOptions};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowContextView, WorkflowResult};

#[workflow]
#[derive(Default)]
struct InteractionWorkflow {
    counter: i32,
    log: Vec<&'static str>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct ReturnVal {
    counter: i32,
    log: Vec<String>,
}

#[workflow_methods]
impl InteractionWorkflow {
    #[run]
    async fn run(
        ctx: &mut WorkflowContext<Self>,
        wait_for_value: i32,
    ) -> WorkflowResult<ReturnVal> {
        ctx.state_mut(|s| s.log.push("run"));
        ctx.wait_condition(|s| s.counter == wait_for_value).await;
        let rval = ctx.state_mut(|s| {
            s.log.push("run_done");
            ReturnVal {
                counter: s.counter,
                log: std::mem::take(&mut s.log)
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect(),
            }
        });
        Ok(rval.into())
    }

    #[signal]
    fn increment(&mut self, _ctx: &mut WorkflowContext<Self>, amount: i32) {
        self.counter += amount;
    }

    #[signal]
    async fn increment_and_wait(ctx: &mut WorkflowContext<Self>, amount_and_target: (i32, i32)) {
        ctx.state_mut(|s| s.counter += amount_and_target.0);
        ctx.wait_condition(|s| s.counter >= amount_and_target.1)
            .await;
        ctx.state_mut(|s| s.log.push("async signal done"));
    }

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

    #[update_validator(validated_set)]
    fn validate_validated_set(
        &self,
        _ctx: &WorkflowContextView,
        value: &i32,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if *value < 0 {
            Err("Value must be non-negative".into())
        } else {
            Ok(())
        }
    }
    #[update]
    fn validated_set(&mut self, _ctx: &mut WorkflowContext<Self>, value: i32) -> i32 {
        let old = self.counter;
        self.counter = value;
        old
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
    assert_eq!(result.counter, 42);
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
    assert_eq!(result.counter, 999);
}

#[tokio::test]
async fn test_typed_query() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_workflow::<InteractionWorkflow>();

    let handle = worker
        .submit_workflow(
            InteractionWorkflow::run,
            starter.get_task_queue().to_owned(),
            100,
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    let querier = async {
        let counter = handle
            .query(InteractionWorkflow::get_counter, ())
            .await
            .unwrap();
        assert_eq!(counter, 0);
        handle
            .signal(InteractionWorkflow::increment, 50)
            .await
            .unwrap();

        let counter = handle
            .query(InteractionWorkflow::get_counter, ())
            .await
            .unwrap();
        assert_eq!(counter, 50);
        handle
            .signal(InteractionWorkflow::increment, 50)
            .await
            .unwrap();
    };

    let (_, worker_res) = tokio::join!(querier, worker.run_until_done());
    worker_res.unwrap();

    let res = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    let result = assert_matches!(res, WorkflowExecutionResult::Succeeded(r) => r);
    assert_eq!(result.counter, 100);
}

#[tokio::test]
async fn test_update_validation() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_workflow::<InteractionWorkflow>();

    let handle = worker
        .submit_workflow(
            InteractionWorkflow::run,
            starter.get_task_queue().to_owned(),
            42,
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    let updater = async {
        let old_value = handle
            .update(InteractionWorkflow::validated_set, 10)
            .await
            .unwrap();
        assert_eq!(old_value, 0);

        let result = handle.update(InteractionWorkflow::validated_set, -5).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("non-negative"));

        let old_value = handle
            .update(InteractionWorkflow::validated_set, 42)
            .await
            .unwrap();
        assert_eq!(old_value, 10);
    };

    let (_, worker_res) = tokio::join!(updater, worker.run_until_done());
    worker_res.unwrap();

    let res = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    let result = assert_matches!(res, WorkflowExecutionResult::Succeeded(r) => r);
    assert_eq!(result.counter, 42);
}

#[tokio::test]
async fn test_async_signal() {
    let wf_name = InteractionWorkflow::name();
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_workflow::<InteractionWorkflow>();

    let handle = worker
        .submit_workflow(
            InteractionWorkflow::run,
            starter.get_task_queue().to_owned(),
            100,
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    let signaler = async {
        // Send async signal that adds 20 and waits for counter >= 50
        handle
            .signal(InteractionWorkflow::increment_and_wait, (20, 50))
            .await
            .unwrap();

        // Send another signal to reach the target (20 + 30 = 50)
        handle
            .signal(InteractionWorkflow::increment, 30)
            .await
            .unwrap();

        // Now send final signal to complete the workflow (50 + 50 = 100)
        handle
            .signal(InteractionWorkflow::increment, 50)
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
    assert_eq!(result.counter, 100);
    assert!(result.log.contains(&"async signal done".to_owned()));
}
