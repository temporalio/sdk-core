use std::time::Duration;
use temporal_client::WorkflowClientTrait;
use temporal_sdk::{WfContext, WorkflowResult};
use temporal_sdk_core_test_utils::{get_integ_server_options, CoreWfStarter, NAMESPACE};

pub(crate) async fn eager_wf(_context: WfContext) -> WorkflowResult<()> {
    Ok(().into())
}

#[tokio::test]
async fn eager_wf_start() {
    let wf_name = "eager_wf_start";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.workflow_options.enable_eager_workflow_start = true;
    // hang the test if eager task dispatch failed
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1500));
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), eager_wf);
    starter.eager_start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn eager_wf_start_different_clients() {
    let wf_name = "eager_wf_start";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.workflow_options.enable_eager_workflow_start = true;
    // hang the test if wf task needs retry
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1500));
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), eager_wf);

    let client = get_integ_server_options()
        .connect(NAMESPACE.to_string(), None)
        .await
        .expect("Should connect");
    let w = starter.get_worker().await;
    let res = client
        .start_workflow(
            vec![],
            w.get_config().task_queue.clone(), // task_queue
            wf_name.to_string(),               // workflow_id
            wf_name.to_string(),               // workflow_type
            None,
            starter.workflow_options.clone(),
        )
        .await
        .unwrap();
    // different clients means no eager_wf_start.
    assert!(res.eager_workflow_task.is_none());

    //wf task delivered through default path
    worker.expect_workflow_completion(wf_name, Some(res.run_id));
    worker.run_until_done().await.unwrap();
}
