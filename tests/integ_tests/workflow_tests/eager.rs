use std::time::Duration;
use temporal_sdk::{WfContext, WorkflowResult};
use temporal_sdk_core_test_utils::CoreWfStarter;

pub async fn eager_wf(_context: WfContext) -> WorkflowResult<()> {
    Ok(().into())
}

#[tokio::test]
async fn eager_wf_start() {
    let wf_name = "eager_wf_start";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.workflow_options.enable_eager_workflow_start = true;
    // hang the test if eager task dispatch failed
    starter.workflow_options.task_timeout = Some(Duration::from_secs(15));
    starter.no_remote_activities();
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), eager_wf);
    starter.eager_start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}
