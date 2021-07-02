use std::{sync::Arc, time::Duration};
use temporal_sdk_core::protos::coresdk::workflow_commands::{
    ContinueAsNewWorkflowExecution, StartTimer,
};
use temporal_sdk_core::test_workflow_driver::{TestRustWorker, WfContext};
use temporal_sdk_core::tracing_init;
use test_utils::{CoreWfStarter, NAMESPACE};

// TODO: Plumb args into command sender (change to context?)
pub async fn continue_as_new_wf(mut ctx: WfContext) {
    dbg!(ctx.get_args());
    let timer = StartTimer {
        timer_id: "sometimer".to_string(),
        start_to_fire_timeout: Some(Duration::from_millis(500).into()),
    };
    ctx.timer(timer).await;
    ctx.continue_as_new(ContinueAsNewWorkflowExecution {
        arguments: vec![[1, 2, 3].into()],
        ..Default::default()
    });
}

#[tokio::test(flavor = "multi_thread")]
async fn continue_as_new_happy_path() {
    tracing_init();
    let wf_name = "continue_as_new_happy_path";
    let mut starter = CoreWfStarter::new(wf_name);
    let tq = starter.get_task_queue().to_owned();
    let core = starter.get_core().await;

    let worker = TestRustWorker::new(core.clone(), NAMESPACE.to_owned(), tq);
    worker
        .submit_wf(wf_name.to_owned(), Arc::new(continue_as_new_wf))
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    core.shutdown().await;
}
