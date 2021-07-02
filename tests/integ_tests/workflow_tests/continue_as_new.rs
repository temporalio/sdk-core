use std::time::Duration;
use temporal_sdk_core::{
    protos::coresdk::workflow_commands::{ContinueAsNewWorkflowExecution, StartTimer},
    test_workflow_driver::{TestRustWorker, WfContext},
    tracing_init,
};
use test_utils::CoreWfStarter;

pub async fn continue_as_new_wf(mut ctx: WfContext) {
    let run_ct = ctx.get_args()[0].data[0];
    dbg!(run_ct);
    let timer = StartTimer {
        timer_id: "sometimer".to_string(),
        start_to_fire_timeout: Some(Duration::from_millis(500).into()),
    };
    ctx.timer(timer).await;
    if run_ct < 5 {
        ctx.continue_as_new(ContinueAsNewWorkflowExecution {
            arguments: vec![[run_ct + 1].into()],
            ..Default::default()
        });
    } else {
        ctx.complete_workflow_execution();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn continue_as_new_happy_path() {
    tracing_init();
    let wf_name = "continue_as_new_happy_path";
    let mut starter = CoreWfStarter::new(wf_name);
    let tq = starter.get_task_queue().to_owned();
    let core = starter.get_core().await;

    let worker = TestRustWorker::new(core.clone(), tq);
    worker
        .submit_wf(vec![[1].into()], wf_name.to_owned(), continue_as_new_wf)
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    core.shutdown().await;
}
