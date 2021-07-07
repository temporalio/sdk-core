use std::time::Duration;
use temporal_sdk_core::{
    protos::coresdk::workflow_commands::{ContinueAsNewWorkflowExecution, StartTimer},
    protos::temporal::api::enums::v1::WorkflowExecutionStatus,
    test_workflow_driver::{TestRustWorker, WfContext},
    tracing_init,
};
use test_utils::CoreWfStarter;

async fn cancelled_wf(mut ctx: WfContext) {
    let run_ct = ctx.get_args()[0].data[0];
    let timer = StartTimer {
        timer_id: "longtimer".to_string(),
        start_to_fire_timeout: Some(Duration::from_secs(500).into()),
    };

    let cancelled = tokio::select! {
        _ = ctx.timer(timer) => false,
        _ = ctx.cancelled() => true
    };

    if cancelled {
        ctx.complete_cancelled();
    } else {
        panic!("Should have been cancelled")
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_during_timer() {
    tracing_init();
    let wf_name = "cancel_during_timer";
    let mut starter = CoreWfStarter::new(wf_name);
    let tq = starter.get_task_queue().to_owned();
    let core = starter.get_core().await;

    let worker = TestRustWorker::new(core.clone(), tq);
    worker
        .submit_wf(vec![[1].into()], wf_name.to_owned(), cancelled_wf)
        .await
        .unwrap();

    let canceller = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        // Cancel the workflow externally
        core.server_gateway()
            .cancel_workflow_execution(wf_name.to_string(), None)
            .await
            .unwrap();
    };

    let (_, res) = tokio::join!(canceller, worker.run_until_done());
    res.unwrap();
    let desc = core
        .server_gateway()
        .describe_workflow_execution(wf_name.to_string(), None)
        .await
        .unwrap();

    assert_eq!(
        desc.workflow_execution_info.unwrap().status,
        WorkflowExecutionStatus::Canceled as i32
    );

    core.shutdown().await;
}
