use crate::integ_tests::workflow_tests::timers::timer_wf;
use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use temporal_sdk_core::{
    protos::coresdk::workflow_commands::StartTimer,
    test_workflow_driver::{TestRustWorker, WfContext},
};
use test_utils::CoreWfStarter;
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread")]
async fn timer_workflow_not_sticky() {
    let wf_name = "timer_wf_not_sticky";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.max_cached_workflows(0);
    let tq = starter.get_task_queue().to_owned();
    let core = starter.get_core().await;

    let worker = TestRustWorker::new(core.clone(), tq.clone());
    worker
        .submit_wf(vec![], wf_name.to_owned(), timer_wf)
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    core.shutdown().await;
}

static TIMED_OUT_ONCE: AtomicBool = AtomicBool::new(false);
async fn timer_timeout_wf(mut command_sink: WfContext) {
    let timer = StartTimer {
        timer_id: "super_timer_id".to_string(),
        start_to_fire_timeout: Some(Duration::from_secs(1).into()),
    };
    let t = command_sink.timer(timer);
    if !TIMED_OUT_ONCE.load(Ordering::SeqCst) {
        sleep(Duration::from_secs(3)).await;
        TIMED_OUT_ONCE.store(true, Ordering::SeqCst);
    }
    t.await;
    command_sink.complete_workflow_execution();
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_workflow_timeout_on_sticky() {
    // This test intentionally times out a workflow task in order to make the next task be scheduled
    // on a not-sticky queue
    let wf_name = "timer_workflow_timeout_on_sticky";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.wft_timeout(Duration::from_secs(2));
    let tq = starter.get_task_queue().to_owned();
    let core = starter.get_core().await;

    let mut worker = TestRustWorker::new(core.clone(), tq.clone());
    worker.override_deadlock(Duration::from_secs(4));
    // let run_id = starter.start_wf().await;
    // worker.start_wf(Arc::new(timer_timeout_wf), run_id);
    worker
        .submit_wf(vec![], wf_name.to_owned(), timer_timeout_wf)
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    core.shutdown().await;
}
