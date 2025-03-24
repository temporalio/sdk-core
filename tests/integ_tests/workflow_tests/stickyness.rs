use crate::integ_tests::workflow_tests::timers::timer_wf;
use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};
use temporal_client::WorkflowOptions;
use temporal_sdk::{WfContext, WorkflowResult};
use temporal_sdk_core_api::worker::PollerBehavior;
use temporal_sdk_core_test_utils::CoreWfStarter;
use tokio::sync::Barrier;

#[tokio::test]
async fn timer_workflow_not_sticky() {
    let wf_name = "timer_wf_not_sticky";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .worker_config
        .no_remote_activities(true)
        .max_cached_workflows(0_usize);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), timer_wf);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

static TIMED_OUT_ONCE: AtomicBool = AtomicBool::new(false);
static RUN_CT: AtomicUsize = AtomicUsize::new(0);
async fn timer_timeout_wf(ctx: WfContext) -> WorkflowResult<()> {
    RUN_CT.fetch_add(1, Ordering::SeqCst);
    let t = ctx.timer(Duration::from_secs(1));
    if !TIMED_OUT_ONCE.load(Ordering::SeqCst) {
        ctx.force_task_fail(anyhow::anyhow!("I AM SLAIN!"));
        TIMED_OUT_ONCE.store(true, Ordering::SeqCst);
    }
    t.await;
    Ok(().into())
}

#[tokio::test]
async fn timer_workflow_timeout_on_sticky() {
    // This test intentionally times out a workflow task in order to make the next task be scheduled
    // on a not-sticky queue
    let wf_name = "timer_workflow_timeout_on_sticky";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(2));
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), timer_timeout_wf);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
    // If it didn't run twice it didn't time out
    assert_eq!(RUN_CT.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cache_miss_ok() {
    let wf_name = "cache_miss_ok";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .worker_config
        .no_remote_activities(true)
        .max_outstanding_workflow_tasks(2_usize)
        .max_cached_workflows(0_usize)
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize));
    let mut worker = starter.worker().await;

    let barr: &'static Barrier = Box::leak(Box::new(Barrier::new(2)));
    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| async move {
        barr.wait().await;
        ctx.timer(Duration::from_secs(1)).await;
        Ok(().into())
    });

    let run_id = worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    let core = starter.get_worker().await;
    let (r1, _) = tokio::join!(worker.run_until_done(), async move {
        barr.wait().await;
        core.request_workflow_eviction(&run_id);
        // We need to signal the barrier again since the wf gets evicted and will hit it again
        barr.wait().await;
    });
    r1.unwrap();
}
