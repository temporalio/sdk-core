use crate::{common::CoreWfStarter, integ_tests::workflow_tests::timers::TimerWf};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use temporalio_client::WorkflowOptions;
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WorkflowContext, WorkflowResult};
use temporalio_sdk_core::{PollerBehavior, TunerHolder};
use tokio::sync::Barrier;

#[tokio::test]
async fn timer_workflow_not_sticky() {
    let wf_name = "timer_wf_not_sticky";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter.sdk_config.max_cached_workflows = 0_usize;
    let mut worker = starter.worker().await;
    worker.register_workflow::<TimerWf>();

    worker
        .submit_workflow(
            TimerWf::run,
            starter.get_task_queue().to_owned(),
            (),
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[workflow]
struct TimerTimeoutWf {
    timed_out_once: Arc<AtomicBool>,
    run_ct: Arc<AtomicUsize>,
}

#[workflow_methods(factory_only)]
impl TimerTimeoutWf {
    #[run]
    pub(crate) async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        self.run_ct.fetch_add(1, Ordering::SeqCst);
        let t = ctx.timer(Duration::from_secs(1));
        if !self.timed_out_once.load(Ordering::SeqCst) {
            ctx.force_task_fail(anyhow::anyhow!("I AM SLAIN!"));
            self.timed_out_once.store(true, Ordering::SeqCst);
        }
        t.await;
        Ok(().into())
    }
}

#[tokio::test]
async fn timer_workflow_timeout_on_sticky() {
    // This test intentionally times out a workflow task in order to make the next task be scheduled
    // on a not-sticky queue
    let wf_name = "timer_workflow_timeout_on_sticky";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter.workflow_options.task_timeout = Some(Duration::from_secs(2));
    let mut worker = starter.worker().await;

    let timed_out_once = Arc::new(AtomicBool::new(false));
    let run_ct = Arc::new(AtomicUsize::new(0));
    let run_ct_clone = run_ct.clone();
    worker.register_workflow_with_factory(move || TimerTimeoutWf {
        timed_out_once: timed_out_once.clone(),
        run_ct: run_ct_clone.clone(),
    });

    worker
        .submit_workflow(
            TimerTimeoutWf::run,
            starter.get_task_queue().to_owned(),
            (),
            std::mem::take(&mut starter.workflow_options),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    // If it didn't run twice it didn't time out
    assert_eq!(run_ct.load(Ordering::SeqCst), 2);
}

#[workflow]
struct CacheMissWf {
    barr: Arc<Barrier>,
}

#[workflow_methods(factory_only)]
impl CacheMissWf {
    #[run]
    pub(crate) async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
        self.barr.wait().await;
        ctx.timer(Duration::from_secs(1)).await;
        Ok(().into())
    }
}

#[tokio::test]
async fn cache_miss_ok() {
    let wf_name = "cache_miss_ok";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(2, 1, 1, 1));
    starter.sdk_config.max_cached_workflows = 0_usize;
    starter.sdk_config.workflow_task_poller_behavior = PollerBehavior::SimpleMaximum(1_usize);
    let mut worker = starter.worker().await;

    let barr = Arc::new(Barrier::new(2));
    let barr_clone = barr.clone();
    worker.register_workflow_with_factory(move || CacheMissWf {
        barr: barr_clone.clone(),
    });

    let handle = worker
        .submit_workflow(
            CacheMissWf::run,
            wf_name.to_owned(),
            (),
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    let core = starter.get_worker().await;
    let run_id = handle.info().run_id.clone().unwrap();
    let (r1, _) = tokio::join!(worker.run_until_done(), async move {
        barr.wait().await;
        core.request_workflow_eviction(&run_id);
        // We need to signal the barrier again since the wf gets evicted and will hit it again
        barr.wait().await;
    });
    r1.unwrap();
}
