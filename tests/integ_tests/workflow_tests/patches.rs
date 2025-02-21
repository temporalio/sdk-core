use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use temporal_client::WorkflowClientTrait;
use tokio::{join, sync::Notify};
use tokio_stream::StreamExt;

use temporal_sdk::{WfContext, WorkflowResult};
use temporal_sdk_core_test_utils::CoreWfStarter;

const MY_PATCH_ID: &str = "integ_test_change_name";

pub(crate) async fn changes_wf(ctx: WfContext) -> WorkflowResult<()> {
    if ctx.patched(MY_PATCH_ID) {
        ctx.timer(Duration::from_millis(100)).await;
    } else {
        ctx.timer(Duration::from_millis(200)).await;
    }
    ctx.timer(Duration::from_millis(200)).await;
    if ctx.patched(MY_PATCH_ID) {
        ctx.timer(Duration::from_millis(100)).await;
    } else {
        ctx.timer(Duration::from_millis(200)).await;
    }
    Ok(().into())
}

#[tokio::test]
async fn writes_change_markers() {
    let wf_name = "writes_change_markers";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), changes_wf);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

/// This one simulates a run as if the worker had the "old" code, then it fails at the end as
/// a cheapo way of being re-run, at which point it runs with change checks and the "new" code.
static DID_DIE: AtomicBool = AtomicBool::new(false);

pub(crate) async fn no_change_then_change_wf(ctx: WfContext) -> WorkflowResult<()> {
    if DID_DIE.load(Ordering::Acquire) {
        assert!(!ctx.patched(MY_PATCH_ID));
    }
    ctx.timer(Duration::from_millis(200)).await;
    ctx.timer(Duration::from_millis(200)).await;
    if DID_DIE.load(Ordering::Acquire) {
        assert!(!ctx.patched(MY_PATCH_ID));
    }
    ctx.timer(Duration::from_millis(200)).await;

    if !DID_DIE.load(Ordering::Acquire) {
        DID_DIE.store(true, Ordering::Release);
        ctx.force_task_fail(anyhow::anyhow!("i'm ded"));
    }
    Ok(().into())
}

#[tokio::test]
async fn can_add_change_markers() {
    let wf_name = "can_add_change_markers";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), no_change_then_change_wf);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

static DID_DIE_2: AtomicBool = AtomicBool::new(false);

pub(crate) async fn replay_with_change_marker_wf(ctx: WfContext) -> WorkflowResult<()> {
    assert!(ctx.patched(MY_PATCH_ID));
    ctx.timer(Duration::from_millis(200)).await;
    if !DID_DIE_2.load(Ordering::Acquire) {
        DID_DIE_2.store(true, Ordering::Release);
        ctx.force_task_fail(anyhow::anyhow!("i'm ded"));
    }
    Ok(().into())
}

#[tokio::test]
async fn replaying_with_patch_marker() {
    let wf_name = "replaying_with_patch_marker";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), replay_with_change_marker_wf);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

/// Test that the internal patching mechanism works on the second workflow task when replaying.
/// Used as regression test for a bug that detected that we did not look ahead far enough to find
/// the next workflow task completion, which the flags are attached to.
#[tokio::test]
async fn patched_on_second_workflow_task_is_deterministic() {
    let wf_name = "timer_patched_timer";
    let mut starter = CoreWfStarter::new(wf_name);
    // Disable caching to force replay from beginning
    starter
        .worker_config
        .max_cached_workflows(0_usize)
        .no_remote_activities(true);
    let mut worker = starter.worker().await;
    // Include a task failure as well to make sure that works
    static FAIL_ONCE: AtomicBool = AtomicBool::new(true);
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        ctx.timer(Duration::from_millis(1)).await;
        if FAIL_ONCE.load(Ordering::Acquire) {
            FAIL_ONCE.store(false, Ordering::Release);
            panic!("Enchi is hungry!");
        }
        assert!(ctx.patched(MY_PATCH_ID));
        ctx.timer(Duration::from_millis(1)).await;
        Ok(().into())
    });

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn can_remove_deprecated_patch_near_other_patch() {
    let wf_name = "can_add_change_markers";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    let did_die = Arc::new(AtomicBool::new(false));
    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| {
        let did_die = did_die.clone();
        async move {
            ctx.timer(Duration::from_millis(200)).await;
            if !did_die.load(Ordering::Acquire) {
                assert!(ctx.deprecate_patch("getting-deprecated"));
                assert!(ctx.patched("staying"));
            } else {
                assert!(ctx.patched("staying"));
            }
            ctx.timer(Duration::from_millis(200)).await;

            if !did_die.load(Ordering::Acquire) {
                did_die.store(true, Ordering::Release);
                ctx.force_task_fail(anyhow::anyhow!("i'm ded"));
            }
            Ok(().into())
        }
    });

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn deprecated_patch_removal() {
    let wf_name = "deprecated_patch_removal";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    let wf_id = starter.get_task_queue().to_string();
    let did_die = Arc::new(AtomicBool::new(false));
    let send_sig = Arc::new(Notify::new());
    let send_sig_c = send_sig.clone();
    worker.register_wf(wf_name, move |ctx: WfContext| {
        let did_die = did_die.clone();
        let send_sig_c = send_sig_c.clone();
        async move {
            if !did_die.load(Ordering::Acquire) {
                assert!(ctx.deprecate_patch("getting-deprecated"));
            }
            send_sig_c.notify_one();
            ctx.make_signal_channel("sig").next().await;

            ctx.timer(Duration::from_millis(1)).await;

            if !did_die.load(Ordering::Acquire) {
                did_die.store(true, Ordering::Release);
                ctx.force_task_fail(anyhow::anyhow!("i'm ded"));
            }
            Ok(().into())
        }
    });

    starter.start_with_worker(wf_name, &mut worker).await;
    let sig_fut = async {
        send_sig.notified().await;
        client
            .signal_workflow_execution(wf_id, "".to_string(), "sig".to_string(), None, None)
            .await
            .unwrap()
    };
    let run_fut = async {
        worker.run_until_done().await.unwrap();
    };
    join!(sig_fut, run_fut);
}
