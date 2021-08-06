use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use temporal_sdk_core::protos::coresdk::workflow_commands::StartTimer;
use temporal_sdk_core::prototype_rust_sdk::{WfContext, WorkflowResult};
use test_utils::CoreWfStarter;

const MY_CHANGE_ID: &str = "integ_test_change_name";

pub async fn changes_wf(mut ctx: WfContext) -> WorkflowResult<()> {
    if ctx.has_change(MY_CHANGE_ID) {
        ctx.timer(StartTimer {
            timer_id: "had_change_1".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(200).into()),
        })
        .await;
    } else {
        ctx.timer(StartTimer {
            timer_id: "no_change_1".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(200).into()),
        })
        .await;
    }
    ctx.timer(StartTimer {
        timer_id: "always_timer".to_string(),
        start_to_fire_timeout: Some(Duration::from_millis(200).into()),
    })
    .await;
    if ctx.has_change(MY_CHANGE_ID) {
        ctx.timer(StartTimer {
            timer_id: "had_change_2".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(200).into()),
        })
        .await;
    } else {
        ctx.timer(StartTimer {
            timer_id: "no_change_2".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(200).into()),
        })
        .await;
    }
    Ok(().into())
}

#[tokio::test]
async fn writes_change_markers() {
    let wf_name = "writes_change_markers";
    let mut starter = CoreWfStarter::new(wf_name);
    let worker = starter.worker().await;

    worker
        .submit_wf(vec![], wf_name.to_owned(), changes_wf)
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    starter.shutdown().await;
}

/// This one simulates a run as if the worker had the "old" code, then it fails at the end as
/// a cheapo way of being re-run, at which point it runs with change checks and the "new" code.
static DID_DIE: AtomicBool = AtomicBool::new(false);
pub async fn no_change_then_change_wf(mut ctx: WfContext) -> WorkflowResult<()> {
    if DID_DIE.load(Ordering::Acquire) {
        assert!(!ctx.has_change(MY_CHANGE_ID));
    }
    ctx.timer(StartTimer {
        timer_id: "no_change_1".to_string(),
        start_to_fire_timeout: Some(Duration::from_millis(200).into()),
    })
    .await;
    ctx.timer(StartTimer {
        timer_id: "always_timer".to_string(),
        start_to_fire_timeout: Some(Duration::from_millis(200).into()),
    })
    .await;
    if DID_DIE.load(Ordering::Acquire) {
        assert!(!ctx.has_change(MY_CHANGE_ID));
    }
    ctx.timer(StartTimer {
        timer_id: "no_change_2".to_string(),
        start_to_fire_timeout: Some(Duration::from_millis(200).into()),
    })
    .await;

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
    let worker = starter.worker().await;

    worker
        .submit_wf(vec![], wf_name.to_owned(), no_change_then_change_wf)
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    starter.shutdown().await;
}
