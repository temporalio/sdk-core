use std::{
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use temporal_client::WorkflowClientTrait;
use tokio_util::sync::CancellationToken;

use temporal_sdk::{WfContext, WorkflowResult};
use temporal_sdk_core_protos::coresdk::workflow_commands::SetPatchMarker;
use temporal_sdk_core_protos::coresdk::workflow_completion::WorkflowActivationCompletion;
use temporal_sdk_core_protos::temporal::api::query::v1::WorkflowQuery;
use temporal_sdk_core_test_utils::CoreWfStarter;

const MY_PATCH_ID: &str = "integ_test_change_name";

pub async fn changes_wf(ctx: WfContext) -> WorkflowResult<()> {
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
    starter.no_remote_activities();
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), changes_wf);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

/// This one simulates a run as if the worker had the "old" code, then it fails at the end as
/// a cheapo way of being re-run, at which point it runs with change checks and the "new" code.
static DID_DIE: AtomicBool = AtomicBool::new(false);
pub async fn no_change_then_change_wf(ctx: WfContext) -> WorkflowResult<()> {
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
    starter.no_remote_activities();
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), no_change_then_change_wf);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

static DID_DIE_2: AtomicBool = AtomicBool::new(false);
pub async fn replay_with_change_marker_wf(ctx: WfContext) -> WorkflowResult<()> {
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
    starter.no_remote_activities();
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), replay_with_change_marker_wf);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn query_replay_with_patch_marker_is_deterministic() {
    let mut starter = CoreWfStarter::new("patch_and_wait");
    starter.max_cached_workflows(0).no_remote_activities();
    let worker = starter.get_worker().await;
    let client = starter.get_client().await;

    let run_id = starter.start_wf().await;
    let token = CancellationToken::new();
    let token_clone = token.clone();

    let query_future = async {
        token.cancelled().await;
        client
            .query_workflow_execution(
                starter.get_wf_id().to_owned(),
                run_id,
                WorkflowQuery {
                    query_type: "irrelevant".to_owned(),
                    query_args: None,
                    header: None,
                },
            )
            .await
            .unwrap();
    };

    let poll_future = async {
        // First task is replayed
        for _ in 0..2 {
            // 1st iteration: StartWorkflow
            // 2nd iteration (replay): StartWorkflow, NotifyHasPatch
            let activation = worker.poll_workflow_activation().await.unwrap();
            println!("p1 {}", activation);
            let response = vec![SetPatchMarker {
                patch_id: "test".to_string(),
                deprecated: false,
            }
            .into()];
            worker
                .complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
                    activation.run_id,
                    response.clone(),
                ))
                .await
                .unwrap();
            // 1st iteration: RemoveFromCache
            // 2nd iteration (replay): QueryWorkflow
            let activation = worker.poll_workflow_activation().await.unwrap();
            println!("p2 {}", activation);
            worker
                .complete_workflow_activation(WorkflowActivationCompletion::empty(
                    activation.run_id,
                ))
                .await
                .unwrap();
            token_clone.cancel();
        }
    };

    tokio::join!(query_future, poll_future);
}
