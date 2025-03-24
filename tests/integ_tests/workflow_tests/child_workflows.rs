use anyhow::anyhow;
use assert_matches::assert_matches;
use std::time::Duration;
use temporal_client::{WorkflowClientTrait, WorkflowOptions};
use temporal_sdk::{ChildWorkflowOptions, WfContext, WfExitValue, WorkflowResult};
use temporal_sdk_core_protos::{
    coresdk::{
        AsJsonPayloadExt,
        child_workflow::{ChildWorkflowCancellationType, Success, child_workflow_result},
    },
    temporal::api::enums::v1::ParentClosePolicy,
};
use temporal_sdk_core_test_utils::CoreWfStarter;
use tokio::sync::Barrier;

static PARENT_WF_TYPE: &str = "parent_wf";
static CHILD_WF_TYPE: &str = "child_wf";

async fn child_wf(ctx: WfContext) -> WorkflowResult<()> {
    assert_eq!(
        ctx.workflow_initial_info()
            .parent_workflow_info
            .as_ref()
            .unwrap()
            .workflow_id,
        ctx.workflow_initial_info()
            .root_workflow
            .as_ref()
            .unwrap()
            .workflow_id
    );
    Ok(().into())
}

async fn parent_wf(ctx: WfContext) -> WorkflowResult<()> {
    let child = ctx.child_workflow(ChildWorkflowOptions {
        workflow_id: "child-1".to_owned(),
        workflow_type: CHILD_WF_TYPE.to_owned(),
        ..Default::default()
    });

    let started = child
        .start(&ctx)
        .await
        .into_started()
        .expect("Child chould start OK");
    match started.result().await.status {
        Some(child_workflow_result::Status::Completed(Success { .. })) => Ok(().into()),
        _ => Err(anyhow!("Unexpected child WF status")),
    }
}

#[tokio::test]
async fn child_workflow_happy_path() {
    let mut starter = CoreWfStarter::new("child-workflows");
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;

    worker.register_wf(PARENT_WF_TYPE.to_string(), parent_wf);
    worker.register_wf(CHILD_WF_TYPE.to_string(), child_wf);

    worker
        .submit_wf(
            "parent".to_string(),
            PARENT_WF_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn abandoned_child_bug_repro() {
    let mut starter = CoreWfStarter::new("child-workflow-abandon-bug");
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    let barr: &'static Barrier = Box::leak(Box::new(Barrier::new(2)));

    worker.register_wf(
        PARENT_WF_TYPE.to_string(),
        move |ctx: WfContext| async move {
            let child = ctx.child_workflow(ChildWorkflowOptions {
                workflow_id: "abandoned-child".to_owned(),
                workflow_type: CHILD_WF_TYPE.to_owned(),
                parent_close_policy: ParentClosePolicy::Abandon,
                cancel_type: ChildWorkflowCancellationType::Abandon,
                ..Default::default()
            });

            let started = child
                .start(&ctx)
                .await
                .into_started()
                .expect("Child chould start OK");
            barr.wait().await;
            // Wait for cancel signal
            ctx.cancelled().await;
            // Cancel the child immediately
            started.cancel(&ctx, "Die reason!".to_string());
            // Need to do something else, so we'll see the ChildWorkflowExecutionCanceled event
            ctx.timer(Duration::from_secs(1)).await;
            started.result().await;
            Ok(().into())
        },
    );
    worker.register_wf(CHILD_WF_TYPE.to_string(), |ctx: WfContext| async move {
        ctx.cancelled().await;
        Ok(WfExitValue::<()>::Cancelled)
    });

    worker
        .submit_wf(
            "parent-abandoner".to_string(),
            PARENT_WF_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    let client = starter.get_client().await;
    let canceller = async {
        barr.wait().await;
        client
            .cancel_workflow_execution(
                "parent-abandoner".to_string(),
                None,
                "die".to_string(),
                None,
            )
            .await
            .unwrap();
        client
            .cancel_workflow_execution("abandoned-child".to_string(), None, "die".to_string(), None)
            .await
            .unwrap();
    };
    let runner = async move {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(canceller, runner);
}

#[tokio::test]
async fn abandoned_child_resolves_post_cancel() {
    let mut starter = CoreWfStarter::new("child-workflow-resolves-post-cancel");
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    let barr: &'static Barrier = Box::leak(Box::new(Barrier::new(2)));

    worker.register_wf(
        PARENT_WF_TYPE.to_string(),
        move |ctx: WfContext| async move {
            let child = ctx.child_workflow(ChildWorkflowOptions {
                workflow_id: "abandoned-child-resolve-post-cancel".to_owned(),
                workflow_type: CHILD_WF_TYPE.to_owned(),
                parent_close_policy: ParentClosePolicy::Abandon,
                cancel_type: ChildWorkflowCancellationType::Abandon,
                ..Default::default()
            });

            let started = child
                .start(&ctx)
                .await
                .into_started()
                .expect("Child chould start OK");
            barr.wait().await;
            // Wait for cancel signal
            ctx.cancelled().await;
            // Cancel the child immediately
            started.cancel(&ctx, "Die reason".to_string());
            // Need to do something else, so we will see the child completing
            ctx.timer(Duration::from_secs(1)).await;
            started.result().await;
            Ok(().into())
        },
    );
    worker.register_wf(CHILD_WF_TYPE.to_string(), |_: WfContext| async move {
        Ok("I'm done".into())
    });

    worker
        .submit_wf(
            "parent-abandoner-resolving".to_string(),
            PARENT_WF_TYPE.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    let client = starter.get_client().await;
    let canceller = async {
        barr.wait().await;
        client
            .cancel_workflow_execution(
                "parent-abandoner-resolving".to_string(),
                None,
                "die".to_string(),
                None,
            )
            .await
            .unwrap();
    };
    let runner = async move {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(canceller, runner);
}

#[tokio::test]
async fn cancelled_child_gets_reason() {
    let wf_name = "cancelled-child-gets-reason";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;

    worker.register_wf(wf_name.to_string(), move |ctx: WfContext| async move {
        let child = ctx.child_workflow(ChildWorkflowOptions {
            workflow_id: format!("{}-child", ctx.task_queue()),
            workflow_type: CHILD_WF_TYPE.to_owned(),
            cancel_type: ChildWorkflowCancellationType::WaitCancellationRequested,
            ..Default::default()
        });

        let started = child
            .start(&ctx)
            .await
            .into_started()
            .expect("Child chould start OK");
        // Cancel the child  after start
        started.cancel(&ctx, "Die reason".to_string());
        let r = started.result().await;
        let out = assert_matches!(r.status,
            Some(child_workflow_result::Status::Completed(reason)) => reason);
        assert_eq!(out.result.unwrap(), "Die reason".as_json_payload().unwrap());
        Ok(().into())
    });
    worker.register_wf(CHILD_WF_TYPE.to_string(), |c: WfContext| async move {
        let r = c.cancelled().await;
        Ok(r.into())
    });

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}
