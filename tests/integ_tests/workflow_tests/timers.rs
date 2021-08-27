use std::time::Duration;
use temporal_sdk_core::prototype_rust_sdk::{WfContext, WorkflowResult};
use temporal_sdk_core_protos::coresdk::{
    workflow_commands::{CancelTimer, CompleteWorkflowExecution, StartTimer},
    workflow_completion::WfActivationCompletion,
};
use test_utils::{init_core_and_create_wf, CoreTestHelpers, CoreWfStarter};

pub async fn timer_wf(mut command_sink: WfContext) -> WorkflowResult<()> {
    command_sink.timer(Duration::from_secs(1)).await;
    Ok(().into())
}

#[tokio::test]
async fn timer_workflow_workflow_driver() {
    let wf_name = "timer_wf_new";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), timer_wf);

    worker
        .submit_wf(wf_name.to_owned(), wf_name.to_owned(), vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    starter.shutdown().await;
}

#[tokio::test]
async fn timer_workflow_manual() {
    let (core, task_q) = init_core_and_create_wf("timer_workflow").await;
    let task = core.poll_workflow_activation(&task_q).await.unwrap();
    core.complete_workflow_activation(WfActivationCompletion::from_cmds(
        &task_q,
        task.run_id,
        vec![StartTimer {
            seq: 0,
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
        }
        .into()],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation(&task_q).await.unwrap();
    core.complete_execution(&task_q, &task.run_id).await;
    core.shutdown().await;
}

#[tokio::test]
async fn timer_cancel_workflow() {
    let (core, task_q) = init_core_and_create_wf("timer_cancel_workflow").await;
    let task = core.poll_workflow_activation(&task_q).await.unwrap();
    core.complete_workflow_activation(WfActivationCompletion::from_cmds(
        &task_q,
        task.run_id,
        vec![
            StartTimer {
                seq: 0,
                start_to_fire_timeout: Some(Duration::from_millis(50).into()),
            }
            .into(),
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(Duration::from_secs(10).into()),
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation(&task_q).await.unwrap();
    core.complete_workflow_activation(WfActivationCompletion::from_cmds(
        &task_q,
        task.run_id,
        vec![
            CancelTimer { seq: 1 }.into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn timer_immediate_cancel_workflow() {
    let (core, task_q) = init_core_and_create_wf("timer_immediate_cancel_workflow").await;
    let task = core.poll_workflow_activation(&task_q).await.unwrap();
    core.complete_workflow_activation(WfActivationCompletion::from_cmds(
        &task_q,
        task.run_id,
        vec![
            StartTimer {
                seq: 0,
                ..Default::default()
            }
            .into(),
            CancelTimer { seq: 0 }.into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();
}

async fn parallel_timer_wf(mut command_sink: WfContext) -> WorkflowResult<()> {
    let t1 = command_sink.timer(Duration::from_secs(1));
    let t2 = command_sink.timer(Duration::from_secs(1));
    let _ = tokio::join!(t1, t2);
    Ok(().into())
}

#[tokio::test]
async fn parallel_timers() {
    let wf_name = "parallel_timers";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), parallel_timer_wf);

    worker
        .submit_wf(wf_name.to_owned(), wf_name.to_owned(), vec![])
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    starter.shutdown().await;
}
