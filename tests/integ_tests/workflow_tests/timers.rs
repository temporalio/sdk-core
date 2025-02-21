use std::time::Duration;

use temporal_sdk::{WfContext, WorkflowResult};
use temporal_sdk_core_protos::coresdk::{
    workflow_commands::{CancelTimer, CompleteWorkflowExecution, StartTimer},
    workflow_completion::WorkflowActivationCompletion,
};
use temporal_sdk_core_test_utils::{
    CoreWfStarter, WorkerTestHelpers, drain_pollers_and_shutdown, init_core_and_create_wf,
    start_timer_cmd,
};

pub(crate) async fn timer_wf(command_sink: WfContext) -> WorkflowResult<()> {
    command_sink.timer(Duration::from_secs(1)).await;
    Ok(().into())
}

#[tokio::test]
async fn timer_workflow_workflow_driver() {
    let wf_name = "timer_wf_new";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), timer_wf);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn timer_workflow_manual() {
    let mut starter = init_core_and_create_wf("timer_workflow").await;
    let core = starter.get_worker().await;
    starter.worker_config.no_remote_activities(true);
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            StartTimer {
                seq: 0,
                start_to_fire_timeout: Some(prost_dur!(from_secs(1))),
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_execution(&task.run_id).await;
    core.handle_eviction().await;
    drain_pollers_and_shutdown(&core).await;
}

#[tokio::test]
async fn timer_cancel_workflow() {
    let mut starter = init_core_and_create_wf("timer_cancel_workflow").await;
    let core = starter.get_worker().await;
    starter.worker_config.no_remote_activities(true);
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            StartTimer {
                seq: 0,
                start_to_fire_timeout: Some(prost_dur!(from_millis(50))),
            }
            .into(),
            StartTimer {
                seq: 1,
                start_to_fire_timeout: Some(prost_dur!(from_secs(10))),
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
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
    let mut starter = init_core_and_create_wf("timer_immediate_cancel_workflow").await;
    let core = starter.get_worker().await;
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            start_timer_cmd(0, Duration::from_secs(1)),
            CancelTimer { seq: 0 }.into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();
}

async fn parallel_timer_wf(command_sink: WfContext) -> WorkflowResult<()> {
    let t1 = command_sink.timer(Duration::from_secs(1));
    let t2 = command_sink.timer(Duration::from_secs(1));
    let _ = tokio::join!(t1, t2);
    Ok(().into())
}

#[tokio::test]
async fn parallel_timers() {
    let wf_name = "parallel_timers";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), parallel_timer_wf);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}
