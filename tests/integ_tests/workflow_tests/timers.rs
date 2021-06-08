use assert_matches::assert_matches;
use std::{sync::Arc, time::Duration};
use temporal_sdk_core::protos::coresdk::{
    workflow_activation::{wf_activation_job, FireTimer, WfActivationJob},
    workflow_commands::{CancelTimer, CompleteWorkflowExecution, StartTimer},
    workflow_completion::WfActivationCompletion,
};
use temporal_sdk_core::test_workflow_driver::{CommandSender, TestRustWorker};
use temporal_sdk_core::tracing_init;
use test_utils::{init_core_and_create_wf, CoreWfStarter, NAMESPACE};

async fn timer_wf(mut command_sink: CommandSender) {
    let timer = StartTimer {
        timer_id: "super_timer_id".to_string(),
        start_to_fire_timeout: Some(Duration::from_secs(1).into()),
    };
    command_sink.timer(timer).await;
    command_sink.complete_workflow_execution();
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_workflow_not_sticky() {
    tracing_init();
    let wf_name = "timer_wf_not_sticky";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.max_cached_workflows(0);
    let tq = starter.get_task_queue().to_owned();
    let core = starter.get_core().await;

    let worker = TestRustWorker::new(core.clone(), NAMESPACE.to_owned(), tq.clone());
    worker
        .submit_wf(wf_name.to_owned(), Arc::new(timer_wf))
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    core.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_workflow_workflow_driver() {
    let wf_name = "timer_wf_new";
    let mut starter = CoreWfStarter::new(wf_name);
    let tq = starter.get_task_queue().to_owned();
    let core = starter.get_core().await;

    let worker = TestRustWorker::new(core.clone(), NAMESPACE.to_owned(), tq);
    worker
        .submit_wf(wf_name.to_owned(), Arc::new(timer_wf))
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn timer_workflow() {
    let (core, task_q) = init_core_and_create_wf("timer_workflow").await;
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![StartTimer {
            timer_id: "timer-1".to_string(),
            start_to_fire_timeout: Some(Duration::from_secs(1).into()),
        }
        .into()],
        task.run_id,
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.run_id,
    ))
    .await
    .unwrap();
    core.shutdown().await;
}

#[tokio::test]
async fn timer_cancel_workflow() {
    let (core, task_q) = init_core_and_create_wf("timer_cancel_workflow").await;
    let timer_id = "wait_timer";
    let cancel_timer_id = "cancel_timer";
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![
            StartTimer {
                timer_id: timer_id.to_string(),
                start_to_fire_timeout: Some(Duration::from_millis(50).into()),
            }
            .into(),
            StartTimer {
                timer_id: cancel_timer_id.to_string(),
                start_to_fire_timeout: Some(Duration::from_secs(10).into()),
            }
            .into(),
        ],
        task.run_id,
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![
            CancelTimer {
                timer_id: cancel_timer_id.to_string(),
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
        task.run_id,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn timer_immediate_cancel_workflow() {
    let (core, task_q) = init_core_and_create_wf("timer_immediate_cancel_workflow").await;
    let cancel_timer_id = "cancel_timer";
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![
            StartTimer {
                timer_id: cancel_timer_id.to_string(),
                ..Default::default()
            }
            .into(),
            CancelTimer {
                timer_id: cancel_timer_id.to_string(),
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
        task.run_id,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn parallel_timer_workflow() {
    let (core, task_q) = init_core_and_create_wf("parallel_timer_workflow").await;
    let timer_id = "timer 1".to_string();
    let timer_2_id = "timer 2".to_string();
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![
            StartTimer {
                timer_id: timer_id.clone(),
                start_to_fire_timeout: Some(Duration::from_millis(50).into()),
            }
            .into(),
            StartTimer {
                timer_id: timer_2_id.clone(),
                start_to_fire_timeout: Some(Duration::from_millis(100).into()),
            }
            .into(),
        ],
        task.run_id,
    ))
    .await
    .unwrap();
    // Wait long enough for both timers to complete. Server seems to be a bit weird about actually
    // sending both of these in one go, so we need to wait longer than you would expect.
    std::thread::sleep(Duration::from_millis(1500));
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(
                    FireTimer { timer_id: t1_id }
                )),
            },
            WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(
                    FireTimer { timer_id: t2_id }
                )),
            }
        ] => {
            assert_eq!(t1_id, &timer_id);
            assert_eq!(t2_id, &timer_2_id);
        }
    );
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![CompleteWorkflowExecution { result: None }.into()],
        task.run_id,
    ))
    .await
    .unwrap();
}
