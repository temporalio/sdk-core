use crate::common::{CoreWfStarter, build_fake_sdk, init_core_and_create_wf};
use std::time::Duration;
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_common::{
    prost_dur,
    protos::{
        DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder, canned_histories,
        coresdk::{
            workflow_commands::{CancelTimer, CompleteWorkflowExecution, StartTimer},
            workflow_completion::WorkflowActivationCompletion,
        },
        temporal::api::{
            enums::v1::{CommandType, EventType, WorkflowTaskFailedCause},
            failure::v1::Failure,
        },
        test_utils::start_timer_cmd,
    },
};
use temporalio_sdk::{CancellableFuture, WfContext, WorkflowResult};
use temporalio_sdk_core::test_help::{MockPollCfg, WorkerTestHelpers, drain_pollers_and_shutdown};

pub(crate) async fn timer_wf(command_sink: WfContext) -> WorkflowResult<()> {
    command_sink.timer(Duration::from_secs(1)).await;
    Ok(().into())
}

#[tokio::test]
async fn timer_workflow_workflow_driver() {
    let wf_name = "timer_wf_new";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .worker_config
        .task_types(WorkerTaskTypes::workflow_only());
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), timer_wf);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn timer_workflow_manual() {
    let mut starter = init_core_and_create_wf("timer_workflow").await;
    let core = starter.get_worker().await;
    starter
        .worker_config
        .task_types(WorkerTaskTypes::workflow_only());
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
    starter
        .worker_config
        .task_types(WorkerTaskTypes::workflow_only());
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
    starter
        .worker_config
        .task_types(WorkerTaskTypes::workflow_only());
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), parallel_timer_wf);

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

async fn happy_timer(ctx: WfContext) -> WorkflowResult<()> {
    ctx.timer(Duration::from_secs(5)).await;
    Ok(().into())
}

#[tokio::test]
async fn test_fire_happy_path_inc() {
    let t = canned_histories::single_timer("1");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(wft.commands[0].command_type(), CommandType::StartTimer);
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_eq!(
                    wft.commands[0].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, happy_timer);
    worker.run().await.unwrap();
}

#[tokio::test]
async fn mismatched_timer_ids_errors() {
    let t = canned_histories::single_timer("badid");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.num_expected_fails = 1;
    mock_cfg.expect_fail_wft_matcher = Box::new(move |_, cause, f| {
        matches!(cause, WorkflowTaskFailedCause::NonDeterministicError)
            && matches!(f, Some(Failure {message, .. })
        if message.contains("Timer fired event did not have expected timer id 1"))
    });
    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, |ctx: WfContext| async move {
        ctx.timer(Duration::from_secs(5)).await;
        Ok(().into())
    });
    worker.run().await.unwrap();
}

async fn cancel_timer(ctx: WfContext) -> WorkflowResult<()> {
    let cancel_timer_fut = ctx.timer(Duration::from_secs(500));
    ctx.timer(Duration::from_secs(5)).await;
    // Cancel the first timer after having waited on the second
    cancel_timer_fut.cancel(&ctx);
    cancel_timer_fut.await;
    Ok(().into())
}

#[tokio::test]
async fn incremental_cancellation() {
    let t = canned_histories::cancel_timer("2", "1");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 2);
                assert_eq!(wft.commands[0].command_type(), CommandType::StartTimer);
                assert_eq!(wft.commands[1].command_type(), CommandType::StartTimer);
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 2);
                assert_eq!(wft.commands[0].command_type(), CommandType::CancelTimer);
                assert_eq!(
                    wft.commands[1].command_type(),
                    CommandType::CompleteWorkflowExecution
                );
            });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, cancel_timer);
    worker.run().await.unwrap();
}

#[tokio::test]
async fn cancel_before_sent_to_server() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_workflow_execution_completed();
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts.then(move |wft| {
            assert_eq!(wft.commands.len(), 1);
            assert_matches!(
                wft.commands[0].command_type(),
                CommandType::CompleteWorkflowExecution
            );
        });
    });
    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, |ctx: WfContext| async move {
        let cancel_timer_fut = ctx.timer(Duration::from_secs(500));
        // Immediately cancel the timer
        cancel_timer_fut.cancel(&ctx);
        cancel_timer_fut.await;
        Ok(().into())
    });
    worker.run().await.unwrap();
}
