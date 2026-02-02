use crate::common::{CoreWfStarter, build_fake_sdk, init_core_and_create_wf};
use std::time::Duration;
use temporalio_client::WorkflowOptions;
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
    worker::WorkerTaskTypes,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{CancellableFuture, WorkflowContext, WorkflowResult};
use temporalio_sdk_core::test_help::{MockPollCfg, WorkerTestHelpers, drain_pollers_and_shutdown};

#[workflow]
#[derive(Default)]
pub(crate) struct TimerWf;

#[workflow_methods]
impl TimerWf {
    #[run(name = "timer_wf_new")]
    pub(crate) async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.timer(Duration::from_secs(1)).await;
        Ok(())
    }
}

#[tokio::test]
async fn timer_workflow_workflow_driver() {
    let wf_name = "timer_wf_new";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<TimerWf>();

    let task_queue = starter.get_task_queue().to_owned();
    let workflow_id = starter.get_task_queue().to_owned();
    worker
        .submit_workflow(
            TimerWf::run,
            (),
            WorkflowOptions::new(task_queue, workflow_id).build(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn timer_workflow_manual() {
    let mut starter = init_core_and_create_wf("timer_workflow").await;
    let core = starter.get_worker().await;
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
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
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
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

#[workflow]
#[derive(Default)]
struct ParallelTimerWf;

#[workflow_methods]
impl ParallelTimerWf {
    #[run(name = "parallel_timers")]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let t1 = ctx.timer(Duration::from_secs(1));
        let t2 = ctx.timer(Duration::from_secs(1));
        let _ = tokio::join!(t1, t2);
        Ok(())
    }
}

#[tokio::test]
async fn parallel_timers() {
    let wf_name = "parallel_timers";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<ParallelTimerWf>();

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct HappyTimerWf;

#[workflow_methods]
impl HappyTimerWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.timer(Duration::from_secs(5)).await;
        Ok(())
    }
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
    worker.register_workflow::<HappyTimerWf>();
    worker.run().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct MismatchedTimerWf;

#[workflow_methods]
impl MismatchedTimerWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.timer(Duration::from_secs(5)).await;
        Ok(())
    }
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
    worker.register_workflow::<MismatchedTimerWf>();
    worker.run().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct CancelTimerWf;

#[workflow_methods]
impl CancelTimerWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let cancel_timer_fut = ctx.timer(Duration::from_secs(500));
        ctx.timer(Duration::from_secs(5)).await;
        cancel_timer_fut.cancel();
        cancel_timer_fut.await;
        Ok(())
    }
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
    worker.register_workflow::<CancelTimerWf>();
    worker.run().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct CancelBeforeSentWf;

#[workflow_methods]
impl CancelBeforeSentWf {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        let cancel_timer_fut = ctx.timer(Duration::from_secs(500));
        cancel_timer_fut.cancel();
        cancel_timer_fut.await;
        Ok(())
    }
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
    worker.register_workflow::<CancelBeforeSentWf>();
    worker.run().await.unwrap();
}
