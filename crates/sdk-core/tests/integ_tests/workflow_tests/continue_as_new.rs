use crate::common::{CoreWfStarter, build_fake_sdk};
use std::time::Duration;
use temporalio_client::WorkflowOptions;
use temporalio_common::protos::{
    DEFAULT_WORKFLOW_TYPE, canned_histories,
    coresdk::workflow_commands::ContinueAsNewWorkflowExecution,
    temporal::api::enums::v1::CommandType,
};
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_sdk::{WfContext, WfExitValue, WorkflowResult};
use temporalio_sdk_core::test_help::MockPollCfg;

async fn continue_as_new_wf(ctx: WfContext) -> WorkflowResult<()> {
    let run_ct = ctx.get_args()[0].data[0];
    ctx.timer(Duration::from_millis(500)).await;
    Ok(if run_ct < 5 {
        WfExitValue::continue_as_new(ContinueAsNewWorkflowExecution {
            arguments: vec![[run_ct + 1].into()],
            ..Default::default()
        })
    } else {
        ().into()
    })
}

#[tokio::test]
async fn continue_as_new_happy_path() {
    let wf_name = "continue_as_new_happy_path";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .worker_config
        .task_types(WorkerTaskTypes::workflow_only());
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_string(), continue_as_new_wf);

    worker
        .submit_wf(
            wf_name.to_string(),
            wf_name.to_string(),
            vec![[1].into()],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn continue_as_new_multiple_concurrent() {
    let wf_name = "continue_as_new_multiple_concurrent";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .worker_config
        .task_types(WorkerTaskTypes::workflow_only())
        .max_cached_workflows(5_usize)
        .max_outstanding_workflow_tasks(5_usize);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_string(), continue_as_new_wf);

    let wf_names = (1..=20).map(|i| format!("{wf_name}-{i}"));
    for name in wf_names.clone() {
        worker
            .submit_wf(
                name.to_string(),
                wf_name.to_string(),
                vec![[1].into()],
                WorkflowOptions::default(),
            )
            .await
            .unwrap();
    }
    worker.run_until_done().await.unwrap();
}

async fn wf_with_timer(ctx: WfContext) -> WorkflowResult<()> {
    ctx.timer(Duration::from_millis(500)).await;
    Ok(WfExitValue::continue_as_new(
        ContinueAsNewWorkflowExecution {
            arguments: vec![[1].into()],
            ..Default::default()
        },
    ))
}

#[tokio::test]
async fn wf_completing_with_continue_as_new() {
    let t = canned_histories::timer_then_continue_as_new("1");
    let mut mock_cfg = MockPollCfg::from_hist_builder(t);
    mock_cfg.completion_asserts_from_expectations(|mut asserts| {
        asserts
            .then(|wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_matches!(wft.commands[0].command_type(), CommandType::StartTimer);
            })
            .then(move |wft| {
                assert_eq!(wft.commands.len(), 1);
                assert_matches!(
                    wft.commands[0].command_type(),
                    CommandType::ContinueAsNewWorkflowExecution
                );
            });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, wf_with_timer);
    worker.run().await.unwrap();
}
