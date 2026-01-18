use crate::common::{CoreWfStarter, build_fake_sdk};
use std::{sync::Arc, time::Duration};
use temporalio_client::WorkflowOptions;
use temporalio_common::{
    protos::{
        DEFAULT_WORKFLOW_TYPE, canned_histories,
        coresdk::{AsJsonPayloadExt, workflow_commands::ContinueAsNewWorkflowExecution},
        temporal::api::enums::v1::CommandType,
    },
    worker::WorkerTaskTypes,
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{WfExitValue, WorkflowContext, WorkflowResult};
use temporalio_sdk_core::{TunerHolder, test_help::MockPollCfg};

#[workflow]
#[derive(Default)]
struct ContinueAsNewWf;

#[workflow_methods]
impl ContinueAsNewWf {
    #[run]
    async fn run(ctx: &mut WorkflowContext<Self>, run_ct: u8) -> WorkflowResult<()> {
        ctx.timer(Duration::from_millis(500)).await;
        Ok(if run_ct < 5 {
            WfExitValue::continue_as_new(ContinueAsNewWorkflowExecution {
                arguments: vec![(run_ct + 1).as_json_payload().unwrap()],
                ..Default::default()
            })
        } else {
            ().into()
        })
    }
}

#[tokio::test]
async fn continue_as_new_happy_path() {
    let wf_name = "continue_as_new_happy_path";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    worker.register_workflow::<ContinueAsNewWf>();

    worker
        .submit_workflow(
            ContinueAsNewWf::run,
            wf_name.to_string(),
            1u8,
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
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    starter.sdk_config.max_cached_workflows = 5_usize;
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(5, 1, 1, 1));
    let mut worker = starter.worker().await;
    worker.register_workflow::<ContinueAsNewWf>();

    let wf_names = (1..=20).map(|i| format!("{wf_name}-{i}"));
    for name in wf_names.clone() {
        worker
            .submit_workflow(ContinueAsNewWf::run, name, 1u8, WorkflowOptions::default())
            .await
            .unwrap();
    }
    worker.run_until_done().await.unwrap();
}

#[workflow]
#[derive(Default)]
struct WfWithTimer;

#[workflow_methods]
impl WfWithTimer {
    #[run(name = DEFAULT_WORKFLOW_TYPE)]
    async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        ctx.timer(Duration::from_millis(500)).await;
        Ok(WfExitValue::continue_as_new(
            ContinueAsNewWorkflowExecution {
                arguments: vec![[1].into()],
                ..Default::default()
            },
        ))
    }
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
    worker.register_workflow::<WfWithTimer>();
    worker.run().await.unwrap();
}
