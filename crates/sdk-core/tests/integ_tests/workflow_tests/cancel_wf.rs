use crate::common::{ActivationAssertionsInterceptor, CoreWfStarter, build_fake_sdk};
use std::time::Duration;
use temporalio_client::WorkflowClientTrait;
use temporalio_common::{
    protos::{
        DEFAULT_WORKFLOW_TYPE, canned_histories,
        coresdk::workflow_activation::{WorkflowActivationJob, workflow_activation_job},
        temporal::api::enums::v1::{CommandType, WorkflowExecutionStatus},
    },
    worker::WorkerTaskTypes,
};
use temporalio_sdk::{WfContext, WfExitValue, WorkflowResult};
use temporalio_sdk_core::test_help::MockPollCfg;

async fn cancelled_wf(ctx: WfContext) -> WorkflowResult<()> {
    let mut reason = "".to_string();
    let cancelled = tokio::select! {
        _ = ctx.timer(Duration::from_secs(500)) => false,
        r = ctx.cancelled() => {
            reason = r;
            true
        }
    };

    assert_eq!(reason, "Dieee");

    if cancelled {
        Ok(WfExitValue::Cancelled)
    } else {
        panic!("Should have been cancelled")
    }
}

#[tokio::test]
async fn cancel_during_timer() {
    let wf_name = "cancel_during_timer";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.task_types = WorkerTaskTypes::workflow_only();
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    worker.register_wf(wf_name.to_string(), cancelled_wf);
    starter.start_with_worker(wf_name, &mut worker).await;
    let wf_id = starter.get_task_queue().to_string();

    let canceller = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        // Cancel the workflow externally
        client
            .cancel_workflow_execution(wf_id.clone(), None, "Dieee".to_string(), None)
            .await
            .unwrap();
    };

    let (_, res) = tokio::join!(canceller, worker.run_until_done());
    res.unwrap();
    let desc = client
        .describe_workflow_execution(wf_id, None)
        .await
        .unwrap();

    assert_eq!(
        desc.workflow_execution_info.unwrap().status,
        WorkflowExecutionStatus::Canceled as i32
    );
}

async fn wf_with_timer(ctx: WfContext) -> WorkflowResult<()> {
    ctx.timer(Duration::from_millis(500)).await;
    Ok(WfExitValue::Cancelled)
}

#[tokio::test]
async fn wf_completing_with_cancelled() {
    let t = canned_histories::timer_wf_cancel_req_cancelled("1");

    let mut aai = ActivationAssertionsInterceptor::default();
    aai.then(|a| {
        assert_matches!(
            a.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
            }]
        )
    });
    aai.then(|a| {
        assert_matches!(
            a.jobs.as_slice(),
            [
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::FireTimer(_)),
                },
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::CancelWorkflow(_)),
                }
            ]
        );
    });

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
                    CommandType::CancelWorkflowExecution
                );
            });
    });

    let mut worker = build_fake_sdk(mock_cfg);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, wf_with_timer);
    worker.set_worker_interceptor(aai);
    worker.run().await.unwrap();
}
