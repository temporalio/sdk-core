use std::time::Duration;
use temporal_client::{WorkflowClientTrait, WorkflowOptions};
use temporal_sdk::{WfContext, WfExitValue, WorkflowResult};
use temporal_sdk_core_protos::temporal::api::enums::v1::WorkflowExecutionStatus;
use temporal_sdk_core_test_utils::CoreWfStarter;

async fn cancelled_wf(mut ctx: WfContext) -> WorkflowResult<()> {
    let cancelled = tokio::select! {
        _ = ctx.timer(Duration::from_secs(500)) => false,
        _ = ctx.cancelled() => true
    };

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
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    worker.register_wf(wf_name.to_string(), cancelled_wf);

    worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();

    let canceller = async {
        tokio::time::sleep(Duration::from_millis(500)).await;
        // Cancel the workflow externally
        client
            .cancel_workflow_execution(wf_name.to_string(), None, "Dieee".to_string(), None)
            .await
            .unwrap();
    };

    let (_, res) = tokio::join!(canceller, worker.run_until_done());
    res.unwrap();
    let desc = client
        .describe_workflow_execution(wf_name.to_string(), None)
        .await
        .unwrap();

    assert_eq!(
        desc.workflow_execution_info.unwrap().status,
        WorkflowExecutionStatus::Canceled as i32
    );
}
