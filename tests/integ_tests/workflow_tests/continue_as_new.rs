use std::time::Duration;
use temporal_client::{SignalWithStartOptions, WorkflowOptions};
use temporal_sdk::{WfContext, WfExitValue, WorkflowResult};
use temporal_sdk_core::WorkflowClientTrait;
use temporal_sdk_core_protos::coresdk::IntoPayloadsExt;
use temporal_sdk_core_protos::coresdk::workflow_commands::ContinueAsNewWorkflowExecution;
use temporal_sdk_core_test_utils::CoreWfStarter;

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
    starter.worker_config.no_remote_activities(true);
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
        .no_remote_activities(true)
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

const SIGNAME: &str = "signame";

async fn continue_as_new_wf_with_sigchan(ctx: WfContext) -> WorkflowResult<()> {
    let continued_from_execution_run_id =
        &ctx.workflow_initial_info().continued_from_execution_run_id;

    if continued_from_execution_run_id != "" {
        Ok(WfExitValue::Normal(()))
    } else {
        let _sigchan = ctx.make_signal_channel(SIGNAME);
        // Even if we drain the channel, the above line makes the workflow stuck.
        // sigchan.drain_all();

        return Ok(WfExitValue::continue_as_new(
            ContinueAsNewWorkflowExecution {
                arguments: vec![[2].into()].into(),
                ..Default::default()
            },
        ));
    }
}

#[tokio::test]
async fn continue_as_new_with_sigchan() {
    let wf_name = "continue_as_new_with_sigchan";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_string(), continue_as_new_wf_with_sigchan);

    let client = starter.get_client().await;
    let options = SignalWithStartOptions::builder()
        .task_queue(worker.inner_mut().task_queue())
        .workflow_id(wf_name)
        .workflow_type(wf_name)
        .input(vec![[1].into()].into_payloads().unwrap())
        .signal_name(SIGNAME)
        .signal_input(vec![b"tada".into()].into_payloads())
        .build()
        .unwrap();
    let res = client
        .signal_with_start_workflow_execution(options, WorkflowOptions::default())
        .await
        .expect("request succeeds.qed");

    worker.expect_workflow_completion(wf_name, Some(res.run_id));
    worker.run_until_done().await.unwrap();
}
