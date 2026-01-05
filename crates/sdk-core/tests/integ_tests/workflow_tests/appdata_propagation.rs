use crate::common::CoreWfStarter;
use assert_matches::assert_matches;
use std::time::Duration;
use temporalio_client::{WfClientExt, WorkflowExecutionResult, WorkflowOptions};
use temporalio_macros::activities;
use temporalio_sdk::{
    ActivityOptions, WfContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};

const TEST_APPDATA_MESSAGE: &str = "custom app data, yay";

struct Data {
    message: String,
}

pub(crate) async fn appdata_activity_wf(ctx: WfContext) -> WorkflowResult<()> {
    ctx.activity(
        AppdataActivities::echo,
        "hi!".to_string(),
        ActivityOptions {
            start_to_close_timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        },
    )
    .unwrap()
    .await;
    Ok(().into())
}

struct AppdataActivities {}
#[activities]
impl AppdataActivities {
    #[activity]
    async fn echo(ctx: ActivityContext, echo_me: String) -> Result<String, ActivityError> {
        let data = ctx.app_data::<Data>().expect("appdata exists. qed");
        assert_eq!(data.message, TEST_APPDATA_MESSAGE.to_owned());
        Ok(echo_me)
    }
}

#[tokio::test]
async fn appdata_access_in_activities_and_workflows() {
    let wf_name = "appdata_activity";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .sdk_config
        .register_activities_static::<AppdataActivities>();
    let mut worker = starter.worker().await;
    worker.inner_mut().insert_app_data(Data {
        message: TEST_APPDATA_MESSAGE.to_owned(),
    });

    let client = starter.get_client().await;
    worker.register_wf(wf_name.to_owned(), appdata_activity_wf);

    let run_id = worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    let handle = client.get_untyped_workflow_handle(wf_name, run_id);
    let res = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    assert_matches!(res, WorkflowExecutionResult::Succeeded(_));
}
