use assert_matches::assert_matches;
use std::time::Duration;
use temporal_client::{WfClientExt, WorkflowExecutionResult, WorkflowOptions};
use temporal_sdk::{ActContext, ActivityOptions, WfContext, WorkflowResult};
use temporal_sdk_core_protos::coresdk::AsJsonPayloadExt;
use temporal_sdk_core_test_utils::CoreWfStarter;

const TEST_APPDATA_MESSAGE: &str = "custom app data, yay";

struct Data {
    message: String,
}

pub async fn appdata_activity_wf(ctx: WfContext) -> WorkflowResult<()> {
    ctx.activity(ActivityOptions {
        activity_type: "echo_activity".to_string(),
        start_to_close_timeout: Some(Duration::from_secs(5)),
        input: "hi!".as_json_payload().expect("serializes fine"),
        ..Default::default()
    })
    .await;
    Ok(().into())
}

#[tokio::test]
async fn appdata_access_in_activities_and_workflows() {
    let wf_name = "appdata_activity";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.inner_mut().insert_app_data(Data {
        message: TEST_APPDATA_MESSAGE.to_owned(),
    });

    let client = starter.get_client().await;
    worker.register_wf(wf_name.to_owned(), appdata_activity_wf);
    worker.register_activity(
        "echo_activity",
        |ctx: ActContext, echo_me: String| async move {
            let data = ctx.app_data::<Data>().expect("appdata exists. qed");
            assert_eq!(data.message, TEST_APPDATA_MESSAGE.to_owned());
            Ok(echo_me)
        },
    );

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
