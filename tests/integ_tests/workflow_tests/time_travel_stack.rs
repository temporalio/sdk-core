use std::time::Duration;
use temporal_client::WorkflowOptions;
use temporal_sdk::{ActContext, ActivityOptions, WfContext};
use temporal_sdk_core_protos::coresdk::AsJsonPayloadExt;
use temporal_sdk_core_test_utils::CoreWfStarter;

#[tokio::test]
async fn time_travel_stacks_example() {
    let wf_name = "time_travel_stacks";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        for _ in 1..=5 {
            ctx.timer(Duration::from_millis(1)).await;
            ctx.activity(ActivityOptions {
                activity_type: "echo".to_string(),
                input: "hi".as_json_payload().unwrap(),
                start_to_close_timeout: Some(Duration::from_secs(1)),
                ..Default::default()
            })
            .await;
        }
        Ok(().into())
    });
    worker.register_activity("echo", |_: ActContext, str: String| async { Ok(str) });

    worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();
    // starter
    //     .fetch_history_and_replay(wf_name, run_id, worker.inner_mut())
    //     .await
    //     .unwrap();
}
