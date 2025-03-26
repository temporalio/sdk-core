use std::time::Duration;
use temporal_client::{Priority, WorkflowClientTrait, WorkflowOptions};
use temporal_sdk::{ActContext, ActivityOptions, ChildWorkflowOptions, WfContext};
use temporal_sdk_core_protos::{
    coresdk::AsJsonPayloadExt, temporal::api::history::v1::history_event::Attributes,
};
use temporal_sdk_core_test_utils::CoreWfStarter;

#[tokio::test]
async fn priority_values_sent_to_server() {
    let mut starter = CoreWfStarter::new("priority-values-sent-to-server");
    starter.workflow_options.priority = Some(Priority { priority_key: 1 });
    let mut worker = starter.worker().await;
    let child_type = "child-wf";

    worker.register_wf(starter.get_task_queue(), move |ctx: WfContext| async move {
        let child = ctx.child_workflow(ChildWorkflowOptions {
            workflow_id: format!("{}-child", ctx.task_queue()),
            workflow_type: child_type.to_owned(),
            options: WorkflowOptions {
                priority: Some(Priority { priority_key: 4 }),
                ..Default::default()
            },
            ..Default::default()
        });

        let started = child
            .start(&ctx)
            .await
            .into_started()
            .expect("Child should start OK");
        let activity = ctx.activity(ActivityOptions {
            activity_type: "echo".to_owned(),
            input: "hello".as_json_payload().unwrap(),
            start_to_close_timeout: Some(Duration::from_secs(5)),
            priority: Some(Priority { priority_key: 5 }),
            ..Default::default()
        });
        started.result().await;
        activity.await;
        Ok(().into())
    });
    worker.register_wf(child_type.to_owned(), |_ctx: WfContext| async move {
        Ok(().into())
    });
    worker.register_activity("echo", |_ctx: ActContext, echo_me: String| async move {
        Ok(echo_me)
    });

    starter
        .start_with_worker(starter.get_task_queue(), &mut worker)
        .await;
    worker.run_until_done().await.unwrap();

    let client = starter.get_client().await;
    let history = client
        .get_workflow_execution_history(starter.get_task_queue().to_owned(), None, vec![])
        .await
        .unwrap()
        .history
        .unwrap();
    let workflow_init_event = history
        .events
        .iter()
        .find_map(|e| {
            if let Attributes::WorkflowExecutionStartedEventAttributes(e) =
                e.attributes.as_ref().unwrap()
            {
                Some(e)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(workflow_init_event.priority.unwrap().priority_key, 1);
    let child_init_event = history
        .events
        .iter()
        .find_map(|e| {
            if let Attributes::StartChildWorkflowExecutionInitiatedEventAttributes(e) =
                e.attributes.as_ref().unwrap()
            {
                Some(e)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(child_init_event.priority.unwrap().priority_key, 4);
    let activity_sched_event = history
        .events
        .iter()
        .find_map(|e| {
            if let Attributes::ActivityTaskScheduledEventAttributes(e) =
                e.attributes.as_ref().unwrap()
            {
                Some(e)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(activity_sched_event.priority.unwrap().priority_key, 5);
}
