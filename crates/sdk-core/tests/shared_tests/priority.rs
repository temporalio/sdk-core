use crate::common::CoreWfStarter;
use std::time::Duration;
use temporalio_client::{
    GetWorkflowResultOptions, Priority, WfClientExt, WorkflowClientTrait, WorkflowOptions,
};
use temporalio_common::protos::temporal::api::{common, history::v1::history_event::Attributes};
use temporalio_macros::{activities, workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ChildWorkflowOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};

pub(crate) async fn priority_values_sent_to_server() {
    let mut starter = if let Some(wfs) =
        CoreWfStarter::new_cloud_or_local("priority_values_sent_to_server", ">=1.29.0-139.2").await
    {
        wfs
    } else {
        return;
    };
    starter.workflow_options.priority = Some(Priority {
        priority_key: 1,
        fairness_key: "fair-wf".to_string(),
        fairness_weight: 4.2,
    });
    let mut worker = starter.worker().await;
    let child_type = "child-wf";

    struct PriorityActivities;
    #[activities]
    impl PriorityActivities {
        #[activity]
        async fn echo(ctx: ActivityContext, echo_me: String) -> Result<String, ActivityError> {
            assert_eq!(
                ctx.get_info().priority,
                Priority {
                    priority_key: 5,
                    fairness_key: "fair-act".to_string(),
                    fairness_weight: 1.1
                }
            );
            Ok(echo_me)
        }
    }

    #[workflow]
    #[derive(Default)]
    struct ParentWf {
        child_type: String,
    }

    #[workflow_methods]
    impl ParentWf {
        #[run]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            let child = ctx.child_workflow(ChildWorkflowOptions {
                workflow_id: format!("{}-child", ctx.task_queue()),
                workflow_type: self.child_type.clone(),
                options: WorkflowOptions {
                    priority: Some(Priority {
                        priority_key: 4,
                        fairness_key: "fair-child".to_string(),
                        fairness_weight: 1.23,
                    }),
                    ..Default::default()
                },
                ..Default::default()
            });

            let started = child
                .start(ctx)
                .await
                .into_started()
                .expect("Child should start OK");
            let activity = ctx
                .start_activity(
                    PriorityActivities::echo,
                    "hello".to_string(),
                    ActivityOptions {
                        start_to_close_timeout: Some(Duration::from_secs(5)),
                        priority: Some(Priority {
                            priority_key: 5,
                            fairness_key: "fair-act".to_string(),
                            fairness_weight: 1.1,
                        }),
                        do_not_eagerly_execute: true,
                        ..Default::default()
                    },
                )
                .unwrap();
            started.result().await;
            activity.await.unwrap_ok_payload();
            Ok(().into())
        }
    }

    #[workflow]
    #[derive(Default)]
    struct ChildWf;

    #[workflow_methods]
    impl ChildWf {
        #[run(name = "child-wf")]
        async fn run(&mut self, ctx: &mut WorkflowContext) -> WorkflowResult<()> {
            assert_eq!(
                ctx.workflow_initial_info().priority,
                Some(common::v1::Priority {
                    priority_key: 4,
                    fairness_key: "fair-child".to_string(),
                    fairness_weight: 1.23
                })
            );
            Ok(().into())
        }
    }

    worker.register_activities(PriorityActivities);
    worker.register_workflow_with_factory::<ParentWf, _>(move || ParentWf {
        child_type: child_type.to_owned(),
    });
    worker.register_workflow::<ChildWf>();

    worker
        .submit_workflow(
            ParentWf::run,
            starter.get_task_queue(),
            (),
            starter.workflow_options.clone(),
        )
        .await
        .unwrap();
    worker.run_until_done().await.unwrap();

    let client = starter.get_client().await;
    let handle = client.get_untyped_workflow_handle(starter.get_task_queue(), "");
    let res = handle
        .get_workflow_result(GetWorkflowResultOptions::default())
        .await
        .unwrap();
    // Expect workflow success
    res.unwrap_success();
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
    assert_eq!(
        workflow_init_event.priority.as_ref().unwrap().priority_key,
        1
    );
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
    assert_eq!(child_init_event.priority.as_ref().unwrap().priority_key, 4);
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
    assert_eq!(
        activity_sched_event.priority.as_ref().unwrap().priority_key,
        5
    );
}
