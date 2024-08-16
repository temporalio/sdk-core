use assert_matches::assert_matches;
use std::{sync::Arc, time::Duration};
use temporal_client::{
    ListClosedFilters, ListOpenFilters, Namespace, RegisterNamespaceOptions, StartTimeFilter,
    WorkflowClientTrait, WorkflowExecutionFilter,
};
use temporal_sdk_core_protos::coresdk::workflow_activation::{
    workflow_activation_job, WorkflowActivationJob,
};
use temporal_sdk_core_test_utils::{
    drain_pollers_and_shutdown, get_integ_server_options, CoreWfStarter, WorkerTestHelpers,
    NAMESPACE,
};
use tokio::time::sleep;

#[tokio::test]
async fn client_list_open_closed_workflow_executions() {
    let wf_name = "client_list_open_closed_workflow_executions".to_owned();
    let mut starter = CoreWfStarter::new(&wf_name);
    let core = starter.get_worker().await;
    let client = starter.get_client().await;

    let earliest = std::time::SystemTime::now();
    let latest = earliest + Duration::from_secs(60);

    // start workflow
    let run_id = starter.start_wf_with_id(wf_name.to_owned()).await;
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
        }]
    );

    // List above OPEN workflow
    let start_time_filter = StartTimeFilter {
        earliest_time: Some(earliest).map(|t| t.into()),
        latest_time: Some(latest).map(|t| t.into()),
    };
    let filter = ListOpenFilters::ExecutionFilter(WorkflowExecutionFilter {
        workflow_id: wf_name.clone(),
        run_id: "".to_owned(),
    });
    let open_workflows = client
        .list_open_workflow_executions(1, Default::default(), Some(start_time_filter), Some(filter))
        .await
        .unwrap();
    assert_eq!(open_workflows.executions.len(), 1);
    let workflow = open_workflows.executions[0].clone();
    assert_eq!(workflow.execution.as_ref().unwrap().workflow_id, wf_name);

    // Complete workflow
    core.complete_execution(&task.run_id).await;
    drain_pollers_and_shutdown(&core).await;

    // List above CLOSED workflow. Visibility doesn't always update immediately so we give this a
    // few tries.
    let mut passed = false;
    for _ in 1..=5 {
        let closed_workflows = client
            .list_closed_workflow_executions(
                1,
                Default::default(),
                Some(StartTimeFilter {
                    earliest_time: Some(earliest).map(|t| t.into()),
                    latest_time: Some(latest).map(|t| t.into()),
                }),
                Some(ListClosedFilters::ExecutionFilter(
                    WorkflowExecutionFilter {
                        workflow_id: wf_name.clone(),
                        run_id: run_id.clone(),
                    },
                )),
            )
            .await
            .unwrap();
        if closed_workflows.executions.len() == 1 {
            let workflow = &closed_workflows.executions[0];
            if workflow.execution.as_ref().unwrap().workflow_id == wf_name {
                passed = true;
                break;
            }
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(passed);
}

#[tokio::test]
async fn client_create_namespace() {
    let client = Arc::new(
        get_integ_server_options()
            .connect(NAMESPACE.to_owned(), None)
            .await
            .expect("Must connect"),
    );

    let register_options = RegisterNamespaceOptions::builder()
        .namespace(uuid::Uuid::new_v4().to_string())
        .description("it's alive")
        .build()
        .unwrap();

    client
        .register_namespace(register_options.clone())
        .await
        .unwrap();

    //#Hack, not sure how else to wait for a proper response.  RegisterNamespace isn't safe to read
    //after write
    let mut attempts = 0;
    let wait_time = Duration::from_secs(1);
    loop {
        attempts += 1;
        let resp = client
            .describe_namespace(Namespace::Name(register_options.namespace.clone()))
            .await;

        match resp {
            Ok(n) => {
                let namespace_info = n.namespace_info.unwrap();
                assert_eq!(namespace_info.name, register_options.namespace);
                assert_eq!(namespace_info.description, register_options.description);
                return;
            }
            _ => {
                if attempts == 12 {
                    panic!("failed to query registered namespace");
                }
                sleep(wait_time).await
            }
        }
    }
}

#[tokio::test]
async fn client_describe_namespace() {
    let client = Arc::new(
        get_integ_server_options()
            .connect(NAMESPACE.to_owned(), None)
            .await
            .expect("Must connect"),
    );

    let namespace_result = client
        .describe_namespace(Namespace::Name(NAMESPACE.to_owned()))
        .await
        .unwrap();
    assert_eq!(namespace_result.namespace_info.unwrap().name, NAMESPACE);
}
