use assert_matches::assert_matches;
use std::{sync::Arc, time::Duration};
use temporal_client::{
    ListClosedFilters, ListOpenFilters, Namespace, StartTimeFilter, WorkflowClientTrait,
    WorkflowExecutionFilter, WorkflowOptions,
};
use temporal_sdk_core_protos::coresdk::workflow_activation::{
    workflow_activation_job, WorkflowActivationJob,
};
use temporal_sdk_core_test_utils::{
    get_integ_server_options, CoreWfStarter, WorkerTestHelpers, NAMESPACE,
};

#[tokio::test]
async fn client_list_open_closed_workflow_executions() {
    let wf_name = "client_list_open_closed_workflow_executions".to_owned();
    let mut starter = CoreWfStarter::new(&wf_name);
    let core = starter.get_worker().await;
    let client = starter.get_client().await;

    let earliest = std::time::SystemTime::now();
    let latest = earliest + Duration::from_secs(60);

    // start workflow
    let run_id = starter
        .start_wf_with_id(wf_name.to_owned(), WorkflowOptions::default())
        .await;
    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::StartWorkflow(_)),
        }]
    );

    // List above OPEN workflow
    let start_time_filter = StartTimeFilter {
        earliest_time: Some(earliest).and_then(|t| t.try_into().ok()),
        latest_time: Some(latest).and_then(|t| t.try_into().ok()),
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

    // List above CLOSED workflow
    let start_time_filter = StartTimeFilter {
        earliest_time: Some(earliest).and_then(|t| t.try_into().ok()),
        latest_time: Some(latest).and_then(|t| t.try_into().ok()),
    };
    let filter = ListClosedFilters::ExecutionFilter(WorkflowExecutionFilter {
        workflow_id: wf_name.clone(),
        run_id,
    });
    let closed_workflows = client
        .list_closed_workflow_executions(
            1,
            Default::default(),
            Some(start_time_filter),
            Some(filter),
        )
        .await
        .unwrap();
    assert_eq!(closed_workflows.executions.len(), 1);
    let workflow = closed_workflows.executions[0].clone();
    assert_eq!(workflow.execution.as_ref().unwrap().workflow_id, wf_name);
}

#[tokio::test]
async fn client_describe_namespace() {
    let client = Arc::new(
        get_integ_server_options()
            .connect(NAMESPACE.to_owned(), None, None)
            .await
            .expect("Must connect"),
    );

    let namespace_result = client
        .describe_namespace(Namespace::Name(NAMESPACE.to_owned()))
        .await
        .unwrap();
    assert_eq!(namespace_result.namespace_info.unwrap().name, NAMESPACE);
}
