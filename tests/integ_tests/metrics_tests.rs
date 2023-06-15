use assert_matches::assert_matches;
use std::{sync::Arc, time::Duration};
use temporal_client::{WorkflowClientTrait, WorkflowOptions, WorkflowService};
use temporal_sdk_core::{init_worker, CoreRuntime};
use temporal_sdk_core_api::{telemetry::MetricsExporter, worker::WorkerConfigBuilder, Worker};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::ActivityExecutionResult,
        workflow_activation::{workflow_activation_job, WorkflowActivationJob},
        workflow_commands::{
            workflow_command, CancelWorkflowExecution, CompleteWorkflowExecution,
            ContinueAsNewWorkflowExecution, FailWorkflowExecution, QueryResult, QuerySuccess,
            ScheduleActivity, ScheduleLocalActivity,
        },
        workflow_completion::WorkflowActivationCompletion,
        ActivityTaskCompletion,
    },
    temporal::api::{
        enums::v1::WorkflowIdReusePolicy, failure::v1::Failure, query::v1::WorkflowQuery,
        workflowservice::v1::ListNamespacesRequest,
    },
};
use temporal_sdk_core_test_utils::{
    get_integ_server_options, get_integ_telem_options, CoreWfStarter, NAMESPACE,
};
use tokio::{join, sync::Barrier};

static ANY_PORT: &str = "127.0.0.1:0";

async fn get_text(endpoint: String) -> String {
    reqwest::get(endpoint).await.unwrap().text().await.unwrap()
}

#[tokio::test]
async fn prometheus_metrics_exported() {
    let mut telemopts = get_integ_telem_options();
    telemopts.metrics = Some(MetricsExporter::Prometheus(ANY_PORT.parse().unwrap()));
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();
    let addr = rt.telemetry().prom_port().unwrap();
    let opts = get_integ_server_options();
    let mut raw_client = opts
        .connect_no_namespace(rt.metric_meter().as_deref(), None)
        .await
        .unwrap();
    assert!(raw_client.get_client().capabilities().is_some());

    let _ = raw_client
        .list_namespaces(ListNamespacesRequest::default())
        .await
        .unwrap();

    let body = get_text(format!("http://{addr}/metrics")).await;
    assert!(body.contains(
        "temporal_request_latency_count{operation=\"ListNamespaces\",service_name=\"temporal-core-sdk\"} 1"
    ));
    assert!(body.contains(
        "temporal_request_latency_count{operation=\"GetSystemInfo\",service_name=\"temporal-core-sdk\"} 1"
    ));
}

#[tokio::test]
async fn one_slot_worker_reports_available_slot() {
    let mut telemopts = get_integ_telem_options();
    let tq = "one_slot_worker_tq";
    telemopts.metrics = Some(MetricsExporter::Prometheus(ANY_PORT.parse().unwrap()));
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();
    let addr = rt.telemetry().prom_port().unwrap();

    let worker_cfg = WorkerConfigBuilder::default()
        .namespace(NAMESPACE)
        .task_queue(tq)
        .worker_build_id("test_build_id")
        .max_cached_workflows(2_usize)
        .max_outstanding_activities(1_usize)
        .max_outstanding_local_activities(1_usize)
        // Need to use two for WFTs because there are a minimum of 2 pollers b/c of sticky polling
        .max_outstanding_workflow_tasks(2_usize)
        .max_concurrent_wft_polls(1_usize)
        .build()
        .unwrap();

    let client = Arc::new(
        get_integ_server_options()
            .connect(worker_cfg.namespace.clone(), None, None)
            .await
            .expect("Must connect"),
    );
    let worker = init_worker(&rt, worker_cfg, client.clone()).expect("Worker inits cleanly");
    let wf_task_barr = Barrier::new(2);
    let act_task_barr = Barrier::new(2);

    let wf_polling = async {
        let task = worker.poll_workflow_activation().await.unwrap();
        wf_task_barr.wait().await;
        wf_task_barr.wait().await;
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                task.run_id,
                ScheduleActivity {
                    seq: 1,
                    activity_id: "1".to_string(),
                    activity_type: "test_act".to_string(),
                    task_queue: tq.to_string(),
                    start_to_close_timeout: Some(prost_dur!(from_secs(30))),
                    ..Default::default()
                }
                .into(),
            ))
            .await
            .unwrap();
        wf_task_barr.wait().await;

        let task = worker.poll_workflow_activation().await.unwrap();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                task.run_id,
                ScheduleLocalActivity {
                    seq: 2,
                    activity_id: "2".to_string(),
                    activity_type: "test_act".to_string(),
                    start_to_close_timeout: Some(prost_dur!(from_secs(30))),
                    ..Default::default()
                }
                .into(),
            ))
            .await
            .unwrap();
    };

    let act_polling = async {
        let task = worker.poll_activity_task().await.unwrap();
        act_task_barr.wait().await;
        worker
            .complete_activity_task(ActivityTaskCompletion {
                task_token: task.task_token,
                result: Some(ActivityExecutionResult::ok(vec![1].into())),
            })
            .await
            .unwrap();
        act_task_barr.wait().await;

        let task = worker.poll_activity_task().await.unwrap();
        act_task_barr.wait().await;
        act_task_barr.wait().await;
        worker
            .complete_activity_task(ActivityTaskCompletion {
                task_token: task.task_token,
                result: Some(ActivityExecutionResult::ok(vec![1].into())),
            })
            .await
            .unwrap();
        act_task_barr.wait().await;
    };

    let testing = async {
        // Wait just a beat for the poller to initiate
        tokio::time::sleep(Duration::from_millis(50)).await;
        let body = get_text(format!("http://{addr}/metrics")).await;
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"WorkflowWorker\"}} 2"
        )));

        // Start a workflow so that a task will get delivered
        client
            .start_workflow(
                vec![],
                tq.to_owned(),
                "one_slot_metric_test".to_owned(),
                "whatever".to_string(),
                None,
                WorkflowOptions {
                    id_reuse_policy: WorkflowIdReusePolicy::TerminateIfRunning,
                    execution_timeout: Some(Duration::from_secs(5)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        wf_task_barr.wait().await;

        // At this point the workflow task is outstanding, so there should be 0 slots, and
        // the activities haven't started, so there should be 1 each.
        let body = get_text(format!("http://{addr}/metrics")).await;
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"WorkflowWorker\"}} 1"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"ActivityWorker\"}} 1"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"LocalActivityWorker\"}} 1"
        )));

        // Now we allow the complete to proceed. Once it goes through, there should be 1 WFT slot
        // open but 0 activity slots
        wf_task_barr.wait().await;
        wf_task_barr.wait().await;
        // Sometimes the recording takes an extra bit. 🤷
        tokio::time::sleep(Duration::from_millis(100)).await;
        let body = get_text(format!("http://{addr}/metrics")).await;
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"WorkflowWorker\"}} 2"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"ActivityWorker\"}} 0"
        )));

        // Now complete the activity and watch it go up
        act_task_barr.wait().await;
        // Wait for completion to finish
        act_task_barr.wait().await;
        let body = get_text(format!("http://{addr}/metrics")).await;
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"ActivityWorker\"}} 1"
        )));

        // Proceed to local activity command
        act_task_barr.wait().await;
        // Ensure that, once we have the LA task, slots are 0
        let body = get_text(format!("http://{addr}/metrics")).await;
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"LocalActivityWorker\"}} 0"
        )));
        // When completion is done, we have 1 again
        act_task_barr.wait().await;
        act_task_barr.wait().await;
        let body = get_text(format!("http://{addr}/metrics")).await;
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_available{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"LocalActivityWorker\"}} 1"
        )));
    };
    join!(wf_polling, act_polling, testing);
}

#[rstest::rstest]
#[tokio::test]
async fn query_of_closed_workflow_doesnt_tick_terminal_metric(
    #[values(
        CompleteWorkflowExecution { result: None }.into(),
        FailWorkflowExecution {
            failure: Some(Failure::application_failure("I'm ded".to_string(), false)),
        }.into(),
        ContinueAsNewWorkflowExecution::default().into(),
        CancelWorkflowExecution { }.into()
    )]
    completion: workflow_command::Variant,
) {
    let mut telemopts = get_integ_telem_options();
    telemopts.metrics = Some(MetricsExporter::Prometheus(ANY_PORT.parse().unwrap()));
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();
    let addr = rt.telemetry().prom_port().unwrap();
    let mut starter =
        CoreWfStarter::new_with_runtime("query_of_closed_workflow_doesnt_tick_terminal_metric", rt);
    // Disable cache to ensure replay happens completely
    starter.max_cached_workflows(0);
    let worker = starter.get_worker().await;
    let run_id = starter.start_wf().await;
    let task = worker.poll_workflow_activation().await.unwrap();
    // Immediately complete the workflow
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            completion.clone(),
        ))
        .await
        .unwrap();

    let metric_name = match &completion {
        workflow_command::Variant::CompleteWorkflowExecution(_) => "temporal_workflow_completed",
        workflow_command::Variant::FailWorkflowExecution(_) => "temporal_workflow_failed",
        workflow_command::Variant::ContinueAsNewWorkflowExecution(_) => {
            "temporal_workflow_continue_as_new"
        }
        workflow_command::Variant::CancelWorkflowExecution(_) => "temporal_workflow_canceled",
        _ => unreachable!(),
    };

    // Verify there is one tick for the completion metric
    let body = get_text(format!("http://{addr}/metrics")).await;
    let matching_line = body
        .lines()
        .find(|l| l.starts_with(metric_name))
        .expect("Must find matching metric");
    assert!(matching_line.ends_with('1'));

    // Handle cache eviction
    let task = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    // Query the now-closed workflow
    let client = starter.get_client().await;
    let queryer = async {
        client
            .query_workflow_execution(
                starter.get_wf_id().to_string(),
                run_id,
                WorkflowQuery {
                    query_type: "fake_query".to_string(),
                    query_args: None,
                    header: None,
                },
            )
            .await
            .unwrap();
    };
    let query_reply = async {
        // Need to re-complete b/c replay
        let task = worker.poll_workflow_activation().await.unwrap();
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                task.run_id,
                completion,
            ))
            .await
            .unwrap();

        let task = worker.poll_workflow_activation().await.unwrap();
        let query = assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
            }] => q
        );
        worker
            .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
                task.run_id,
                QueryResult {
                    query_id: query.query_id.clone(),
                    variant: Some(
                        QuerySuccess {
                            response: Some("hi".into()),
                        }
                        .into(),
                    ),
                }
                .into(),
            ))
            .await
            .unwrap()
    };
    join!(query_reply, queryer);

    // Verify there is still only one tick
    let body = get_text(format!("http://{addr}/metrics")).await;
    let matching_line = body
        .lines()
        .find(|l| l.starts_with(metric_name))
        .expect("Must find matching metric");
    assert!(matching_line.ends_with('1'));
}
