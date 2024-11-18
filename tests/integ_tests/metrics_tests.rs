use anyhow::anyhow;
use assert_matches::assert_matches;
use std::{env, net::SocketAddr, sync::Arc, time::Duration};
use temporal_client::{WorkflowClientTrait, WorkflowOptions, WorkflowService};
use temporal_sdk::{
    ActContext, ActivityError, ActivityOptions, CancellableFuture, LocalActivityOptions, WfContext,
};
use temporal_sdk_core::{
    init_worker,
    telemetry::{build_otlp_metric_exporter, start_prometheus_metric_exporter},
    CoreRuntime, TokioRuntimeBuilder,
};
use temporal_sdk_core_api::{
    telemetry::{
        metrics::{CoreMeter, MetricAttributes, MetricParameters},
        OtelCollectorOptionsBuilder, PrometheusExporterOptionsBuilder, TelemetryOptions,
        TelemetryOptionsBuilder,
    },
    worker::WorkerConfigBuilder,
    Worker,
};
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
        ActivityTaskCompletion, AsJsonPayloadExt,
    },
    temporal::api::{
        common::v1::RetryPolicy,
        enums::v1::WorkflowIdReusePolicy,
        failure::v1::Failure,
        query::v1::WorkflowQuery,
        workflowservice::v1::{DescribeNamespaceRequest, ListNamespacesRequest},
    },
};
use temporal_sdk_core_test_utils::{
    get_integ_server_options, get_integ_telem_options, CoreWfStarter, NAMESPACE, OTEL_URL_ENV_VAR,
};
use tokio::{join, sync::Barrier, task::AbortHandle};
use url::Url;

static ANY_PORT: &str = "127.0.0.1:0";

pub(crate) async fn get_text(endpoint: String) -> String {
    reqwest::get(endpoint).await.unwrap().text().await.unwrap()
}

pub(crate) struct AbortOnDrop {
    ah: AbortHandle,
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.ah.abort();
    }
}

pub(crate) fn prom_metrics(
    use_seconds: bool,
    show_units: bool,
) -> (TelemetryOptions, SocketAddr, AbortOnDrop) {
    let mut telemopts = get_integ_telem_options();
    let prom_info = start_prometheus_metric_exporter(
        PrometheusExporterOptionsBuilder::default()
            .socket_addr(ANY_PORT.parse().unwrap())
            .use_seconds_for_durations(use_seconds)
            .unit_suffix(show_units)
            .build()
            .unwrap(),
    )
    .unwrap();
    telemopts.metrics = Some(prom_info.meter as Arc<dyn CoreMeter>);
    (
        telemopts,
        prom_info.bound_addr,
        AbortOnDrop {
            ah: prom_info.abort_handle,
        },
    )
}

#[rstest::rstest]
#[tokio::test]
async fn prometheus_metrics_exported(#[values(true, false)] use_seconds_latency: bool) {
    let (telemopts, addr, _aborter) = prom_metrics(use_seconds_latency, false);
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();
    let opts = get_integ_server_options();
    let mut raw_client = opts
        .connect_no_namespace(rt.telemetry().get_temporal_metric_meter())
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
    if use_seconds_latency {
        assert!(body.contains(
            "temporal_request_latency_bucket{\
             operation=\"GetSystemInfo\",service_name=\"temporal-core-sdk\",le=\"0.05\"}"
        ));
    } else {
        assert!(body.contains(
            "temporal_request_latency_bucket{\
             operation=\"GetSystemInfo\",service_name=\"temporal-core-sdk\",le=\"50\"}"
        ));
    }
    // Verify counter names are appropriate (don't end w/ '_total')
    assert!(body.contains("temporal_request{"));
    // Verify non-temporal metrics meter does not prefix
    let mm = rt.telemetry().get_metric_meter().unwrap();
    let g = mm.inner.gauge(MetricParameters::from("mygauge"));
    g.record(
        42,
        &MetricAttributes::OTel {
            kvs: Arc::new(vec![]),
        },
    );
    let body = get_text(format!("http://{addr}/metrics")).await;
    assert!(body.contains("\nmygauge 42"));
}

#[tokio::test]
async fn one_slot_worker_reports_available_slot() {
    let (telemopts, addr, _aborter) = prom_metrics(false, false);
    let tq = "one_slot_worker_tq";
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();

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
            .connect(worker_cfg.namespace.clone(), None)
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

        // At this point the workflow task is outstanding and the activities haven't started
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
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_used{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"WorkflowWorker\"}} 1"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_used{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"ActivityWorker\"}} 0"
        )));
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_used{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"LocalActivityWorker\"}} 0"
        )));

        // Now we allow the complete to proceed. Once it goes through, there should be 2 WFT slot
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
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_used{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"ActivityWorker\"}} 1"
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
        assert!(body.contains(&format!(
            "temporal_worker_task_slots_used{{namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"one_slot_worker_tq\",\
             worker_type=\"LocalActivityWorker\"}} 1"
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
    let (telemopts, addr, _aborter) = prom_metrics(false, false);
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();
    let mut starter =
        CoreWfStarter::new_with_runtime("query_of_closed_workflow_doesnt_tick_terminal_metric", rt);
    // Disable cache to ensure replay happens completely
    starter.worker_config.max_cached_workflows(0_usize);
    let worker = starter.get_worker().await;
    let run_id = starter.start_wf().await;
    let task = worker.poll_workflow_activation().await.unwrap();
    // Fail wf task
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::fail(
            task.run_id,
            "whatever".into(),
            None,
        ))
        .await
        .unwrap();
    // Handle cache eviction
    let task = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    // Immediately complete the workflow
    let task = worker.poll_workflow_activation().await.unwrap();
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

#[test]
fn runtime_new() {
    let mut rt =
        CoreRuntime::new(get_integ_telem_options(), TokioRuntimeBuilder::default()).unwrap();
    let handle = rt.tokio_handle();
    let _rt = handle.enter();
    let (telemopts, addr, _aborter) = prom_metrics(false, false);
    rt.telemetry_mut()
        .attach_late_init_metrics(telemopts.metrics.unwrap());
    let opts = get_integ_server_options();
    handle.block_on(async {
        let mut raw_client = opts
            .connect_no_namespace(rt.telemetry().get_temporal_metric_meter())
            .await
            .unwrap();
        assert!(raw_client.get_client().capabilities().is_some());
        let _ = raw_client
            .list_namespaces(ListNamespacesRequest::default())
            .await
            .unwrap();
        let body = get_text(format!("http://{addr}/metrics")).await;
        assert!(body.contains("temporal_request"));
    });
}

#[rstest::rstest]
#[tokio::test]
async fn latency_metrics(
    #[values(true, false)] use_seconds_latency: bool,
    #[values(true, false)] show_units: bool,
) {
    let (telemopts, addr, _aborter) = prom_metrics(use_seconds_latency, show_units);
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime("latency_metrics", rt);
    let worker = starter.get_worker().await;
    starter.start_wf().await;
    // Immediately finish workflow
    let task = worker.poll_workflow_activation().await.unwrap();
    worker
        .complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            CompleteWorkflowExecution { result: None }.into(),
        ))
        .await
        .unwrap();

    let body = get_text(format!("http://{addr}/metrics")).await;
    let matching_line = body
        .lines()
        .find(|l| l.starts_with("temporal_workflow_endtoend_latency"))
        .unwrap();

    if use_seconds_latency {
        if show_units {
            assert!(matching_line.contains("temporal_workflow_endtoend_latency_seconds"));
        }
        assert!(matching_line.contains("le=\"0.1\""));
    } else {
        if show_units {
            assert!(matching_line.contains("temporal_workflow_endtoend_latency_milliseconds"));
        }
        assert!(matching_line.contains("le=\"100\""));
    }
}

#[tokio::test]
async fn request_fail_codes() {
    let (telemopts, addr, _aborter) = prom_metrics(false, false);
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();
    let opts = get_integ_server_options();
    let mut client = opts
        .connect(NAMESPACE, rt.telemetry().get_temporal_metric_meter())
        .await
        .unwrap();

    // Describe namespace w/ invalid argument (unset namespace field)
    WorkflowService::describe_namespace(&mut client, DescribeNamespaceRequest::default())
        .await
        .unwrap_err();

    let body = get_text(format!("http://{addr}/metrics")).await;
    let matching_line = body
        .lines()
        .find(|l| l.starts_with("temporal_request_failure"))
        .unwrap();
    assert!(matching_line.contains("operation=\"DescribeNamespace\""));
    assert!(matching_line.contains("status_code=\"INVALID_ARGUMENT\""));
    assert!(matching_line.contains("} 1"));
}

// OTel collector shutdown hangs in a single-threaded Tokio environment. We used to, in the past
// have a dedicated runtime just for telemetry which was meant to address problems like this.
// In reality, users are unlikely to run a single-threaded runtime.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn request_fail_codes_otel() {
    let exporter = if let Some(url) = env::var(OTEL_URL_ENV_VAR)
        .ok()
        .map(|x| x.parse::<Url>().unwrap())
    {
        let opts = OtelCollectorOptionsBuilder::default()
            .url(url)
            .build()
            .unwrap();
        build_otlp_metric_exporter(opts).unwrap()
    } else {
        // skip
        return;
    };
    let mut telemopts = TelemetryOptionsBuilder::default();
    let exporter = Arc::new(exporter);
    telemopts.metrics(exporter as Arc<dyn CoreMeter>);

    let rt = CoreRuntime::new_assume_tokio(telemopts.build().unwrap()).unwrap();
    let opts = get_integ_server_options();
    let mut client = opts
        .connect(NAMESPACE, rt.telemetry().get_temporal_metric_meter())
        .await
        .unwrap();

    for _ in 0..10 {
        // Describe namespace w/ invalid argument (unset namespace field)
        WorkflowService::describe_namespace(&mut client, DescribeNamespaceRequest::default())
            .await
            .unwrap_err();

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[tokio::test]
async fn activity_metrics() {
    let (telemopts, addr, _aborter) = prom_metrics(false, false);
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();
    let wf_name = "activity_metrics";
    let mut starter = CoreWfStarter::new_with_runtime(wf_name, rt);
    let task_queue = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;

    worker.register_wf(wf_name.to_string(), |ctx: WfContext| async move {
        let normal_act_pass = ctx.activity(ActivityOptions {
            activity_type: "pass_fail_act".to_string(),
            input: "pass".as_json_payload().expect("serializes fine"),
            start_to_close_timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        });
        let local_act_pass = ctx.local_activity(LocalActivityOptions {
            activity_type: "pass_fail_act".to_string(),
            input: "pass".as_json_payload().expect("serializes fine"),
            ..Default::default()
        });
        let normal_act_fail = ctx.activity(ActivityOptions {
            activity_type: "pass_fail_act".to_string(),
            input: "fail".as_json_payload().expect("serializes fine"),
            start_to_close_timeout: Some(Duration::from_secs(1)),
            retry_policy: Some(RetryPolicy {
                maximum_attempts: 1,
                ..Default::default()
            }),
            ..Default::default()
        });
        let local_act_fail = ctx.local_activity(LocalActivityOptions {
            activity_type: "pass_fail_act".to_string(),
            input: "fail".as_json_payload().expect("serializes fine"),
            retry_policy: RetryPolicy {
                maximum_attempts: 1,
                ..Default::default()
            },
            ..Default::default()
        });
        let local_act_cancel = ctx.local_activity(LocalActivityOptions {
            activity_type: "pass_fail_act".to_string(),
            input: "cancel".as_json_payload().expect("serializes fine"),
            retry_policy: RetryPolicy {
                maximum_attempts: 1,
                ..Default::default()
            },
            ..Default::default()
        });
        join!(
            normal_act_pass,
            local_act_pass,
            normal_act_fail,
            local_act_fail
        );
        local_act_cancel.cancel(&ctx);
        local_act_cancel.await;
        Ok(().into())
    });
    worker.register_activity("pass_fail_act", |ctx: ActContext, i: String| async move {
        match i.as_str() {
            "pass" => Ok("pass"),
            "cancel" => {
                // TODO: Cancel is taking until shutdown to come through :|
                ctx.cancelled().await;
                Err(ActivityError::cancelled())
            }
            _ => Err(anyhow!("fail").into()),
        }
    });

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

    let body = get_text(format!("http://{addr}/metrics")).await;
    assert!(body.contains(&format!(
        "temporal_activity_execution_failed{{activity_type=\"pass_fail_act\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\",workflow_type=\"{wf_name}\"}} 1"
    )));
    assert!(body.contains(&format!(
        "temporal_activity_schedule_to_start_latency_count{{\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\"}} 2"
    )));
    assert!(body.contains(&format!(
        "temporal_activity_execution_latency_count{{activity_type=\"pass_fail_act\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\",workflow_type=\"{wf_name}\"}} 2"
    )));
    assert!(body.contains(&format!(
        "temporal_activity_succeed_endtoend_latency_count{{activity_type=\"pass_fail_act\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\",workflow_type=\"{wf_name}\"}} 1"
    )));

    assert!(body.contains(&format!(
        "temporal_local_activity_total{{activity_type=\"pass_fail_act\",namespace=\"{NAMESPACE}\",\
             service_name=\"temporal-core-sdk\",task_queue=\"{task_queue}\",\
             workflow_type=\"{wf_name}\"}} 3"
    )));
    assert!(body.contains(&format!(
        "temporal_local_activity_execution_failed{{activity_type=\"pass_fail_act\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\",\
             workflow_type=\"{wf_name}\"}} 1"
    )));
    assert!(body.contains(&format!(
        "temporal_local_activity_execution_cancelled{{activity_type=\"pass_fail_act\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\",\
             workflow_type=\"{wf_name}\"}} 1"
    )));
    assert!(body.contains(&format!(
        "temporal_local_activity_execution_latency_count{{activity_type=\"pass_fail_act\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\",\
             workflow_type=\"{wf_name}\"}} 3"
    )));
    assert!(body.contains(&format!(
        "temporal_local_activity_succeed_endtoend_latency_count{{activity_type=\"pass_fail_act\",\
             namespace=\"{NAMESPACE}\",service_name=\"temporal-core-sdk\",\
             task_queue=\"{task_queue}\",\
             workflow_type=\"{wf_name}\"}} 1"
    )));
}
