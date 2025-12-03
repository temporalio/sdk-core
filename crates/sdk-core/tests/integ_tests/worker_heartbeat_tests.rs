use crate::common::{ANY_PORT, CoreWfStarter, eventually, get_integ_telem_options};
use anyhow::anyhow;
use crossbeam_utils::atomic::AtomicCell;
use futures_util::StreamExt;
use prost_types::{Duration as PbDuration, Timestamp};
use std::{
    collections::HashSet,
    env,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use temporalio_client::{
    Client, NamespacedClient, RetryClient, WfClientExt, WorkflowClientTrait, WorkflowService,
};
use temporalio_common::{
    prost_dur,
    protos::{
        coresdk::{AsJsonPayloadExt, FromJsonPayloadExt},
        temporal::api::{
            common::v1::RetryPolicy,
            enums::v1::WorkerStatus,
            worker::v1::{PluginInfo, WorkerHeartbeat},
            workflowservice::v1::{DescribeWorkerRequest, ListWorkersRequest},
        },
    },
    telemetry::{
        OtelCollectorOptionsBuilder, PrometheusExporterOptionsBuilder, TelemetryOptionsBuilder,
    },
    worker::PollerBehavior,
};
use temporalio_sdk::{ActContext, ActivityOptions, WfContext};
use temporalio_sdk_core::{
    CoreRuntime, ResourceBasedTuner, ResourceSlotOptions, RuntimeOptionsBuilder,
    telemetry::{build_otlp_metric_exporter, start_prometheus_metric_exporter},
};
use tokio::{sync::Notify, time::sleep};
use tonic::IntoRequest;
use url::Url;

fn within_two_minutes_ts(ts: Timestamp) -> bool {
    let ts_time = UNIX_EPOCH + Duration::new(ts.seconds as u64, ts.nanos as u32);

    let now = SystemTime::now();
    // ts should be at most 2 minutes before the current time
    now.duration_since(ts_time).unwrap() <= Duration::from_secs(2 * 60)
}

fn within_duration(dur: PbDuration, threshold: Duration) -> bool {
    let std_dur = Duration::new(dur.seconds as u64, dur.nanos as u32);
    std_dur <= threshold
}

fn new_no_metrics_starter(wf_name: &str) -> CoreWfStarter {
    let runtimeopts = RuntimeOptionsBuilder::default()
        .telemetry_options(TelemetryOptionsBuilder::default().build().unwrap())
        .heartbeat_interval(Some(Duration::from_secs(1)))
        .build()
        .unwrap();
    CoreWfStarter::new_with_runtime(wf_name, CoreRuntime::new_assume_tokio(runtimeopts).unwrap())
}

fn to_system_time(ts: Timestamp) -> SystemTime {
    UNIX_EPOCH + Duration::new(ts.seconds as u64, ts.nanos as u32)
}

async fn list_worker_heartbeats(
    client: &Arc<RetryClient<Client>>,
    query: impl Into<String>,
) -> Vec<WorkerHeartbeat> {
    let mut raw_client = client.as_ref().clone();
    WorkflowService::list_workers(
        &mut raw_client,
        ListWorkersRequest {
            namespace: client.namespace().to_owned(),
            page_size: 200,
            next_page_token: Vec::new(),
            query: query.into(),
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner()
    .workers_info
    .into_iter()
    .filter_map(|info| info.worker_heartbeat)
    .collect()
}

// Tests that rely on Prometheus running in a docker container need to start
// with `docker_` and set the `DOCKER_PROMETHEUS_RUNNING` env variable to run
#[rstest::rstest]
#[tokio::test]
async fn docker_worker_heartbeat_basic(#[values("otel", "prom", "no_metrics")] backing: &str) {
    if env::var("DOCKER_PROMETHEUS_RUNNING").is_err() {
        return;
    }
    let telemopts = if backing == "no_metrics" {
        TelemetryOptionsBuilder::default().build().unwrap()
    } else {
        get_integ_telem_options()
    };
    let runtimeopts = RuntimeOptionsBuilder::default()
        .telemetry_options(telemopts)
        .heartbeat_interval(Some(Duration::from_secs(1)))
        .build()
        .unwrap();
    let mut rt = CoreRuntime::new_assume_tokio(runtimeopts).unwrap();
    match backing {
        "otel" => {
            let url = Some("grpc://localhost:4317")
                .map(|x| x.parse::<Url>().unwrap())
                .unwrap();
            let mut opts_build = OtelCollectorOptionsBuilder::default();
            let opts = opts_build.url(url).build().unwrap();
            rt.telemetry_mut()
                .attach_late_init_metrics(Arc::new(build_otlp_metric_exporter(opts).unwrap()));
        }
        "prom" => {
            let mut opts_build = PrometheusExporterOptionsBuilder::default();
            opts_build.socket_addr(ANY_PORT.parse().unwrap());
            let opts = opts_build.build().unwrap();
            rt.telemetry_mut()
                .attach_late_init_metrics(start_prometheus_metric_exporter(opts).unwrap().meter);
        }
        "no_metrics" => {}
        _ => unreachable!(),
    }
    let wf_name = format!("worker_heartbeat_basic_{backing}");
    let mut starter = CoreWfStarter::new_with_runtime(&wf_name, rt);
    starter
        .worker_config
        .max_outstanding_workflow_tasks(5_usize)
        .max_cached_workflows(5_usize)
        .max_outstanding_activities(5_usize)
        .plugins(vec![
            PluginInfo {
                name: "plugin1".to_string(),
                version: "1".to_string(),
            },
            PluginInfo {
                name: "plugin2".to_string(),
                version: "2".to_string(),
            },
        ]);
    let mut worker = starter.worker().await;
    let worker_instance_key = worker.worker_instance_key();

    worker.register_wf(wf_name.to_string(), |ctx: WfContext| async move {
        ctx.activity(ActivityOptions {
            activity_type: "pass_fail_act".to_string(),
            input: "pass".as_json_payload().expect("serializes fine"),
            start_to_close_timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .await;
        Ok(().into())
    });

    let acts_started = Arc::new(Notify::new());
    let acts_done = Arc::new(Notify::new());

    let acts_started_act = acts_started.clone();
    let acts_done_act = acts_done.clone();
    worker.register_activity("pass_fail_act", move |_ctx: ActContext, i: String| {
        let acts_started = acts_started_act.clone();
        let acts_done = acts_done_act.clone();
        async move {
            acts_started.notify_one();
            acts_done.notified().await;
            Ok(i)
        }
    });

    starter
        .start_with_worker(wf_name.clone(), &mut worker)
        .await;

    let start_time = AtomicCell::new(None);
    let heartbeat_time = AtomicCell::new(None);

    let test_fut = async {
        // Give enough time to ensure heartbeat interval has been hit
        tokio::time::sleep(Duration::from_millis(1500)).await;
        acts_started.notified().await;
        let client = starter.get_client().await;
        let mut raw_client = (*client).clone();
        let workers_list = WorkflowService::list_workers(
            &mut raw_client,
            ListWorkersRequest {
                namespace: client.namespace().to_owned(),
                page_size: 100,
                next_page_token: Vec::new(),
                query: String::new(),
            }
            .into_request(),
        )
        .await
        .unwrap()
        .into_inner();
        let worker_info = workers_list
            .workers_info
            .iter()
            .find(|worker_info| {
                if let Some(hb) = worker_info.worker_heartbeat.as_ref() {
                    hb.worker_instance_key == worker_instance_key.to_string()
                } else {
                    false
                }
            })
            .unwrap();
        let heartbeat = worker_info.worker_heartbeat.as_ref().unwrap();
        assert_eq!(
            heartbeat.worker_instance_key,
            worker_instance_key.to_string()
        );
        in_activity_checks(heartbeat, &start_time, &heartbeat_time);
        acts_done.notify_one();
    };

    let runner = async move {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(test_fut, runner);

    let client = starter.get_client().await;
    let mut raw_client = (*client).clone();
    let workers_list = WorkflowService::list_workers(
        &mut raw_client,
        ListWorkersRequest {
            namespace: client.namespace().to_owned(),
            page_size: 100,
            next_page_token: Vec::new(),
            query: String::new(),
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner();
    // Since list_workers finds all workers in the namespace, must find specific worker used in this
    // test
    let worker_info = workers_list
        .workers_info
        .iter()
        .find(|worker_info| {
            if let Some(hb) = worker_info.worker_heartbeat.as_ref() {
                hb.worker_instance_key == worker_instance_key.to_string()
            } else {
                false
            }
        })
        .unwrap();
    let heartbeat = worker_info.worker_heartbeat.as_ref().unwrap();
    after_shutdown_checks(heartbeat, &wf_name, &start_time, &heartbeat_time);
}

// Tests that rely on Prometheus running in a docker container need to start
// with `docker_` and set the `DOCKER_PROMETHEUS_RUNNING` env variable to run
#[tokio::test]
async fn docker_worker_heartbeat_tuner() {
    if env::var("DOCKER_PROMETHEUS_RUNNING").is_err() {
        return;
    }
    let runtimeopts = RuntimeOptionsBuilder::default()
        .telemetry_options(get_integ_telem_options())
        .heartbeat_interval(Some(Duration::from_secs(1)))
        .build()
        .unwrap();
    let mut rt = CoreRuntime::new_assume_tokio(runtimeopts).unwrap();

    let url = Some("grpc://localhost:4317")
        .map(|x| x.parse::<Url>().unwrap())
        .unwrap();
    let mut opts_build = OtelCollectorOptionsBuilder::default();
    let opts = opts_build.url(url).build().unwrap();

    rt.telemetry_mut()
        .attach_late_init_metrics(Arc::new(build_otlp_metric_exporter(opts).unwrap()));
    let wf_name = "worker_heartbeat_tuner";
    let mut starter = CoreWfStarter::new_with_runtime(wf_name, rt);
    let mut tuner = ResourceBasedTuner::new(0.0, 0.0);
    tuner
        .with_workflow_slots_options(ResourceSlotOptions::new(2, 10, Duration::from_millis(0)))
        .with_activity_slots_options(ResourceSlotOptions::new(5, 10, Duration::from_millis(50)));
    starter
        .worker_config
        .workflow_task_poller_behavior(PollerBehavior::Autoscaling {
            minimum: 1,
            maximum: 200,
            initial: 5,
        })
        .nexus_task_poller_behavior(PollerBehavior::Autoscaling {
            minimum: 1,
            maximum: 200,
            initial: 5,
        })
        .clear_max_outstanding_opts()
        .tuner(Arc::new(tuner));
    let mut worker = starter.worker().await;
    let worker_instance_key = worker.worker_instance_key();

    // Run a workflow
    worker.register_wf(wf_name.to_string(), |ctx: WfContext| async move {
        ctx.activity(ActivityOptions {
            activity_type: "pass_fail_act".to_string(),
            input: "pass".as_json_payload().expect("serializes fine"),
            start_to_close_timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .await;
        Ok(().into())
    });
    worker.register_activity("pass_fail_act", |_ctx: ActContext, i: String| async move {
        Ok(i)
    });

    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();

    let client = starter.get_client().await;
    let mut raw_client = (*client).clone();
    let workers_list = WorkflowService::list_workers(
        &mut raw_client,
        ListWorkersRequest {
            namespace: client.namespace().to_owned(),
            page_size: 100,
            next_page_token: Vec::new(),
            query: String::new(),
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner();
    // Since list_workers finds all workers in the namespace, must find specific worker used in this
    // test
    let worker_info = workers_list
        .workers_info
        .iter()
        .find(|worker_info| {
            if let Some(hb) = worker_info.worker_heartbeat.as_ref() {
                hb.worker_instance_key == worker_instance_key.to_string()
            } else {
                false
            }
        })
        .unwrap();
    let heartbeat = worker_info.worker_heartbeat.as_ref().unwrap();
    assert!(heartbeat.task_queue.starts_with(wf_name));

    assert_eq!(
        heartbeat
            .workflow_task_slots_info
            .clone()
            .unwrap()
            .slot_supplier_kind,
        "ResourceBased"
    );
    assert_eq!(
        heartbeat
            .activity_task_slots_info
            .clone()
            .unwrap()
            .slot_supplier_kind,
        "ResourceBased"
    );
    assert_eq!(
        heartbeat
            .nexus_task_slots_info
            .clone()
            .unwrap()
            .slot_supplier_kind,
        "ResourceBased"
    );
    assert_eq!(
        heartbeat
            .local_activity_slots_info
            .clone()
            .unwrap()
            .slot_supplier_kind,
        "ResourceBased"
    );

    let workflow_poller_info = heartbeat.workflow_poller_info.unwrap();
    assert!(workflow_poller_info.is_autoscaling);
    assert!(within_two_minutes_ts(
        workflow_poller_info.last_successful_poll_time.unwrap()
    ));
    let sticky_poller_info = heartbeat.workflow_sticky_poller_info.unwrap();
    assert!(sticky_poller_info.is_autoscaling);
    assert!(within_two_minutes_ts(
        sticky_poller_info.last_successful_poll_time.unwrap()
    ));
    let nexus_poller_info = heartbeat.nexus_poller_info.unwrap();
    assert!(nexus_poller_info.is_autoscaling);
    assert!(nexus_poller_info.last_successful_poll_time.is_none());
    let activity_poller_info = heartbeat.activity_poller_info.unwrap();
    assert!(!activity_poller_info.is_autoscaling);
    assert!(within_two_minutes_ts(
        activity_poller_info.last_successful_poll_time.unwrap()
    ));
}

fn in_activity_checks(
    heartbeat: &WorkerHeartbeat,
    start_time: &AtomicCell<Option<Timestamp>>,
    heartbeat_time: &AtomicCell<Option<Timestamp>>,
) {
    assert_eq!(heartbeat.status, WorkerStatus::Running as i32);

    let workflow_task_slots = heartbeat.workflow_task_slots_info.clone().unwrap();
    assert_eq!(workflow_task_slots.total_processed_tasks, 1);
    assert_eq!(workflow_task_slots.current_available_slots, 5);
    assert_eq!(workflow_task_slots.current_used_slots, 0);
    assert_eq!(workflow_task_slots.slot_supplier_kind, "Fixed");
    let activity_task_slots = heartbeat.activity_task_slots_info.clone().unwrap();
    assert_eq!(activity_task_slots.current_available_slots, 4);
    assert_eq!(activity_task_slots.current_used_slots, 1);
    assert_eq!(activity_task_slots.slot_supplier_kind, "Fixed");
    let nexus_task_slots = heartbeat.nexus_task_slots_info.clone().unwrap();
    assert_eq!(nexus_task_slots.current_available_slots, 0);
    assert_eq!(nexus_task_slots.current_used_slots, 0);
    assert_eq!(nexus_task_slots.slot_supplier_kind, "Fixed");
    let local_activity_task_slots = heartbeat.local_activity_slots_info.clone().unwrap();
    assert_eq!(local_activity_task_slots.current_available_slots, 100);
    assert_eq!(local_activity_task_slots.current_used_slots, 0);
    assert_eq!(local_activity_task_slots.slot_supplier_kind, "Fixed");

    let workflow_poller_info = heartbeat.workflow_poller_info.unwrap();
    assert_eq!(workflow_poller_info.current_pollers, 1);
    let sticky_poller_info = heartbeat.workflow_sticky_poller_info.unwrap();
    assert_ne!(sticky_poller_info.current_pollers, 0);
    let nexus_poller_info = heartbeat.nexus_poller_info.unwrap();
    assert_eq!(nexus_poller_info.current_pollers, 0);
    let activity_poller_info = heartbeat.activity_poller_info.unwrap();
    assert_ne!(activity_poller_info.current_pollers, 0);
    assert_ne!(heartbeat.current_sticky_cache_size, 0);
    start_time.store(Some(heartbeat.start_time.unwrap()));
    heartbeat_time.store(Some(heartbeat.heartbeat_time.unwrap()));
}

fn after_shutdown_checks(
    heartbeat: &WorkerHeartbeat,
    wf_name: &str,
    start_time: &AtomicCell<Option<Timestamp>>,
    heartbeat_time: &AtomicCell<Option<Timestamp>>,
) {
    assert_eq!(heartbeat.worker_identity, "integ_tester");
    let host_info = heartbeat.host_info.clone().unwrap();
    assert!(!host_info.host_name.is_empty());
    assert!(!host_info.process_key.is_empty());
    assert!(!host_info.process_id.is_empty());
    assert!(host_info.current_host_cpu_usage >= 0.0);
    assert!(host_info.current_host_mem_usage >= 0.0);

    assert!(heartbeat.task_queue.starts_with(wf_name));
    assert_eq!(
        heartbeat.deployment_version.clone().unwrap().build_id,
        "test_build_id"
    );
    assert_eq!(heartbeat.sdk_name, "temporal-core");
    assert_eq!(heartbeat.sdk_version, "0.1.0");
    assert_eq!(heartbeat.status, WorkerStatus::Shutdown as i32);

    assert_eq!(start_time.load().unwrap(), heartbeat.start_time.unwrap());
    assert_ne!(
        heartbeat_time.load().unwrap(),
        heartbeat.heartbeat_time.unwrap()
    );
    assert!(within_two_minutes_ts(heartbeat.start_time.unwrap()));
    assert!(within_two_minutes_ts(heartbeat.heartbeat_time.unwrap()));
    assert!(
        to_system_time(heartbeat_time.load().unwrap())
            < to_system_time(heartbeat.heartbeat_time.unwrap())
    );
    assert!(within_duration(
        heartbeat.elapsed_since_last_heartbeat.unwrap(),
        Duration::from_millis(2000)
    ));

    let workflow_task_slots = heartbeat.workflow_task_slots_info.clone().unwrap();
    assert_eq!(workflow_task_slots.current_available_slots, 5);
    assert_eq!(workflow_task_slots.current_used_slots, 1);
    assert_eq!(workflow_task_slots.total_processed_tasks, 2);
    assert_eq!(workflow_task_slots.slot_supplier_kind, "Fixed");
    let activity_task_slots = heartbeat.activity_task_slots_info.clone().unwrap();
    assert_eq!(activity_task_slots.current_available_slots, 5);
    assert_eq!(workflow_task_slots.current_used_slots, 1);
    assert_eq!(activity_task_slots.slot_supplier_kind, "Fixed");
    assert_eq!(activity_task_slots.last_interval_processed_tasks, 1);
    let nexus_task_slots = heartbeat.nexus_task_slots_info.clone().unwrap();
    assert_eq!(nexus_task_slots.current_available_slots, 0);
    assert_eq!(nexus_task_slots.current_used_slots, 0);
    assert_eq!(nexus_task_slots.slot_supplier_kind, "Fixed");
    let local_activity_task_slots = heartbeat.local_activity_slots_info.clone().unwrap();
    assert_eq!(local_activity_task_slots.current_available_slots, 100);
    assert_eq!(local_activity_task_slots.current_used_slots, 0);
    assert_eq!(local_activity_task_slots.slot_supplier_kind, "Fixed");

    let workflow_poller_info = heartbeat.workflow_poller_info.unwrap();
    assert!(!workflow_poller_info.is_autoscaling);
    assert!(within_two_minutes_ts(
        workflow_poller_info.last_successful_poll_time.unwrap()
    ));
    let sticky_poller_info = heartbeat.workflow_sticky_poller_info.unwrap();
    assert!(!sticky_poller_info.is_autoscaling);
    assert!(within_two_minutes_ts(
        sticky_poller_info.last_successful_poll_time.unwrap()
    ));
    let nexus_poller_info = heartbeat.nexus_poller_info.unwrap();
    assert!(!nexus_poller_info.is_autoscaling);
    assert!(nexus_poller_info.last_successful_poll_time.is_none());
    let activity_poller_info = heartbeat.activity_poller_info.unwrap();
    assert!(!activity_poller_info.is_autoscaling);
    assert!(within_two_minutes_ts(
        activity_poller_info.last_successful_poll_time.unwrap()
    ));

    assert_eq!(heartbeat.total_sticky_cache_hit, 2);
    assert_eq!(heartbeat.current_sticky_cache_size, 0);
    assert_eq!(
        heartbeat.plugins,
        vec![
            PluginInfo {
                name: "plugin1".to_string(),
                version: "1".to_string()
            },
            PluginInfo {
                name: "plugin2".to_string(),
                version: "2".to_string()
            }
        ]
    );
}

#[tokio::test]
async fn worker_heartbeat_sticky_cache_miss() {
    let wf_name = "worker_heartbeat_cache_miss";
    let mut starter = new_no_metrics_starter(wf_name);
    starter
        .worker_config
        .max_cached_workflows(1_usize)
        .max_outstanding_workflow_tasks(2_usize);

    let mut worker = starter.worker().await;
    worker.fetch_results = false;
    let worker_key = worker.worker_instance_key().to_string();
    let worker_core = worker.core_worker.clone();
    let submitter = worker.get_submitter_handle();
    let wf_opts = starter.workflow_options.clone();
    let client = starter.get_client().await;
    let client_for_orchestrator = client.clone();

    static HISTORY_WF1_ACTIVITY_STARTED: Notify = Notify::const_new();
    static HISTORY_WF1_ACTIVITY_FINISH: Notify = Notify::const_new();
    static HISTORY_WF2_ACTIVITY_STARTED: Notify = Notify::const_new();
    static HISTORY_WF2_ACTIVITY_FINISH: Notify = Notify::const_new();

    worker.register_wf(wf_name.to_string(), |ctx: WfContext| async move {
        let wf_marker = ctx
            .get_args()
            .first()
            .and_then(|p| String::from_json_payload(p).ok())
            .unwrap_or_else(|| "wf1".to_string());

        ctx.activity(ActivityOptions {
            activity_type: "sticky_cache_history_act".to_string(),
            input: wf_marker.clone().as_json_payload().expect("serialize"),
            start_to_close_timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .await;

        Ok(().into())
    });
    worker.register_activity(
        "sticky_cache_history_act",
        |_ctx: ActContext, marker: String| async move {
            match marker.as_str() {
                "wf1" => {
                    HISTORY_WF1_ACTIVITY_STARTED.notify_one();
                    HISTORY_WF1_ACTIVITY_FINISH.notified().await;
                }
                "wf2" => {
                    HISTORY_WF2_ACTIVITY_STARTED.notify_one();
                    HISTORY_WF2_ACTIVITY_FINISH.notified().await;
                }
                _ => {}
            }
            Ok(marker)
        },
    );

    let wf1_id = format!("{wf_name}_wf1");
    let wf2_id = format!("{wf_name}_wf2");

    let orchestrator = async move {
        let wf1_run = submitter
            .submit_wf(
                wf1_id.clone(),
                wf_name.to_string(),
                vec!["wf1".to_string().as_json_payload().unwrap()],
                wf_opts.clone(),
            )
            .await
            .unwrap();

        HISTORY_WF1_ACTIVITY_STARTED.notified().await;

        let wf2_run = submitter
            .submit_wf(
                wf2_id.clone(),
                wf_name.to_string(),
                vec!["wf2".to_string().as_json_payload().unwrap()],
                wf_opts,
            )
            .await
            .unwrap();

        HISTORY_WF2_ACTIVITY_STARTED.notified().await;

        HISTORY_WF1_ACTIVITY_FINISH.notify_one();
        let handle1 = client_for_orchestrator.get_untyped_workflow_handle(wf1_id, wf1_run);
        handle1
            .get_workflow_result(Default::default())
            .await
            .expect("wf1 result");

        HISTORY_WF2_ACTIVITY_FINISH.notify_one();
        let handle2 = client_for_orchestrator.get_untyped_workflow_handle(wf2_id, wf2_run);
        handle2
            .get_workflow_result(Default::default())
            .await
            .expect("wf2 result");

        worker_core.initiate_shutdown();
    };

    let mut worker_runner = worker;
    let runner = async move {
        worker_runner.run_until_done().await.unwrap();
    };

    tokio::join!(orchestrator, runner);

    sleep(Duration::from_secs(2)).await;
    let mut heartbeats =
        list_worker_heartbeats(&client, format!("WorkerInstanceKey=\"{worker_key}\"")).await;
    assert_eq!(heartbeats.len(), 1);
    let heartbeat = heartbeats.pop().unwrap();

    assert!(heartbeat.total_sticky_cache_miss >= 1);
    assert_eq!(heartbeat.worker_instance_key, worker_key);
}

#[tokio::test]
async fn worker_heartbeat_multiple_workers() {
    let wf_name = "worker_heartbeat_multi_workers";
    let mut starter = new_no_metrics_starter(wf_name);
    starter
        .worker_config
        .max_outstanding_workflow_tasks(5_usize)
        .max_cached_workflows(5_usize);

    let client = starter.get_client().await;
    let starting_hb_len = list_worker_heartbeats(&client, String::new()).await.len();

    let mut worker_a = starter.worker().await;
    worker_a.register_wf(wf_name.to_string(), |_ctx: WfContext| async move {
        Ok(().into())
    });
    worker_a.register_activity("failing_act", |_ctx: ActContext, _: String| async move {
        Ok(())
    });

    let mut starter_b = starter.clone_no_worker();
    let mut worker_b = starter_b.worker().await;
    worker_b.register_wf(wf_name.to_string(), |_ctx: WfContext| async move {
        Ok(().into())
    });
    worker_b.register_activity("failing_act", |_ctx: ActContext, _: String| async move {
        Ok(())
    });

    let worker_a_key = worker_a.worker_instance_key().to_string();
    let worker_b_key = worker_b.worker_instance_key().to_string();
    let _ = starter.start_with_worker(wf_name, &mut worker_a).await;
    worker_a.run_until_done().await.unwrap();

    let _ = starter_b.start_with_worker(wf_name, &mut worker_b).await;
    worker_b.run_until_done().await.unwrap();

    sleep(Duration::from_secs(2)).await;

    let all = list_worker_heartbeats(&client, String::new()).await;
    let keys: HashSet<_> = all
        .iter()
        .map(|hb| hb.worker_instance_key.clone())
        .collect();
    assert!(keys.contains(&worker_a_key));
    assert!(keys.contains(&worker_b_key));

    // Verify both heartbeats contain the same shared process_key
    let process_keys: HashSet<_> = all
        .iter()
        .filter_map(|hb| hb.host_info.as_ref().map(|info| info.process_key.clone()))
        .collect();
    assert!(process_keys.len() > starting_hb_len);

    let filtered =
        list_worker_heartbeats(&client, format!("WorkerInstanceKey=\"{worker_a_key}\"")).await;
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].worker_instance_key, worker_a_key);

    // Verify describe worker gives the same heartbeat as listworker
    let mut raw_client = client.as_ref().clone();
    let describe_worker_a = WorkflowService::describe_worker(
        &mut raw_client,
        DescribeWorkerRequest {
            namespace: client.namespace().to_owned(),
            worker_instance_key: worker_a_key.to_string(),
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner()
    .worker_info
    .unwrap()
    .worker_heartbeat
    .unwrap();
    assert_eq!(describe_worker_a, filtered[0]);

    let filtered_b =
        list_worker_heartbeats(&client, format!("WorkerInstanceKey = \"{worker_b_key}\"")).await;
    assert_eq!(filtered_b.len(), 1);
    assert_eq!(filtered_b[0].worker_instance_key, worker_b_key);
    let describe_worker_b = WorkflowService::describe_worker(
        &mut raw_client,
        DescribeWorkerRequest {
            namespace: client.namespace().to_owned(),
            worker_instance_key: worker_b_key.to_string(),
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner()
    .worker_info
    .unwrap()
    .worker_heartbeat
    .unwrap();
    assert_eq!(describe_worker_b, filtered_b[0]);
}

#[tokio::test]
async fn worker_heartbeat_failure_metrics() {
    const WORKFLOW_CONTINUE_SIGNAL: &str = "workflow-continue";

    let wf_name = "worker_heartbeat_failure_metrics";
    let mut starter = new_no_metrics_starter(wf_name);
    starter.worker_config.max_outstanding_activities(5_usize);

    let mut worker = starter.worker().await;
    let worker_instance_key = worker.worker_instance_key();
    static ACT_COUNT: AtomicU64 = AtomicU64::new(0);
    static WF_COUNT: AtomicU64 = AtomicU64::new(0);
    static ACT_FAIL: Notify = Notify::const_new();
    static WF_FAIL: Notify = Notify::const_new();
    worker.register_wf(wf_name.to_string(), |ctx: WfContext| async move {
        let _ = ctx
            .activity(ActivityOptions {
                activity_type: "failing_act".to_string(),
                input: "boom".as_json_payload().expect("serialize"),
                start_to_close_timeout: Some(Duration::from_secs(5)),
                retry_policy: Some(RetryPolicy {
                    initial_interval: Some(prost_dur!(from_millis(10))),
                    backoff_coefficient: 1.0,
                    maximum_attempts: 4,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await;

        if WF_COUNT.load(Ordering::Relaxed) == 0 {
            WF_COUNT.fetch_add(1, Ordering::Relaxed);
            WF_FAIL.notify_one();
            panic!("expected WF panic");
        }

        // Signal here to avoid workflow from completing and shutdown heartbeat from sending
        // before we check workflow_slots.last_interval_failure_tasks
        let mut proceed_signal = ctx.make_signal_channel(WORKFLOW_CONTINUE_SIGNAL);
        proceed_signal.next().await.unwrap();
        Ok(().into())
    });

    worker.register_activity("failing_act", |_ctx: ActContext, _: String| async move {
        if ACT_COUNT.load(Ordering::Relaxed) == 3 {
            return Ok(());
        }
        ACT_COUNT.fetch_add(1, Ordering::Relaxed);
        ACT_FAIL.notify_one();
        Err(anyhow!("Expected error").into())
    });

    let worker_key = worker_instance_key.to_string();
    starter.workflow_options.retry_policy = Some(RetryPolicy {
        maximum_attempts: 2,
        ..Default::default()
    });

    let _ = starter.start_with_worker(wf_name, &mut worker).await;

    let test_fut = async {
        ACT_FAIL.notified().await;
        let client = starter.get_client().await;
        eventually(
            || async {
                let mut raw_client = (*client).clone();

                let workers_list = WorkflowService::list_workers(
                    &mut raw_client,
                    ListWorkersRequest {
                        namespace: client.namespace().to_owned(),
                        page_size: 100,
                        next_page_token: Vec::new(),
                        query: String::new(),
                    }
                    .into_request(),
                )
                .await
                .unwrap()
                .into_inner();
                let worker_info = workers_list
                    .workers_info
                    .iter()
                    .find(|worker_info| {
                        if let Some(hb) = worker_info.worker_heartbeat.as_ref() {
                            hb.worker_instance_key == worker_instance_key.to_string()
                        } else {
                            false
                        }
                    })
                    .unwrap();
                let heartbeat = worker_info.worker_heartbeat.as_ref().unwrap();
                assert_eq!(
                    heartbeat.worker_instance_key,
                    worker_instance_key.to_string()
                );
                let activity_slots = heartbeat.activity_task_slots_info.clone().unwrap();
                if activity_slots.last_interval_failure_tasks >= 1 {
                    return Ok(());
                }
                Err("activity_slots.last_interval_failure_tasks still 0, retrying")
            },
            Duration::from_millis(1500),
        )
        .await
        .unwrap();

        WF_FAIL.notified().await;

        eventually(
            || async {
                let mut raw_client = (*client).clone();
                let workers_list = WorkflowService::list_workers(
                    &mut raw_client,
                    ListWorkersRequest {
                        namespace: client.namespace().to_owned(),
                        page_size: 100,
                        next_page_token: Vec::new(),
                        query: String::new(),
                    }
                    .into_request(),
                )
                .await
                .unwrap()
                .into_inner();
                let worker_info = workers_list
                    .workers_info
                    .iter()
                    .find(|worker_info| {
                        if let Some(hb) = worker_info.worker_heartbeat.as_ref() {
                            hb.worker_instance_key == worker_instance_key.to_string()
                        } else {
                            false
                        }
                    })
                    .unwrap();

                let heartbeat = worker_info.worker_heartbeat.as_ref().unwrap();
                let workflow_slots = heartbeat.workflow_task_slots_info.clone().unwrap();
                if workflow_slots.last_interval_failure_tasks >= 1 {
                    return Ok(());
                }
                Err("workflow_slots.last_interval_failure_tasks still 0, retrying")
            },
            Duration::from_millis(1500),
        )
        .await
        .unwrap();
        client
            .signal_workflow_execution(
                starter.get_wf_id().to_string(),
                String::new(),
                WORKFLOW_CONTINUE_SIGNAL.to_string(),
                None,
                None,
            )
            .await
            .unwrap();
    };

    let runner = async move {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(test_fut, runner);

    let client = starter.get_client().await;
    let mut heartbeats =
        list_worker_heartbeats(&client, format!("WorkerInstanceKey=\"{worker_key}\"")).await;
    assert_eq!(heartbeats.len(), 1);
    let heartbeat = heartbeats.pop().unwrap();

    let activity_slots = heartbeat.activity_task_slots_info.unwrap();
    assert_eq!(activity_slots.total_failed_tasks, 3);

    let workflow_slots = heartbeat.workflow_task_slots_info.unwrap();
    assert_eq!(workflow_slots.total_failed_tasks, 1);
}

#[tokio::test]
async fn worker_heartbeat_no_runtime_heartbeat() {
    let wf_name = "worker_heartbeat_no_runtime_heartbeat";
    let runtimeopts = RuntimeOptionsBuilder::default()
        .telemetry_options(get_integ_telem_options())
        .heartbeat_interval(None) // Turn heartbeating off
        .build()
        .unwrap();
    let rt = CoreRuntime::new_assume_tokio(runtimeopts).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime(wf_name, rt);
    let mut worker = starter.worker().await;
    let worker_instance_key = worker.worker_instance_key();

    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        ctx.activity(ActivityOptions {
            activity_type: "pass_fail_act".to_string(),
            input: "pass".as_json_payload().expect("serializes fine"),
            start_to_close_timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .await;
        Ok(().into())
    });

    worker.register_activity("pass_fail_act", |_ctx: ActContext, i: String| async move {
        Ok(i)
    });

    starter
        .start_with_worker(wf_name.to_owned(), &mut worker)
        .await;

    worker.run_until_done().await.unwrap();
    let client = starter.get_client().await;
    let mut raw_client = (*client).clone();
    let workers_list = WorkflowService::list_workers(
        &mut raw_client,
        ListWorkersRequest {
            namespace: client.namespace().to_owned(),
            page_size: 100,
            next_page_token: Vec::new(),
            query: String::new(),
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner();

    // Ensure worker has not ever heartbeated
    let heartbeat = workers_list.workers_info.iter().find(|worker_info| {
        if let Some(hb) = worker_info.worker_heartbeat.as_ref() {
            hb.worker_instance_key == worker_instance_key.to_string()
        } else {
            false
        }
    });
    assert!(heartbeat.is_none());
}

#[tokio::test]
async fn worker_heartbeat_skip_client_worker_set_check() {
    let wf_name = "worker_heartbeat_skip_client_worker_set_check";
    let runtimeopts = RuntimeOptionsBuilder::default()
        .telemetry_options(get_integ_telem_options())
        .heartbeat_interval(Some(Duration::from_secs(1)))
        .build()
        .unwrap();
    let rt = CoreRuntime::new_assume_tokio(runtimeopts).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime(wf_name, rt);
    starter.worker_config.skip_client_worker_set_check(true);
    let mut worker = starter.worker().await;
    let worker_instance_key = worker.worker_instance_key();

    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        ctx.activity(ActivityOptions {
            activity_type: "pass_fail_act".to_string(),
            input: "pass".as_json_payload().expect("serializes fine"),
            start_to_close_timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .await;
        Ok(().into())
    });

    worker.register_activity("pass_fail_act", |_ctx: ActContext, i: String| async move {
        Ok(i)
    });

    starter
        .start_with_worker(wf_name.to_owned(), &mut worker)
        .await;

    worker.run_until_done().await.unwrap();
    let client = starter.get_client().await;
    let mut raw_client = (*client).clone();
    let workers_list = WorkflowService::list_workers(
        &mut raw_client,
        ListWorkersRequest {
            namespace: client.namespace().to_owned(),
            page_size: 100,
            next_page_token: Vec::new(),
            query: String::new(),
        }
        .into_request(),
    )
    .await
    .unwrap()
    .into_inner();

    // Ensure worker still heartbeats
    let heartbeat = workers_list.workers_info.iter().find(|worker_info| {
        if let Some(hb) = worker_info.worker_heartbeat.as_ref() {
            hb.worker_instance_key == worker_instance_key.to_string()
        } else {
            false
        }
    });
    assert!(heartbeat.is_some());
}
