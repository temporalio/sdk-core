use crate::common::{ANY_PORT, CoreWfStarter, get_integ_telem_options};
use prost_types::Duration as PbDuration;
use prost_types::Timestamp;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use temporal_client::WorkflowClientTrait;
use temporal_sdk::{ActContext, ActivityOptions, WfContext};
use temporal_sdk_core::telemetry::{build_otlp_metric_exporter, start_prometheus_metric_exporter};
use temporal_sdk_core::{CoreRuntime, RuntimeOptionsBuilder};
use temporal_sdk_core_api::telemetry::{
    OtelCollectorOptionsBuilder, PrometheusExporterOptionsBuilder,
};
use temporal_sdk_core_protos::coresdk::AsJsonPayloadExt;
use temporal_sdk_core_protos::temporal::api::enums::v1::WorkerStatus;
use tokio::sync::Semaphore;
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

// Tests that rely on Prometheus running in a docker container need to start
// with `docker_` and set the `DOCKER_PROMETHEUS_RUNNING` env variable to run
#[rstest::rstest]
#[tokio::test]
async fn docker_worker_heartbeat_basic(
    #[values(
    "otel",
    // "prom"
)]
    backing: &str,
) {
    let runtimeopts = RuntimeOptionsBuilder::default()
        .telemetry_options(get_integ_telem_options())
        .heartbeat_interval(Some(Duration::from_millis(100)))
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
            // If wanna add more options: https://github.com/temporalio/sdk-ruby/blob/143e421d82d16e58bd45226998363d55e4bc3bbb/temporalio/ext/src/runtime.rs#L113C21-L135C22

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
        _ => unreachable!(),
    }
    let wf_name = format!("worker_heartbeat_basic_{backing}");
    static ACTS_STARTED: Semaphore = Semaphore::const_new(0);
    static ACTS_DONE: Semaphore = Semaphore::const_new(0);
    let mut starter = CoreWfStarter::new_with_runtime(&wf_name, rt);
    starter
        .worker_config
        .max_outstanding_workflow_tasks(5_usize)
        .max_cached_workflows(5_usize)
        .max_outstanding_activities(5_usize);
    let mut worker = starter.worker().await;
    let worker_instance_key = worker.worker_instance_key();

    // Run a workflow
    worker.register_wf(wf_name.to_string(), |ctx: WfContext| async move {
        println!("asdfasdfasdfasdfasdfasdf");
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
        println!("pass_fail_act");
        ACTS_STARTED.add_permits(1);
        let _ = ACTS_DONE.acquire().await.unwrap();
        Ok(i)
    });

    starter
        .start_with_worker(wf_name.clone(), &mut worker)
        .await;

    let test_fut = async {
        let _ = ACTS_STARTED.acquire().await.unwrap();
        let client = starter.get_client().await;
        let workers_list = client
            .list_workers(100, Vec::new(), String::new())
            .await
            .unwrap();
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
        println!("1231231231231: {heartbeat:#?}");
        let workflow_poller_info = heartbeat.workflow_poller_info.unwrap();
        assert_eq!(workflow_poller_info.current_pollers, 1);
        let sticky_poller_info = heartbeat.workflow_sticky_poller_info.unwrap();
        assert_eq!(sticky_poller_info.current_pollers, 3);
        let nexus_poller_info = heartbeat.nexus_poller_info.unwrap();
        assert_eq!(nexus_poller_info.current_pollers, 0);
        let activity_poller_info = heartbeat.activity_poller_info.unwrap();
        assert_eq!(activity_poller_info.current_pollers, 4);
        assert_eq!(heartbeat.current_sticky_cache_size, 1);
        ACTS_DONE.add_permits(1);
    };

    // for i in 1..5 {
    //     worker.submit_wf(
    //         format!("{wf_name}-{i}"),
    //         wf_name,
    //         vec![],
    //         starter.workflow_options.clone(),
    //     )
    //         .await
    //         .unwrap();
    // }

    let runner = async move {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(test_fut, runner);

    // TODO: clone_no_worker() for new worker
    let client = starter.get_client().await;
    let workers_list = client
        .list_workers(100, Vec::new(), String::new())
        .await
        .unwrap();
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
    println!("heartbeat: {heartbeat:#?}");
    assert!(heartbeat.task_queue.starts_with(&wf_name));
    assert_eq!(heartbeat.worker_identity, "integ_tester");
    assert_eq!(heartbeat.sdk_name, "temporal-core");
    assert_eq!(heartbeat.sdk_version, "0.1.0");
    assert_eq!(heartbeat.status, WorkerStatus::Shutdown as i32);
    assert!(within_two_minutes_ts(heartbeat.start_time.unwrap()));
    assert!(within_two_minutes_ts(heartbeat.heartbeat_time.unwrap()));
    assert!(within_duration(
        heartbeat.elapsed_since_last_heartbeat.unwrap(),
        Duration::from_secs(1)
    ));

    // TODO:
    // workflow_task_slots_info: Some(
    //     WorkerSlotsInfo {
    //     current_available_slots: 5,
    //     current_used_slots: 0,
    //     slot_supplier_kind: "Workflow",
    //     total_processed_tasks: 2,
    //     total_failed_tasks: 0,
    //     last_interval_processed_tasks: 0,
    //     last_interval_failure_tasks: 0,
    // },
    // ),
    // activity_task_slots_info: None,
    // nexus_task_slots_info: None,
    // local_activity_slots_info: None,

    // workflow_poller_info: Some(
    //     WorkerPollerInfo {
    //     current_pollers: 0,
    //     last_successful_poll_time: Some(
    //         Timestamp {
    //         seconds: 1759209070,
    //         nanos: 909377000,
    //     },
    //     ),
    //     is_autoscaling: false,
    // },
    // ),
    // workflow_sticky_poller_info: Some(
    //     WorkerPollerInfo {
    //     current_pollers: 0,
    //     last_successful_poll_time: Some(
    //         Timestamp {
    //         seconds: 1759209072,
    //         nanos: 281956000,
    //     },
    //     ),
    //     is_autoscaling: false,
    // },
    // ),
    // activity_poller_info: Some(
    //     WorkerPollerInfo {
    //     current_pollers: 0,
    //     last_successful_poll_time: Some(
    //         Timestamp {
    //         seconds: 1759209071,
    //         nanos: 737598000,
    //     },
    //     ),
    //     is_autoscaling: false,
    // },
    // ),
    // nexus_poller_info: Some(
    //     WorkerPollerInfo {
    //     current_pollers: 0,
    //     last_successful_poll_time: None,
    //     is_autoscaling: false,
    // },
    // ),
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
    // total_sticky_cache_miss: 0,
    // current_sticky_cache_size: 0,
    // plugins: [],
}
