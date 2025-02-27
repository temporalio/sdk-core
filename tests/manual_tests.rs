//! These tests are expensive and meant to be run manually. Use them for things like perf and
//! load testing.

use futures_util::{
    StreamExt,
    future::{AbortHandle, Abortable},
    sink, stream,
    stream::FuturesUnordered,
};
use rand::{Rng, SeedableRng};
use std::{
    mem,
    net::SocketAddr,
    time::{Duration, Instant},
};
use temporal_client::{GetWorkflowResultOpts, WfClientExt, WorkflowClientTrait, WorkflowOptions};
use temporal_sdk::{ActContext, ActivityOptions, WfContext};
use temporal_sdk_core::CoreRuntime;
use temporal_sdk_core_api::{telemetry::PrometheusExporterOptionsBuilder, worker::PollerBehavior};
use temporal_sdk_core_protos::coresdk::AsJsonPayloadExt;
use temporal_sdk_core_test_utils::{CoreWfStarter, prom_metrics, rand_6_chars};
use tracing::info;

#[tokio::test]
async fn poller_load_spiky() {
    const SIGNAME: &str = "signame";
    let num_workflows = 1000;
    let wf_name = "poller_load";
    let (telemopts, addr, _aborter) =
        if std::env::var("PAR_JOBNUM").unwrap_or("1".to_string()) == "1" {
            prom_metrics(Some(
                PrometheusExporterOptionsBuilder::default()
                    .socket_addr(SocketAddr::V4("0.0.0.0:9999".parse().unwrap()))
                    .build()
                    .unwrap(),
            ))
        } else {
            prom_metrics(None)
        };
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime("poller_load", rt);
    starter
        .worker_config
        .max_cached_workflows(5000_usize)
        .max_outstanding_workflow_tasks(1000_usize)
        .max_outstanding_activities(1000_usize)
        .workflow_task_poller_behavior(PollerBehavior::Autoscaling {
            minimum: 1,
            maximum: 200,
            initial: 5,
        })
        .activity_task_poller_behavior(PollerBehavior::Autoscaling {
            minimum: 1,
            maximum: 200,
            initial: 5,
        });
    let mut worker = starter.worker().await;
    let submitter = worker.get_submitter_handle();
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let sigchan = ctx.make_signal_channel(SIGNAME).map(Ok);
        let drained_fut = sigchan.forward(sink::drain());

        let real_stuff = async move {
            for _ in 0..5 {
                ctx.activity(ActivityOptions {
                    activity_type: "echo".to_string(),
                    start_to_close_timeout: Some(Duration::from_secs(5)),
                    input: "hi!".as_json_payload().expect("serializes fine"),
                    ..Default::default()
                })
                .await;
            }
        };
        tokio::select! {
            _ = drained_fut => {}
            _ = real_stuff => {}
        }

        Ok(().into())
    });
    worker.register_activity("echo", |_: ActContext, echo: String| async move {
        // Add some jitter to completions
        let rand_millis = rand::rng().random_range(0..500);
        tokio::time::sleep(Duration::from_millis(rand_millis)).await;
        Ok(echo)
    });
    let client = starter.get_client().await;

    info!("Prom bound to {:?}", addr);

    let mut workflow_handles = vec![];
    info!("Starting workflows...");
    for i in 0..num_workflows {
        let wfid = format!("{wf_name}_{i}-{}", rand_6_chars());
        let rid = worker
            .submit_wf(
                wfid.clone(),
                wf_name.to_owned(),
                vec![],
                WorkflowOptions {
                    execution_timeout: Some(Duration::from_secs(120)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        workflow_handles.push(client.get_untyped_workflow_handle(wfid, rid));
    }
    info!("Done starting workflows");
    let start_processing = Instant::now();

    let (ah, abort_reg) = AbortHandle::new_pair();
    let all_workflows_are_done = async {
        stream::iter(mem::take(&mut workflow_handles))
            .for_each_concurrent(25, |handle| async move {
                let _ = handle
                    .get_workflow_result(GetWorkflowResultOpts::default())
                    .await;
            })
            .await;
        info!("Initial load ran for {:?}", start_processing.elapsed());
        ah.abort();
        // Wait a minute for poller count to drop
        tokio::time::sleep(Duration::from_secs(60)).await;
        // round two
        let start_processing = Instant::now();
        for i in 0..500 {
            let wfid = format!("{wf_name}_2_{i}-{}", rand_6_chars());
            let rid = submitter
                .submit_wf(
                    wfid.clone(),
                    wf_name.to_owned(),
                    vec![],
                    WorkflowOptions {
                        execution_timeout: Some(Duration::from_secs(120)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            workflow_handles.push(client.get_untyped_workflow_handle(wfid, rid));
        }
        stream::iter(workflow_handles)
            .for_each_concurrent(25, |handle| async move {
                let _ = handle
                    .get_workflow_result(GetWorkflowResultOpts::default())
                    .await;
            })
            .await;
        info!("Second load ran for {:?}", start_processing.elapsed());

        info!("Done. Go look at yer metrics, must interrupt to end.");
    };

    let sig_sender = Abortable::new(
        async {
            loop {
                let sends: FuturesUnordered<_> = (0..num_workflows)
                    .map(|i| {
                        client.signal_workflow_execution(
                            format!("{wf_name}_{i}"),
                            "".to_string(),
                            SIGNAME.to_string(),
                            None,
                            None,
                        )
                    })
                    .collect();
                sends
                    .map(|_| Ok(()))
                    .forward(sink::drain())
                    .await
                    .expect("Sending signals works");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        },
        abort_reg,
    );
    let (runres, _, _) = tokio::join!(worker.inner_mut().run(), sig_sender, all_workflows_are_done);
    runres.unwrap();
}

#[tokio::test]
async fn poller_load_sustained() {
    const SIGNAME: &str = "signame";
    let num_workflows = 150;
    let wf_name = "poller_load";
    let (telemopts, addr, _aborter) =
        if std::env::var("PAR_JOBNUM").unwrap_or("1".to_string()) == "1" {
            prom_metrics(Some(
                PrometheusExporterOptionsBuilder::default()
                    .socket_addr(SocketAddr::V4("0.0.0.0:9999".parse().unwrap()))
                    .build()
                    .unwrap(),
            ))
        } else {
            prom_metrics(None)
        };
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime("poller_load", rt);
    starter
        .worker_config
        .max_cached_workflows(5000_usize)
        .max_outstanding_workflow_tasks(1000_usize)
        .workflow_task_poller_behavior(PollerBehavior::Autoscaling {
            minimum: 1,
            maximum: 200,
            initial: 5,
        })
        .no_remote_activities(true);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let sigchan = ctx.make_signal_channel(SIGNAME).map(Ok);
        let drained_fut = sigchan.forward(sink::drain());

        let real_stuff = async move {
            let rs = ctx.random_seed();
            let mut rand = rand::rngs::SmallRng::seed_from_u64(rs);
            for _ in 0..100 {
                let jitterms = rand.random_range(1000..3000);
                ctx.timer(Duration::from_millis(jitterms)).await;
            }
        };
        tokio::select! {
            _ = drained_fut => {}
            _ = real_stuff => {}
        }

        Ok(().into())
    });
    let client = starter.get_client().await;

    info!("Prom bound to {:?}", addr);

    let mut workflow_handles = vec![];
    info!("Starting workflows...");
    for i in 0..num_workflows {
        let wfid = format!("{wf_name}_{i}-{}", rand_6_chars());
        let rid = worker
            .submit_wf(
                wfid.clone(),
                wf_name.to_owned(),
                vec![],
                WorkflowOptions {
                    execution_timeout: Some(Duration::from_secs(800)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        workflow_handles.push(client.get_untyped_workflow_handle(wfid, rid));
    }
    info!("Done starting workflows");
    let start_processing = Instant::now();

    let all_workflows_are_done = async {
        stream::iter(mem::take(&mut workflow_handles))
            .for_each_concurrent(25, |handle| async move {
                let _ = handle
                    .get_workflow_result(GetWorkflowResultOpts::default())
                    .await;
            })
            .await;
        info!("Initial load ran for {:?}", start_processing.elapsed());
        info!("Done. Go look at yer metrics, must interrupt to end.");
    };

    let (runres, _) = tokio::join!(worker.inner_mut().run(), all_workflows_are_done);
    runres.unwrap();
}

// Here we want to see pollers scale up a lot to handle the spike, then scale down, ideally avoiding
// overshooting, and definitely avoiding overshooting and staying there.
#[tokio::test]
async fn poller_load_spike_then_sustained() {
    const SIGNAME: &str = "signame";
    let num_workflows = 1000;
    let wf_name = "poller_load";
    let (telemopts, addr, _aborter) =
        if std::env::var("PAR_JOBNUM").unwrap_or("1".to_string()) == "1" {
            prom_metrics(Some(
                PrometheusExporterOptionsBuilder::default()
                    .socket_addr(SocketAddr::V4("0.0.0.0:9999".parse().unwrap()))
                    .build()
                    .unwrap(),
            ))
        } else {
            prom_metrics(None)
        };
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime("poller_load", rt);
    starter
        .worker_config
        .max_cached_workflows(5000_usize)
        .max_outstanding_workflow_tasks(1000_usize)
        .workflow_task_poller_behavior(PollerBehavior::Autoscaling {
            minimum: 1,
            maximum: 200,
            initial: 5,
        })
        .activity_task_poller_behavior(PollerBehavior::Autoscaling {
            minimum: 1,
            maximum: 200,
            initial: 5,
        });
    let mut worker = starter.worker().await;
    let submitter = worker.get_submitter_handle();
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let sigchan = ctx.make_signal_channel(SIGNAME).map(Ok);
        let drained_fut = sigchan.forward(sink::drain());

        let real_stuff = async move {
            for _ in 0..5 {
                ctx.activity(ActivityOptions {
                    activity_type: "echo".to_string(),
                    start_to_close_timeout: Some(Duration::from_secs(5)),
                    input: "hi!".as_json_payload().expect("serializes fine"),
                    ..Default::default()
                })
                .await;
            }
        };
        tokio::select! {
            _ = drained_fut => {}
            _ = real_stuff => {}
        }

        Ok(().into())
    });
    worker.register_activity("echo", |_: ActContext, echo: String| async move {
        // Add some jitter to completions
        let rand_millis = rand::rng().random_range(0..500);
        tokio::time::sleep(Duration::from_millis(rand_millis)).await;
        Ok(echo)
    });
    let client = starter.get_client().await;

    info!("Prom bound to {:?}", addr);

    let mut workflow_handles = vec![];
    info!("Starting workflows...");
    for i in 0..num_workflows {
        let wfid = format!("{wf_name}_{i}-{}", rand_6_chars());
        let rid = worker
            .submit_wf(
                wfid.clone(),
                wf_name.to_owned(),
                vec![],
                WorkflowOptions {
                    execution_timeout: Some(Duration::from_secs(100)),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        workflow_handles.push(client.get_untyped_workflow_handle(wfid, rid));
    }
    info!("Done starting workflows");
    let start_processing = Instant::now();

    let (ah, abort_reg) = AbortHandle::new_pair();
    let all_workflows_are_done = async {
        stream::iter(mem::take(&mut workflow_handles))
            .for_each_concurrent(25, |handle| async move {
                let _ = handle
                    .get_workflow_result(GetWorkflowResultOpts::default())
                    .await;
            })
            .await;
        info!("Initial load ran for {:?}", start_processing.elapsed());
        ah.abort();
        info!("beginning sustained load");
        // round two
        let start_processing = Instant::now();
        for i in 0..100 {
            let wfid = format!("{wf_name}_2_{i}-{}", rand_6_chars());
            let rid = submitter
                .submit_wf(
                    wfid.clone(),
                    wf_name.to_owned(),
                    vec![],
                    WorkflowOptions {
                        execution_timeout: Some(Duration::from_secs(100)),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            workflow_handles.push(client.get_untyped_workflow_handle(wfid, rid));
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        stream::iter(workflow_handles)
            .for_each_concurrent(25, |handle| async move {
                let _ = handle
                    .get_workflow_result(GetWorkflowResultOpts::default())
                    .await;
            })
            .await;
        info!("Second load ran for {:?}", start_processing.elapsed());

        info!("Done. Go look at yer metrics, must interrupt to end.");
    };

    let sig_sender = Abortable::new(
        async {
            loop {
                let sends: FuturesUnordered<_> = (0..num_workflows)
                    .map(|i| {
                        client.signal_workflow_execution(
                            format!("{wf_name}_{i}"),
                            "".to_string(),
                            SIGNAME.to_string(),
                            None,
                            None,
                        )
                    })
                    .collect();
                sends
                    .map(|_| Ok(()))
                    .forward(sink::drain())
                    .await
                    .expect("Sending signals works");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        },
        abort_reg,
    );
    let (runres, _, _) = tokio::join!(worker.inner_mut().run(), sig_sender, all_workflows_are_done);
    runres.unwrap();
}
