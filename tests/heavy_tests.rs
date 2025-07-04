use futures_util::{
    StreamExt,
    future::{AbortHandle, Abortable, join_all},
    sink, stream,
    stream::FuturesUnordered,
};
use rand::Rng;
use std::{
    mem,
    sync::Arc,
    time::{Duration, Instant},
};
use temporal_client::{GetWorkflowResultOpts, WfClientExt, WorkflowClientTrait, WorkflowOptions};
use temporal_sdk::{ActContext, ActivityOptions, WfContext, WorkflowResult};
use temporal_sdk_core::{CoreRuntime, ResourceBasedTuner, ResourceSlotOptions};
use temporal_sdk_core_api::worker::PollerBehavior;
use temporal_sdk_core_protos::{
    coresdk::{AsJsonPayloadExt, workflow_commands::ActivityCancellationType},
    temporal::api::enums::v1::WorkflowIdReusePolicy,
};
use temporal_sdk_core_test_utils::{
    CoreWfStarter, init_integ_telem, prom_metrics, rand_6_chars, workflows::la_problem_workflow,
};

mod fuzzy_workflow;

#[tokio::test]
async fn activity_load() {
    const CONCURRENCY: usize = 512;

    let mut starter = CoreWfStarter::new("activity_load");
    starter
        .worker_config
        .max_outstanding_workflow_tasks(CONCURRENCY)
        .max_cached_workflows(CONCURRENCY)
        .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(10_usize))
        .max_outstanding_activities(CONCURRENCY);
    let mut worker = starter.worker().await;

    let activity_id = "act-1";
    let activity_timeout = Duration::from_secs(8);
    let task_queue = Some(starter.get_task_queue().to_owned());

    let wf_fn = move |ctx: WfContext| {
        let task_queue = task_queue.clone();
        let payload = "yo".as_json_payload().unwrap();
        async move {
            let activity = ActivityOptions {
                activity_id: Some(activity_id.to_string()),
                activity_type: "test_activity".to_string(),
                input: payload.clone(),
                task_queue,
                schedule_to_start_timeout: Some(activity_timeout),
                start_to_close_timeout: Some(activity_timeout),
                schedule_to_close_timeout: Some(activity_timeout),
                heartbeat_timeout: Some(activity_timeout),
                cancellation_type: ActivityCancellationType::TryCancel,
                ..Default::default()
            };
            let res = ctx.activity(activity).await.unwrap_ok_payload();
            assert_eq!(res.data, payload.data);
            Ok(().into())
        }
    };

    let starting = Instant::now();
    let wf_type = "activity_load";
    worker.register_wf(wf_type.to_owned(), wf_fn);
    worker.register_activity(
        "test_activity",
        |_ctx: ActContext, echo: String| async move { Ok(echo) },
    );
    join_all((0..CONCURRENCY).map(|i| {
        let worker = &worker;
        let wf_id = format!("activity_load_{i}");
        async move {
            worker
                .submit_wf(
                    wf_id,
                    wf_type.to_owned(),
                    vec![],
                    WorkflowOptions::default(),
                )
                .await
                .unwrap();
        }
    }))
    .await;
    dbg!(starting.elapsed());

    let running = Instant::now();

    worker.run_until_done().await.unwrap();
    dbg!(running.elapsed());
}

#[tokio::test]
async fn chunky_activities_resource_based() {
    const WORKFLOWS: usize = 100;

    let mut starter = CoreWfStarter::new("chunky_activities_resource_based");
    starter
        .worker_config
        .clear_max_outstanding_opts()
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(10_usize))
        .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(10_usize));
    let mut tuner = ResourceBasedTuner::new(0.7, 0.7);
    tuner
        .with_workflow_slots_options(ResourceSlotOptions::new(
            25,
            WORKFLOWS,
            Duration::from_millis(0),
        ))
        .with_activity_slots_options(ResourceSlotOptions::new(5, 1000, Duration::from_millis(50)));
    starter.worker_config.tuner(Arc::new(tuner));
    let mut worker = starter.worker().await;

    let activity_id = "act-1";
    let activity_timeout = Duration::from_secs(30);

    let wf_fn = move |ctx: WfContext| {
        let payload = "yo".as_json_payload().unwrap();
        async move {
            let activity = ActivityOptions {
                activity_id: Some(activity_id.to_string()),
                activity_type: "test_activity".to_string(),
                input: payload.clone(),
                start_to_close_timeout: Some(activity_timeout),
                ..Default::default()
            };
            let res = ctx.activity(activity).await.unwrap_ok_payload();
            assert_eq!(res.data, payload.data);
            Ok(().into())
        }
    };

    let starting = Instant::now();
    let wf_type = "chunky_activity_wf";
    worker.register_wf(wf_type.to_owned(), wf_fn);
    worker.register_activity(
        "test_activity",
        |_ctx: ActContext, echo: String| async move {
            tokio::task::spawn_blocking(move || {
                // Allocate a gig and then do some CPU stuff on it
                let mut mem = vec![0_u8; 1000 * 1024 * 1024];
                for _ in 1..10 {
                    for i in 0..mem.len() {
                        mem[i] &= mem[mem.len() - 1 - i]
                    }
                }
                Ok(echo)
            })
            .await?
        },
    );
    join_all((0..WORKFLOWS).map(|i| {
        let worker = &worker;
        let wf_id = format!("chunk_activity_{i}");
        async move {
            worker
                .submit_wf(
                    wf_id,
                    wf_type.to_owned(),
                    vec![],
                    WorkflowOptions {
                        id_reuse_policy: WorkflowIdReusePolicy::TerminateIfRunning,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
        }
    }))
    .await;
    dbg!(starting.elapsed());

    let running = Instant::now();

    worker.run_until_done().await.unwrap();
    dbg!(running.elapsed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_load() {
    const SIGNAME: &str = "signame";
    let num_workflows = 500;
    let wf_name = "workflow_load";
    let (mut telemopts, _, _aborter) = prom_metrics(None);
    // Avoid initting two logging systems, since when this test is run with others it can
    // cause us to encounter the tracing span drop bug
    telemopts.logging = None;
    init_integ_telem();
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime("workflow_load", rt);
    starter
        .worker_config
        .max_outstanding_workflow_tasks(5_usize)
        .max_cached_workflows(200_usize)
        .activity_task_poller_behavior(PollerBehavior::SimpleMaximum(10_usize))
        .max_outstanding_activities(100_usize);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let sigchan = ctx.make_signal_channel(SIGNAME).map(Ok);
        let drained_fut = sigchan.forward(sink::drain());

        let real_stuff = async move {
            for _ in 0..5 {
                ctx.activity(ActivityOptions {
                    activity_type: "echo_activity".to_string(),
                    start_to_close_timeout: Some(Duration::from_secs(5)),
                    input: "hi!".as_json_payload().expect("serializes fine"),
                    ..Default::default()
                })
                .await;
                ctx.timer(Duration::from_secs(1)).await;
            }
        };
        tokio::select! {
            _ = drained_fut => {}
            _ = real_stuff => {}
        }

        Ok(().into())
    });
    worker.register_activity(
        "echo_activity",
        |_ctx: ActContext, echo_me: String| async move { Ok(echo_me) },
    );
    let client = starter.get_client().await;

    let mut workflow_handles = vec![];
    for i in 0..num_workflows {
        let wfid = format!("{wf_name}_{i}");
        let rid = worker
            .submit_wf(
                wfid.clone(),
                wf_name.to_owned(),
                vec![],
                WorkflowOptions::default(),
            )
            .await
            .unwrap();
        workflow_handles.push(client.get_untyped_workflow_handle(wfid, rid));
    }

    let sig_sender = async {
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
    };
    tokio::select! { r1 = worker.run_until_done() => {r1.unwrap()}, _ = sig_sender => {}}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn evict_while_la_running_no_interference() {
    let wf_name = "evict_while_la_running_no_interference";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .worker_config
        .max_outstanding_local_activities(20_usize)
        .max_cached_workflows(20_usize);
    // Though it doesn't make sense to set wft higher than cached workflows, leaving this commented
    // introduces more instability that can be useful in the test.
    // starter.max_wft(20);
    let mut worker = starter.worker().await;

    worker.register_wf(wf_name.to_owned(), la_problem_workflow);
    worker.register_activity("delay", |_: ActContext, _: String| async {
        tokio::time::sleep(Duration::from_secs(15)).await;
        Ok(())
    });

    let client = starter.get_client().await;
    let subfs = FuturesUnordered::new();
    for i in 1..100 {
        let wf_id = format!("{wf_name}-{i}");
        let run_id = worker
            .submit_wf(
                &wf_id,
                wf_name.to_owned(),
                vec![],
                WorkflowOptions::default(),
            )
            .await
            .unwrap();
        let cw = worker.core_worker.clone();
        let client = client.clone();
        subfs.push(async move {
            // Evict the workflow
            tokio::time::sleep(Duration::from_secs(1)).await;
            cw.request_workflow_eviction(&run_id);
            // Wake up workflow by sending signal
            client
                .signal_workflow_execution(
                    wf_id,
                    run_id.clone(),
                    "whaatever".to_string(),
                    None,
                    None,
                )
                .await
                .unwrap();
        });
    }
    let runf = async {
        worker.run_until_done().await.unwrap();
    };
    tokio::join!(subfs.collect::<Vec<_>>(), runf);
}

pub async fn many_parallel_timers_longhist(ctx: WfContext) -> WorkflowResult<()> {
    for _ in 0..120 {
        let mut futs = vec![];
        for _ in 0..100 {
            futs.push(ctx.timer(Duration::from_millis(100)));
        }
        join_all(futs).await;
    }
    Ok(().into())
}

#[tokio::test]
async fn can_paginate_long_history() {
    let wf_name = "can_paginate_long_history";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .worker_config
        .no_remote_activities(true)
        // Do not use sticky queues so we are forced to paginate once history gets long
        .max_cached_workflows(0_usize);

    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), many_parallel_timers_longhist);
    let run_id = worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions::default(),
        )
        .await
        .unwrap();
    let client = starter.get_client().await;
    tokio::spawn(async move {
        loop {
            for _ in 0..10 {
                client
                    .signal_workflow_execution(
                        wf_name.to_owned(),
                        run_id.clone(),
                        "sig".to_string(),
                        None,
                        None,
                    )
                    .await
                    .unwrap();
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    });
    worker.run_until_done().await.unwrap();
}

#[tokio::test]
async fn poller_autoscaling_basic_loadtest() {
    const SIGNAME: &str = "signame";
    let num_workflows = 100;
    let wf_name = "poller_load";
    let mut starter = CoreWfStarter::new("poller_load");
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
    let shutdown_handle = worker.inner_mut().shutdown_handle();
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

    let mut workflow_handles = vec![];
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

    let (ah, abort_reg) = AbortHandle::new_pair();
    let all_workflows_are_done = async {
        stream::iter(mem::take(&mut workflow_handles))
            .for_each_concurrent(25, |handle| async move {
                let _ = handle
                    .get_workflow_result(GetWorkflowResultOpts::default())
                    .await;
            })
            .await;
        ah.abort();
        shutdown_handle();
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
