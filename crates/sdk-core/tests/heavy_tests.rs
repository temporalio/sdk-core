// All non-main.rs tests ignore dead common code so that the linter doesn't complain about about it.
#[allow(dead_code)]
pub(crate) mod common;
// This prevents cargo from auto-picking up this file as its own test binary. Odd but functional.
#[path = "heavy_tests/fuzzy_workflow.rs"]
mod fuzzy_workflow;

use crate::common::get_integ_runtime_options;
use common::{
    CoreWfStarter, activity_functions::StdActivities, init_integ_telem, prom_metrics, rand_6_chars,
    workflows::la_problem_workflow,
};
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
use temporalio_client::{
    GetWorkflowResultOptions, WfClientExt, WorkflowClientTrait, WorkflowOptions,
};
use temporalio_macros::activities;

use temporalio_common::{
    protos::{
        coresdk::{AsJsonPayloadExt, workflow_commands::ActivityCancellationType},
        temporal::api::enums::v1::WorkflowIdReusePolicy,
    },
    worker::WorkerTaskTypes,
};
use temporalio_sdk::{
    ActivityOptions, WfContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
};
use temporalio_sdk_core::{
    CoreRuntime, PollerBehavior, ResourceBasedTuner, ResourceSlotOptions, TunerHolder,
};

#[tokio::test]
async fn activity_load() {
    const CONCURRENCY: usize = 512;

    let mut starter = CoreWfStarter::new("activity_load");
    starter.sdk_config.max_cached_workflows = CONCURRENCY;
    starter.sdk_config.activity_task_poller_behavior = PollerBehavior::SimpleMaximum(10);
    starter.sdk_config.tuner =
        Arc::new(TunerHolder::fixed_size(CONCURRENCY, CONCURRENCY, 100, 100));
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;

    let activity_id = "act-1";
    let activity_timeout = Duration::from_secs(8);
    let task_queue = Some(starter.get_task_queue().to_owned());

    let wf_fn = move |ctx: WfContext| {
        let task_queue = task_queue.clone();
        let input_str = "yo".to_string();
        async move {
            let res = ctx
                .start_activity(
                    StdActivities::echo,
                    input_str.clone(),
                    ActivityOptions {
                        activity_id: Some(activity_id.to_string()),
                        task_queue,
                        schedule_to_start_timeout: Some(activity_timeout),
                        start_to_close_timeout: Some(activity_timeout),
                        schedule_to_close_timeout: Some(activity_timeout),
                        heartbeat_timeout: Some(activity_timeout),
                        cancellation_type: ActivityCancellationType::TryCancel,
                        ..Default::default()
                    },
                )
                .unwrap()
                .await
                .unwrap_ok_payload();
            let payload = input_str.as_json_payload().unwrap();
            assert_eq!(res.data, payload.data);
            Ok(().into())
        }
    };

    let starting = Instant::now();
    let wf_type = "activity_load";
    worker.register_wf(wf_type.to_owned(), wf_fn);
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
    starter.sdk_config.workflow_task_poller_behavior = PollerBehavior::SimpleMaximum(10_usize);
    starter.sdk_config.activity_task_poller_behavior = PollerBehavior::SimpleMaximum(10_usize);
    let mut tuner = ResourceBasedTuner::new(0.7, 0.7);
    tuner
        .with_workflow_slots_options(ResourceSlotOptions::new(
            25,
            WORKFLOWS,
            Duration::from_millis(0),
        ))
        .with_activity_slots_options(ResourceSlotOptions::new(5, 1000, Duration::from_millis(50)));
    starter.sdk_config.tuner = Arc::new(tuner);

    struct ChunkyActivities;
    #[activities]
    impl ChunkyActivities {
        #[activity]
        async fn chunky_echo(_ctx: ActivityContext, echo: String) -> Result<String, ActivityError> {
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
        }
    }

    starter.sdk_config.register_activities(ChunkyActivities);
    let mut worker = starter.worker().await;

    let activity_id = "act-1";
    let activity_timeout = Duration::from_secs(30);

    let wf_fn = move |ctx: WfContext| {
        let input_str = "yo".to_string();
        async move {
            let res = ctx
                .start_activity(
                    ChunkyActivities::chunky_echo,
                    input_str.clone(),
                    ActivityOptions {
                        activity_id: Some(activity_id.to_string()),
                        start_to_close_timeout: Some(activity_timeout),
                        ..Default::default()
                    },
                )
                .unwrap()
                .await
                .unwrap_ok_payload();
            let payload = input_str.as_json_payload().unwrap();
            assert_eq!(res.data, payload.data);
            Ok(().into())
        }
    };

    let starting = Instant::now();
    let wf_type = "chunky_activity_wf";
    worker.register_wf(wf_type.to_owned(), wf_fn);
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
    let rt = CoreRuntime::new_assume_tokio(get_integ_runtime_options(telemopts)).unwrap();
    let mut starter = CoreWfStarter::new_with_runtime("workflow_load", rt);
    starter.sdk_config.max_cached_workflows = 200;
    starter.sdk_config.activity_task_poller_behavior = PollerBehavior::SimpleMaximum(10);
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(5, 100, 100, 100));
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let sigchan = ctx.make_signal_channel(SIGNAME).map(Ok);
        let drained_fut = sigchan.forward(sink::drain());

        let real_stuff = async move {
            for _ in 0..5 {
                ctx.start_activity(
                    StdActivities::echo,
                    "hi!".to_string(),
                    ActivityOptions {
                        start_to_close_timeout: Some(Duration::from_secs(5)),
                        ..Default::default()
                    },
                )
                .unwrap()
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
    starter.sdk_config.max_cached_workflows = 20;
    // Though it doesn't make sense to set wft higher than cached workflows, leaving this commented
    // introduces more instability that can be useful in the test.
    // starter.max_wft(20);
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(100, 10, 20, 1));
    starter.sdk_config.register_activities(StdActivities);
    let mut worker = starter.worker().await;

    worker.register_wf(wf_name.to_owned(), la_problem_workflow);

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
        let cw = worker.core_worker();
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
    starter.sdk_config.task_types = WorkerTaskTypes::workflow_only();
    // Do not use sticky queues so we are forced to paginate once history gets long
    starter.sdk_config.max_cached_workflows = 0;

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
    starter.sdk_config.max_cached_workflows = 5000;
    starter.sdk_config.tuner = Arc::new(TunerHolder::fixed_size(1000, 1000, 100, 1));
    starter.sdk_config.workflow_task_poller_behavior = PollerBehavior::Autoscaling {
        minimum: 1,
        maximum: 200,
        initial: 5,
    };
    starter.sdk_config.activity_task_poller_behavior = PollerBehavior::Autoscaling {
        minimum: 1,
        maximum: 200,
        initial: 5,
    };

    struct JitteryActivities;
    #[activities]
    impl JitteryActivities {
        #[activity]
        async fn jittery_echo(
            _ctx: ActivityContext,
            echo: String,
        ) -> Result<String, ActivityError> {
            // Add some jitter to completions
            let rand_millis = rand::rng().random_range(0..500);
            tokio::time::sleep(Duration::from_millis(rand_millis)).await;
            Ok(echo)
        }
    }

    starter.sdk_config.register_activities(JitteryActivities);
    let mut worker = starter.worker().await;
    let shutdown_handle = worker.inner_mut().shutdown_handle();
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let sigchan = ctx.make_signal_channel(SIGNAME).map(Ok);
        let drained_fut = sigchan.forward(sink::drain());

        let real_stuff = async move {
            for _ in 0..5 {
                ctx.start_activity(
                    JitteryActivities::jittery_echo,
                    "hi!".to_string(),
                    ActivityOptions {
                        start_to_close_timeout: Some(Duration::from_secs(5)),
                        ..Default::default()
                    },
                )
                .unwrap()
                .await;
            }
        };
        tokio::select! {
            _ = drained_fut => {}
            _ = real_stuff => {}
        }

        Ok(().into())
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
                    .get_workflow_result(GetWorkflowResultOptions::default())
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
