use futures::{future::join_all, sink, stream::FuturesUnordered, StreamExt};
use std::time::{Duration, Instant};
use temporal_client::{WfClientExt, WorkflowClientTrait, WorkflowOptions};
use temporal_sdk::{ActContext, ActivityOptions, WfContext, WorkflowResult};
use temporal_sdk_core_protos::coresdk::{
    workflow_commands::ActivityCancellationType, AsJsonPayloadExt,
};
use temporal_sdk_core_test_utils::{workflows::la_problem_workflow, CoreWfStarter};

mod fuzzy_workflow;

#[tokio::test]
async fn activity_load() {
    const CONCURRENCY: usize = 512;

    let mut starter = CoreWfStarter::new("activity_load");
    starter
        .max_wft(CONCURRENCY)
        .max_cached_workflows(CONCURRENCY)
        .max_at_polls(10)
        .max_at(CONCURRENCY);
    let mut worker = starter.worker().await;

    let activity_id = "act-1";
    let activity_timeout = Duration::from_secs(8);
    let task_queue = starter.get_task_queue().to_owned();

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_load() {
    const SIGNAME: &str = "signame";
    let num_workflows = 200;
    let wf_name = "workflow_load";
    let mut starter = CoreWfStarter::new("workflow_load");
    starter
        .max_wft(5)
        .max_cached_workflows(5)
        .max_at_polls(10)
        .max_at(100);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        let sigchan = ctx.make_signal_channel(SIGNAME).map(Ok);
        let drained_fut = sigchan.forward(sink::drain());

        let real_stuff = async move {
            for _ in 0..20 {
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
    starter.max_local_at(20);
    starter.max_cached_workflows(20);
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
    starter.no_remote_activities();
    // Do not use sticky queues so we are forced to paginate once history gets long
    starter.max_cached_workflows(0);

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
