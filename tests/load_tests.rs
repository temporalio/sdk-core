use assert_matches::assert_matches;
use futures::{future::join_all, sink, stream::FuturesUnordered, StreamExt};
use std::time::{Duration, Instant};
use temporal_client::{WorkflowClientTrait, WorkflowOptions};
use temporal_sdk::{ActivityOptions, WfContext};
use temporal_sdk_core_protos::coresdk::{
    activity_result::ActivityExecutionResult, activity_task::activity_task as act_task,
    workflow_commands::ActivityCancellationType, ActivityTaskCompletion, AsJsonPayloadExt,
};
use temporal_sdk_core_test_utils::CoreWfStarter;

#[tokio::test]
async fn activity_load() {
    const CONCURRENCY: usize = 1000;

    let mut starter = CoreWfStarter::new("activity_load");
    starter
        .max_wft(CONCURRENCY)
        .max_cached_workflows(CONCURRENCY)
        .max_at_polls(10)
        .max_at(CONCURRENCY);
    let mut worker = starter.worker().await;

    let activity_id = "act-1";
    let activity_timeout = Duration::from_secs(8);
    let payload_dat = b"hello".to_vec();
    let task_queue = starter.get_task_queue().to_owned();

    let pd = payload_dat.clone();
    let wf_fn = move |ctx: WfContext| {
        let task_queue = task_queue.clone();
        let payload_dat = pd.clone();

        async move {
            let activity = ActivityOptions {
                activity_id: Some(activity_id.to_string()),
                activity_type: "test_activity".to_string(),
                input: Default::default(),
                task_queue,
                schedule_to_start_timeout: Some(activity_timeout),
                start_to_close_timeout: Some(activity_timeout),
                schedule_to_close_timeout: Some(activity_timeout),
                heartbeat_timeout: Some(activity_timeout),
                cancellation_type: ActivityCancellationType::TryCancel,
            };
            let res = ctx.activity(activity).await.unwrap_ok_payload();
            assert_eq!(res.data, payload_dat);
            Ok(().into())
        }
    };

    let starting = Instant::now();
    let wf_type = "activity_load";
    worker.register_wf(wf_type.to_owned(), wf_fn);
    join_all((0..CONCURRENCY).map(|i| {
        let worker = &worker;
        let wf_id = format!("activity_load_{}", i);
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
    let core = starter.get_worker().await;

    // Poll for and complete all activities
    let c2 = core.clone();
    let all_acts = async move {
        let mut act_complete_futs = vec![];
        for _ in 0..CONCURRENCY {
            let task = c2.poll_activity_task().await.unwrap();
            assert_matches!(
                task.variant,
                Some(act_task::Variant::Start(ref start_activity)) => {
                    assert_eq!(start_activity.activity_type, "test_activity")
                }
            );
            let pd = payload_dat.clone();
            let core = c2.clone();
            act_complete_futs.push(tokio::spawn(async move {
                core.complete_activity_task(ActivityTaskCompletion {
                    task_token: task.task_token,
                    result: Some(ActivityExecutionResult::ok(pd.into())),
                })
                .await
                .unwrap()
            }));
        }
        join_all(act_complete_futs)
            .await
            .into_iter()
            .for_each(|h| h.unwrap());
    };
    tokio::join! {
        async {
            worker.run_until_done().await.unwrap();
        },
        all_acts
    };
    dbg!(running.elapsed());
}

#[tokio::test]
async fn workflow_load() {
    const SIGNAME: &str = "signame";
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
        |echo_me: String| async move { Ok(echo_me) },
    );
    for i in 0..200 {
        worker
            .submit_wf(
                format!("{}_{}", wf_name, i),
                wf_name.to_owned(),
                vec![],
                WorkflowOptions::default(),
            )
            .await
            .unwrap();
    }

    let client = starter.get_client().await;
    let sig_sender = async {
        for _ in 1..=90 {
            let sends: FuturesUnordered<_> = (0..200)
                .map(|i| {
                    client.signal_workflow_execution(
                        format!("{}_{}", wf_name, i),
                        "".to_string(),
                        SIGNAME.to_string(),
                        None,
                    )
                })
                .collect();
            sends.map(|_| Ok(())).forward(sink::drain()).await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    };

    worker.incr_expected_run_count(200);
    let run_fut = worker.run_until_done();
    let (r1, _) = tokio::join!(run_fut, sig_sender);
    r1.unwrap();
}
