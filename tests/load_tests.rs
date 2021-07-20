use assert_matches::assert_matches;
use futures::future::join_all;
use std::time::{Duration, Instant};
use temporal_sdk_core::{
    protos::coresdk::{
        activity_result::ActivityResult,
        activity_task::activity_task as act_task,
        workflow_commands::{ActivityCancellationType, ScheduleActivity},
        ActivityTaskCompletion,
    },
    test_workflow_driver::WfContext,
    tracing_init,
};
use test_utils::CoreWfStarter;

const CONCURRENCY: usize = 1000;

#[tokio::test(flavor = "multi_thread")]
async fn activity_load() {
    tracing_init();

    let mut starter = CoreWfStarter::new("activity_load");
    starter
        .max_wft(CONCURRENCY)
        .max_cached_workflows(CONCURRENCY)
        .max_at_polls(10)
        .max_at(CONCURRENCY);
    let worker = starter.worker().await;
    let task_q = &starter.get_task_queue().to_owned();

    let activity_id = "act-1";
    let activity_timeout = Duration::from_secs(8);
    let payload_dat = b"hello".to_vec();
    let task_queue = starter.get_task_queue().to_owned();

    let pd = payload_dat.clone();
    let wf_fn = move |mut command_sink: WfContext| {
        let task_queue = task_queue.clone();
        let payload_dat = pd.clone();

        async move {
            let activity = ScheduleActivity {
                activity_id: activity_id.to_string(),
                activity_type: "test_activity".to_string(),
                task_queue,
                schedule_to_start_timeout: Some(activity_timeout.into()),
                start_to_close_timeout: Some(activity_timeout.into()),
                schedule_to_close_timeout: Some(activity_timeout.into()),
                heartbeat_timeout: Some(activity_timeout.into()),
                cancellation_type: ActivityCancellationType::TryCancel as i32,
                ..Default::default()
            };
            let res = command_sink
                .activity(activity)
                .await
                .unwrap()
                .unwrap_ok_payload();
            assert_eq!(res.data, payload_dat);
            command_sink.complete_workflow_execution();
        }
    };

    let starting = Instant::now();
    join_all((0..CONCURRENCY).map(|i| {
        let worker = &worker;
        let wf_fn = wf_fn.clone();
        async move {
            worker
                .submit_wf(vec![], format!("activity_load_{}", i), wf_fn)
                .await
                .unwrap();
        }
    }))
    .await;
    dbg!(starting.elapsed());

    let running = Instant::now();
    let core = starter.get_core().await;

    // Poll for and complete all activities
    let c2 = core.clone();
    let all_acts = async move {
        let mut act_complete_futs = vec![];
        for _ in 0..CONCURRENCY {
            let task = c2.poll_activity_task(task_q).await.unwrap();
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
                    result: Some(ActivityResult::ok(pd.into())),
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
    core.shutdown().await;
}
