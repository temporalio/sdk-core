use crate::integ_tests::CoreWfStarter;
use assert_matches::assert_matches;
use std::time::{Duration, Instant};
use temporal_sdk_core::test_workflow_driver::{CommandSender, TestWorkflowDriver};
use temporal_sdk_core::{
    protos::coresdk::{
        activity_result::{self, activity_result as act_res, ActivityResult},
        activity_task::activity_task as act_task,
        common::{Payload, UserCodeFailure},
        workflow_activation::{wf_activation_job, FireTimer, ResolveActivity, WfActivationJob},
        workflow_commands::{
            ActivityCancellationType, CompleteWorkflowExecution, RequestCancelActivity,
            ScheduleActivity, StartTimer,
        },
        workflow_completion::WfActivationCompletion,
        ActivityTaskCompletion,
    },
    IntoCompletion,
};

#[tokio::test(flavor = "multi_thread")]
async fn activity_load() {
    let mut starter = CoreWfStarter::new("activity_load");
    starter.max_wft(1000).max_at(1000);
    let mut worker = starter.worker().await;

    let activity_id = "act-1";
    let activity_timeout = Duration::from_secs(5);
    let payload_dat = b"hello".to_vec();

    let starting = Instant::now();
    for i in 0..1000 {
        let twd = TestWorkflowDriver::new(|mut command_sink: CommandSender| {
            let task_queue = starter.get_task_queue().to_owned();
            let payload_dat = payload_dat.clone();
            async move {
                let activity = ScheduleActivity {
                    activity_id: activity_id.to_string(),
                    activity_type: "test_activity".to_string(),
                    task_queue: task_queue.clone(),
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
        });
        worker
            .submit_wf(format!("activity_load_{}", i), twd)
            .await
            .unwrap();
    }
    dbg!(starting.elapsed());

    let running = Instant::now();
    let core = starter.get_core().await;
    let mut handles = vec![];
    for _ in 0..1000 {
        let core = core.clone();
        let payload_dat = payload_dat.clone();
        // Poll for and complete all activities
        let h = tokio::spawn(async move {
            let task = core.poll_activity_task().await.unwrap();
            assert_matches!(
                task.variant,
                Some(act_task::Variant::Start(start_activity)) => {
                    assert_eq!(start_activity.activity_type, "test_activity".to_string())
                }
            );
            core.complete_activity_task(ActivityTaskCompletion {
                task_token: task.task_token,
                result: Some(ActivityResult::ok(payload_dat.into())),
            })
            .await
            .unwrap();
        });
        handles.push(h);
    }
    tokio::join! {
        async {
            worker.run_until_done().await.unwrap();
        },
        futures::future::join_all(handles)
    };
    dbg!(running.elapsed());
}
