use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};
use temporal_sdk::{ActContext, ActivityOptions, WfContext, WorkflowResult};
use temporal_sdk_core_protos::coresdk::AsJsonPayloadExt;
use temporal_sdk_core_test_utils::{CoreWfStarter, WorkflowHandleExt};

static RUN_CT: AtomicUsize = AtomicUsize::new(1);

pub(crate) async fn timer_wf_nondeterministic(ctx: WfContext) -> WorkflowResult<()> {
    let run_ct = RUN_CT.fetch_add(1, Ordering::Relaxed);

    match run_ct {
        1 | 3 => {
            // If we have not run yet or are on the third attempt, schedule a timer
            ctx.timer(Duration::from_secs(1)).await;
            if run_ct == 1 {
                // on first attempt we need to blow up after the timer fires so we will replay
                panic!("dying on purpose");
            }
        }
        2 => {
            // On the second attempt we should cause a nondeterminism error
            ctx.activity(ActivityOptions {
                activity_type: "whatever".to_string(),
                ..Default::default()
            })
            .await;
        }
        _ => panic!("Ran too many times"),
    }
    Ok(().into())
}

#[tokio::test]
async fn test_determinism_error_then_recovers() {
    let wf_name = "test_determinism_error_then_recovers";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;

    worker.register_wf(wf_name.to_owned(), timer_wf_nondeterministic);
    starter.start_with_worker(wf_name, &mut worker).await;
    worker.run_until_done().await.unwrap();
    // 4 because we still add on the 3rd and final attempt
    assert_eq!(RUN_CT.load(Ordering::Relaxed), 4);
}

#[tokio::test]
async fn task_fail_causes_replay_unset_too_soon() {
    let wf_name = "task_fail_causes_replay_unset_too_soon";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;

    static DID_FAIL: AtomicBool = AtomicBool::new(false);
    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| async move {
        if DID_FAIL.load(Ordering::Relaxed) {
            assert!(ctx.is_replaying());
        }
        ctx.activity(ActivityOptions {
            activity_type: "echo".to_string(),
            input: "hi!".as_json_payload().expect("serializes fine"),
            start_to_close_timeout: Some(Duration::from_secs(2)),
            ..Default::default()
        })
        .await;
        if !DID_FAIL.load(Ordering::Relaxed) {
            DID_FAIL.store(true, Ordering::Relaxed);
            panic!("Die on purpose");
        }
        Ok(().into())
    });
    worker.register_activity("echo", |_ctx: ActContext, echo_me: String| async move {
        Ok(echo_me)
    });

    let handle = starter.start_with_worker(wf_name, &mut worker).await;

    worker.run_until_done().await.unwrap();
    handle
        .fetch_history_and_replay(worker.inner_mut())
        .await
        .unwrap();
}
