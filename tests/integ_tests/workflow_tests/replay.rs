use assert_matches::assert_matches;
use std::time::Duration;
use temporal_sdk::{WfContext, Worker, WorkflowFunction};
use temporal_sdk_core::telemetry_init;
use temporal_sdk_core_api::errors::{PollActivityError, PollWfError};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::remove_from_cache::EvictionReason,
        workflow_commands::{ScheduleActivity, StartTimer},
        workflow_completion::WorkflowActivationCompletion,
    },
    DEFAULT_WORKFLOW_TYPE,
};
use temporal_sdk_core_test_utils::{
    canned_histories, get_integ_telem_options, history_from_proto_binary,
    init_core_replay_preloaded, WorkerTestHelpers,
};
use tokio::join;

#[tokio::test]
async fn timer_workflow_replay() {
    let (core, _) = init_core_replay_preloaded(
        "timer_workflow_replay",
        &history_from_proto_binary("histories/timer_workflow_history.bin")
            .await
            .unwrap(),
    );
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![StartTimer {
            seq: 0,
            start_to_fire_timeout: Some(prost_dur!(from_secs(1))),
        }
        .into()],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    // Verify that an in-progress poll is interrupted by completion finishing processing history
    let act_poll_fut = async {
        assert_matches!(
            core.poll_activity_task().await,
            Err(PollActivityError::ShutDown)
        );
    };
    let poll_fut = async {
        assert_matches!(
            core.poll_workflow_activation().await,
            Err(PollWfError::ShutDown)
        );
    };
    let complete_fut = async {
        core.complete_execution(&task.run_id).await;
    };
    join!(act_poll_fut, poll_fut, complete_fut);

    // Subsequent polls should still return shutdown
    assert_matches!(
        core.poll_workflow_activation().await,
        Err(PollWfError::ShutDown)
    );

    core.shutdown().await;
}

#[tokio::test]
async fn workflow_nondeterministic_replay() {
    let (core, _) = init_core_replay_preloaded(
        "timer_workflow_replay",
        &history_from_proto_binary("histories/timer_workflow_history.bin")
            .await
            .unwrap(),
    );
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![ScheduleActivity {
            seq: 0,
            activity_id: "0".to_string(),
            activity_type: "fake_act".to_string(),
            ..Default::default()
        }
        .into()],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    assert_eq!(task.eviction_reason(), Some(EvictionReason::Nondeterminism));
    // Complete eviction
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    // Call shutdown explicitly because we saw a nondeterminism eviction
    core.shutdown().await;
    assert_matches!(
        core.poll_workflow_activation().await,
        Err(PollWfError::ShutDown)
    );
}

#[tokio::test]
async fn replay_using_wf_function() {
    telemetry_init(&get_integ_telem_options()).unwrap();
    let num_timers = 10;
    let t = canned_histories::long_sequential_timers(num_timers as usize);
    let func = timers_wf(num_timers);
    let (worker, _) =
        init_core_replay_preloaded("replay_bench", &t.get_full_history_info().unwrap().into());
    let mut worker = Worker::new_from_core(worker, "replay_bench".to_string());
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, func);
    worker.run().await.unwrap();
}

#[tokio::test]
async fn replay_ok_ending_with_terminated_or_timed_out() {
    let mut t1 = canned_histories::single_timer("1");
    t1.add_workflow_execution_terminated();
    let mut t2 = canned_histories::single_timer("1");
    t2.add_workflow_execution_timed_out();
    telemetry_init(&get_integ_telem_options()).unwrap();
    for t in [t1, t2] {
        let func = timers_wf(1);
        let (worker, _) = init_core_replay_preloaded(
            "replay_ok_terminate",
            &t.get_full_history_info().unwrap().into(),
        );
        let mut worker = Worker::new_from_core(worker, "replay_ok_terminate".to_string());
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, func);
        worker.run().await.unwrap();
    }
}

fn timers_wf(num_timers: u32) -> WorkflowFunction {
    WorkflowFunction::new(move |ctx: WfContext| async move {
        for _ in 1..=num_timers {
            ctx.timer(Duration::from_secs(1)).await;
        }
        Ok(().into())
    })
}
