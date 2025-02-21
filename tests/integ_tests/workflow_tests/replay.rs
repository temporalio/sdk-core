use crate::integ_tests::workflow_tests::patches::changes_wf;
use assert_matches::assert_matches;
use parking_lot::Mutex;
use std::{collections::HashSet, sync::Arc, time::Duration};
use temporal_sdk::{WfContext, Worker, WorkflowFunction, interceptors::WorkerInterceptor};
use temporal_sdk_core::replay::{HistoryFeeder, HistoryForReplay};
use temporal_sdk_core_api::errors::PollError;
use temporal_sdk_core_protos::{
    DEFAULT_WORKFLOW_TYPE, TestHistoryBuilder,
    coresdk::{
        workflow_activation::remove_from_cache::EvictionReason,
        workflow_commands::{ScheduleActivity, StartTimer},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::enums::v1::EventType,
};
use temporal_sdk_core_test_utils::{
    WorkerTestHelpers, canned_histories, history_from_proto_binary, init_core_replay_preloaded,
    replay_sdk_worker, replay_sdk_worker_stream,
};
use tokio::join;

fn test_hist_to_replay(t: TestHistoryBuilder) -> HistoryForReplay {
    let hi = t.get_full_history_info().unwrap().into();
    HistoryForReplay::new(hi, "fake".to_string())
}

#[tokio::test]
async fn timer_workflow_replay() {
    let core = init_core_replay_preloaded(
        "timer_workflow_replay",
        [HistoryForReplay::new(
            history_from_proto_binary("histories/timer_workflow_history.bin")
                .await
                .unwrap(),
            "fake".to_owned(),
        )],
    );
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            StartTimer {
                seq: 0,
                start_to_fire_timeout: Some(prost_dur!(from_secs(1))),
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    // Verify that an in-progress poll is interrupted by completion finishing processing history
    let act_poll_fut = async {
        assert_matches!(core.poll_activity_task().await, Err(PollError::ShutDown));
    };
    let poll_fut = async {
        let evict_task = core
            .poll_workflow_activation()
            .await
            .expect("Should be an eviction activation");
        assert!(evict_task.eviction_reason().is_some());
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(evict_task.run_id))
            .await
            .unwrap();
        assert_matches!(
            core.poll_workflow_activation().await,
            Err(PollError::ShutDown)
        );
    };
    let complete_fut = async {
        core.complete_execution(&task.run_id).await;
    };
    join!(act_poll_fut, poll_fut, complete_fut);

    // Subsequent polls should still return shutdown
    assert_matches!(
        core.poll_workflow_activation().await,
        Err(PollError::ShutDown)
    );

    core.shutdown().await;
}

#[tokio::test]
async fn workflow_nondeterministic_replay() {
    let core = init_core_replay_preloaded(
        "timer_workflow_replay",
        [HistoryForReplay::new(
            history_from_proto_binary("histories/timer_workflow_history.bin")
                .await
                .unwrap(),
            "fake".to_owned(),
        )],
    );
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            ScheduleActivity {
                seq: 0,
                activity_id: "0".to_string(),
                activity_type: "fake_act".to_string(),
                ..Default::default()
            }
            .into(),
        ],
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
        Err(PollError::ShutDown)
    );
}

#[tokio::test]
async fn replay_using_wf_function() {
    let num_timers = 10;
    let t = canned_histories::long_sequential_timers(num_timers as usize);
    let func = timers_wf(num_timers);
    let mut worker = replay_sdk_worker([test_hist_to_replay(t)]);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, func);
    worker.run().await.unwrap();
}

#[tokio::test]
async fn replay_ending_wft_complete_with_commands_but_no_scheduled_started() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();

    for i in 1..=2 {
        let timer_started_event_id = t.add_by_type(EventType::TimerStarted);
        t.add_timer_fired(timer_started_event_id, i.to_string());
        t.add_full_wf_task();
    }
    let func = timers_wf(3);
    let mut worker = replay_sdk_worker([test_hist_to_replay(t)]);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, func);
    worker.run().await.unwrap();
}

async fn replay_abrupt_ending(t: TestHistoryBuilder) {
    let func = timers_wf(1);
    let mut worker = replay_sdk_worker([test_hist_to_replay(t)]);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, func);
    worker.run().await.unwrap();
}
#[tokio::test]
async fn replay_ok_ending_with_terminated() {
    let mut t1 = canned_histories::single_timer("1");
    t1.add_workflow_execution_terminated();
    replay_abrupt_ending(t1).await;
}
#[tokio::test]
async fn replay_ok_ending_with_timed_out() {
    let mut t2 = canned_histories::single_timer("1");
    t2.add_workflow_execution_timed_out();
    replay_abrupt_ending(t2).await;
}

#[tokio::test]
async fn replay_shutdown_worker() {
    let t = canned_histories::single_timer("1");
    let func = timers_wf(1);
    let mut worker = replay_sdk_worker([test_hist_to_replay(t)]);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, func);
    let shutdown_ctr_i = UniqueShutdownWorker::default();
    let shutdown_ctr = shutdown_ctr_i.runs.clone();
    worker.set_worker_interceptor(shutdown_ctr_i);
    worker.run().await.unwrap();
    assert_eq!(shutdown_ctr.lock().len(), 1);
}

#[rstest::rstest]
#[tokio::test]
async fn multiple_histories_replay(#[values(false, true)] use_feeder: bool) {
    let num_timers = 10;
    let seq_timer_wf = timers_wf(num_timers);
    let one_timer_wf = timers_wf(1);
    let mut one_timer_hist = canned_histories::single_timer("1");
    one_timer_hist.set_wf_type("onetimer");
    let mut seq_timer_hist = canned_histories::long_sequential_timers(num_timers as usize);
    seq_timer_hist.set_wf_type("seqtimer");
    let (feeder, stream) = HistoryFeeder::new(1);
    let mut worker = if use_feeder {
        replay_sdk_worker_stream(stream)
    } else {
        replay_sdk_worker([
            test_hist_to_replay(one_timer_hist.clone()),
            test_hist_to_replay(seq_timer_hist.clone()),
        ])
    };
    let runs_ctr_i = UniqueRunsCounter::default();
    let runs_ctr = runs_ctr_i.runs.clone();
    worker.set_worker_interceptor(runs_ctr_i);
    worker.register_wf("onetimer", one_timer_wf);
    worker.register_wf("seqtimer", seq_timer_wf);

    if use_feeder {
        let feed_fut = async move {
            feeder
                .feed(test_hist_to_replay(one_timer_hist))
                .await
                .unwrap();
            feeder
                .feed(test_hist_to_replay(seq_timer_hist))
                .await
                .unwrap();
        };
        let (_, runr) = join!(feed_fut, worker.run());
        runr.unwrap();
    } else {
        worker.run().await.unwrap();
    }
    assert_eq!(runs_ctr.lock().len(), 2);
}

#[tokio::test]
async fn multiple_histories_can_handle_dupe_run_ids() {
    let mut hist1 = canned_histories::single_timer("1");
    hist1.set_wf_type("onetimer");
    let mut worker = replay_sdk_worker([
        test_hist_to_replay(hist1.clone()),
        test_hist_to_replay(hist1.clone()),
        test_hist_to_replay(hist1),
    ]);
    worker.register_wf("onetimer", timers_wf(1));
    worker.run().await.unwrap();
}

// Verifies SDK can decode patch markers before changing them to use json encoding
#[tokio::test]
async fn replay_old_patch_format() {
    let mut worker = replay_sdk_worker([HistoryForReplay::new(
        history_from_proto_binary("histories/old_change_marker_format.bin")
            .await
            .unwrap(),
        "fake".to_owned(),
    )]);
    worker.register_wf("writes_change_markers", changes_wf);
    worker.run().await.unwrap();
}

#[tokio::test]
async fn replay_ends_with_empty_wft() {
    let core = init_core_replay_preloaded(
        "SayHelloWorkflow",
        [HistoryForReplay::new(
            history_from_proto_binary("histories/ends_empty_wft_complete.bin")
                .await
                .unwrap(),
            "fake".to_owned(),
        )],
    );
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            ScheduleActivity {
                seq: 1,
                activity_id: "1".to_string(),
                activity_type: "say_hello".to_string(),
                ..Default::default()
            }
            .into(),
        ],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.eviction_reason().is_some());
}

fn timers_wf(num_timers: u32) -> WorkflowFunction {
    WorkflowFunction::new(move |ctx: WfContext| async move {
        for _ in 1..=num_timers {
            ctx.timer(Duration::from_secs(1)).await;
        }
        Ok(().into())
    })
}

#[derive(Default)]
struct UniqueRunsCounter {
    runs: Arc<Mutex<HashSet<String>>>,
}
#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for UniqueRunsCounter {
    async fn on_workflow_activation_completion(&self, completion: &WorkflowActivationCompletion) {
        self.runs.lock().insert(completion.run_id.clone());
    }
}

#[derive(Default)]
struct UniqueShutdownWorker {
    runs: Arc<Mutex<HashSet<String>>>,
}
#[async_trait::async_trait(?Send)]
impl WorkerInterceptor for UniqueShutdownWorker {
    fn on_shutdown(&self, _sdk_worker: &Worker) {
        // Assumed one worker per task queue.
        self.runs
            .lock()
            .insert(_sdk_worker.task_queue().to_string());
    }
}
