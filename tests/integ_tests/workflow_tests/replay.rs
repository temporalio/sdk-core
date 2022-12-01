use assert_matches::assert_matches;
use parking_lot::Mutex;
use std::{collections::HashSet, sync::Arc, time::Duration};
use temporal_sdk::{interceptors::WorkerInterceptor, WfContext, Worker, WorkflowFunction};
use temporal_sdk_core::replay::{HistoryFeeder, HistoryForReplay};
use temporal_sdk_core_api::errors::{PollActivityError, PollWfError};
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::remove_from_cache::EvictionReason,
        workflow_commands::{ScheduleActivity, StartTimer},
        workflow_completion::WorkflowActivationCompletion,
    },
    TestHistoryBuilder, DEFAULT_WORKFLOW_TYPE,
};
use temporal_sdk_core_test_utils::{
    canned_histories, history_from_proto_binary, init_core_replay_preloaded, replay_sdk_worker,
    replay_sdk_worker_stream, WorkerTestHelpers,
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
    let num_timers = 10;
    let t = canned_histories::long_sequential_timers(num_timers as usize);
    let func = timers_wf(num_timers);
    let mut worker = replay_sdk_worker([test_hist_to_replay(t)]);
    worker.register_wf(DEFAULT_WORKFLOW_TYPE, func);
    worker.run().await.unwrap();
}

#[tokio::test]
async fn replay_ok_ending_with_terminated_or_timed_out() {
    let mut t1 = canned_histories::single_timer("1");
    t1.add_workflow_execution_terminated();
    let mut t2 = canned_histories::single_timer("1");
    t2.add_workflow_execution_timed_out();
    for t in [t1, t2] {
        let func = timers_wf(1);
        let mut worker = replay_sdk_worker([test_hist_to_replay(t)]);
        worker.register_wf(DEFAULT_WORKFLOW_TYPE, func);
        worker.run().await.unwrap();
    }
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
    worker.set_worker_interceptor(Box::new(runs_ctr_i));
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

    fn on_shutdown(&self, _: &Worker) {}
}
