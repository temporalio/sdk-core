mod activities;
mod appdata_propagation;
mod cancel_external;
mod cancel_wf;
mod child_workflows;
mod continue_as_new;
mod determinism;
mod local_activities;
mod modify_wf_properties;
mod patches;
mod replay;
mod resets;
mod signals;
mod stickyness;
mod timers;
mod upsert_search_attrs;

use assert_matches::assert_matches;
use futures::{channel::mpsc::UnboundedReceiver, future, SinkExt, StreamExt};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use temporal_client::{WorkflowClientTrait, WorkflowOptions};
use temporal_sdk::{
    interceptors::WorkerInterceptor, ActContext, ActivityOptions, WfContext, WorkflowResult,
};
use temporal_sdk_core::replay::HistoryForReplay;
use temporal_sdk_core_api::{errors::PollWfError, Worker};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::ActivityExecutionResult,
        workflow_activation::{workflow_activation_job, WorkflowActivation, WorkflowActivationJob},
        workflow_commands::{ActivityCancellationType, FailWorkflowExecution, StartTimer},
        workflow_completion::WorkflowActivationCompletion,
        ActivityTaskCompletion, AsJsonPayloadExt, IntoCompletion,
    },
    temporal::api::{failure::v1::Failure, history::v1::history_event},
};
use temporal_sdk_core_test_utils::{
    history_from_proto_binary, init_core_and_create_wf, init_core_replay_preloaded,
    schedule_activity_cmd, CoreWfStarter, WorkerTestHelpers,
};
use tokio::time::sleep;
use uuid::Uuid;

// TODO: We should get expected histories for these tests and confirm that the history at the end
//  matches.

#[tokio::test]
async fn parallel_workflows_same_queue() {
    let mut starter = CoreWfStarter::new("parallel_workflows_same_queue");
    let core = starter.get_worker().await;
    let num_workflows = 25usize;

    let run_ids: Vec<_> = future::join_all(
        (0..num_workflows)
            .map(|i| starter.start_wf_with_id(format!("wf-id-{}", i), WorkflowOptions::default())),
    )
    .await;

    let mut send_chans = HashMap::new();
    async fn wf_task(
        worker: Arc<dyn Worker>,
        mut task_chan: UnboundedReceiver<WorkflowActivation>,
    ) {
        let task = task_chan.next().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::StartWorkflow(_)),
            }]
        );
        worker
            .complete_timer(&task.run_id, 1, Duration::from_secs(1))
            .await;
        let task = task_chan.next().await.unwrap();
        worker.complete_execution(&task.run_id).await;
    }

    let handles: Vec<_> = run_ids
        .iter()
        .map(|run_id| {
            let (tx, rx) = futures::channel::mpsc::unbounded();
            send_chans.insert(run_id.clone(), tx);
            tokio::spawn(wf_task(core.clone(), rx))
        })
        .collect();

    for _ in 0..num_workflows * 2 {
        let task = core.poll_workflow_activation().await.unwrap();
        send_chans
            .get(&task.run_id)
            .unwrap()
            .send(task)
            .await
            .unwrap();
    }

    for handle in handles {
        handle.await.unwrap()
    }
    core.shutdown().await;
}

static RUN_CT: AtomicUsize = AtomicUsize::new(0);
pub async fn cache_evictions_wf(command_sink: WfContext) -> WorkflowResult<()> {
    RUN_CT.fetch_add(1, Ordering::SeqCst);
    command_sink.timer(Duration::from_secs(1)).await;
    Ok(().into())
}

#[tokio::test]
async fn workflow_lru_cache_evictions() {
    let wf_type = "workflow_lru_cache_evictions";
    let mut starter = CoreWfStarter::new(wf_type);
    starter.max_cached_workflows(1);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_type.to_string(), cache_evictions_wf);

    let n_workflows = 3;
    for _ in 0..n_workflows {
        worker
            .submit_wf(
                format!("wce-{}", Uuid::new_v4()),
                wf_type.to_string(),
                vec![],
                WorkflowOptions::default(),
            )
            .await
            .unwrap();
    }
    struct CacheAsserter;
    #[async_trait::async_trait(?Send)]
    impl WorkerInterceptor for CacheAsserter {
        async fn on_workflow_activation_completion(&self, _: &WorkflowActivationCompletion) {}
        fn on_shutdown(&self, sdk_worker: &temporal_sdk::Worker) {
            // 0 since the sdk worker force-evicts and drains everything on shutdown.
            assert_eq!(sdk_worker.cached_workflows(), 0);
        }
    }
    worker
        .run_until_done_intercepted(Some(CacheAsserter))
        .await
        .unwrap();
    // The wf must have started more than # workflows times, since all but one must experience
    // an eviction
    assert!(RUN_CT.load(Ordering::SeqCst) > n_workflows);
}

// Ideally this would be a unit test, but returning a pending future with mockall bloats the mock
// code a bunch and just isn't worth it. Do it when https://github.com/asomers/mockall/issues/189 is
// fixed.
#[tokio::test]
async fn shutdown_aborts_actively_blocked_poll() {
    let mut starter = CoreWfStarter::new("shutdown_aborts_actively_blocked_poll");
    let core = starter.get_worker().await;
    // Begin the poll, and request shutdown from another thread after a small period of time.
    let tcore = core.clone();
    let handle = tokio::spawn(async move {
        std::thread::sleep(Duration::from_millis(100));
        tcore.shutdown().await;
    });
    assert_matches!(
        core.poll_workflow_activation().await.unwrap_err(),
        PollWfError::ShutDown
    );
    handle.await.unwrap();
    // Ensure double-shutdown doesn't explode
    core.shutdown().await;
    assert_matches!(
        core.poll_workflow_activation().await.unwrap_err(),
        PollWfError::ShutDown
    );
}

#[rstest::rstest]
#[tokio::test]
async fn fail_wf_task(#[values(true, false)] replay: bool) {
    let core = if replay {
        // We need to send the history twice, since we fail it the first time.
        let mut hist_proto = history_from_proto_binary("histories/fail_wf_task.bin")
            .await
            .unwrap();
        let hist = HistoryForReplay::new(hist_proto.clone(), "fake".to_string());
        if let Some(history_event::Attributes::WorkflowExecutionStartedEventAttributes(
            ref mut attrs,
        )) = hist_proto.events[0].attributes
        {
            attrs.original_execution_run_id = "run2".to_string();
            attrs.first_execution_run_id = "run2".to_string();
        }
        let hist2 = HistoryForReplay::new(hist_proto, "fake".to_string());
        init_core_replay_preloaded("fail_wf_task", [hist, hist2])
    } else {
        let mut starter = init_core_and_create_wf("fail_wf_task").await;
        starter.get_worker().await
    };
    // Start with a timer
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_timer(&task.run_id, 0, Duration::from_millis(200))
        .await;

    // Then break for whatever reason
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::fail(
        task.run_id,
        Failure::application_failure("I did an oopsie".to_string(), false),
    ))
    .await
    .unwrap();

    // The server will want to retry the task. This time we finish the workflow -- but we need
    // to poll a couple of times as there will be more than one required workflow activation.
    let task = core.poll_workflow_activation().await.unwrap();
    // The first poll response will tell us to evict
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![StartTimer {
            seq: 0,
            start_to_fire_timeout: Some(prost_dur!(from_millis(200))),
        }
        .into()],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn fail_workflow_execution() {
    let core = init_core_and_create_wf("fail_workflow_execution")
        .await
        .get_worker()
        .await;
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_timer(&task.run_id, 0, Duration::from_secs(1))
        .await;
    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![FailWorkflowExecution {
            failure: Some(Failure::application_failure("I'm ded".to_string(), false)),
        }
        .into()],
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn signal_workflow() {
    let mut starter = init_core_and_create_wf("signal_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue().to_string();

    let signal_id_1 = "signal1";
    let signal_id_2 = "signal2";
    let res = core.poll_workflow_activation().await.unwrap();
    // Task is completed with no commands
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id.clone(),
        vec![],
    ))
    .await
    .unwrap();

    // Send the signals to the server
    client
        .signal_workflow_execution(
            workflow_id.to_string(),
            res.run_id.to_string(),
            signal_id_1.to_string(),
            None,
            None,
        )
        .await
        .unwrap();
    client
        .signal_workflow_execution(
            workflow_id.to_string(),
            res.run_id.to_string(),
            signal_id_2.to_string(),
            None,
            None,
        )
        .await
        .unwrap();

    let mut res = core.poll_workflow_activation().await.unwrap();
    // Sometimes both signals are complete at once, sometimes only one, depending on server
    // Converting test to wf function type would make this shorter
    if res.jobs.len() == 2 {
        assert_matches!(
            res.jobs.as_slice(),
            [
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
                },
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
                }
            ]
        );
    } else if res.jobs.len() == 1 {
        assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
            },]
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![],
        ))
        .await
        .unwrap();
        res = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
            },]
        );
    }
    core.complete_execution(&res.run_id).await;
}

#[tokio::test]
async fn signal_workflow_signal_not_handled_on_workflow_completion() {
    let mut starter =
        init_core_and_create_wf("signal_workflow_signal_not_handled_on_workflow_completion").await;
    let core = starter.get_worker().await;
    let workflow_id = starter.get_task_queue().to_string();
    let signal_id_1 = "signal1";
    for i in 1..=2 {
        let res = core.poll_workflow_activation().await.unwrap();
        // Task is completed with a timer
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![StartTimer {
                seq: 0,
                start_to_fire_timeout: Some(prost_dur!(from_millis(10))),
            }
            .into()],
        ))
        .await
        .unwrap();

        let res = core.poll_workflow_activation().await.unwrap();

        if i == 1 {
            // First attempt we should only see the timer being fired
            assert_matches!(
                res.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::FireTimer(_)),
                }]
            );

            let run_id = res.run_id.clone();

            // Send the signal to the server
            starter
                .get_client()
                .await
                .signal_workflow_execution(
                    workflow_id.clone(),
                    res.run_id.to_string(),
                    signal_id_1.to_string(),
                    None,
                    None,
                )
                .await
                .unwrap();

            // Send completion - not having seen a poll response with a signal in it yet (unhandled
            // command error will be logged as a warning and an eviction will be issued)
            core.complete_execution(&run_id).await;

            // We should be told to evict
            let res = core.poll_workflow_activation().await.unwrap();
            assert_matches!(
                res.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
                }]
            );
            core.complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id))
                .await
                .unwrap();
            // Loop to the top to handle wf from the beginning
            continue;
        }

        // On the second attempt, we will see the signal we failed to handle as well as the timer
        assert_matches!(
            res.jobs.as_slice(),
            [
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::FireTimer(_)),
                },
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
                }
            ]
        );
        core.complete_execution(&res.run_id).await;
    }
}

#[tokio::test]
async fn wft_timeout_doesnt_create_unsolvable_autocomplete() {
    let activity_id = "act-1";
    let signal_at_start = "at-start";
    let signal_at_complete = "at-complete";
    let mut wf_starter = CoreWfStarter::new("wft_timeout_doesnt_create_unsolvable_autocomplete");
    wf_starter
        // Test needs eviction on and a short timeout
        .max_cached_workflows(0)
        .max_wft(1)
        .wft_timeout(Duration::from_secs(1));
    let core = wf_starter.get_worker().await;
    let client = wf_starter.get_client().await;
    let task_q = wf_starter.get_task_queue();
    let wf_id = &wf_starter.get_wf_id().to_owned();

    // Set up some helpers for polling and completing
    let poll_sched_act = || async {
        let wf_task = core.poll_workflow_activation().await.unwrap();
        core.complete_workflow_activation(
            schedule_activity_cmd(
                0,
                task_q,
                activity_id,
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .into_completion(wf_task.run_id.clone()),
        )
        .await
        .unwrap();
        wf_task
    };
    wf_starter.start_wf().await;

    // Poll and schedule the activity
    let wf_task = poll_sched_act().await;
    // Before polling for a task again, we start and complete the activity and send the
    // corresponding signals.
    let ac_task = core.poll_activity_task().await.unwrap();
    let rid = wf_task.run_id.clone();
    // Send the signals to the server & resolve activity -- sometimes this happens too fast
    sleep(Duration::from_millis(200)).await;
    client
        .signal_workflow_execution(
            wf_id.to_string(),
            rid,
            signal_at_start.to_string(),
            None,
            None,
        )
        .await
        .unwrap();
    // Complete activity successfully.
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: ac_task.task_token,
        result: Some(ActivityExecutionResult::ok(Default::default())),
    })
    .await
    .unwrap();
    let rid = wf_task.run_id.clone();
    client
        .signal_workflow_execution(
            wf_id.to_string(),
            rid,
            signal_at_complete.to_string(),
            None,
            None,
        )
        .await
        .unwrap();
    // Now poll again, it will be an eviction b/c non-sticky mode.
    let wf_task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(wf_task.run_id))
        .await
        .unwrap();
    // Start from the beginning
    poll_sched_act().await;
    let wf_task = core.poll_workflow_activation().await.unwrap();
    // Time out this time
    sleep(Duration::from_secs(2)).await;
    // Poll again, which should not have any work to do and spin, until the complete goes through.
    // Which will be rejected with not found, producing an eviction.
    let (wf_task, _) = tokio::join!(
        async { core.poll_workflow_activation().await.unwrap() },
        async {
            sleep(Duration::from_millis(500)).await;
            // Reply to the first one, finally
            core.complete_execution(&wf_task.run_id).await;
        }
    );
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(wf_task.run_id))
        .await
        .unwrap();
    // Do it all over again, without timing out this time
    poll_sched_act().await;
    let wf_task = core.poll_workflow_activation().await.unwrap();
    // Server can sometimes arbitrarily re-order the activity complete to be after the second signal
    // Seeing 3 jobs is enough info.
    assert_eq!(wf_task.jobs.len(), 3);
    core.complete_execution(&wf_task.run_id).await;
}

/// We had a bug related to polling being faster than completion causing issues with cache
/// overflow. This test intentionally makes completes slower than polls to evaluate that.
///
/// It's expected that this test may generate some task timeouts.
#[tokio::test]
async fn slow_completes_with_small_cache() {
    let wf_name = "slow_completes_with_small_cache";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.max_wft(5).max_cached_workflows(5);
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        for _ in 0..3 {
            ctx.activity(ActivityOptions {
                activity_type: "echo_activity".to_string(),
                start_to_close_timeout: Some(Duration::from_secs(5)),
                input: "hi!".as_json_payload().expect("serializes fine"),
                ..Default::default()
            })
            .await;
            ctx.timer(Duration::from_secs(1)).await;
        }
        Ok(().into())
    });
    worker.register_activity(
        "echo_activity",
        |_ctx: ActContext, echo_me: String| async move { Ok(echo_me) },
    );
    for i in 0..20 {
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

    struct SlowCompleter {}
    #[async_trait::async_trait(?Send)]
    impl WorkerInterceptor for SlowCompleter {
        async fn on_workflow_activation_completion(&self, _: &WorkflowActivationCompletion) {
            // They don't need to be much slower
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        fn on_shutdown(&self, _: &temporal_sdk::Worker) {}
    }
    worker
        .run_until_done_intercepted(Some(SlowCompleter {}))
        .await
        .unwrap();
}
