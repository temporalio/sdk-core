mod activities;
mod cancel_external;
mod cancel_wf;
mod child_workflows;
mod continue_as_new;
mod determinism;
mod local_activities;
mod patches;
mod signals;
mod stickyness;
mod timers;

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
use temporal_sdk::{WfContext, WorkflowResult};
use temporal_sdk_core_api::{errors::PollWfError, Core};
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::ActivityExecutionResult,
        workflow_activation::{workflow_activation_job, WorkflowActivation, WorkflowActivationJob},
        workflow_commands::{ActivityCancellationType, FailWorkflowExecution, StartTimer},
        workflow_completion::WorkflowActivationCompletion,
        ActivityTaskCompletion, IntoCompletion,
    },
    temporal::api::failure::v1::Failure,
};
use test_utils::{
    history_from_proto_binary, init_core_and_create_wf, init_core_replay, schedule_activity_cmd,
    with_gw, CoreTestHelpers, CoreWfStarter, GwApi, TEST_Q,
};
use tokio::time::sleep;
use uuid::Uuid;

// TODO: We should get expected histories for these tests and confirm that the history at the end
//  matches.

#[tokio::test]
async fn parallel_workflows_same_queue() {
    let mut starter = CoreWfStarter::new("parallel_workflows_same_queue");
    let core = starter.get_core().await;
    let task_q = starter.get_task_queue().to_string();
    let num_workflows = 25usize;

    let run_ids: Vec<_> = future::join_all(
        (0..num_workflows).map(|i| starter.start_wf_with_id(format!("wf-id-{}", i))),
    )
    .await;

    let mut send_chans = HashMap::new();
    async fn wf_task(
        core: Arc<dyn Core>,
        task_q: String,
        mut task_chan: UnboundedReceiver<WorkflowActivation>,
    ) {
        let task = task_chan.next().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::StartWorkflow(_)),
            }]
        );
        core.complete_timer(&task_q, &task.run_id, 1, Duration::from_secs(1))
            .await;
        let task = task_chan.next().await.unwrap();
        core.complete_execution(&task_q, &task.run_id).await;
    }

    let handles: Vec<_> = run_ids
        .iter()
        .map(|run_id| {
            let (tx, rx) = futures::channel::mpsc::unbounded();
            send_chans.insert(run_id.clone(), tx);
            tokio::spawn(wf_task(core.clone(), task_q.clone(), rx))
        })
        .collect();

    for _ in 0..num_workflows * 2 {
        let task = core.poll_workflow_activation(&task_q).await.unwrap();
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
            )
            .await
            .unwrap();
    }
    worker.run_until_done().await.unwrap();
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
    let core = starter.get_core().await;
    let task_q = starter.get_task_queue();
    // Begin the poll, and request shutdown from another thread after a small period of time.
    let tcore = core.clone();
    let handle = tokio::spawn(async move {
        std::thread::sleep(Duration::from_millis(100));
        tcore.shutdown().await;
    });
    assert_matches!(
        core.poll_workflow_activation(task_q).await.unwrap_err(),
        PollWfError::ShutDown
    );
    handle.await.unwrap();
    // Ensure double-shutdown doesn't explode
    core.shutdown().await;
    assert_matches!(
        core.poll_workflow_activation(task_q).await.unwrap_err(),
        PollWfError::ShutDown
    );
}

#[rstest::rstest]
#[tokio::test]
async fn fail_wf_task(#[values(true, false)] replay: bool) {
    let (core, task_q) = if replay {
        (
            init_core_replay(
                &history_from_proto_binary("histories/fail_wf_task.bin")
                    .await
                    .unwrap(),
            )
            .await,
            TEST_Q.to_string(),
        )
    } else {
        init_core_and_create_wf("fail_wf_task").await
    };
    // Start with a timer
    let task = core.poll_workflow_activation(&task_q).await.unwrap();
    core.complete_timer(&task_q, &task.run_id, 0, Duration::from_millis(200))
        .await;

    // Then break for whatever reason
    let task = core.poll_workflow_activation(&task_q).await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::fail(
        &task_q,
        task.run_id,
        Failure::application_failure("I did an oopsie".to_string(), false),
    ))
    .await
    .unwrap();

    // The server will want to retry the task. This time we finish the workflow -- but we need
    // to poll a couple of times as there will be more than one required workflow activation.
    let task = core.poll_workflow_activation(&task_q).await.unwrap();
    // The first poll response will tell us to evict
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(&task_q, task.run_id))
        .await
        .unwrap();

    let task = core.poll_workflow_activation(&task_q).await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        &task_q,
        task.run_id,
        vec![StartTimer {
            seq: 0,
            start_to_fire_timeout: Some(Duration::from_millis(200).into()),
        }
        .into()],
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_activation(&task_q).await.unwrap();
    core.complete_execution(&task_q, &task.run_id).await;
}

#[tokio::test]
async fn fail_workflow_execution() {
    let (core, task_q) = init_core_and_create_wf("fail_workflow_execution").await;
    let task = core.poll_workflow_activation(&task_q).await.unwrap();
    core.complete_timer(&task_q, &task.run_id, 0, Duration::from_secs(1))
        .await;
    let task = core.poll_workflow_activation(&task_q).await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        &task_q,
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
    let (core, task_q) = init_core_and_create_wf("signal_workflow").await;
    let workflow_id = task_q.clone();

    let signal_id_1 = "signal1";
    let signal_id_2 = "signal2";
    let res = core.poll_workflow_activation(&task_q).await.unwrap();
    // Task is completed with no commands
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        &task_q,
        res.run_id.clone(),
        vec![],
    ))
    .await
    .unwrap();

    // Send the signals to the server
    with_gw(core.as_ref(), |gw: GwApi| async move {
        gw.signal_workflow_execution(
            workflow_id.to_string(),
            res.run_id.to_string(),
            signal_id_1.to_string(),
            None,
        )
        .await
        .unwrap();
        gw.signal_workflow_execution(
            workflow_id.to_string(),
            res.run_id.to_string(),
            signal_id_2.to_string(),
            None,
        )
        .await
        .unwrap();
    })
    .await;

    let mut res = core.poll_workflow_activation(&task_q).await.unwrap();
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
            &task_q,
            res.run_id,
            vec![],
        ))
        .await
        .unwrap();
        res = core.poll_workflow_activation(&task_q).await.unwrap();
        assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
            },]
        );
    }
    core.complete_execution(&task_q, &res.run_id).await;
}

#[tokio::test]
async fn signal_workflow_signal_not_handled_on_workflow_completion() {
    let (core, task_q) =
        init_core_and_create_wf("signal_workflow_signal_not_handled_on_workflow_completion").await;
    let workflow_id = task_q.as_str();
    let signal_id_1 = "signal1";
    for i in 1..=2 {
        let res = core.poll_workflow_activation(&task_q).await.unwrap();
        // Task is completed with a timer
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            &task_q,
            res.run_id,
            vec![StartTimer {
                seq: 0,
                start_to_fire_timeout: Some(Duration::from_millis(10).into()),
            }
            .into()],
        ))
        .await
        .unwrap();

        let res = core.poll_workflow_activation(&task_q).await.unwrap();

        if i == 1 {
            // First attempt we should only see the timer being fired
            assert_matches!(
                res.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::FireTimer(_)),
                }]
            );

            let run_id = res.run_id.clone();

            // Send the signals to the server
            with_gw(core.as_ref(), |gw: GwApi| async move {
                gw.signal_workflow_execution(
                    workflow_id.to_string(),
                    res.run_id.to_string(),
                    signal_id_1.to_string(),
                    None,
                )
                .await
                .unwrap();
            })
            .await;

            // Send completion - not having seen a poll response with a signal in it yet (unhandled
            // command error will be logged as a warning and an eviction will be issued)
            core.complete_execution(&task_q, &run_id).await;

            // We should be told to evict
            let res = core.poll_workflow_activation(&task_q).await.unwrap();
            assert_matches!(
                res.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
                }]
            );
            core.complete_workflow_activation(WorkflowActivationCompletion::empty(
                task_q.clone(),
                res.run_id,
            ))
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
        core.complete_execution(&task_q, &res.run_id).await;
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
        .max_cached_workflows(0usize)
        .wft_timeout(Duration::from_secs(1));
    let core = wf_starter.get_core().await;
    let task_q = wf_starter.get_task_queue();
    let wf_id = &wf_starter.get_wf_id().to_owned();

    // Set up some helpers for polling and completing
    let poll_sched_act = || async {
        let wf_task = core.poll_workflow_activation(task_q).await.unwrap();
        core.complete_workflow_activation(
            schedule_activity_cmd(
                0,
                task_q,
                activity_id,
                ActivityCancellationType::TryCancel,
                Duration::from_secs(60),
                Duration::from_secs(60),
            )
            .into_completion(task_q.to_string(), wf_task.run_id.clone()),
        )
        .await
        .unwrap();
        wf_task
    };
    let poll_sched_act_poll = || async {
        poll_sched_act().await;
        let wf_task = core.poll_workflow_activation(task_q).await.unwrap();
        assert_matches!(
            wf_task.jobs.as_slice(),
            [
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
                },
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
                },
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
                }
            ]
        );
        wf_task
    };

    wf_starter.start_wf().await;

    // Poll and schedule the activity
    let wf_task = poll_sched_act().await;
    // Before polling for a task again, we start and complete the activity and send the
    // corresponding signals.
    let ac_task = core.poll_activity_task(task_q).await.unwrap();
    let rid = wf_task.run_id.clone();
    // Send the signals to the server & resolve activity -- sometimes this happens too fast
    sleep(Duration::from_millis(200)).await;
    with_gw(core.as_ref(), |gw: GwApi| async move {
        gw.signal_workflow_execution(wf_id.to_string(), rid, signal_at_start.to_string(), None)
            .await
            .unwrap();
    })
    .await;
    // Complete activity successfully.
    core.complete_activity_task(ActivityTaskCompletion {
        task_token: ac_task.task_token,
        task_queue: task_q.to_string(),
        result: Some(ActivityExecutionResult::ok(Default::default())),
    })
    .await
    .unwrap();
    let rid = wf_task.run_id.clone();
    with_gw(core.as_ref(), |gw: GwApi| async move {
        gw.signal_workflow_execution(wf_id.to_string(), rid, signal_at_complete.to_string(), None)
            .await
            .unwrap();
    })
    .await;
    // Now poll again, it will be an eviction b/c non-sticky mode.
    let wf_task = core.poll_workflow_activation(task_q).await.unwrap();
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task_q, wf_task.run_id))
        .await
        .unwrap();
    // Start from the beginning
    let wf_task = poll_sched_act_poll().await;
    // Time out this time
    sleep(Duration::from_secs(2)).await;
    // Poll again, which should not have any work to do and spin, until the complete goes through.
    // Which will be rejected with not found, producing an eviction.
    let (wf_task, _) = tokio::join!(
        async { core.poll_workflow_activation(task_q).await.unwrap() },
        async {
            sleep(Duration::from_millis(500)).await;
            // Reply to the first one, finally
            core.complete_execution(task_q, &wf_task.run_id).await;
        }
    );
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(task_q, wf_task.run_id))
        .await
        .unwrap();
    // Do it all over again, without timing out this time
    let wf_task = poll_sched_act_poll().await;
    core.complete_execution(task_q, &wf_task.run_id).await;
}
