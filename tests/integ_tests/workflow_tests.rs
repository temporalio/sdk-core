mod activities;
mod cancel_wf;
mod continue_as_new;
mod stickyness;
mod timers;

use assert_matches::assert_matches;
use futures::{channel::mpsc::UnboundedReceiver, future, SinkExt, StreamExt};
use std::{collections::HashMap, sync::Arc, time::Duration};
use temporal_sdk_core::{
    protos::coresdk::{
        activity_result::ActivityResult,
        common::UserCodeFailure,
        workflow_activation::{wf_activation_job, WfActivation, WfActivationJob},
        workflow_commands::{ActivityCancellationType, FailWorkflowExecution, StartTimer},
        workflow_completion::WfActivationCompletion,
        ActivityTaskCompletion,
    },
    Core, IntoCompletion, PollWfError,
};
use test_utils::{
    init_core_and_create_wf, schedule_activity_cmd, with_gw, CoreTestHelpers, CoreWfStarter, GwApi,
};
use tokio::time::sleep;
use tracing::debug;
use uuid::Uuid;

// TODO: We should get expected histories for these tests and confirm that the history at the end
//  matches.

#[tokio::test]
async fn parallel_workflows_same_queue() {
    let mut starter = CoreWfStarter::new("parallel_workflows_same_queue");
    let core = starter.get_core().await;
    let task_q = starter.get_task_queue();
    let num_workflows = 25usize;

    let run_ids: Vec<_> = future::join_all(
        (0..num_workflows).map(|i| starter.start_wf_with_id(format!("wf-id-{}", i))),
    )
    .await;

    let mut send_chans = HashMap::new();
    async fn wf_task(core: Arc<dyn Core>, mut task_chan: UnboundedReceiver<WfActivation>) {
        let task = task_chan.next().await.unwrap();
        assert_matches!(
            task.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::StartWorkflow(_)),
            }]
        );
        core.complete_timer(&task.run_id, "timer", Duration::from_secs(1))
            .await;
        let task = task_chan.next().await.unwrap();
        core.complete_execution(&task.run_id).await;
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
        let task = core.poll_workflow_task(&task_q).await.unwrap();
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

#[tokio::test]
async fn workflow_cache_evictions() {
    let mut starter = CoreWfStarter::new("workflow_cache_evictions");
    starter
        .max_cached_workflows(1)
        .wft_timeout(Duration::from_secs(1));
    let core = starter.get_core().await;
    let task_q = starter.get_task_queue();
    let num_workflows = 2;

    let run_ids: Vec<_> =
        future::join_all((0..2).map(|_| starter.start_wf_with_id(format!("{}", Uuid::new_v4()))))
            .await;

    let mut send_chans = HashMap::new();
    async fn wf_task(core: Arc<dyn Core>, mut task_chan: UnboundedReceiver<WfActivation>) {
        loop {
            let task = task_chan.next().await.unwrap();
            if let [WfActivationJob {
                variant: Some(wf_activation_job::Variant::StartWorkflow(_)),
            }] = task.jobs.as_slice()
            {
                debug!(run_id=%task.run_id.clone(), "scheduling timer");
                core.complete_timer(&task.run_id, "timer", Duration::from_secs(1))
                    .await;
            } else if let [WfActivationJob {
                variant: Some(wf_activation_job::Variant::FireTimer(_)),
            }] = task.jobs.as_slice()
            {
                debug!(run_id=%task.run_id.clone(), "completing execution");
                core.complete_execution(&task.run_id).await;
                break;
            }
        }
    }

    let handles: Vec<_> = run_ids
        .iter()
        .map(|run_id| {
            let (tx, rx) = futures::channel::mpsc::unbounded();
            send_chans.insert(run_id.clone(), tx);
            tokio::spawn(wf_task(core.clone(), rx))
        })
        .collect();

    for _ in 0..num_workflows * 2 + 1 {
        let task = core.poll_workflow_task(&task_q).await.unwrap();
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
        core.poll_workflow_task(&task_q).await.unwrap_err(),
        PollWfError::ShutDown
    );
    handle.await.unwrap();
    // Ensure double-shutdown doesn't explode
    core.shutdown().await;
    assert_matches!(
        core.poll_workflow_task(&task_q).await.unwrap_err(),
        PollWfError::ShutDown
    );
}

#[tokio::test]
async fn fail_wf_task() {
    let (core, task_q) = init_core_and_create_wf("fail_wf_task").await;
    // Start with a timer
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_timer(&task.run_id, "timer-1", Duration::from_millis(200))
        .await;

    // Then break for whatever reason
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::fail(
        task.run_id,
        UserCodeFailure {
            message: "I did an oopsie".to_string(),
            ..Default::default()
        },
    ))
    .await
    .unwrap();

    // The server will want to retry the task. This time we finish the workflow -- but we need
    // to poll a couple of times as there will be more than one required workflow activation.
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    // The first poll response will tell us to evict
    assert_matches!(
        task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    // So poll again
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![StartTimer {
            timer_id: "timer-1".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(200).into()),
        }
        .into()],
        task.run_id,
    ))
    .await
    .unwrap();
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_execution(&task.run_id).await;
}

#[tokio::test]
async fn fail_workflow_execution() {
    let (core, task_q) = init_core_and_create_wf("fail_workflow_execution").await;
    let timer_id = "timer-1";
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_timer(&task.run_id, timer_id, Duration::from_secs(1))
        .await;
    let task = core.poll_workflow_task(&task_q).await.unwrap();
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![FailWorkflowExecution {
            failure: Some(UserCodeFailure {
                message: "I'm ded".to_string(),
                ..Default::default()
            }),
        }
        .into()],
        task.run_id,
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn signal_workflow() {
    let workflow_id = "signal_workflow";
    let (core, task_q) = init_core_and_create_wf(workflow_id).await;

    let signal_id_1 = "signal1";
    let signal_id_2 = "signal2";
    let res = core.poll_workflow_task(&task_q).await.unwrap();
    // Task is completed with no commands
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![],
        res.run_id.clone(),
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

    let mut res = core.poll_workflow_task(&task_q).await.unwrap();
    // Sometimes both signals are complete at once, sometimes only one, depending on server
    // Converting test to wf function type would make this shorter
    if res.jobs.len() == 2 {
        assert_matches!(
            res.jobs.as_slice(),
            [
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
                },
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
                }
            ]
        );
    } else if res.jobs.len() == 1 {
        assert_matches!(
            res.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
            },]
        );
        core.complete_workflow_task(WfActivationCompletion::from_cmds(vec![], res.run_id))
            .await
            .unwrap();
        res = core.poll_workflow_task(&task_q).await.unwrap();
        assert_matches!(
            res.jobs.as_slice(),
            [WfActivationJob {
                variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
            },]
        );
    }
    core.complete_execution(&res.run_id).await;
}

#[tokio::test]
async fn signal_workflow_signal_not_handled_on_workflow_completion() {
    let workflow_id = "signal_workflow_signal_not_handled_on_workflow_completion";
    let (core, task_q) = init_core_and_create_wf(workflow_id).await;

    let signal_id_1 = "signal1";
    let res = core.poll_workflow_task(&task_q).await.unwrap();
    // Task is completed with a timer
    core.complete_workflow_task(WfActivationCompletion::from_cmds(
        vec![StartTimer {
            timer_id: "sometimer".to_string(),
            start_to_fire_timeout: Some(Duration::from_millis(10).into()),
        }
        .into()],
        res.run_id,
    ))
    .await
    .unwrap();

    // Poll before sending the signal - we should have the timer job
    let res = core.poll_workflow_task(&task_q).await.unwrap();
    assert_matches!(
        res.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::FireTimer(_)),
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

    // Send completion - not having seen a poll response with a signal in it yet (unhandled command
    // error will be silenced)
    core.complete_execution(&run_id).await;

    // We should get a new task with the signal
    let res = core.poll_workflow_task(&task_q).await.unwrap();
    assert_matches!(
        res.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    core.complete_execution(&res.run_id).await;
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
        let wf_task = core.poll_workflow_task(&task_q).await.unwrap();
        core.complete_workflow_task(
            schedule_activity_cmd(
                &task_q,
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
    let poll_sched_act_poll = || async {
        poll_sched_act().await;
        let wf_task = core.poll_workflow_task(&task_q).await.unwrap();
        assert_matches!(
            wf_task.jobs.as_slice(),
            [
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
                },
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::ResolveActivity(_)),
                },
                WfActivationJob {
                    variant: Some(wf_activation_job::Variant::SignalWorkflow(_)),
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
    let ac_task = core.poll_activity_task(&task_q).await.unwrap();
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
        result: Some(ActivityResult::ok(Default::default())),
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
    let wf_task = core.poll_workflow_task(&task_q).await.unwrap();
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    // Start from the beginning
    let wf_task = poll_sched_act_poll().await;
    // Time out this time
    sleep(Duration::from_secs(2)).await;
    // Poll again, which should not have any work to do and spin, until the complete goes through.
    // Which will be rejected with not found, producing an eviction.
    let (wf_task, _) = tokio::join!(
        async { core.poll_workflow_task(&task_q).await.unwrap() },
        async {
            sleep(Duration::from_millis(500)).await;
            // Reply to the first one, finally
            core.complete_execution(&wf_task.run_id).await;
        }
    );
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    // Do it all over again, without timing out this time
    let wf_task = poll_sched_act_poll().await;
    core.complete_execution(&wf_task.run_id).await;
}
