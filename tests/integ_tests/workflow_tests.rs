mod activities;
mod cancel_wf;
mod child_workflows;
mod continue_as_new;
mod patches;
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
use temporal_sdk_core::{
    errors::PollWfError,
    protos::coresdk::{
        activity_result::ActivityResult,
        workflow_activation::{wf_activation_job, WfActivation, WfActivationJob},
        workflow_commands::{ActivityCancellationType, FailWorkflowExecution, StartTimer},
        workflow_completion::WfActivationCompletion,
        ActivityTaskCompletion,
    },
    protos::temporal::api::failure::v1::Failure,
    prototype_rust_sdk::{WfContext, WorkflowResult},
    Core, IntoCompletion,
};
use test_utils::{
    init_core_and_create_wf, schedule_activity_cmd, with_gw, CoreTestHelpers, CoreWfStarter, GwApi,
};
use tokio::time::sleep;
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
        let task = core.poll_workflow_task(task_q).await.unwrap();
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
pub async fn cache_evictions_wf(mut command_sink: WfContext) -> WorkflowResult<()> {
    RUN_CT.fetch_add(1, Ordering::SeqCst);
    let timer = StartTimer {
        timer_id: "super_timer_id".to_string(),
        start_to_fire_timeout: Some(Duration::from_secs(1).into()),
    };
    command_sink.timer(timer).await;
    Ok(().into())
}

#[tokio::test]
async fn workflow_lru_cache_evictions() {
    let wf_type = "workflow_lru_cache_evictions";
    let mut starter = CoreWfStarter::new(wf_type);
    starter.max_cached_workflows(1);
    let worker = starter.worker().await;
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
    starter.shutdown().await;
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
        core.poll_workflow_task(task_q).await.unwrap_err(),
        PollWfError::ShutDown
    );
    handle.await.unwrap();
    // Ensure double-shutdown doesn't explode
    core.shutdown().await;
    assert_matches!(
        core.poll_workflow_task(task_q).await.unwrap_err(),
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
        Failure::application_failure("I did an oopsie".to_string(), false),
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
    core.complete_workflow_task(WfActivationCompletion::empty(task.run_id))
        .await
        .unwrap();

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
            failure: Some(Failure::application_failure("I'm ded".to_string(), false)),
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
        let wf_task = core.poll_workflow_task(task_q).await.unwrap();
        core.complete_workflow_task(
            schedule_activity_cmd(
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
    let poll_sched_act_poll = || async {
        poll_sched_act().await;
        let wf_task = core.poll_workflow_task(task_q).await.unwrap();
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
    let wf_task = core.poll_workflow_task(task_q).await.unwrap();
    assert_matches!(
        wf_task.jobs.as_slice(),
        [WfActivationJob {
            variant: Some(wf_activation_job::Variant::RemoveFromCache(_)),
        }]
    );
    core.complete_workflow_task(WfActivationCompletion::empty(wf_task.run_id))
        .await
        .unwrap();
    // Start from the beginning
    let wf_task = poll_sched_act_poll().await;
    // Time out this time
    sleep(Duration::from_secs(2)).await;
    // Poll again, which should not have any work to do and spin, until the complete goes through.
    // Which will be rejected with not found, producing an eviction.
    let (wf_task, _) = tokio::join!(
        async { core.poll_workflow_task(task_q).await.unwrap() },
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
    core.complete_workflow_task(WfActivationCompletion::empty(wf_task.run_id))
        .await
        .unwrap();
    // Do it all over again, without timing out this time
    let wf_task = poll_sched_act_poll().await;
    core.complete_execution(&wf_task.run_id).await;
}
