mod activities;
mod appdata_propagation;
mod cancel_external;
mod cancel_wf;
mod child_workflows;
mod continue_as_new;
mod determinism;
mod eager;
mod local_activities;
mod modify_wf_properties;
mod nexus;
mod patches;
mod priority;
mod replay;
mod resets;
mod signals;
mod stickyness;
mod timers;
mod upsert_search_attrs;

use crate::integ_tests::{activity_functions::echo, metrics_tests};
use assert_matches::assert_matches;
use std::{
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use temporal_client::{WfClientExt, WorkflowClientTrait, WorkflowExecutionResult, WorkflowOptions};
use temporal_sdk::{
    ActivityOptions, LocalActivityOptions, WfContext, WorkflowResult,
    interceptors::WorkerInterceptor,
};
use temporal_sdk_core::{CoreRuntime, replay::HistoryForReplay};
use temporal_sdk_core_api::{
    errors::{PollError, WorkflowErrorType},
    worker::{
        PollerBehavior, WorkerDeploymentOptions, WorkerDeploymentVersion, WorkerVersioningStrategy,
    },
};
use temporal_sdk_core_protos::{
    coresdk::{
        ActivityTaskCompletion, AsJsonPayloadExt, IntoCompletion,
        activity_result::ActivityExecutionResult,
        workflow_activation::{WorkflowActivationJob, workflow_activation_job},
        workflow_commands::{
            ActivityCancellationType, FailWorkflowExecution, QueryResult, QuerySuccess, StartTimer,
        },
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        enums::v1::EventType, failure::v1::Failure, history::v1::history_event,
        query::v1::WorkflowQuery,
    },
};
use temporal_sdk_core_test_utils::{
    CoreWfStarter, WorkerTestHelpers, drain_pollers_and_shutdown, history_from_proto_binary,
    init_core_and_create_wf, init_core_replay_preloaded, prom_metrics, schedule_activity_cmd,
};
use tokio::{join, sync::Notify, time::sleep};
use uuid::Uuid;
// TODO: We should get expected histories for these tests and confirm that the history at the end
//  matches.

#[tokio::test]
async fn parallel_workflows_same_queue() {
    let wf_name = "parallel_workflows_same_queue";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut core = starter.worker().await;

    core.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        ctx.timer(Duration::from_secs(1)).await;
        Ok(().into())
    });
    for i in 0..25 {
        core.submit_wf(
            format!("{}-{}", wf_name, i),
            wf_name,
            vec![],
            starter.workflow_options.clone(),
        )
        .await
        .unwrap();
    }
    core.run_until_done().await.unwrap();
}

static RUN_CT: AtomicUsize = AtomicUsize::new(0);

pub(crate) async fn cache_evictions_wf(command_sink: WfContext) -> WorkflowResult<()> {
    RUN_CT.fetch_add(1, Ordering::SeqCst);
    command_sink.timer(Duration::from_secs(1)).await;
    Ok(().into())
}

#[tokio::test]
async fn workflow_lru_cache_evictions() {
    let wf_type = "workflow_lru_cache_evictions";
    let mut starter = CoreWfStarter::new(wf_type);
    starter
        .worker_config
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize))
        .no_remote_activities(true)
        .max_cached_workflows(1_usize);
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
        tokio::time::sleep(Duration::from_millis(100)).await;
        drain_pollers_and_shutdown(&tcore).await;
    });
    assert_matches!(
        core.poll_workflow_activation().await.unwrap_err(),
        PollError::ShutDown
    );
    handle.await.unwrap();
    // Ensure double-shutdown doesn't explode
    core.shutdown().await;
    assert_matches!(
        core.poll_workflow_activation().await.unwrap_err(),
        PollError::ShutDown
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
        None,
    ))
    .await
    .unwrap();

    // The server will want to retry the task. This time we finish the workflow -- but we need
    // to poll a couple of times as there will be more than one required workflow activation.
    // The first poll response will tell us to evict.
    core.handle_eviction().await;

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            StartTimer {
                seq: 0,
                start_to_fire_timeout: Some(prost_dur!(from_millis(200))),
            }
            .into(),
        ],
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
        vec![
            FailWorkflowExecution {
                failure: Some(Failure::application_failure("I'm ded".to_string(), false)),
            }
            .into(),
        ],
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
            vec![
                StartTimer {
                    seq: 0,
                    start_to_fire_timeout: Some(prost_dur!(from_millis(10))),
                }
                .into(),
            ],
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
            core.handle_eviction().await;
            // Loop to the top to handle wf from the beginning
            continue;
        }

        // On the second attempt, we will see the signal we failed to handle as well as the timer
        assert_matches!(
            res.jobs.as_slice(),
            [
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
                },
                WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::FireTimer(_)),
                },
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
        .worker_config
        // Test needs eviction on and a short timeout
        .max_cached_workflows(0_usize)
        .max_outstanding_workflow_tasks(1_usize);
    wf_starter
        .worker_config
        .workflow_task_poller_behavior(PollerBehavior::SimpleMaximum(1_usize));
    wf_starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
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
    wf_starter.start_wf_with_id(wf_id.to_string()).await;

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
    core.handle_eviction().await;
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
    starter
        .worker_config
        .max_outstanding_workflow_tasks(5_usize)
        .max_cached_workflows(5_usize);
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
    worker.register_activity("echo_activity", echo);
    for i in 0..20 {
        worker
            .submit_wf(
                format!("{wf_name}_{i}"),
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

#[tokio::test]
#[rstest::rstest]
async fn deployment_version_correct_in_wf_info(#[values(true, false)] use_only_build_id: bool) {
    let wf_type = "deployment_version_correct_in_wf_info";
    let mut starter = CoreWfStarter::new(wf_type);
    let version_strat = if use_only_build_id {
        WorkerVersioningStrategy::None {
            build_id: "1.0".to_owned(),
        }
    } else {
        WorkerVersioningStrategy::WorkerDeploymentBased(WorkerDeploymentOptions {
            version: WorkerDeploymentVersion {
                deployment_name: "deployment-1".to_string(),
                build_id: "1.0".to_string(),
            },
            use_worker_versioning: false,
            default_versioning_behavior: None,
        })
    };
    starter
        .worker_config
        .versioning_strategy(version_strat)
        .no_remote_activities(true);
    let core = starter.get_worker().await;
    starter.start_wf().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue().to_string();

    let res = core.poll_workflow_activation().await.unwrap();
    assert_eq!(
        res.deployment_version_for_current_task
            .as_ref()
            .unwrap()
            .build_id,
        "1.0"
    );
    if !use_only_build_id {
        assert_eq!(
            res.deployment_version_for_current_task
                .unwrap()
                .deployment_name,
            "deployment-1"
        );
    }
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id.clone(),
        vec![],
    ))
    .await
    .unwrap();

    // Ensure a query on first wft also sees the correct id
    let query_fut = async {
        client
            .query_workflow_execution(
                workflow_id.clone(),
                res.run_id.to_string(),
                WorkflowQuery {
                    query_type: "q1".to_string(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
    };
    let complete_fut = async {
        let task = core.poll_workflow_activation().await.unwrap();
        let query = assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
            }] => q
        );
        assert_eq!(
            task.deployment_version_for_current_task
                .as_ref()
                .unwrap()
                .build_id,
            "1.0"
        );
        if !use_only_build_id {
            assert_eq!(
                task.deployment_version_for_current_task
                    .unwrap()
                    .deployment_name,
                "deployment-1"
            );
        }
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            QueryResult {
                query_id: query.query_id.clone(),
                variant: Some(
                    QuerySuccess {
                        response: Some("done".into()),
                    }
                    .into(),
                ),
            }
            .into(),
        ))
        .await
        .unwrap();
    };
    join!(query_fut, complete_fut);
    starter.shutdown().await;
    client
        .reset_sticky_task_queue(workflow_id.clone(), "".to_string())
        .await
        .unwrap();

    let mut starter = starter.clone_no_worker();
    let version_strat = if use_only_build_id {
        WorkerVersioningStrategy::None {
            build_id: "2.0".to_owned(),
        }
    } else {
        WorkerVersioningStrategy::WorkerDeploymentBased(WorkerDeploymentOptions {
            version: WorkerDeploymentVersion {
                deployment_name: "deployment-1".to_string(),
                build_id: "2.0".to_string(),
            },
            use_worker_versioning: false,
            default_versioning_behavior: None,
        })
    };
    starter.worker_config.versioning_strategy(version_strat);

    let core = starter.get_worker().await;

    let query_fut = async {
        client
            .query_workflow_execution(
                workflow_id.clone(),
                res.run_id.to_string(),
                WorkflowQuery {
                    query_type: "q2".to_string(),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
    };
    let complete_fut = async {
        let res = core.poll_workflow_activation().await.unwrap();
        assert_eq!(
            res.deployment_version_for_current_task
                .as_ref()
                .unwrap()
                .build_id,
            "1.0"
        );
        if !use_only_build_id {
            assert_eq!(
                res.deployment_version_for_current_task
                    .unwrap()
                    .deployment_name,
                "deployment-1"
            );
        }
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id.clone(),
            vec![],
        ))
        .await
        .unwrap();
        let task = core.poll_workflow_activation().await.unwrap();
        let query = assert_matches!(
            task.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::QueryWorkflow(q)),
            }] => q
        );
        assert_eq!(
            task.deployment_version_for_current_task
                .as_ref()
                .unwrap()
                .build_id,
            "1.0"
        );
        if !use_only_build_id {
            assert_eq!(
                task.deployment_version_for_current_task
                    .unwrap()
                    .deployment_name,
                "deployment-1"
            );
        }
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
            task.run_id,
            QueryResult {
                query_id: query.query_id.clone(),
                variant: Some(
                    QuerySuccess {
                        response: Some("done".into()),
                    }
                    .into(),
                ),
            }
            .into(),
        ))
        .await
        .unwrap();
    };
    join!(query_fut, complete_fut);

    client
        .signal_workflow_execution(
            workflow_id.clone(),
            "".to_string(),
            "whatever".to_string(),
            None,
            None,
        )
        .await
        .unwrap();

    let res = core.poll_workflow_activation().await.unwrap();
    assert_eq!(
        res.deployment_version_for_current_task
            .as_ref()
            .unwrap()
            .build_id,
        "2.0"
    );
    if !use_only_build_id {
        assert_eq!(
            res.deployment_version_for_current_task
                .unwrap()
                .deployment_name,
            "deployment-1"
        );
    }
    core.complete_execution(&res.run_id).await;
}

#[rstest::rstest]
#[tokio::test]
async fn nondeterminism_errors_fail_workflow_when_configured_to(
    #[values(true, false)] whole_worker: bool,
) {
    let (telemopts, addr, _aborter) = prom_metrics(None);
    let rt = CoreRuntime::new_assume_tokio(telemopts).unwrap();
    let wf_name = "nondeterminism_errors_fail_workflow_when_configured_to";
    let mut starter = CoreWfStarter::new_with_runtime(wf_name, rt);
    starter.worker_config.no_remote_activities(true);
    let typeset = HashSet::from([WorkflowErrorType::Nondeterminism]);
    if whole_worker {
        starter.worker_config.workflow_failure_errors(typeset);
    } else {
        starter
            .worker_config
            .workflow_types_to_failure_errors(HashMap::from([(wf_name.to_owned(), typeset)]));
    }
    let wf_id = starter.get_task_queue().to_owned();
    let mut worker = starter.worker().await;
    worker.fetch_results = false;

    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| async move {
        ctx.timer(Duration::from_secs(1000)).await;
        Ok(().into())
    });
    let client = starter.get_client().await;
    let core_worker = worker.core_worker.clone();
    starter.start_with_worker(wf_name, &mut worker).await;

    let stopper = async {
        // Wait for the timer to show up in history and then stop the worker
        loop {
            let hist = starter.get_history().await;
            let has_timer_event = hist
                .events
                .iter()
                .any(|e| matches!(e.event_type(), EventType::TimerStarted));
            if has_timer_event {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        core_worker.initiate_shutdown();
    };
    let runner = async {
        worker.run_until_done().await.unwrap();
    };
    join!(stopper, runner);

    // Restart the worker with a new, incompatible wf definition which will cause nondeterminism
    let mut starter = starter.clone_no_worker();
    let mut worker = starter.worker().await;
    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| async move {
        ctx.activity(ActivityOptions {
            activity_type: "echo_activity".to_string(),
            start_to_close_timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .await;
        Ok(().into())
    });
    // We need to generate a task so that we'll encounter the error (first avoid WFT timeout)
    client
        .reset_sticky_task_queue(wf_id.clone(), "".to_string())
        .await
        .unwrap();
    client
        .signal_workflow_execution(wf_id.clone(), "".to_string(), "hi".to_string(), None, None)
        .await
        .unwrap();
    worker.expect_workflow_completion(&wf_id, None);
    // If we don't fail the workflow on nondeterminism, we'll get stuck here retrying the WFT
    worker.run_until_done().await.unwrap();

    let body = metrics_tests::get_text(format!("http://{addr}/metrics")).await;
    let match_this = format!(
        "temporal_workflow_failed{{namespace=\"default\",\
         service_name=\"temporal-core-sdk\",\
         task_queue=\"{wf_id}\",workflow_type=\"{wf_name}\"}} 1"
    );
    assert!(body.contains(&match_this));
}

#[tokio::test]
async fn history_out_of_order_on_restart() {
    let wf_name = "history_out_of_order_on_restart";
    let mut starter = CoreWfStarter::new(wf_name);
    starter
        .worker_config
        .workflow_failure_errors([WorkflowErrorType::Nondeterminism]);
    let mut worker = starter.worker().await;
    let mut starter2 = starter.clone_no_worker();
    let mut worker2 = starter2.worker().await;

    static HIT_SLEEP: Notify = Notify::const_new();

    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        ctx.local_activity(LocalActivityOptions {
            activity_type: "echo".to_owned(),
            input: "hi".as_json_payload().unwrap(),
            start_to_close_timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .await;
        ctx.activity(ActivityOptions {
            activity_type: "echo".to_owned(),
            input: "hi".as_json_payload().unwrap(),
            start_to_close_timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .await;
        // Interrupt this sleep on first go
        HIT_SLEEP.notify_one();
        ctx.timer(Duration::from_secs(5)).await;
        Ok(().into())
    });
    worker.register_activity("echo", echo);

    worker2.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        ctx.local_activity(LocalActivityOptions {
            activity_type: "echo".to_owned(),
            input: "hi".as_json_payload().unwrap(),
            start_to_close_timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .await;
        // Timer is added after restarting workflow
        ctx.timer(Duration::from_secs(1)).await;
        ctx.activity(ActivityOptions {
            activity_type: "echo".to_owned(),
            input: "hi".as_json_payload().unwrap(),
            start_to_close_timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .await;
        ctx.timer(Duration::from_secs(2)).await;
        Ok(().into())
    });
    worker2.register_activity("echo", echo);
    worker
        .submit_wf(
            wf_name.to_owned(),
            wf_name.to_owned(),
            vec![],
            WorkflowOptions {
                execution_timeout: Some(Duration::from_secs(20)),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let w1 = async {
        worker.run_until_done().await.unwrap();
    };
    let w2 = async {
        // wait to hit sleep
        HIT_SLEEP.notified().await;
        starter.shutdown().await;
        // start new worker
        worker2.expect_workflow_completion(wf_name, None);
        worker2.run_until_done().await.unwrap();
    };
    join!(w1, w2);
    // The workflow should fail with the nondeterminism error
    let handle = starter
        .get_client()
        .await
        .get_untyped_workflow_handle(wf_name, "");
    let res = handle
        .get_workflow_result(Default::default())
        .await
        .unwrap();
    assert_matches!(res, WorkflowExecutionResult::Failed(_));
}
