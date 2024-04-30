use anyhow::{anyhow, bail};
use assert_matches::assert_matches;
use futures_util::{future, future::join_all, StreamExt};
use once_cell::sync::Lazy;
use std::{
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::Duration,
};
use temporal_client::WorkflowClientTrait;
use temporal_sdk::{ActContext, ActivityOptions, LocalActivityOptions, UpdateContext, WfContext};
use temporal_sdk_core::replay::HistoryForReplay;
use temporal_sdk_core_api::Worker;
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::ActivityExecutionResult,
        workflow_activation::{
            remove_from_cache::EvictionReason, workflow_activation_job, WorkflowActivationJob,
        },
        workflow_commands::{
            update_response, CompleteWorkflowExecution, ScheduleLocalActivity, UpdateResponse,
        },
        workflow_completion::WorkflowActivationCompletion,
        ActivityTaskCompletion, AsJsonPayloadExt, IntoPayloadsExt,
    },
    temporal::api::{
        enums::v1::{EventType, UpdateWorkflowExecutionLifecycleStage},
        update,
        update::v1::WaitPolicy,
    },
};
use temporal_sdk_core_test_utils::{
    init_core_and_create_wf, init_core_replay_preloaded, start_timer_cmd, CoreWfStarter,
    WorkerTestHelpers,
};
use tokio::{join, sync::Barrier};

#[rstest::rstest]
#[tokio::test]
async fn update_workflow(#[values(true, false)] will_fail: bool) {
    let mut starter = init_core_and_create_wf("update_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue().to_string();

    let update_id = "some_update";
    // Task is completed with no commands
    let res = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id.clone()))
        .await
        .unwrap();

    // Send the update to the server
    let update_task = async {
        client
            .update_workflow_execution(
                workflow_id.to_string(),
                res.run_id.to_string(),
                update_id.to_string(),
                WaitPolicy {
                    lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
                },
                Some("hi".into()),
            )
            .await
            .unwrap()
    };

    let processing_task = _do_update_workflow(will_fail, core.as_ref());
    let (ur, _) = join!(update_task, processing_task);

    let v = ur.outcome.unwrap().value.unwrap();
    if will_fail {
        assert_matches!(v, update::v1::outcome::Value::Failure(_));
    } else {
        assert_matches!(v, update::v1::outcome::Value::Success(_));
    }

    // Make sure replay works
    let history = client
        .get_workflow_execution_history(workflow_id.clone(), None, vec![])
        .await
        .unwrap()
        .history
        .unwrap();
    let with_id = HistoryForReplay::new(history, workflow_id.clone());
    let replay_worker = init_core_replay_preloaded(&workflow_id, [with_id]);
    _do_update_workflow(will_fail, replay_worker.as_ref()).await;
}

async fn _do_update_workflow(will_fail: bool, core: &dyn Worker) {
    let res = core.poll_workflow_activation().await.unwrap();
    // On replay, the first activation has update & start workflow, but on first execution, it does
    // not - can happen if update is waiting on some condition.
    let pid = assert_matches!(
        &res.jobs[0],
        WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::DoUpdate(d)),
        } => &d.protocol_instance_id
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id,
        vec![
            UpdateResponse {
                protocol_instance_id: pid.to_string(),
                response: Some(update_response::Response::Accepted(())),
            }
            .into(),
            start_timer_cmd(1, Duration::from_millis(1)),
        ],
    ))
    .await
    .unwrap();

    let response = if will_fail {
        UpdateResponse {
            protocol_instance_id: pid.to_string(),
            response: Some(update_response::Response::Rejected(
                "uh oh spaghettios!".into(),
            )),
        }
    } else {
        UpdateResponse {
            protocol_instance_id: pid.to_string(),
            response: Some(update_response::Response::Completed("done!".into())),
        }
    };
    let res = core.poll_workflow_activation().await.unwrap();
    // Timer fires
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id,
        vec![
            response.into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();
}

#[tokio::test]
async fn update_rejection() {
    let mut starter = init_core_and_create_wf("update_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue().to_string();

    let update_id = "some_update";
    let res = core.poll_workflow_activation().await.unwrap();
    // Task is completed with no commands
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id.clone(),
        vec![],
    ))
    .await
    .unwrap();

    // Send the update to the server
    let update_task = async {
        client
            .update_workflow_execution(
                workflow_id.to_string(),
                res.run_id.to_string(),
                update_id.to_string(),
                WaitPolicy {
                    lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
                },
                Some("hi".into()),
            )
            .await
            .unwrap();
    };

    let processing_task = async {
        let res = core.poll_workflow_activation().await.unwrap();
        let pid = assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(d)),
            }] => &d.protocol_instance_id
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![
                UpdateResponse {
                    protocol_instance_id: pid.to_string(),
                    response: Some(update_response::Response::Rejected("bah humbug".into())),
                }
                .into(),
                start_timer_cmd(1, Duration::from_millis(1)),
            ],
        ))
        .await
        .unwrap();

        let res = core.poll_workflow_activation().await.unwrap();
        core.complete_execution(&res.run_id).await;
    };
    join!(update_task, processing_task);
    let history = client
        .get_workflow_execution_history(workflow_id, None, vec![])
        .await
        .unwrap()
        .history
        .unwrap();
    let has_update_event = history.events.iter().any(|e| {
        matches!(
            e.event_type(),
            EventType::WorkflowExecutionUpdateAccepted | EventType::WorkflowExecutionUpdateRejected
        )
    });
    assert!(!has_update_event);
}

#[rstest::rstest]
#[tokio::test]
async fn update_insta_complete(#[values(true, false)] accept_first: bool) {
    let mut starter = init_core_and_create_wf("update_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue().to_string();

    let update_id = "some_update";
    let res = core.poll_workflow_activation().await.unwrap();
    // Task is completed with no commands
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id.clone(),
        vec![],
    ))
    .await
    .unwrap();

    // Send the update to the server
    let (update_task, stop_wait_update) = future::abortable(async {
        client
            .update_workflow_execution(
                workflow_id.to_string(),
                res.run_id.to_string(),
                update_id.to_string(),
                WaitPolicy {
                    lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
                },
                Some("hi".into()),
            )
            .await
            .unwrap();
    });

    let processing_task = async {
        let res = core.poll_workflow_activation().await.unwrap();
        let pid = assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(d)),
            }] => &d.protocol_instance_id
        );
        let mut cmds = vec![
            UpdateResponse {
                protocol_instance_id: pid.to_string(),
                response: Some(update_response::Response::Completed("done!".into())),
            }
            .into(),
            start_timer_cmd(1, Duration::from_millis(1)),
        ];
        if accept_first {
            cmds.insert(
                0,
                UpdateResponse {
                    protocol_instance_id: pid.to_string(),
                    response: Some(update_response::Response::Accepted(())),
                }
                .into(),
            )
        };
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id, cmds,
        ))
        .await
        .unwrap();

        if !accept_first {
            // evicted, because lang must send accept first before completing
            let res = core.poll_workflow_activation().await.unwrap();
            let cause = assert_matches!(
                res.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::RemoveFromCache(c))
                }] => c
            );
            assert_eq!(cause.reason(), EvictionReason::Nondeterminism);
            core.complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id))
                .await
                .unwrap();
            // Need to do this b/c currently server doesn't properly terminate update client call
            // if workflow completes
            stop_wait_update.abort();
        }

        let res = core.poll_workflow_activation().await.unwrap();
        core.complete_execution(&res.run_id).await;
    };
    let _ = join!(update_task, processing_task);
}

#[tokio::test]
async fn update_complete_after_accept_without_new_task() {
    let mut starter = init_core_and_create_wf("update_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue().to_string();

    let update_id = "some_update";
    let res = core.poll_workflow_activation().await.unwrap();
    // Task is completed with no commands
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        res.run_id.clone(),
        vec![],
    ))
    .await
    .unwrap();

    // Send the update to the server
    let update_task = async {
        client
            .update_workflow_execution(
                workflow_id.to_string(),
                res.run_id.to_string(),
                update_id.to_string(),
                WaitPolicy {
                    lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
                },
                Some("hi".into()),
            )
            .await
            .unwrap();
    };

    let act_polling = async {
        let task = core.poll_activity_task().await.unwrap();
        core.complete_activity_task(ActivityTaskCompletion {
            task_token: task.task_token,
            result: Some(ActivityExecutionResult::ok(vec![1].into())),
        })
        .await
        .unwrap();
    };

    let processing_task = async {
        let res = core.poll_workflow_activation().await.unwrap();
        let pid = assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(d)),
            }] => &d.protocol_instance_id
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![
                UpdateResponse {
                    protocol_instance_id: pid.to_string(),
                    response: Some(update_response::Response::Accepted(())),
                }
                .into(),
                ScheduleLocalActivity {
                    seq: 1,
                    activity_id: "1".to_string(),
                    activity_type: "test_act".to_string(),
                    start_to_close_timeout: Some(prost_dur!(from_secs(30))),
                    ..Default::default()
                }
                .into(),
            ],
        ))
        .await
        .unwrap();

        let res = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::ResolveActivity(_)),
            }]
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![
                UpdateResponse {
                    protocol_instance_id: pid.to_string(),
                    response: Some(update_response::Response::Completed("done!".into())),
                }
                .into(),
                CompleteWorkflowExecution { result: None }.into(),
            ],
        ))
        .await
        .unwrap();
    };
    join!(update_task, act_polling, processing_task);
}

#[tokio::test]
async fn update_speculative_wft() {
    let mut starter = init_core_and_create_wf("update_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue().to_string();

    let update_id = "some_update";

    // Send update after the timer has fired
    let barr = Barrier::new(2);
    let sender_task = async {
        barr.wait().await;
        client
            .update_workflow_execution(
                workflow_id.to_string(),
                "".to_string(),
                update_id.to_string(),
                WaitPolicy {
                    lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
                },
                Some("hi".into()),
            )
            .await
            .unwrap();
        barr.wait().await;
        client
            .signal_workflow_execution(
                workflow_id.to_string(),
                "".to_string(),
                "hi".into(),
                None,
                None,
            )
            .await
            .unwrap();
    };

    let processing_task = async {
        let res = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::StartWorkflow(_)),
            }]
        );
        core.complete_workflow_activation(WorkflowActivationCompletion::empty(res.run_id))
            .await
            .unwrap();
        barr.wait().await;

        let res = core.poll_workflow_activation().await.unwrap();
        let pid = assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(d)),
            }] => &d.protocol_instance_id
        );
        // Reject the update
        core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
            res.run_id,
            vec![UpdateResponse {
                protocol_instance_id: pid.to_string(),
                response: Some(update_response::Response::Rejected("nope!".into())),
            }
            .into()],
        ))
        .await
        .unwrap();
        // Send the signal
        barr.wait().await;
        // Without proper implementation of last-started-wft reset, we'd get stuck here b/c we'd
        // skip over the signal event.
        let res = core.poll_workflow_activation().await.unwrap();
        assert_matches!(
            res.jobs.as_slice(),
            [WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
            }]
        );
        core.complete_execution(&res.run_id).await;
    };
    join!(sender_task, processing_task);
}

#[tokio::test]
async fn update_with_local_acts() {
    let wf_name = "update_with_local_acts";
    let mut starter = CoreWfStarter::new(wf_name);
    // Short task timeout to get activities to heartbeat without taking ages
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| async move {
        ctx.update_handler(
            "update",
            |_: &_, _: ()| Ok(()),
            move |ctx: UpdateContext, _: ()| async move {
                ctx.wf_ctx
                    .local_activity(LocalActivityOptions {
                        activity_type: "echo_activity".to_string(),
                        input: "hi!".as_json_payload().expect("serializes fine"),
                        ..Default::default()
                    })
                    .await;
                Ok("hi")
            },
        );
        let mut sig = ctx.make_signal_channel("done");
        sig.next().await;
        Ok(().into())
    });
    worker.register_activity(
        "echo_activity",
        |_ctx: ActContext, echo_me: String| async move {
            // Sleep so we'll heartbeat
            tokio::time::sleep(Duration::from_secs(3)).await;
            Ok(echo_me)
        },
    );

    let run_id = starter.start_with_worker(wf_name, &mut worker).await;
    let wf_id = starter.get_task_queue().to_string();
    let update = async {
        // make sure update has a chance to get registered
        tokio::time::sleep(Duration::from_millis(100)).await;
        // do a handful at once
        let updates = (1..=5).map(|_| {
            client.update_workflow_execution(
                wf_id.clone(),
                "".to_string(),
                "update".to_string(),
                WaitPolicy {
                    lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
                },
                [().as_json_payload().unwrap()].into_payloads(),
            )
        });
        for res in join_all(updates).await {
            assert!(res.unwrap().outcome.unwrap().is_success());
        }
        client
            .signal_workflow_execution(
                wf_id.clone(),
                "".to_string(),
                "done".to_string(),
                None,
                None,
            )
            .await
            .unwrap();
    };
    let run = async {
        worker.run_until_done().await.unwrap();
    };
    join!(update, run);
    starter
        .fetch_history_and_replay(wf_id, run_id, worker.inner_mut())
        .await
        .unwrap();
}

#[tokio::test]
async fn update_rejection_sdk() {
    let wf_name = "update_rejection_sdk";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        ctx.update_handler(
            "update",
            |_: &_, _: ()| Err(anyhow!("ahhhhh noooo")),
            move |_: UpdateContext, _: ()| async { Ok("hi") },
        );
        ctx.timer(Duration::from_secs(1)).await;
        Ok(().into())
    });

    let run_id = starter.start_with_worker(wf_name, &mut worker).await;
    let wf_id = starter.get_task_queue().to_string();
    let update = async {
        let res = client
            .update_workflow_execution(
                wf_id.clone(),
                "".to_string(),
                "update".to_string(),
                WaitPolicy {
                    lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
                },
                [().as_json_payload().unwrap()].into_payloads(),
            )
            .await
            .unwrap();
        assert!(!res.outcome.unwrap().is_success());
    };
    let run = async {
        worker.run_until_done().await.unwrap();
    };
    join!(update, run);
    starter
        .fetch_history_and_replay(wf_id, run_id, worker.inner_mut())
        .await
        .unwrap();
}

#[tokio::test]
async fn update_fail_sdk() {
    let wf_name = "update_fail_sdk";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        ctx.update_handler(
            "update",
            |_: &_, _: ()| Ok(()),
            move |_: UpdateContext, _: ()| async { Err::<(), _>(anyhow!("nooooo")) },
        );
        ctx.timer(Duration::from_secs(1)).await;
        Ok(().into())
    });

    let run_id = starter.start_with_worker(wf_name, &mut worker).await;
    let wf_id = starter.get_task_queue().to_string();
    let update = async {
        let res = client
            .update_workflow_execution(
                wf_id.clone(),
                "".to_string(),
                "update".to_string(),
                WaitPolicy {
                    lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
                },
                [().as_json_payload().unwrap()].into_payloads(),
            )
            .await
            .unwrap();
        assert!(!res.outcome.unwrap().is_success());
    };
    let run = async {
        worker.run_until_done().await.unwrap();
    };
    join!(update, run);
    starter
        .fetch_history_and_replay(wf_id, run_id, worker.inner_mut())
        .await
        .unwrap();
}

#[tokio::test]
async fn update_timer_sequence() {
    let wf_name = "update_timer_sequence";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        ctx.update_handler(
            "update",
            |_: &_, _: ()| Ok(()),
            move |ctx: UpdateContext, _: ()| async move {
                ctx.wf_ctx.timer(Duration::from_millis(1)).await;
                ctx.wf_ctx.timer(Duration::from_millis(1)).await;
                Ok("done")
            },
        );
        ctx.timer(Duration::from_secs(2)).await;
        Ok(().into())
    });

    let run_id = starter.start_with_worker(wf_name, &mut worker).await;
    let wf_id = starter.get_task_queue().to_string();
    let update = async {
        let res = client
            .update_workflow_execution(
                wf_id.clone(),
                "".to_string(),
                "update".to_string(),
                WaitPolicy {
                    lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
                },
                [().as_json_payload().unwrap()].into_payloads(),
            )
            .await
            .unwrap();
        assert!(res.outcome.unwrap().is_success());
    };
    let run = async {
        worker.run_until_done().await.unwrap();
    };
    join!(update, run);
    starter
        .fetch_history_and_replay(wf_id, run_id, worker.inner_mut())
        .await
        .unwrap();
}

#[tokio::test]
async fn task_failure_during_validation() {
    let wf_name = "task_failure_during_validation";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    static FAILCT: AtomicUsize = AtomicUsize::new(0);
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        ctx.update_handler(
            "update",
            |_: &_, _: ()| {
                if FAILCT.fetch_add(1, Ordering::Relaxed) < 2 {
                    panic!("ahhhhhh");
                }
                Ok(())
            },
            move |_: UpdateContext, _: ()| async move { Ok("done") },
        );
        ctx.timer(Duration::from_secs(1)).await;
        Ok(().into())
    });

    let run_id = starter.start_with_worker(wf_name, &mut worker).await;
    let wf_id = starter.get_task_queue().to_string();
    let update = async {
        let res = client
            .update_workflow_execution(
                wf_id.clone(),
                "".to_string(),
                "update".to_string(),
                WaitPolicy {
                    lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
                },
                [().as_json_payload().unwrap()].into_payloads(),
            )
            .await
            .unwrap();
        assert!(res.outcome.unwrap().is_success());
    };
    let run = async {
        worker.run_until_done().await.unwrap();
    };
    join!(update, run);
    starter
        .fetch_history_and_replay(wf_id.clone(), run_id, worker.inner_mut())
        .await
        .unwrap();
    // Verify we did not spam task failures. There should only be one.
    let history = client
        .get_workflow_execution_history(wf_id, None, vec![])
        .await
        .unwrap()
        .history
        .unwrap();
    assert_eq!(
        history
            .events
            .iter()
            .filter(|he| he.event_type() == EventType::WorkflowTaskFailed)
            .count(),
        1
    );
}

#[tokio::test]
async fn task_failure_after_update() {
    let wf_name = "task_failure_after_update";
    let mut starter = CoreWfStarter::new(wf_name);
    starter.worker_config.no_remote_activities(true);
    starter.workflow_options.task_timeout = Some(Duration::from_secs(1));
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;
    static FAILCT: AtomicUsize = AtomicUsize::new(0);
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        ctx.update_handler(
            "update",
            |_: &_, _: ()| Ok(()),
            move |_: UpdateContext, _: ()| async move { Ok("done") },
        );
        ctx.timer(Duration::from_millis(1)).await;
        if FAILCT.fetch_add(1, Ordering::Relaxed) < 1 {
            panic!("ahhhhhh");
        }
        Ok(().into())
    });

    let run_id = starter.start_with_worker(wf_name, &mut worker).await;
    let wf_id = starter.get_task_queue().to_string();
    let update = async {
        let res = client
            .update_workflow_execution(
                wf_id.clone(),
                "".to_string(),
                "update".to_string(),
                WaitPolicy {
                    lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
                },
                [().as_json_payload().unwrap()].into_payloads(),
            )
            .await
            .unwrap();
        assert!(res.outcome.unwrap().is_success());
    };
    let run = async {
        worker.run_until_done().await.unwrap();
    };
    join!(update, run);
    starter
        .fetch_history_and_replay(wf_id.clone(), run_id, worker.inner_mut())
        .await
        .unwrap();
}

#[tokio::test]
async fn worker_restarted_in_middle_of_update() {
    let wf_name = "worker_restarted_in_middle_of_update";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;

    static BARR: Lazy<Barrier> = Lazy::new(|| Barrier::new(2));
    static ACT_RAN: AtomicBool = AtomicBool::new(false);
    worker.register_wf(wf_name.to_owned(), |ctx: WfContext| async move {
        ctx.update_handler(
            "update",
            |_: &_, _: ()| Ok(()),
            move |ctx: UpdateContext, _: ()| async move {
                ctx.wf_ctx
                    .activity(ActivityOptions {
                        activity_type: "blocks".to_string(),
                        input: "hi!".as_json_payload().expect("serializes fine"),
                        start_to_close_timeout: Some(Duration::from_secs(2)),
                        ..Default::default()
                    })
                    .await;
                Ok(())
            },
        );
        let mut sig = ctx.make_signal_channel("done");
        sig.next().await;
        Ok(().into())
    });
    worker.register_activity("blocks", |_ctx: ActContext, echo_me: String| async move {
        BARR.wait().await;
        if !ACT_RAN.fetch_or(true, Ordering::Relaxed) {
            // On first run fail the task so we'll get retried on the new worker
            bail!("Fail first time");
        }
        Ok(echo_me)
    });

    let run_id = starter.start_with_worker(wf_name, &mut worker).await;
    let wf_id = starter.get_task_queue().to_string();
    let update = async {
        let res = client
            .update_workflow_execution(
                wf_id.clone(),
                "".to_string(),
                "update".to_string(),
                WaitPolicy {
                    lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
                },
                [().as_json_payload().unwrap()].into_payloads(),
            )
            .await
            .unwrap();
        assert!(res.outcome.unwrap().is_success());
        client
            .signal_workflow_execution(
                wf_id.clone(),
                "".to_string(),
                "done".to_string(),
                None,
                None,
            )
            .await
            .unwrap();
    };
    let core_worker = starter.get_worker().await;
    let mut second_starter = starter.clone_no_worker();
    let stopper = async {
        // Wait for the activity to start
        BARR.wait().await;
        core_worker.initiate_shutdown();
        // Allow it to start again, the second time
        BARR.wait().await;
        // Poke the workflow off the sticky queue to get it to complete faster than WFT timeout
        client
            .reset_sticky_task_queue(wf_id.clone(), run_id.clone())
            .await
            .unwrap();
    };
    let run = async {
        // This run attempt will get shut down
        worker.inner_mut().run().await.unwrap();
        // Start up a new worker
        let new_worker = second_starter.get_worker().await;
        // Replace with new core and run again
        worker.inner_mut().with_new_core_worker(new_worker);
        worker.run_until_done().await.unwrap();
    };
    join!(update, run, stopper);
    starter
        .fetch_history_and_replay(wf_id, run_id, worker.inner_mut())
        .await
        .unwrap();
}
