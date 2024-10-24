use anyhow::anyhow;
use assert_matches::assert_matches;
use futures_util::{future, future::join_all, StreamExt};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, LazyLock,
    },
    time::Duration,
};
use temporal_client::{Client, RetryClient, WorkflowClientTrait, WorkflowService};
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
        common::v1::WorkflowExecution,
        enums::v1::{EventType, ResetReapplyType, UpdateWorkflowExecutionLifecycleStage},
        update::{self, v1::WaitPolicy},
        workflowservice::v1::ResetWorkflowExecutionRequest,
    },
};
use temporal_sdk_core_test_utils::{
    drain_pollers_and_shutdown, init_core_and_create_wf, init_core_replay_preloaded,
    start_timer_cmd, CoreWfStarter, WorkerTestHelpers,
};
use tokio::{join, sync::Barrier};
use uuid::Uuid;

#[derive(Clone, Copy)]
enum FailUpdate {
    Yes,
    No,
}

#[derive(Clone, Copy)]
enum CompleteWorkflow {
    Yes,
    No,
}

#[rstest::rstest]
#[tokio::test]
async fn update_workflow(#[values(FailUpdate::Yes, FailUpdate::No)] will_fail: FailUpdate) {
    let mut starter = init_core_and_create_wf("update_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue();
    let update_id = "some_update";
    send_and_handle_update(
        workflow_id,
        update_id,
        will_fail,
        CompleteWorkflow::Yes,
        core.as_ref(),
        client.as_ref(),
    )
    .await;

    // Make sure replay works
    let history = client
        .get_workflow_execution_history(workflow_id.to_string(), None, vec![])
        .await
        .unwrap()
        .history
        .unwrap();
    let with_id = HistoryForReplay::new(history, workflow_id.to_string());
    let replay_worker = init_core_replay_preloaded(workflow_id, [with_id]);
    // Init workflow comes by itself
    let act = replay_worker.poll_workflow_activation().await.unwrap();
    replay_worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
        .await
        .unwrap();
    handle_update(will_fail, CompleteWorkflow::Yes, replay_worker.as_ref(), 0).await;
}

#[tokio::test]
async fn reapplied_updates_due_to_reset() {
    let mut starter = init_core_and_create_wf("update_workflow").await;
    let core = starter.get_worker().await;
    let client = starter.get_client().await;
    let workflow_id = starter.get_task_queue();
    let pre_reset_run_id = send_and_handle_update(
        workflow_id,
        "first-update",
        FailUpdate::No,
        CompleteWorkflow::Yes,
        core.as_ref(),
        client.as_ref(),
    )
    .await;

    // Reset to before the update was accepted
    let workflow_task_finish_event_id = 4;

    let mut client_mut = client.clone();
    let reset_response = WorkflowService::reset_workflow_execution(
        Arc::make_mut(&mut client_mut),
        ResetWorkflowExecutionRequest {
            namespace: client.namespace().into(),
            workflow_execution: Some(WorkflowExecution {
                workflow_id: workflow_id.into(),
                run_id: pre_reset_run_id.clone(),
            }),
            workflow_task_finish_event_id,
            reset_reapply_type: ResetReapplyType::AllEligible as i32,
            request_id: Uuid::new_v4().to_string(),
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .into_inner();

    // Accept and complete the reapplied update
    // Index here is 2 because there will be start workflow & update random seed (from the reset)
    // first.
    handle_update(FailUpdate::No, CompleteWorkflow::No, core.as_ref(), 2).await;

    // Send a second update and complete the workflow
    let post_reset_run_id = send_and_handle_update(
        workflow_id,
        "second-update",
        FailUpdate::No,
        CompleteWorkflow::Yes,
        core.as_ref(),
        client.as_ref(),
    )
    .await;

    assert_eq!(post_reset_run_id, reset_response.run_id);

    // Make sure replay works
    let history = client
        .get_workflow_execution_history(workflow_id.to_string(), Some(post_reset_run_id), vec![])
        .await
        .unwrap()
        .history
        .unwrap();
    let with_id = HistoryForReplay::new(history, workflow_id.to_string());

    let replay_worker = init_core_replay_preloaded(workflow_id, [with_id]);
    // Init workflow comes by itself
    let act = replay_worker.poll_workflow_activation().await.unwrap();
    replay_worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
        .await
        .unwrap();
    // We now recapitulate the actions that the worker took on first execution above, pretending
    // that we always followed the post-reset history.
    // First, we handled the post-reset reapplied update and did not complete the workflow.
    handle_update(
        FailUpdate::No,
        CompleteWorkflow::No,
        replay_worker.as_ref(),
        1,
    )
    .await;
    // Then the timer fires
    let act = replay_worker.poll_workflow_activation().await.unwrap();
    replay_worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
        .await
        .unwrap();
    // Then the client sent a second update; we handled it and completed the workflow.
    handle_update(
        FailUpdate::No,
        CompleteWorkflow::Yes,
        replay_worker.as_ref(),
        0,
    )
    .await;

    // This is a replay worker and there is a remaining activation containing a RemoveFromCache job.
    let act = replay_worker.poll_workflow_activation().await.unwrap();
    replay_worker
        .complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id))
        .await
        .unwrap();
    drain_pollers_and_shutdown(&replay_worker).await;
}

// Start a workflow, send an update, accept the update, complete the update, complete the workflow.
async fn send_and_handle_update(
    workflow_id: &str,
    update_id: &str,
    fail_update: FailUpdate,
    complete_workflow: CompleteWorkflow,
    core: &dyn Worker,
    client: &RetryClient<Client>,
) -> String {
    // Complete first task with no commands
    let act = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::empty(act.run_id.clone()))
        .await
        .unwrap();

    // Send the update to the server
    let update_task = async {
        client
            .update_workflow_execution(
                workflow_id.to_string(),
                act.run_id.to_string(),
                update_id.to_string(),
                WaitPolicy {
                    lifecycle_stage: UpdateWorkflowExecutionLifecycleStage::Completed as i32,
                },
                Some("hi".into()),
            )
            .await
            .unwrap()
    };

    // Accept update, complete update and complete workflow
    let processing_task = handle_update(fail_update, complete_workflow, core, 0);
    let (ur, _) = join!(update_task, processing_task);

    let v = ur.outcome.unwrap().value.unwrap();
    match fail_update {
        FailUpdate::Yes => assert_matches!(v, update::v1::outcome::Value::Failure(_)),
        FailUpdate::No => assert_matches!(v, update::v1::outcome::Value::Success(_)),
    }
    act.run_id
}

// Accept and then complete update. If `FailUpdate::Yes` then complete the update as a failure; if
// `CompleteWorkflow::Yes` then additionally complete the workflow. Timers are created when further
// activations are required (i.e., on accepting-but-not-completing the update, and on completing the
// update if not also completing the workflow).
async fn handle_update(
    fail_update: FailUpdate,
    complete_workflow: CompleteWorkflow,
    core: &dyn Worker,
    update_job_index: usize,
) {
    let act = core.poll_workflow_activation().await.unwrap();
    let pid = assert_matches!(
        &act.jobs[update_job_index],
        WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::DoUpdate(d)),
        } => &d.protocol_instance_id
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id,
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

    // Timer fires
    let act = core.poll_workflow_activation().await.unwrap();
    let update_response = match fail_update {
        FailUpdate::Yes => UpdateResponse {
            protocol_instance_id: pid.to_string(),
            response: Some(update_response::Response::Rejected(
                "uh oh spaghettios!".into(),
            )),
        },
        FailUpdate::No => UpdateResponse {
            protocol_instance_id: pid.to_string(),
            response: Some(update_response::Response::Completed("done!".into())),
        },
    };
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        act.run_id,
        vec![
            update_response.into(),
            match complete_workflow {
                CompleteWorkflow::Yes => CompleteWorkflowExecution { result: None }.into(),
                CompleteWorkflow::No => start_timer_cmd(1, Duration::from_millis(1)),
            },
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
                variant: Some(workflow_activation_job::Variant::InitializeWorkflow(_)),
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

    static BARR: LazyLock<Barrier> = LazyLock::new(|| Barrier::new(2));
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
            return Err(anyhow!("Fail first time").into());
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

#[tokio::test]
async fn update_after_empty_wft() {
    let wf_name = "update_after_empty_wft";
    let mut starter = CoreWfStarter::new(wf_name);
    let mut worker = starter.worker().await;
    let client = starter.get_client().await;

    static ACT_STARTED: AtomicBool = AtomicBool::new(false);
    worker.register_wf(wf_name.to_owned(), move |ctx: WfContext| async move {
        ctx.update_handler(
            "update",
            |_: &_, _: ()| Ok(()),
            move |ctx: UpdateContext, _: ()| async move {
                if ACT_STARTED.load(Ordering::Acquire) {
                    return Ok(());
                }
                ctx.wf_ctx
                    .activity(ActivityOptions {
                        activity_type: "echo".to_string(),
                        input: "hi!".as_json_payload().expect("serializes fine"),
                        start_to_close_timeout: Some(Duration::from_secs(2)),
                        ..Default::default()
                    })
                    .await;
                Ok(())
            },
        );
        let mut sig = ctx.make_signal_channel("signal");
        let sig_handle = async {
            sig.next().await;
            ACT_STARTED.store(true, Ordering::Release);
            ctx.activity(ActivityOptions {
                activity_type: "echo".to_string(),
                input: "hi!".as_json_payload().expect("serializes fine"),
                start_to_close_timeout: Some(Duration::from_secs(2)),
                ..Default::default()
            })
            .await;
            ACT_STARTED.store(false, Ordering::Release);
        };
        join!(sig_handle, async {
            ctx.timer(Duration::from_secs(2)).await;
        });
        Ok(().into())
    });
    worker.register_activity("echo", |_ctx: ActContext, echo_me: String| async move {
        Ok(echo_me)
    });

    let run_id = starter.start_with_worker(wf_name, &mut worker).await;
    let wf_id = starter.get_task_queue().to_string();
    let update = async {
        client
            .signal_workflow_execution(
                wf_id.clone(),
                "".to_string(),
                "signal".to_string(),
                None,
                None,
            )
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
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
    let runner = async {
        worker.run_until_done().await.unwrap();
    };
    join!(update, runner);
    starter
        .fetch_history_and_replay(wf_id, run_id, worker.inner_mut())
        .await
        .unwrap();
}
