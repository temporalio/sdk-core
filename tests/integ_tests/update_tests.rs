use assert_matches::assert_matches;
use futures_util::future;
use std::time::Duration;
use temporal_client::WorkflowClientTrait;
use temporal_sdk_core::replay::HistoryForReplay;
use temporal_sdk_core_api::Worker;
use temporal_sdk_core_protos::{
    coresdk::{
        activity_result::ActivityExecutionResult,
        workflow_activation::{workflow_activation_job, WorkflowActivationJob},
        workflow_commands::{
            update_response, CompleteWorkflowExecution, ScheduleLocalActivity, UpdateResponse,
        },
        workflow_completion::WorkflowActivationCompletion,
        ActivityTaskCompletion,
    },
    temporal::api::{
        enums::v1::UpdateWorkflowExecutionLifecycleStage, update, update::v1::WaitPolicy,
    },
};
use temporal_sdk_core_test_utils::{
    init_core_and_create_wf, init_core_replay_preloaded, start_timer_cmd, WorkerTestHelpers,
};
use tokio::join;

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
    // On replay, the first activation has start workflow & update, but on first execution, it does
    // not - can happen if update is waiting on some condition.
    let pid = assert_matches!(
        res.jobs.last().unwrap(),
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
    // TODO: Verify no update in history
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
            assert_matches!(
                res.jobs.as_slice(),
                [WorkflowActivationJob {
                    variant: Some(workflow_activation_job::Variant::RemoveFromCache(_))
                }]
            );
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
