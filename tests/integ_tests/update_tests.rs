use assert_matches::assert_matches;
use std::time::Duration;
use temporal_client::WorkflowClientTrait;
use temporal_sdk_core::replay::HistoryForReplay;
use temporal_sdk_core_api::Worker;
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{workflow_activation_job, WorkflowActivationJob},
        workflow_commands::{update_response, CompleteWorkflowExecution, UpdateResponse},
        workflow_completion::WorkflowActivationCompletion,
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
}

#[tokio::test]
async fn update_insta_complete() {
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
                    response: Some(update_response::Response::Completed("done!".into())),
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
}
