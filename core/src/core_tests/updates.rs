use crate::test_help::{build_mock_pollers, mock_worker, MockPollCfg, ResponseType};
use temporal_sdk_core_api::Worker;
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{workflow_activation_job, WorkflowActivationJob},
        workflow_commands::{update_response::Response, CompleteWorkflowExecution, UpdateResponse},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{common::v1::Payload, enums::v1::EventType},
    TestHistoryBuilder,
};

#[tokio::test]
async fn replay_with_empty_first_task() {
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_full_wf_task();
    t.add_full_wf_task();
    let accept_id = t.add_update_accepted("upd1", "update");
    t.add_we_signaled("hi", vec![]);
    t.add_full_wf_task();
    t.add_update_completed(accept_id);
    t.add_workflow_execution_completed();

    let mock = MockPollCfg::from_resps(t, [ResponseType::AllHistory]);
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::DoUpdate(_)),
            },
            WorkflowActivationJob {
                variant: Some(workflow_activation_job::Variant::StartWorkflow(_)),
            },
        ]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        UpdateResponse {
            protocol_instance_id: "upd1".to_string(),
            response: Some(Response::Accepted(())),
        }
        .into(),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert_matches!(
        task.jobs.as_slice(),
        [WorkflowActivationJob {
            variant: Some(workflow_activation_job::Variant::SignalWorkflow(_)),
        }]
    );
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
        task.run_id,
        vec![
            UpdateResponse {
                protocol_instance_id: "upd1".to_string(),
                response: Some(Response::Completed(Payload::default())),
            }
            .into(),
            CompleteWorkflowExecution { result: None }.into(),
        ],
    ))
    .await
    .unwrap();
}
