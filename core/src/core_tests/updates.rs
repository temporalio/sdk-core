use crate::{
    test_help::{build_mock_pollers, hist_to_poll_resp, mock_worker, MockPollCfg, ResponseType},
    worker::client::mocks::mock_workflow_client,
};
use temporal_sdk_core_api::Worker;
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_activation::{workflow_activation_job, WorkflowActivationJob},
        workflow_commands::{update_response::Response, CompleteWorkflowExecution, UpdateResponse},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{
        common::v1::Payload,
        enums::v1::EventType,
        protocol::v1::{message, Message},
        update,
        update::v1::Acceptance,
        workflowservice::v1::RespondWorkflowTaskCompletedResponse,
    },
    utilities::pack_any,
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

#[tokio::test]
async fn initial_request_sent_back() {
    crate::telemetry::test_telem_console();
    let wfid = "fakeid";
    let mut t = TestHistoryBuilder::default();
    t.add_by_type(EventType::WorkflowExecutionStarted);
    t.add_workflow_task_scheduled_and_started();

    let update_id = "upd-1";
    let mut poll_resp = hist_to_poll_resp(&t, wfid, ResponseType::AllHistory);
    let upd_req_body = update::v1::Request {
        meta: Some(update::v1::Meta {
            update_id: update_id.to_string(),
            identity: "bro".to_string(),
        }),
        input: Some(update::v1::Input {
            header: None,
            name: "updayte".to_string(),
            args: None,
        }),
    };
    poll_resp.messages.push(Message {
        id: "upd-req-1".to_string(),
        protocol_instance_id: update_id.to_string(),
        body: Some(
            pack_any(
                "type.googleapis.com/temporal.api.update.v1.Request".to_string(),
                &upd_req_body,
            )
            .unwrap(),
        ),
        sequencing_id: Some(message::SequencingId::EventId(1)),
    });

    let mut mock_client = mock_workflow_client();
    mock_client
        .expect_complete_workflow_task()
        .times(1)
        .returning(move |mut resp| {
            let accept_msg = resp.messages.pop().unwrap();
            let acceptance = accept_msg
                .body
                .unwrap()
                .unpack_as(Acceptance::default())
                .unwrap();
            assert_eq!(acceptance.accepted_request.unwrap(), upd_req_body);
            Ok(RespondWorkflowTaskCompletedResponse::default())
        });
    let mh = MockPollCfg::from_resp_batches(wfid, t, [poll_resp], mock_client);
    let mut mock = build_mock_pollers(mh);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 1);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        UpdateResponse {
            protocol_instance_id: update_id.to_string(),
            response: Some(Response::Accepted(())),
        }
        .into(),
    ))
    .await
    .unwrap();
}
