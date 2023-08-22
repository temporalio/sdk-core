use crate::{
    test_help::{
        build_mock_pollers, canned_histories, hist_to_poll_resp, mock_worker, MockPollCfg,
    },
    worker::{client::mocks::mock_workflow_client, ManagedWFFunc, LEGACY_QUERY_ID},
};
use rstest::{fixture, rstest};
use std::{collections::VecDeque, time::Duration};
use temporal_sdk::{WfContext, WorkflowFunction};
use temporal_sdk_core_api::Worker;
use temporal_sdk_core_protos::{
    coresdk::{
        workflow_commands::{workflow_command::Variant::RespondToQuery, QueryResult, QuerySuccess},
        workflow_completion::WorkflowActivationCompletion,
    },
    temporal::api::{enums::v1::CommandType, query::v1::WorkflowQuery},
};
use temporal_sdk_core_test_utils::start_timer_cmd;

fn timers_wf(num_timers: u32) -> WorkflowFunction {
    WorkflowFunction::new(move |command_sink: WfContext| async move {
        for _ in 1..=num_timers {
            command_sink.timer(Duration::from_secs(1)).await;
        }
        Ok(().into())
    })
}

#[fixture(num_timers = 1)]
fn fire_happy_hist(num_timers: u32) -> ManagedWFFunc {
    let func = timers_wf(num_timers);
    // Add 1 b/c history takes # wf tasks, not timers
    let t = canned_histories::long_sequential_timers(num_timers as usize);
    ManagedWFFunc::new(t, func, vec![])
}

#[rstest]
#[case::one_timer(fire_happy_hist(1), 1)]
#[case::five_timers(fire_happy_hist(5), 5)]
#[tokio::test]
async fn replay_flag_is_correct(#[case] mut wfm: ManagedWFFunc, #[case] num_timers: usize) {
    // Verify replay flag is correct by constructing a workflow manager that already has a complete
    // history fed into it. It should always be replaying, because history is complete.

    for _ in 1..=num_timers {
        let act = wfm.get_next_activation().await.unwrap();
        assert!(act.is_replaying);
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
    }

    let act = wfm.get_next_activation().await.unwrap();
    assert!(act.is_replaying);
    let commands = wfm.get_server_commands().commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(
        commands[0].command_type,
        CommandType::CompleteWorkflowExecution as i32
    );
    wfm.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn replay_flag_is_correct_partial_history() {
    let func = timers_wf(1);
    // Add 1 b/c history takes # wf tasks, not timers
    let t = canned_histories::long_sequential_timers(2);
    let mut wfm =
        ManagedWFFunc::new_from_update(t.get_history_info(1).unwrap().into(), func, vec![]);

    let act = wfm.get_next_activation().await.unwrap();
    assert!(!act.is_replaying);
    let commands = wfm.get_server_commands().commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
    wfm.shutdown().await.unwrap();
}

#[tokio::test]
async fn replay_flag_correct_with_query() {
    let wfid = "fake_wf_id";
    let t = canned_histories::single_timer("1");
    let tasks = VecDeque::from(vec![
        {
            let mut pr = hist_to_poll_resp(&t, wfid.to_owned(), 2.into());
            // Server can issue queries that contain the WFT completion and the subsequent
            // commands, but not the consequences yet.
            pr.query = Some(WorkflowQuery {
                query_type: "query-type".to_string(),
                query_args: Some(b"hi".into()),
                header: None,
            });
            let h = pr.history.as_mut().unwrap();
            h.events.truncate(5);
            pr.started_event_id = 3;
            dbg!(&pr.resp);
            pr
        },
        hist_to_poll_resp(&t, wfid.to_owned(), 2.into()),
    ]);
    let mut mock = MockPollCfg::from_resp_batches(wfid, t, tasks, mock_workflow_client());
    mock.num_expected_legacy_query_resps = 1;
    let mut mock = build_mock_pollers(mock);
    mock.worker_cfg(|wc| wc.max_cached_workflows = 10);
    let core = mock_worker(mock);

    let task = core.poll_workflow_activation().await.unwrap();
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        start_timer_cmd(1, Duration::from_secs(1)),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(task.is_replaying);
    core.complete_workflow_activation(WorkflowActivationCompletion::from_cmd(
        task.run_id,
        RespondToQuery(QueryResult {
            query_id: LEGACY_QUERY_ID.to_string(),
            variant: Some(
                QuerySuccess {
                    response: Some("hi".into()),
                }
                .into(),
            ),
        }),
    ))
    .await
    .unwrap();

    let task = core.poll_workflow_activation().await.unwrap();
    assert!(!task.is_replaying);
}
