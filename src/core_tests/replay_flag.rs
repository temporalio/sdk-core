use crate::{
    machines::{test_help::TestHistoryBuilder, WorkflowMachines},
    protos::coresdk::workflow_commands::StartTimer,
    protos::temporal::api::enums::v1::CommandType,
    test_help::canned_histories,
    test_workflow_driver::{CommandSender, TestWorkflowDriver},
    workflow::WorkflowManager,
};
use rstest::{fixture, rstest};
use std::time::Duration;

// TODO: Dedupe
#[fixture]
fn fire_happy_hist() -> (TestHistoryBuilder, WorkflowMachines) {
    let twd = TestWorkflowDriver::new(|mut command_sink: CommandSender| async move {
        let timer = StartTimer {
            timer_id: "timer1".to_string(),
            start_to_fire_timeout: Some(Duration::from_secs(5).into()),
        };
        command_sink.timer(timer).await;
        command_sink.complete_workflow_execution();
    });

    let t = canned_histories::single_timer("timer1");
    let state_machines = WorkflowMachines::new(
        "wfid".to_string(),
        "runid".to_string(),
        Box::new(twd).into(),
    );

    assert_eq!(2, t.as_history().get_workflow_task_count(None).unwrap());
    (t, state_machines)
}

#[rstest]
fn replay_flag_is_correct(fire_happy_hist: (TestHistoryBuilder, WorkflowMachines)) {
    let (t, state_machines) = fire_happy_hist;
    let mut wfm = WorkflowManager::new_from_machines(t.as_history(), state_machines).unwrap();

    let act = wfm.get_next_activation().unwrap().unwrap();
    assert!(act.is_replaying);
    let commands = wfm.get_server_commands().commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);

    let act = wfm.get_next_activation().unwrap().unwrap();
    assert!(!act.is_replaying);
    let commands = wfm.get_server_commands().commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands.len(), 1);
    assert_eq!(
        commands[0].command_type,
        CommandType::CompleteWorkflowExecution as i32
    );
}

#[rstest]
fn replay_flag_is_correct_partial_history(fire_happy_hist: (TestHistoryBuilder, WorkflowMachines)) {
    let (t, state_machines) = fire_happy_hist;
    let mut wfm =
        WorkflowManager::new_from_machines(t.as_partial_history(3), state_machines).unwrap();

    let act = wfm.get_next_activation().unwrap().unwrap();
    assert!(!act.is_replaying);
    let commands = wfm.get_server_commands().commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
}
