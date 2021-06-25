use crate::{
    machines::WorkflowMachines,
    protos::coresdk::workflow_commands::StartTimer,
    protos::temporal::api::enums::v1::CommandType,
    test_help::canned_histories,
    test_workflow_driver::{CommandSender, TestWorkflowDriver},
    workflow::WorkflowManager,
};
use rstest::{fixture, rstest};
use std::time::Duration;

fn timers_wf(num_timers: usize) -> TestWorkflowDriver {
    TestWorkflowDriver::new(|mut command_sink: CommandSender| async move {
        for tnum in 1..=num_timers {
            let timer = StartTimer {
                timer_id: format!("timer-{}", tnum),
                start_to_fire_timeout: Some(Duration::from_secs(1).into()),
            };
            command_sink.timer(timer).await;
        }
        command_sink.complete_workflow_execution();
    })
}

#[fixture(num_timers = 1)]
fn fire_happy_hist(num_timers: usize) -> WorkflowMachines {
    let twd = timers_wf(num_timers);
    // Add 1 b/c history takes # wf tasks, not timers
    let t = canned_histories::long_sequential_timers(num_timers + 1);
    WorkflowMachines::new(
        "wfid".to_string(),
        "runid".to_string(),
        t.as_history_update(),
        Box::new(twd).into(),
    )
}

#[rstest]
#[case::one_timer(fire_happy_hist(1), 1)]
#[case::five_timers(fire_happy_hist(5), 5)]
fn replay_flag_is_correct(#[case] setup: WorkflowMachines, #[case] num_timers: usize) {
    // Verify replay flag is correct by constructing a workflow manager that already has a complete
    // history fed into it. The first (few, depending on test a params) activation(s) will be under
    // replay while the last should not
    let mut wfm = WorkflowManager::new_from_machines(setup);

    for _ in 1..=num_timers {
        let act = wfm.get_next_activation().unwrap();
        assert!(act.is_replaying);
        let commands = wfm.get_server_commands().commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
    }

    let act = wfm.get_next_activation().unwrap();
    assert!(!act.is_replaying);
    let commands = wfm.get_server_commands().commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(
        commands[0].command_type,
        CommandType::CompleteWorkflowExecution as i32
    );
}

#[test]
fn replay_flag_is_correct_partial_history() {
    let twd = timers_wf(1);
    // Add 1 b/c history takes # wf tasks, not timers
    let t = canned_histories::long_sequential_timers(2);
    let state_machines = WorkflowMachines::new(
        "wfid".to_string(),
        "runid".to_string(),
        // Only provide history up to the first workflow task
        t.get_history_info(1).unwrap().into(),
        Box::new(twd).into(),
    );
    let mut wfm = WorkflowManager::new_from_machines(state_machines);

    let act = wfm.get_next_activation().unwrap();
    assert!(!act.is_replaying);
    let commands = wfm.get_server_commands().commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
}
