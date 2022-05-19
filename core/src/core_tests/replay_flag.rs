use crate::{test_help::canned_histories, worker::ManagedWFFunc};
use rstest::{fixture, rstest};
use std::time::Duration;
use temporal_sdk::{WfContext, WorkflowFunction};
use temporal_sdk_core_protos::temporal::api::enums::v1::CommandType;

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
