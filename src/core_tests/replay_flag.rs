use crate::workflow::managed_wf::ManagedWFFunc;
use crate::{
    protos::coresdk::workflow_commands::StartTimer,
    protos::temporal::api::enums::v1::CommandType,
    test_help::canned_histories,
    test_workflow_driver::{WfContext, WorkflowFunction},
};
use rstest::{fixture, rstest};
use std::time::Duration;

fn timers_wf(num_timers: usize) -> WorkflowFunction {
    WorkflowFunction::new(move |mut command_sink: WfContext| async move {
        for tnum in 1..=num_timers {
            let timer = StartTimer {
                timer_id: format!("timer-{}", tnum),
                start_to_fire_timeout: Some(Duration::from_secs(1).into()),
            };
            command_sink.timer(timer).await;
        }
        Ok(().into())
    })
}

#[fixture(num_timers = 1)]
fn fire_happy_hist(num_timers: usize) -> ManagedWFFunc {
    let func = timers_wf(num_timers);
    // Add 1 b/c history takes # wf tasks, not timers
    let t = canned_histories::long_sequential_timers(num_timers + 1);
    ManagedWFFunc::new(t, func, vec![])
}

#[rstest]
#[case::one_timer(fire_happy_hist(1), 1)]
#[case::five_timers(fire_happy_hist(5), 5)]
#[tokio::test]
async fn replay_flag_is_correct(#[case] mut wfm: ManagedWFFunc, #[case] num_timers: usize) {
    // Verify replay flag is correct by constructing a workflow manager that already has a complete
    // history fed into it. The first (few, depending on test a params) activation(s) will be under
    // replay while the last should not

    for _ in 1..=num_timers {
        let act = wfm.get_next_activation().await.unwrap();
        assert!(act.is_replaying);
        let commands = wfm.get_server_commands().await.commands;
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
    }

    let act = wfm.get_next_activation().await.unwrap();
    assert!(!act.is_replaying);
    let commands = wfm.get_server_commands().await.commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(
        commands[0].command_type,
        CommandType::CompleteWorkflowExecution as i32
    );
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
    let commands = wfm.get_server_commands().await.commands;
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0].command_type, CommandType::StartTimer as i32);
}
