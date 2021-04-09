//! Look, I'm not happy about the code in here, and you shouldn't be either. This is all an awful,
//! dirty hack to work around the fact that there's no such thing as "run this after all unit tests"
//! in stable Rust. Don't do the things in here. They're bad. This is test only code, and should
//! never ever be removed from behind `#[cfg(test)]` compilation.

use dashmap::{mapref::entry::Entry, DashMap, DashSet};
use std::{
    sync::{
        mpsc::{sync_channel, SyncSender},
        Mutex,
    },
    thread::JoinHandle,
    time::Duration,
};

// During test we want to know about which transitions we've covered in state machines
lazy_static::lazy_static! {
    static ref COVERED_TRANSITIONS: DashMap<String, DashSet<CoveredTransition>>
        = DashMap::new();
    static ref COVERAGE_SENDER: SyncSender<(String, CoveredTransition)> =
        spawn_save_coverage_at_end();
    static ref THREAD_HANDLE: Mutex<Option<JoinHandle<()>>> = Mutex::new(None);
}

#[derive(Eq, PartialEq, Hash, Debug)]
struct CoveredTransition {
    from_state: String,
    to_state: String,
    event: String,
}

pub fn add_coverage(machine_name: String, from_state: String, to_state: String, event: String) {
    let ct = CoveredTransition {
        from_state,
        to_state,
        event,
    };
    COVERAGE_SENDER.send((machine_name, ct)).unwrap();
}

fn spawn_save_coverage_at_end() -> SyncSender<(String, CoveredTransition)> {
    let (tx, rx) = sync_channel(1000);
    let handle = std::thread::spawn(move || {
        // Assume that, if the entire program hasn't run a state machine transition in the
        // last second that we are probably done running all the tests. This is to avoid
        // needing to instrument every single test.
        while let Ok((machine_name, ct)) = rx.recv_timeout(Duration::from_secs(1)) {
            match COVERED_TRANSITIONS.entry(machine_name) {
                Entry::Occupied(o) => {
                    o.get().insert(ct);
                }
                Entry::Vacant(v) => {
                    v.insert({
                        let ds = DashSet::new();
                        ds.insert(ct);
                        ds
                    });
                }
            }
        }
    });
    *THREAD_HANDLE.lock().unwrap() = Some(handle);
    tx
}

#[cfg(test)]
mod machine_coverage_report {
    use super::*;
    use crate::machines::fail_workflow_state_machine::FailWorkflowMachine;
    use crate::machines::{
        activity_state_machine::ActivityMachine,
        complete_workflow_state_machine::CompleteWorkflowMachine,
        timer_state_machine::TimerMachine, workflow_task_state_machine::WorkflowTaskMachine,
    };
    use rustfsm::StateMachine;

    // This "test" needs to exist so that we have a way to join the spawned thread. Otherwise
    // it'll just get abandoned.
    #[test]
    // Use `cargo test -- --include-ignored` to run this. We don't want to bother with it by default
    // because it takes a minimum of a second.
    #[ignore]
    fn reporter() {
        // Make sure thread handle exists
        #[allow(clippy::no_effect)] // Haha clippy, you'd be wrong here.
        {
            &*COVERAGE_SENDER;
        }
        // Join it
        THREAD_HANDLE
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .join()
            .unwrap();

        dbg!(&*COVERED_TRANSITIONS);

        // Gather visualizations for all machines
        let mut activity = ActivityMachine::visualizer().to_owned();
        let mut timer = TimerMachine::visualizer().to_owned();
        let mut complete_wf = CompleteWorkflowMachine::visualizer().to_owned();
        let mut wf_task = WorkflowTaskMachine::visualizer().to_owned();
        let mut fail_wf = FailWorkflowMachine::visualizer().to_owned();

        // This isn't at all efficient but doesn't need to be.
        // Replace transitions in the vizzes with green color if they are covered.
        for item in COVERED_TRANSITIONS.iter() {
            let (machine, coverage) = item.pair();
            match machine.as_ref() {
                "ActivityMachine" => cover_transitions(&mut activity, coverage),
                "TimerMachine" => cover_transitions(&mut timer, coverage),
                "CompleteWorkflowMachine" => cover_transitions(&mut complete_wf, coverage),
                "WorkflowTaskMachine" => cover_transitions(&mut wf_task, coverage),
                "FailWorkflowMachine" => cover_transitions(&mut fail_wf, coverage),
                m => panic!("Unknown machine {}", m),
            }
        }
        println!("{}", activity);
        println!("{}", timer);
        println!("{}", wf_task);
    }

    fn cover_transitions(viz: &mut String, cov: &DashSet<CoveredTransition>) {
        for trans in cov.iter() {
            let find_line = format!(
                "{} --> {}: {}",
                trans.from_state, trans.to_state, trans.event
            );
            if let Some(start) = viz.find(&find_line) {
                let new_line = format!(
                    "{} -[#blue]-> {}: {}",
                    trans.from_state, trans.to_state, trans.event
                );
                viz.replace_range(start..start + find_line.len(), &new_line);
            }
        }
    }
}
