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
        dbg!(&*COVERED_TRANSITIONS);
    });
    *THREAD_HANDLE.lock().unwrap() = Some(handle);
    tx
}

#[cfg(test)]
mod machine_coverage_report {
    use super::*;

    // This "test" needs to exist so that we have a way to join the spawned thread. Otherwise
    // it'll just get abandoned.
    #[test]
    fn reporter() {
        // Make sure thread handle exists
        #[allow(clippy::no_effect)] // Haha clippy, you'd be wrong here.
        &*COVERAGE_SENDER;
        // Join it
        THREAD_HANDLE
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .join()
            .unwrap();
    }
}
