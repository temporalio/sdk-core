use dashmap::{mapref::entry::Entry, DashMap, DashSet};

// During test we want to know about which transitions we've covered in state machines
lazy_static::lazy_static! {
    static ref COVERED_TRANSITIONS: DashMap<String, DashSet<CoveredTransition>>
        = DashMap::new();
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
    dbg!(&*COVERED_TRANSITIONS);
}
