use parking_lot::RwLock;
use slotmap::SlotMap;
use std::{
    cmp::Ordering,
    collections::{HashMap, VecDeque},
};
use temporal_sdk_core_protos::coresdk::workflow_activation::{
    wf_activation_job, WfActivation, WfActivationJob,
};

/// Tracks pending activations using an internal queue, while also allowing lookup and removal of
/// any pending activations by run ID.
#[derive(Default)]
pub struct PendingActivations {
    inner: RwLock<PaInner>,
}

slotmap::new_key_type! { struct ActivationKey; }

#[derive(Default)]
struct PaInner {
    activations: SlotMap<ActivationKey, WfActivation>,
    by_run_id: HashMap<String, ActivationKey>,
    // Holds the actual queue of activations
    queue: VecDeque<ActivationKey>,
}

impl PendingActivations {
    /// Push a pending activation
    ///
    /// Importantly, if there already exist activations for same workflow run in the queue, and the
    /// new activation (or pending one) is just an eviction job, it can be merged into the existing
    /// activation.
    ///
    /// Any other attempt to enqueue a pending activation while there is already one for a given
    /// run is a violation of our logic and will cause a panic. This is because there should never
    /// be new activations with real work in them queued up when one is already queued, because they
    /// should not be produced until the last one is completed (only one outstanding activation at
    /// a time rule). New work from the server similarily should not be pushed here, as it is
    /// buffered until any oustanding workflow task is completed.
    pub fn push(&self, v: WfActivation) {
        let mut inner = self.inner.write();

        // Check if an activation with the same ID already exists, and merge joblist if so
        if let Some(key) = inner.by_run_id.get(&v.run_id).copied() {
            let act = inner
                .activations
                .get_mut(key)
                .expect("PA run id mapping is always in sync with slot map");
            if v.is_only_eviction() || act.is_only_eviction() {
                merge_joblists(&mut act.jobs, v.jobs.into_iter());
            } else {
                panic!(
                    "Cannot enqueue non-eviction pending activation for run with an \
                    existing non-eviction PA: {:?}",
                    v
                );
            }
        } else {
            let run_id = v.run_id.clone();
            let key = inner.activations.insert(v);
            inner.by_run_id.insert(run_id, key);
            inner.queue.push_back(key);
        };
    }

    pub fn pop_first_matching(&self, predicate: impl Fn(&str) -> bool) -> Option<WfActivation> {
        let mut inner = self.inner.write();
        let mut key_queue = inner.queue.iter().copied();
        let maybe_key = key_queue.position(|k| {
            inner
                .activations
                .get(k)
                .map_or(false, |activation| predicate(&activation.run_id))
        });

        let maybe_key = maybe_key.map(|pos| inner.queue.remove(pos).unwrap());
        maybe_key.and_then(|key| {
            if let Some(pa) = inner.activations.remove(key) {
                inner.by_run_id.remove(&pa.run_id);
                Some(pa)
            } else {
                // Keys no longer in the slot map are ignored, since they may have been removed
                // by run id or anything else. Try to pop the next thing from the queue. Recurse
                // to avoid double mutable borrow.
                drop(inner); // Will deadlock when we recurse w/o this
                self.pop()
            }
        })
    }

    pub fn pop(&self) -> Option<WfActivation> {
        self.pop_first_matching(|_| true)
    }

    pub fn has_pending(&self, run_id: &str) -> bool {
        self.inner.read().by_run_id.contains_key(run_id)
    }

    pub fn remove_all_with_run_id(&self, run_id: &str) {
        let mut inner = self.inner.write();

        if let Some(k) = inner.by_run_id.remove(run_id) {
            inner.activations.remove(k);
        }
    }
}

fn merge_joblists(
    existing_list: &mut Vec<WfActivationJob>,
    other_jobs: impl Iterator<Item = WfActivationJob>,
) {
    existing_list.extend(other_jobs);
    // Move any evictions to the end of the list
    existing_list
        .as_mut_slice()
        .sort_by(evictions_always_last_compare);
    // Drop any duplicate evictions
    let truncate_len = existing_list
        .iter()
        .rev()
        .position(|j| {
            !matches!(
                j.variant,
                Some(wf_activation_job::Variant::RemoveFromCache(_))
            )
        })
        .map_or(1, |last_non_evict_job| {
            existing_list.len() - last_non_evict_job + 1
        });
    existing_list.truncate(truncate_len);
}

fn evictions_always_last_compare(a: &WfActivationJob, b: &WfActivationJob) -> Ordering {
    if a == b {
        return Ordering::Equal;
    }
    // Any eviction always goes last
    if matches!(
        a.variant,
        Some(wf_activation_job::Variant::RemoveFromCache(_))
    ) {
        return Ordering::Greater;
    }
    if matches!(
        b.variant,
        Some(wf_activation_job::Variant::RemoveFromCache(_))
    ) {
        return Ordering::Less;
    }
    // All jobs should not change order except evictions
    Ordering::Equal
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporal_sdk_core_protos::coresdk::workflow_activation::create_evict_activation;

    #[test]
    fn merges_same_ids_with_evictions() {
        let pas = PendingActivations::default();
        let rid1 = "1".to_string();
        let rid2 = "2".to_string();
        pas.push(WfActivation {
            run_id: rid1.clone(),
            ..Default::default()
        });
        pas.push(create_evict_activation(
            rid1.clone(),
            "whatever".to_string(),
        ));
        pas.push(create_evict_activation(
            rid2.clone(),
            "whatever".to_string(),
        ));
        pas.push(WfActivation {
            run_id: rid2.clone(),
            ..Default::default()
        });
        assert!(pas.has_pending(&rid1));
        assert!(pas.has_pending(&rid2));
        let last = pas.pop().unwrap();
        assert_eq!(&last.run_id, &rid1);
        assert!(!pas.has_pending(&rid1));
        assert!(pas.has_pending(&rid2));
        // Should only be one id 2, they are all merged
        let last = pas.pop().unwrap();
        assert_eq!(&last.run_id, &rid2);
        assert!(!pas.has_pending(&rid2));
        assert!(pas.pop().is_none());
    }

    #[test]
    #[should_panic(expected = "Cannot enqueue non-eviction")]
    fn panics_merging_non_evict() {
        let pas = PendingActivations::default();
        let rid1 = "1".to_string();
        pas.push(WfActivation {
            run_id: rid1.clone(),
            ..Default::default()
        });
        pas.push(WfActivation {
            run_id: rid1,
            ..Default::default()
        });
    }

    #[test]
    fn can_remove_all_with_id() {
        let pas = PendingActivations::default();
        let remove_me = "2".to_string();
        pas.push(WfActivation {
            run_id: "1".to_owned(),
            ..Default::default()
        });
        pas.push(WfActivation {
            run_id: remove_me.clone(),
            ..Default::default()
        });
        pas.push(WfActivation {
            run_id: "3".to_owned(),
            ..Default::default()
        });
        pas.remove_all_with_run_id(&remove_me);
        assert!(!pas.has_pending(&remove_me));
        assert_eq!(&pas.pop().unwrap().run_id, "1");
        assert_eq!(&pas.pop().unwrap().run_id, "3");
        assert!(pas.pop().is_none());
    }

    #[test]
    fn can_ignore_specific_runs() {
        let pas = PendingActivations::default();
        pas.push(WfActivation {
            run_id: "1_1".to_owned(),
            ..Default::default()
        });
        pas.push(WfActivation {
            run_id: "1_2".to_owned(),
            ..Default::default()
        });
        assert_eq!(
            &pas.pop_first_matching(|rid| rid != "1_1").unwrap().run_id,
            "1_2"
        );
        assert_eq!(&pas.pop().unwrap().run_id, "1_1");
    }
}
