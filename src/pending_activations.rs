use crate::protos::coresdk::workflow_activation::{
    wf_activation_job, WfActivation, WfActivationJob,
};
use parking_lot::RwLock;
use slotmap::SlotMap;
use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};

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
    // Holds the actual queue of activations per-task-queue
    by_task_queue: HashMap<String, VecDeque<ActivationKey>>,
}

impl PendingActivations {
    /// Push a pending activation into the given task queue.
    ///
    /// Importantly, if there already exist activations for same workflow run in the queue, those
    /// activations will be merged.
    pub fn push(&self, v: WfActivation, task_queue: String) {
        let mut inner = self.inner.write();

        // Check if an activation with the same ID already exists, and merge joblist if so
        if let Some(key) = inner.by_run_id.get(&v.run_id).copied() {
            let act = inner
                .activations
                .get_mut(key)
                .expect("PA run id mapping is always in sync with slot map");
            merge_joblists(&mut act.jobs, v.jobs.into_iter());
        } else {
            let run_id = v.run_id.clone();
            let key = inner.activations.insert(v);
            inner.by_run_id.insert(run_id, key);
            inner
                .by_task_queue
                .entry(task_queue)
                .or_default()
                .push_back(key);
        };
    }

    pub fn pop_first_matching(
        &self,
        task_queue: &str,
        predicate: impl Fn(&str) -> bool,
    ) -> Option<WfActivation> {
        let mut inner = self.inner.write();
        if let Some(mut qq) = inner.by_task_queue.remove(task_queue) {
            let maybe_key = qq.iter().position(|k| {
                if let Some(activation) = inner.activations.get(*k) {
                    predicate(&activation.run_id)
                } else {
                    false
                }
            });

            let maybe_key = maybe_key.map(|pos| qq.remove(pos).unwrap());
            if !qq.is_empty() {
                inner.by_task_queue.insert(task_queue.to_owned(), qq);
            }
            if let Some(key) = maybe_key {
                if let Some(pa) = inner.activations.remove(key) {
                    inner.by_run_id.remove(&pa.run_id);
                    Some(pa)
                } else {
                    // Keys no longer in the slot map are ignored, since they may have been removed
                    // by run id or anything else. Try to pop the next thing from the queue. Recurse
                    // to avoid double mutable borrow.
                    drop(inner); // Will deadlock when we recurse w/o this
                    self.pop(task_queue)
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn pop(&self, task_queue: &str) -> Option<WfActivation> {
        self.pop_first_matching(task_queue, |_| true)
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

/// TODO: This might break with legacy queries, which cannot be merged with anything. Fix.
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
    let truncate_len = if let Some(last_non_evict_job) = existing_list.iter().rev().position(|j| {
        !matches!(
            j.variant,
            Some(wf_activation_job::Variant::RemoveFromCache(_))
        )
    }) {
        existing_list.len() - last_non_evict_job + 1
    } else {
        1
    };
    existing_list.truncate(truncate_len)
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
    use rstest::{fixture, rstest};

    const TQ: &str = "task_q";
    const TQ2: &str = "task_q_2";

    #[test]
    fn merges_same_ids() {
        let pas = PendingActivations::default();
        let rid1 = "1".to_string();
        let rid2 = "2".to_string();
        pas.push(
            WfActivation {
                run_id: rid1.clone(),
                ..Default::default()
            },
            TQ.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: rid2.clone(),
                ..Default::default()
            },
            TQ.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: rid2.clone(),
                ..Default::default()
            },
            TQ.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: rid2.clone(),
                ..Default::default()
            },
            TQ.to_owned(),
        );
        assert!(pas.has_pending(&rid1));
        assert!(pas.has_pending(&rid2));
        let last = pas.pop(TQ).unwrap();
        assert_eq!(&last.run_id, &rid1);
        assert!(!pas.has_pending(&rid1));
        assert!(pas.has_pending(&rid2));
        // Should only be one id 2, they are all merged
        let last = pas.pop(TQ).unwrap();
        assert_eq!(&last.run_id, &rid2);
        assert!(!pas.has_pending(&rid2));
        assert!(pas.pop(TQ).is_none());
    }

    #[test]
    fn can_remove_all_with_id() {
        let pas = PendingActivations::default();
        let remove_me = "2".to_string();
        pas.push(
            WfActivation {
                run_id: "1".to_owned(),
                ..Default::default()
            },
            TQ.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: remove_me.clone(),
                ..Default::default()
            },
            TQ.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: remove_me.clone(),
                ..Default::default()
            },
            TQ.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: "3".to_owned(),
                ..Default::default()
            },
            TQ.to_owned(),
        );
        pas.remove_all_with_run_id(&remove_me);
        assert!(!pas.has_pending(&remove_me));
        assert_eq!(&pas.pop(TQ).unwrap().run_id, "1");
        assert_eq!(&pas.pop(TQ).unwrap().run_id, "3");
        assert!(pas.pop(TQ).is_none());
    }

    #[fixture]
    fn two_queues_two_runs() -> PendingActivations {
        let pas = PendingActivations::default();
        pas.push(
            WfActivation {
                run_id: "1_1".to_owned(),
                ..Default::default()
            },
            TQ.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: "2_1".to_owned(),
                ..Default::default()
            },
            TQ2.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: "1_2".to_owned(),
                ..Default::default()
            },
            TQ.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: "2_2".to_owned(),
                ..Default::default()
            },
            TQ2.to_owned(),
        );
        pas
    }

    #[rstest]
    fn multiple_task_queues(#[from(two_queues_two_runs)] pas: PendingActivations) {
        assert_eq!(&pas.pop(TQ).unwrap().run_id, "1_1");
        assert_eq!(&pas.pop(TQ2).unwrap().run_id, "2_1");
        assert_eq!(&pas.pop(TQ).unwrap().run_id, "1_2");
        assert_eq!(pas.pop(TQ), None);
        assert_eq!(pas.inner.read().by_task_queue.get(TQ), None);
        assert_eq!(&pas.pop(TQ2).unwrap().run_id, "2_2");
        assert_eq!(pas.pop(TQ2), None);
        assert_eq!(pas.inner.read().by_task_queue.get(TQ2), None);
    }

    #[rstest]
    fn can_ignore_specific_runs(#[from(two_queues_two_runs)] pas: PendingActivations) {
        assert_eq!(
            &pas.pop_first_matching(TQ, |rid| rid != "1_1")
                .unwrap()
                .run_id,
            "1_2"
        );
        assert_eq!(&pas.pop(TQ2).unwrap().run_id, "2_1");
        assert_eq!(pas.pop_first_matching(TQ, |rid| rid != "1_1"), None);
        assert_eq!(&pas.pop(TQ).unwrap().run_id, "1_1");
        assert_eq!(pas.pop(TQ), None);
        assert_eq!(&pas.pop(TQ2).unwrap().run_id, "2_2");
        assert_eq!(pas.pop(TQ2), None);
    }
}
