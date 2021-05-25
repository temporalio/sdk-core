use crate::protos::coresdk::workflow_activation::WfActivation;
use parking_lot::RwLock;
use slotmap::SlotMap;
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
    pub fn push(&self, v: WfActivation, task_queue: String) {
        let mut inner = self.inner.write();

        // Check if an activation with the same ID already exists, and merge joblist if so
        if let Some(key) = inner.by_run_id.get(&v.run_id).copied() {
            let act = inner
                .activations
                .get_mut(key)
                .expect("PA run id mapping is always in sync with slot map");
            act.jobs.extend(v.jobs);
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

    pub fn pop(&self, task_queue: &str) -> Option<WfActivation> {
        let mut inner = self.inner.write();
        if let Some(qq) = inner.by_task_queue.get_mut(task_queue) {
            if let Some(key) = qq.pop_front() {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_same_ids() {
        let tq = "task_q";
        let pas = PendingActivations::default();
        let rid1 = "1".to_string();
        let rid2 = "2".to_string();
        pas.push(
            WfActivation {
                run_id: rid1.clone(),
                ..Default::default()
            },
            tq.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: rid2.clone(),
                ..Default::default()
            },
            tq.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: rid2.clone(),
                ..Default::default()
            },
            tq.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: rid2.clone(),
                ..Default::default()
            },
            tq.to_owned(),
        );
        assert!(pas.has_pending(&rid1));
        assert!(pas.has_pending(&rid2));
        let last = pas.pop(tq).unwrap();
        assert_eq!(&last.run_id, &rid1);
        assert!(!pas.has_pending(&rid1));
        assert!(pas.has_pending(&rid2));
        // Should only be one id 2, they are all merged
        let last = pas.pop(tq).unwrap();
        assert_eq!(&last.run_id, &rid2);
        assert!(!pas.has_pending(&rid2));
        assert!(pas.pop(tq).is_none());
    }

    #[test]
    fn can_remove_all_with_id() {
        let tq = "task_q";
        let pas = PendingActivations::default();
        let remove_me = "2".to_string();
        pas.push(
            WfActivation {
                run_id: "1".to_owned(),
                ..Default::default()
            },
            tq.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: remove_me.clone(),
                ..Default::default()
            },
            tq.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: remove_me.clone(),
                ..Default::default()
            },
            tq.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: "3".to_owned(),
                ..Default::default()
            },
            tq.to_owned(),
        );
        pas.remove_all_with_run_id(&remove_me);
        assert!(!pas.has_pending(&remove_me));
        assert_eq!(&pas.pop(tq).unwrap().run_id, "1");
        assert_eq!(&pas.pop(tq).unwrap().run_id, "3");
        assert!(pas.pop(tq).is_none());
    }

    #[test]
    fn multiple_task_queues() {
        let tq = "task_q";
        let tq2 = "task_q_2";
        let pas = PendingActivations::default();
        pas.push(
            WfActivation {
                run_id: "1_1".to_owned(),
                ..Default::default()
            },
            tq.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: "2_1".to_owned(),
                ..Default::default()
            },
            tq2.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: "1_2".to_owned(),
                ..Default::default()
            },
            tq.to_owned(),
        );
        pas.push(
            WfActivation {
                run_id: "2_2".to_owned(),
                ..Default::default()
            },
            tq2.to_owned(),
        );
        assert_eq!(&pas.pop(tq).unwrap().run_id, "1_1");
        assert_eq!(&pas.pop(tq2).unwrap().run_id, "2_1");
        assert_eq!(&pas.pop(tq).unwrap().run_id, "1_2");
        assert_eq!(pas.pop(tq), None);
        assert_eq!(&pas.pop(tq2).unwrap().run_id, "2_2");
        assert_eq!(pas.pop(tq2), None);
    }
}
