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

// This could be made more memory efficient with an arena or something if ever needed
#[derive(Default)]
struct PaInner {
    queue: VecDeque<ActivationKey>,
    activations: SlotMap<ActivationKey, WfActivation>,
    by_run_id: HashMap<String, ActivationKey>,
}

impl PendingActivations {
    pub fn push(&self, v: WfActivation) {
        let mut inner = self.inner.write();

        // Check if an activation with the same ID already exists, and merge joblist if so
        if let Some(key) = inner.by_run_id.get(&v.run_id).copied() {
            let act = inner
                .activations
                .get_mut(key)
                .expect("PA run id mapping is always in sync with slot map");
            assert_eq!(
                v.task_token, act.task_token,
                "New PAs that match an existing PA's run id must have the same task token"
            );
            act.jobs.extend(v.jobs);
        } else {
            let run_id = v.run_id.clone();
            let key = inner.activations.insert(v);
            inner.by_run_id.insert(run_id, key);
            inner.queue.push_back(key);
        };
    }

    pub fn pop(&self) -> Option<WfActivation> {
        let mut inner = self.inner.write();
        let rme = inner.queue.pop_front();
        if let Some(key) = rme {
            let pa = inner
                .activations
                .remove(key)
                .expect("PAs in queue exist in slot map");
            inner.by_run_id.remove(&pa.run_id);
            Some(pa)
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
            inner.queue.retain(|pa_k| *pa_k != k);
            inner.activations.remove(k);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_same_ids() {
        let pas = PendingActivations::default();
        let rid1 = "1".to_string();
        let rid2 = "2".to_string();
        pas.push(WfActivation {
            run_id: rid1.clone(),
            ..Default::default()
        });
        pas.push(WfActivation {
            run_id: rid2.clone(),
            ..Default::default()
        });
        pas.push(WfActivation {
            run_id: rid2.clone(),
            ..Default::default()
        });
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
    #[should_panic(
        expected = "New PAs that match an existing PA's run id must have the same task token"
    )]
    fn panics_on_same_run_id_different_tokens() {
        let pas = PendingActivations::default();
        let rid = "1".to_string();
        pas.push(WfActivation {
            run_id: rid.clone(),
            ..Default::default()
        });
        pas.push(WfActivation {
            run_id: rid,
            task_token: vec![9],
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
}
