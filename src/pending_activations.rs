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
    activations: SlotMap<ActivationKey, PendingActInfo>,
    by_run_id: HashMap<String, ActivationKey>,
    // Holds the actual queue of activations
    queue: VecDeque<ActivationKey>,
}

pub struct PendingActInfo {
    pub needs_eviction: bool,
    pub run_id: String,
}

impl PendingActivations {
    /// Indicate that a run needs to be activated
    pub fn notify_needs_activation(&self, run_id: &str) {
        let mut inner = self.inner.write();

        if inner.by_run_id.get(run_id).is_none() {
            let key = inner.activations.insert(PendingActInfo {
                needs_eviction: false,
                run_id: run_id.to_string(),
            });
            inner.by_run_id.insert(run_id.to_string(), key);
            inner.queue.push_back(key);
        };
    }
    pub fn notify_needs_eviction(&self, run_id: &str) {
        let mut inner = self.inner.write();

        if let Some(key) = inner.by_run_id.get(run_id).copied() {
            let act = inner
                .activations
                .get_mut(key)
                .expect("PA run id mapping is always in sync with slot map");
            act.needs_eviction = true;
        } else {
            let key = inner.activations.insert(PendingActInfo {
                needs_eviction: true,
                run_id: run_id.to_string(),
            });
            inner.by_run_id.insert(run_id.to_string(), key);
            inner.queue.push_back(key);
        };
    }

    pub fn pop_first_matching(&self, predicate: impl Fn(&str) -> bool) -> Option<PendingActInfo> {
        let mut inner = self.inner.write();
        let mut key_queue = inner.queue.iter().copied();
        let maybe_key = key_queue.position(|k| {
            if let Some(activation) = inner.activations.get(k) {
                predicate(&activation.run_id)
            } else {
                false
            }
        });

        let maybe_key = maybe_key.map(|pos| inner.queue.remove(pos).unwrap());
        if let Some(key) = maybe_key {
            if let Some(pa) = inner.activations.remove(key) {
                inner.by_run_id.remove(&pa.run_id);
                Some(pa)
            } else {
                // Keys no longer in the slot map are ignored, since they may have been removed
                // by run id or anything else. Try to pop the next thing from the queue. Recurse
                // to avoid double mutable borrow.
                drop(inner); // Will deadlock when we recurse w/o this
                self.pop_first_matching(predicate)
            }
        } else {
            None
        }
    }

    #[cfg(test)]
    pub fn pop(&self) -> Option<PendingActInfo> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merges_same_ids_with_evictions() {
        let pas = PendingActivations::default();
        let rid1 = "1";
        let rid2 = "2";
        pas.notify_needs_activation(rid1);
        pas.notify_needs_eviction(rid1);
        pas.notify_needs_eviction(rid2);
        pas.notify_needs_activation(rid2);
        assert!(pas.has_pending(rid1));
        assert!(pas.has_pending(rid2));
        let last = pas.pop().unwrap();
        assert_eq!(&last.run_id, &rid1);
        assert!(!pas.has_pending(rid1));
        assert!(pas.has_pending(rid2));
        // Should only be one id 2, they are all merged
        let last = pas.pop().unwrap();
        assert_eq!(&last.run_id, &rid2);
        assert!(!pas.has_pending(rid2));
        assert!(pas.pop().is_none());
    }

    #[test]
    fn can_remove_all_with_id() {
        let pas = PendingActivations::default();
        let remove_me = "2";
        pas.notify_needs_activation("1");
        pas.notify_needs_activation(remove_me);
        pas.notify_needs_activation("3");
        pas.remove_all_with_run_id(remove_me);
        assert!(!pas.has_pending(remove_me));
        assert_eq!(&pas.pop().unwrap().run_id, "1");
        assert_eq!(&pas.pop().unwrap().run_id, "3");
        assert!(pas.pop().is_none());
    }

    #[test]
    fn can_ignore_specific_runs() {
        let pas = PendingActivations::default();
        pas.notify_needs_activation("1");
        pas.notify_needs_activation("2");
        assert_eq!(
            &pas.pop_first_matching(|rid| rid != "1").unwrap().run_id,
            "2"
        );
        assert_eq!(&pas.pop().unwrap().run_id, "1");
    }
}
