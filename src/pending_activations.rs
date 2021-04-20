use crate::protos::coresdk::workflow_activation::WfActivation;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};

/// Tracks pending activations using an internal queue, while also allowing lookup and removal of
/// any pending activations by run ID.
#[derive(Default)]
pub struct PendingActivations {
    inner: RwLock<PaInner>,
}

#[derive(Default)]
struct PaInner {
    queue: VecDeque<WfActivation>,
    // Keys are run ids
    count_by_id: HashMap<String, usize>,
}

impl PendingActivations {
    pub fn push(&self, v: WfActivation) {
        let mut inner = self.inner.write();
        *inner
            .count_by_id
            .entry(v.run_id.clone())
            .or_insert_with(|| 0) += 1;
        inner.queue.push_back(v);
    }

    pub fn pop(&self) -> Option<WfActivation> {
        let mut inner = self.inner.write();
        let rme = inner.queue.pop_front();
        if let Some(pa) = &rme {
            let do_remove = if let Some(c) = inner.count_by_id.get_mut(&pa.run_id) {
                *c -= 1;
                *c == 0
            } else {
                false
            };
            if do_remove {
                inner.count_by_id.remove(&pa.run_id);
            }
        }
        rme
    }

    pub fn has_pending(&self, run_id: &str) -> bool {
        self.inner.read().count_by_id.contains_key(run_id)
    }

    pub fn remove_all_with_run_id(&self, run_id: &str) {
        let mut inner = self.inner.write();

        // The perf here can obviously be improved if it ever becomes an issue, but is left for
        // later since it would require some careful fiddling
        inner.queue.retain(|pa| pa.run_id != run_id);
        inner.count_by_id.remove(run_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_run_ids() {
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
        for _ in 0..3 {
            let last = pas.pop().unwrap();
            assert_eq!(&last.run_id, &rid2);
        }
        assert!(!pas.has_pending(&rid2));
        assert!(pas.pop().is_none());
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
