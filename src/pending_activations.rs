use crate::protos::coresdk::workflow_activation::WfActivation;
use parking_lot::RwLock;
use std::collections::{HashMap, VecDeque};

/// Tracks pending activations using an internal queue, while also allowing lookup and removal of
/// any pending activations by run ID.
#[derive(Default)]
pub struct PendingActivations {
    inner: RwLock<PaInner>,
}

// An activation is identified by it's task token / run id combo. There could potentially be
// pending activations for the same workflow run, but with different task tokens if for example
// the workflow times out while we are relaying a task we were issued, and we poll again
// TODO: In theory we should never actually queue them though since we handle PAs first. Assert?
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct ActivationId {
    task_token: Vec<u8>,
    run_id: String,
}

// This could be made more memory efficient with an arena or something if ever needed
#[derive(Default)]
struct PaInner {
    queue: VecDeque<ActivationId>,
    activations: HashMap<ActivationId, WfActivation>,
    count_by_run_id: HashMap<String, usize>,
}

impl PendingActivations {
    pub fn push(&self, v: WfActivation) {
        let mut inner = self.inner.write();
        let wf_id = ActivationId {
            run_id: v.run_id.clone(),
            task_token: v.task_token.clone(),
        };

        // Check if an activation with the same ID already exists, and merge joblist if so
        if let Some(act) = inner.activations.get_mut(&wf_id) {
            act.jobs.extend(v.jobs);
        } else {
            // Incr run id count
            *inner
                .count_by_run_id
                .entry(v.run_id.clone())
                .or_insert_with(|| 0) += 1;
            // TODO: Make better if true
            assert!(
                *inner.count_by_run_id.get(&v.run_id).unwrap() < 2,
                "Should never be multiple PAs with same run id"
            );

            inner.activations.insert(wf_id.clone(), v);
        }

        inner.queue.push_back(wf_id);
    }

    pub fn pop(&self) -> Option<WfActivation> {
        let mut inner = self.inner.write();
        let rme = inner.queue.pop_front();
        if let Some(pa) = &rme {
            let do_remove = if let Some(c) = inner.count_by_run_id.get_mut(&pa.run_id) {
                *c -= 1;
                *c == 0
            } else {
                false
            };
            if do_remove {
                inner.count_by_run_id.remove(&pa.run_id);
            }
            inner.activations.remove(&pa)
        } else {
            None
        }
    }

    pub fn has_pending(&self, run_id: &str) -> bool {
        self.inner.read().count_by_run_id.contains_key(run_id)
    }

    pub fn remove_all_with_run_id(&self, run_id: &str) {
        let mut inner = self.inner.write();

        // The perf here can obviously be improved if it ever becomes an issue, but is left for
        // later since it would require some careful fiddling
        inner.queue.retain(|pa| pa.run_id != run_id);
        inner.count_by_run_id.remove(run_id);
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
        // Should only be one id 2, they are all merged
        let last = pas.pop().unwrap();
        assert_eq!(&last.run_id, &rid2);
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
